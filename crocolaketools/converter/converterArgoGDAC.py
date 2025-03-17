#!/usr/bin/env python3

## @file converterArgoGDAC.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 11 Feb 2025

##########################################################################
import dask
import dask.dataframe as dd
from dask.distributed import Client
import numpy as np
import pandas as pd
from pathlib import Path
import pyarrow as pa
import time
import xarray as xr
from crocolakeloader import params
from crocolaketools.converter.converter import Converter
from crocolaketools.converter.dask_tools import daskTools
from crocolaketools.converter.generate_schema import generateSchema
##########################################################################

class ConverterArgoGDAC(Converter):

    """class ConverterSprayGliders: methods to generate parquet schemas for
    Spray Gliders netCDF files

    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, flists=None, metadata=None, db_types=None, outdir_parquet=None, schema_path=None, db=None):
        if not db == "ARGO-GDAC":
            raise ValueError("Database must be ARGO-GDAC.")

        if flists is None:
            raise ValueError("flists should contain two lists.")
        self.flist_phy = flists[0]
        self.flist_bgc = flists[1]

        self.metadata_phy = metadata[0]
        self.metadata_bgc = metadata[1]

        self.db_types = db_types

        self.outdir_parquet = outdir_parquet
        self.schema_path = schema_path

        self.chunk_size = 1000

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

## Convert series of argo files
    @staticmethod
    def convert_dask_tools(flists, metadata, db_names, outdir_parquet, schema_path):
        """Performs conversion by building compute graph and triggering
        operations; note that this conversion is quite different from the other
        converters because of the specificity of Argo's data (multiple files
        needing some sort of parallel processing)

        Arguments:
        flist    -- list of paths to files to convert
        chunk    -- number of files processed at a time

        """

        flist_phy = flists[0]
        flist_bgc = flists[1]

        metadata_phy = metadata[0]
        metadata_bgc = metadata[1]

        for k in range(len(db_names)):

            start_time = time.time()

            db_name = db_names[k]
            print("Converting " + db_name + " database...")
            genSchema = generateSchema(outdir=schema_path, db=db_name)
            schema_fname = genSchema.schema_fname
            print("Schema file for " + db_name + " database: " + schema_fname)

            if db_name=="phy":
                flist = flist_phy
                metadata = metadata_phy
                nw = 18
                tw = 1
                memlim = "5.5GB"
            elif db_name=="bgc":
                flist = flist_bgc
                metadata = metadata_bgc
                nw = 9
                tw = 1
                memlim = "11GB"

            print("(nw, tw)")
            print( (nw, tw) )

            # convert metadata
            if len(metadata) > 0:
                metadata_dir = outdir_parquet + "metadata/"
                Path(metadata_dir).mkdir(parents = True, exist_ok = True)
                parquet_filename = metadata_dir + "Argo" + db_name.upper() + "_metadata.parquet"
                metadata.to_parquet(parquet_filename)
                print("Metadata stored to " + str(parquet_filename) + ".")

                client = Client(
                n_workers=nw,
                threads_per_worker=tw,
                processes=True,
                memory_limit=memlim,
            )

            chunksize = 1000

            daskConverter = daskTools(
                db_type = db_name.upper(),
                out_dir = outdir_parquet,
                flist = flist,
                schema_path = schema_fname,
                chunk = chunksize,
            )

            daskConverter.convert_to_parquet()

            client.shutdown()

            elapsed_time = time.time() - start_time
            print("Time to convert " + db_name + " database: " + str(elapsed_time))

#------------------------------------------------------------------------------#
## Convert series of argo files
    def convert(self):
        """Performs conversion by building compute graph and triggering
        operations; note that this conversion is quite different from the other
        converters because of the specificity of Argo's data (multiple files
        needing some sort of parallel processing)

        Arguments:
        flist    -- list of paths to files to convert
        chunk    -- number of files processed at a time

        """

        for k in range(len(self.db_types)):

            start_time = time.time()

            db_type = self.db_types[k].upper()
            print("Converting " + db_type + " database...")
            self.generate_reference_schema("_ALL", db_type)
            schema_fname = self.reference_schema_name
            print("Schema file for " + db_type + " database: " + schema_fname)

            nw = 1
            tw = 1
            metadata = []
            if db_type=="PHY":
                flist = self.flist_phy
                metadata = self.metadata_phy
                nw = 18
                tw = 1
                memlim = "8GB"
            elif db_type=="BGC":
                flist = self.flist_bgc
                metadata = self.metadata_bgc
                nw = 9
                tw = 1
                memlim = "11GB"

            print("(nw, tw)")
            print( (nw, tw) )

            if len(metadata) > 0:
                metadata_dir = self.outdir_parquet + "metadata/"
                Path(metadata_dir).mkdir(parents = True, exist_ok = True)
                parquet_filename = metadata_dir + "Argo" + db_type + "_metadata.parquet"
                metadata.to_parquet(parquet_filename)
                print("Metadata stored to " + str(parquet_filename) + ".")

                client = Client(
                    n_workers=nw,
                    threads_per_worker=tw,
                    processes=True,
                    memory_limit=memlim,
                )

            self.convert_to_parquet(flist, db_type, self.outdir_parquet, self.chunk_size, self.reference_schema)

            client.shutdown()

            return

#------------------------------------------------------------------------------#
    def convert_to_parquet(self, flist, db_type, out_dir, chunk, reference_schema):

        #out_dir = outdir_parquet
        #chunk = chunk_size

        self.VARS = params.params["Argo"+db_type].copy()
        self.pd_dict = self._Converter__translate_pq_to_pd(reference_schema)

        for j in range( int(np.ceil(len(flist)/chunk)) ):
            initchunk = j*chunk
            endchunk=min((j+1)*chunk, len(flist))

            df = [ self.read_to_df(file) for file in flist[initchunk:endchunk] ]

            df = dd.from_delayed(df) # creating unique df from list of df

            df = df.repartition(partition_size="300MB")

            name_function = lambda x: f"Argo{db_type}_{x}.parquet"

            # to_parquet() triggers execution of lazy functions
            append_db = False
            if j>0:
                append_db = True # append to pre-existing partition
            overwrite_db = not append_db

            df.to_parquet(
                out_dir,
                engine="pyarrow",
                name_function = name_function,
                append = append_db,
                write_metadata_file = True,
                write_index=False,
                schema = reference_schema,
                overwrite = overwrite_db
            )

            print()

        print("stored.")

#------------------------------------------------------------------------------#
## Read file to convert into a pandas dataframe
    @dask.delayed(nout=1)
    def read_to_df(self, filename=None, lock=None):
        """Read ARGO parquet database into a dask dataframe and update its
        columns to match the format for higher QC data

        Arguments:
        filename  --  name of argo profile file
        lock      --  (unused, needed for compatibility with super.read_to_df() )

        Returns:
        ddf -- dask dataframe containing ARGO observations for high QC version
        """

        okflag = -1

        try:
            ds = xr.open_dataset(filename, engine="argo") #loading into memory the profile

            # updating data modes for BGC argo floats data
            if 'PARAMETER_DATA_MODE' in list(ds.data_vars):
                if (ds['PARAMETER'].isel(N_CALIB=0) == ds['PARAMETER']).all():
                    ds = self.__assign_data_mode(ds)
                else:
                    raise ValueError("PARAMETER not independent of N_CALIB.")

            ds_vars = list(ds.data_vars)
            invars = list(set(self.VARS) & set(ds_vars))
            df = ds[invars].to_dataframe()
            df = df.reset_index() #flatten dataframe
            okflag = 1

        except Exception as e:
            print("The following exception occurred:", e)
            okflag = 0
            # create empty dataframe
            df = pd.DataFrame({c: pd.Series(dtype=t) for c, t in self.pd_dict.items()})

        if okflag == -1:
            print('No comms from ' + str(filename))
        elif okflag == 0:
            print('Failed on ' + str(filename))
        elif okflag == 1:
            print('Processing    ' + str(filename))

        # NB: we are not using super.standardize() because Argo's GDAC is sort
        # of a special case

        # ensures that all data frames have all the columns and in the same
        # order; it creates NaNs where not present
        df = df.reindex( columns=self.VARS )

        # enforcing dtypes otherwise to_parquet() gives error when appending
        df = df.astype(self.pd_dict)

        return df

#------------------------------------------------------------------------------#
## Generating schema
    def generate_reference_schema(self, db_type):
        """Generate reference schema specifically for Argo GDAC conversions
        (other converters normally call parent's class method)

        Arguments:
        db_type -- "PHY" or "BGC" (equals to db_type), name differs for
                       slightly different use in super()

        """

        param = params.params["Argo"+db_type].copy()

        self.fields = []
        for p in param:

            if p in ["PLATFORM_NUMBER","N_PROF","N_LEVELS","CYCLE_NUMBER"]:
                f = pa.field( p, pa.int32() )

            elif "_QC" in p:
                f = pa.field( p, pa.uint8() )

            elif p in ["LATITUDE","LONGITUDE"]:
                f = pa.field( p, pa.float64() )

            elif p=="JULD":
                f = pa.field( p, pa.from_numpy_dtype(np.dtype("datetime64[ns]") ) )

            elif (p=='DIRECTION') or ('DATA_MODE' in p):
                f = pa.field( p, pa.string() )  #this should become a category

            else:
                f = pa.field( p, pa.float32() )

            self.fields.append(f)

        self.reference_schema = pa.schema( self.fields )
        self.reference_schema_name = "Argo_"+ db_type + "_schema.metadata"

        # generate also pyarrow2pandas mapping
        #self.pd_dict = self._Converter__translate_pq_to_pd(self.reference_schema)

#------------------------------------------------------------------------------#
## Assign data mode to each parameter
    def __assign_data_mode(self,ds):
        """Spread 'PARAM_DATA_MODE' value across as many <PARAM>_DATA_MODE
        variables as N_PARAM in the dataset

        Arguments:
        ds    -- xarray Argo dataset

        Returns:
        ds    -- xarray Argo dataset with <PARAM>_DATA_MODE variables
        """

        nparam = ds.sizes["N_PARAM"]
        nprof = ds.sizes["N_PROF"]
        nlevels = ds.sizes["N_LEVELS"]
        for v in self.VARS:
            if "_DATA_MODE" in v:
                ds[v] = xr.DataArray(
                    np.full( (nprof,nlevels), "", dtype=str ),
                    dims=["N_PROF","N_LEVELS"]
                )

        parameter = ds["PARAMETER"].isel(N_CALIB=0)
        skipped_params = []
        for p in range(nparam):
            for j in range(nprof):

                param_name = str(parameter.isel(N_PARAM=p,N_PROF=j).values).strip()
                if len(param_name) < 1:
                    continue

                param_data_mode_name = param_name + "_DATA_MODE"
                if param_data_mode_name not in self.VARS:
                    if param_data_mode_name not in skipped_params:
                        skipped_params.append(param_data_mode_name)
                    continue

                data_mode = str( ds["PARAMETER_DATA_MODE"].isel(N_PARAM=p,N_PROF=j).values ).strip()
                if len(data_mode) > 1:
                    raise ValueError("Data mode should be a one-character long string.")

                for k in range(nlevels):
                    ds[param_data_mode_name][j,k] = data_mode

        if len(skipped_params)>0:
            print("The following parameters were not found in the target variables to be converted, its <PARAM>_DATA_MODE has not been created: ")
            print(skipped_params)

        return ds

##########################################################################
if __name__ == "__main__":
    ConverterArgoGDAC()
