#!/usr/bin/env python3

## @file converterSprayGliders.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 04 Feb 2025

##########################################################################
import os
import warnings
import dask
import dask.dataframe as dd
from dask.distributed import Lock
import gsw
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr
from crocolakeloader import params
from crocolaketools.converter.converter import Converter
##########################################################################

class ConverterSprayGliders(Converter):

    """class ConverterSprayGliders: methods to generate parquet schemas for
    Spray Gliders netCDF files

    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, db=None, db_type=None, input_path=None, outdir_pq=None, outdir_schema=None, fname_pq=None, add_derived_vars=False, overwrite=False, tmp_path=None):
        if not db == "SprayGliders":
            raise ValueError("Database must be SprayGliders.")
        Converter.__init__(self, db, db_type, input_path, outdir_pq, outdir_schema, fname_pq, add_derived_vars, overwrite, tmp_path)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#
## Chunk large netcdf files
    def prepare_data(self, flist=None, lock=None):
        """Read list of netCDF files and chunk them and save them into smaller
        files. This is because dask is not efficient at lazingly converting dask
        arrays to dask dataframes.

        Arguments:
        flist -- list of files to process
        lock -- dask lock to use for concurrency
        """

        tmp_path = self.tmp_path
        if not os.path.exists(tmp_path):
            os.makedirs(tmp_path, exist_ok=True)
        else:
            raise ValueError(f"Temporary path {tmp_path} already exists. Please remove it before proceeding.")

        if lock is None:
            warnings.warn("No lock provided. This might lead to concurrency or segmentation fault errors.")

        for fname in flist:
            if not fname.endswith(".nc"):
                raise ValueError(f"{fname} does not end with '.nc'.")

            self.prepare_nc(fname, lock)

        return

#------------------------------------------------------------------------------#
## Chunk large netcdf files
    def prepare_nc(self,filename,lock):
        """Take a netCDF file and chunk it into smaller files

        Arguments:
        filename -- file name, excluding relative path
        lock -- dask lock to use for concurrency
        """

        input_fname = self.input_path + filename
        print("Reading file: ", input_fname)

        # chunking is empirical to force small chunks
        chunk_profile = 5000
        chunk_depth = -1
        chunk_trajectory = -1
        chunk_dict = {
            "profile":chunk_profile,
            "depth":chunk_depth,
            "trajectory":chunk_trajectory
        }

        ds = xr.open_dataset(
            input_fname,
            cache=False,
            chunks=chunk_dict
        )

        tmp_path = self.tmp_path

        chunks = ds.chunks
        for chunk_dim in chunks:
            if len(chunks[chunk_dim]) > 1 and chunk_dim != "profile":
                raise ValueError(f"Chunking on {chunk_dim} is not supported; chunking is allowed only on profile.")

        tasks = [self.store_chunks(ds, j, chunk_size, filename, tmp_path, lock) for j, chunk_size in enumerate(chunks["profile"])]
        dask.compute(*tasks)

        ds.close()

        return

#------------------------------------------------------------------------------#
## Store netcdf chunks
    @dask.delayed
    def store_chunks(self, ds, j, chunk_size, filename, tmp_path, lock):
        """Store j-th chunk of netCDF file

        Arguments:
        ds         -- (chunked) xarray dataset
        j          -- chunk index
        chunk_size -- size of chunk
        filename   -- file name, excluding relative path
        tmp_path   -- path to store chunks
        lock       -- dask lock to use for concurrency
        """

        initc = j * chunk_size # first index of chunk
        endc = initc + chunk_size # last index of chunk
        chunk_filename = f'{filename[:-3]}_chunk_{j}.nc'
        chunk_filepath = os.path.join(tmp_path, chunk_filename)

        # acquire lock to avoid concurrency issues on ds
        lock.acquire(timeout=600)

        # load into memory the slice of ds that corresponds to chunk
        ds_tmp = ds.isel(profile=slice(initc, endc)).compute()

        # store slice to netCDF file
        ds_tmp.to_netcdf(
            chunk_filepath,
            engine="netcdf4"
        )

        lock.release()

        return

#------------------------------------------------------------------------------#
## Read netcdf files and convert them to dask dataframe
    def read_to_ddf(self, flist=None, lock=None):
        """Read list of netCDF files and generate list of delayed objects with
        processed data

        Arguments:
        flist -- list of files to process
        lock  -- dask lock to use for concurrency

        Returns:
        results -- list of dask dataframes
        """

        # self.prepare_data(
        #     flist=flist,
        #     lock=lock
        # )

        if lock is None:
            warnings.warn("No lock provided. This might lead to concurrency or segmentation fault errors.")

        results = []
        for fname in flist:
            read_result = self.read_to_df(fname, lock)
            proc_result = self.process_df(read_result[0], read_result[1])
            results.append(proc_result)

        ddf = dd.from_delayed(results)
        self.call_guess_schema = True

        return ddf

#------------------------------------------------------------------------------#
## Read file to convert into a pandas dataframe
    @dask.delayed(nout=2)
    def read_to_df(self, filename=None, lock=None, path=None):
        """Read file into a pandas dataframe

        Argument:
        filename -- file name, excluding relative path
        lock     -- dask lock to use for concurrency
        path     -- path to file

        Returns
        df     -- pandas dataframe
        invars -- list of variables in df
        """

        if lock is None:
            warnings.warn("No lock provided. This might lead to concurrency or segmentation fault errors.")

        if filename is None:
            raise ValueError("No filename provided for Spray Gliders database.")

        if path is None:
            path = self.tmp_path

        input_fname = path + filename
        print("Reading file: ", input_fname)

        lock.acquire(timeout=600)
        try:
            with xr.open_dataset(input_fname,cache=True,chunks=None,engine="h5netcdf") as ds:
                ds_vars = list(ds.data_vars) + list(ds.coords)
                invars = list(set(params.params["SprayGliders"]) & set(ds_vars))
                df = ds[invars].to_dataframe()

        except Exception as e:
            print(f"Error reading file {input_fname}: {e}")
            raise

        finally: # always release lock in case of error in try block
            lock.release()

        return df, invars

#------------------------------------------------------------------------------#
## Process pandas dataframe to standardize it to CrocoLake schema
    @dask.delayed(nout=1)
    def process_df(self,df,invars):
        """Process pandas dataframe to standardize it to CrocoLake schema

        Arguments:
        df     -- pandas dataframe as generated from .nc file
        invars -- list of variables in df

        Returns:
        df    -- pandas dataframe with standardized schema
        """

        df = df.reset_index(drop=False)

        # deal with different variable names in different files
        if "temp" in df.columns.to_list():
            df = df.rename(columns={
                "temp": "temperature",
            })
            invars = invars + ["temperature"]
        if "sal" in df.columns.to_list():
            df = df.rename(columns={
                "sal": "salinity"
            })
            invars = invars + ["salinity"]

        # only keep variables in invars
        cols_to_drop = [item for item in df.columns.to_list() if item not in invars]
        df = df.drop(columns=cols_to_drop)

        # make df consistent with CrocoLake schema
        df = self.standardize_data(df)

        return df

#------------------------------------------------------------------------------#
## Convert parquet schema to xarray
    def standardize_data(self,df):
        """Standardize xarray dataset to schema consistent across databases

        Argument:
        ds -- xarray dataset

        Returns:
        df -- homogenized dataframe
        """

        # convert depth to pressure using the Gibbs SeaWater (GSW) Oceanographic
        # Toolbox of TEOS-10

        df["PRES"] = gsw.p_from_z(-df["depth"], df["lat"])
        df["PRES"] = df["PRES"].astype("float32[pyarrow]")

        # standardize data and generate schemas
        df = super().standardize_data(df)

        # add qc flag = 1 for temperature and salinity
        df = super().add_qc_flags(df, ["TEMP","PSAL","PRES"], 1)

        df = df[sorted(df.columns.tolist())]

        return df


##########################################################################
if __name__ == "__main__":
    ConverterSprayGliders()
