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
from crocolaketools import db_params
from crocolaketools.converter.converter import Converter
from crocolaketools.converter.dask_tools import daskTools
from crocolaketools.config import config_paths as cfgp
from crocolaketools.converter.generate_schema import generateSchema
##########################################################################

#: Files converted at a time when datasets.yaml does not say.
DEFAULT_CHUNK_SIZE = 1000

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

        self.outdir_parquet = None if outdir_parquet is None else Path(outdir_parquet)
        self.schema_path = None if schema_path is None else Path(schema_path)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

## Convert series of argo files
    @staticmethod
    def convert_dask_tools(flists, metadata, db_names, outdir_parquet,
                           schema_path, cluster_key=None, client=None,
                           chunk_size=None):
        """Performs conversion by building compute graph and triggering
        operations; note that this conversion is quite different from the other
        converters because of the specificity of Argo's data (multiple files
        needing some sort of parallel processing)

        Arguments:
        flists         -- [PHY, BGC] lists of paths to files to convert
        metadata       -- [PHY, BGC] filtered GDAC index frames, each written to
                          <outdir_parquet>/metadata/Argo<db_type>_metadata.parquet;
                          one row per profile file considered, carrying its
                          path, date and date_update
        db_names       -- database types to convert (PHY and/or BGC)
        outdir_parquet -- destination directory for the parquet output
        schema_path    -- directory to write the generated schema to
        cluster_key    -- key in dask_cluster.yaml to size the client from
                          (default: ARGO-GDAC_<db_type>)
        client         -- an existing dask client to use instead of building
                          one; it is left running for the caller to close
        chunk_size     -- files converted at a time
                          (default: chunk_size in datasets.yaml, else 1000)
        """

        flist_phy = flists[0]
        flist_bgc = flists[1]

        metadata_phy = metadata[0]
        metadata_bgc = metadata[1]

        for k in range(len(db_names)):

            start_time = time.time()

            db_name = db_names[k].upper()
            print("Converting " + db_name + " database...")
            genSchema = generateSchema(outdir=schema_path, db=db_name)
            schema_fname = genSchema.schema_fname
            print("Schema file for " + db_name + " database: " + str(schema_fname))

            if db_name=="PHY":
                flist = flist_phy
                metadata = metadata_phy
            elif db_name=="BGC":
                flist = flist_bgc
                metadata = metadata_bgc

            db_key = "ARGO-GDAC_" + db_name
            if chunk_size is None:
                chunk = cfgp.get_config_paths_db_dict(db_key).get(
                    "chunk_size", DEFAULT_CHUNK_SIZE
                )
            else:
                chunk = chunk_size

            # convert metadata
            if len(metadata) > 0:
                metadata_dir = Path(outdir_parquet) / "metadata"
                metadata_dir.mkdir(parents = True, exist_ok = True)
                parquet_filename = metadata_dir / ("Argo" + db_name + "_metadata.parquet")
                metadata.to_parquet(parquet_filename)
                print("Metadata stored to " + str(parquet_filename) + ".")

            if client is None:
                cluster = cfgp.get_config_cluster_db_dict(cluster_key or db_key)
                print("Cluster settings for " + db_name + ": " + str(cluster))
                active_client = Client(**cluster)
                close_client = True
            else:
                active_client = client
                close_client = False

            try:
                daskConverter = daskTools(
                    db_type = db_name,
                    out_dir = outdir_parquet,
                    flist = flist,
                    schema_path = schema_fname,
                    chunk = chunk,
                )

                daskConverter.convert_to_parquet()
            finally:
                if close_client:
                    active_client.shutdown()

            elapsed_time = time.time() - start_time
            print("Time to convert " + db_name + " database: " + str(elapsed_time))


##########################################################################
if __name__ == "__main__":
    ConverterArgoGDAC()
