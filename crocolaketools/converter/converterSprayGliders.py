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
import dask.dataframe as dd
from dask.distributed import Lock
import gsw
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr
from crocolaketools.utils import params
from crocolaketools.converter.converter import Converter
##########################################################################

class ConverterSprayGliders(Converter):

    """class ConverterSprayGliders: methods to generate parquet schemas for
    Spray Gliders netCDF files

    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, db=None, db_type=None, input_path=None, outdir_pq=None, outdir_schema=None, fname_pq=None, add_derived_vars=False, overwrite=False):
        if not db == "SprayGliders":
            raise ValueError("Database must be SprayGliders.")
        Converter.__init__(self, db, db_type, input_path, outdir_pq, outdir_schema, fname_pq, add_derived_vars, overwrite)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#
## Read file to convert into a pandas dataframe
    def read_to_df(self, filename=None, lock=None):
        """Read file into a pandas dataframe

        Argument:
        filename -- file name, including relative path

        Returns
        df -- pandas dataframe
        """

        if lock is None:
            warnings.warn("No lock provided. This might lead to concurrency or segmentation fault errors.")

        if filename is None:
            raise ValueError("No filename provided for Spray Gliders database.")

        input_fname = self.input_path + filename
        print("Reading file: ", input_fname)

        if lock is None:
            ds = xr.open_dataset(input_fname, engine="argo")
            invars = list(set(params.params["SprayGliders"]) & set(list(ds.data_vars)))
            df = ds[invars].to_dataframe()
        else:
            lock.acquire(timeout=600)
            #with lock:
            ds = xr.open_dataset(input_fname, engine="argo", cache=False)
            invars = list(set(params.params["SprayGliders"]) & set(list(ds.data_vars)))
            df = ds[invars].to_dataframe()
            lock.release()

        if "temp" in df.columns.to_list():
            df = df.rename(columns={
                "temp": "temperature",
            })
        if "sal" in df.columns.to_list():
            df = df.rename(columns={
                "sal": "salinity"
            })

        df["time"] = df["time"].astype("timestamp[ns][pyarrow]")

        df = df.reset_index()

        df = df.reindex(columns=params.params["SprayGliders"])

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

        return df


##########################################################################
if __name__ == "__main__":
    ConverterSprayGliders()
