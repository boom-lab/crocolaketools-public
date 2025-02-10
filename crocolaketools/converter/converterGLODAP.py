#!/usr/bin/env python3

## @file converterGLODAP.py
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

class ConverterGLODAP(Converter):

    """class ConverterGLODAP: methods to generate parquet schemas for
    GLODAP database

    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, db=None, db_type=None, input_path=None, outdir_pq=None, outdir_schema=None, fname_pq=None, add_derived_vars=False, overwrite=False):
        if not db == "GLODAP":
            raise ValueError("Database must be GLODAP.")
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

        if filename is None:
            filename = "GLODAPv2.2023_Merged_Master_File.csv"

        input_fname = self.input_path + filename
        print("Reading GLODAP file: ", input_fname)

        # low_memory=False as GLODAP is a small db
        df = pd.read_csv(input_fname, delimiter=",", header=0, low_memory=False)

        return self.standardize_data(df)

#------------------------------------------------------------------------------#
## Convert parquet schema to pandas
    def standardize_data(self,df):
        """Standardize pandas dataframe to schema consistent across databases

        Argument:
        df -- pandas dataframe

        Returns:
        df -- homogenized dataframe
        """

        # convert GLODAP multiple time columns to one datetime
        print("Converting GLODAP multiple time columns to one datetime")
        rename_datetime = {
            "G2year": "year",
            "G2month": "month",
            "G2day": "day",
            "G2hour": "hour",
            "G2minute": "minute"
        }
        df = df.rename(columns=rename_datetime) #pd.to_datetime expects these column names
        df["JULD"] = pd.to_datetime(df[["year", "month", "day", "hour", "minute"]])

        return super().standardize_data(df)

##########################################################################
if __name__ == "__main__":
    ConverterGLODAP()
