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
import gsw
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr
from crocolakeloader import params
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
        ddf = dd.read_csv(
            input_fname,
            assume_missing=True,
            delimiter=",",
            header=0,
            low_memory=False,
            dtype_backend='pyarrow'
        )

        return self.standardize_data(ddf)

#------------------------------------------------------------------------------#
## Convert parquet schema to pandas
    def standardize_data(self,ddf):
        """Standardize dask dataframe to schema consistent across databases

        Argument:
        ddf -- dask dataframe

        Returns:
        ddf -- homogenized (dask) dataframe
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
        ddf = ddf.rename(columns=rename_datetime) #pd.to_datetime expects these column names
        ddf["JULD"] = dd.to_datetime(ddf[["year", "month", "day", "hour", "minute"]])

        # keep only good QC values
        params_to_check = []
        for param in params.params["GLODAP2CROCOLAKE"].keys():
            if param.endswith("f") and param in ddf.columns:
                ddf = ddf.map_partitions(
                    self.keep_best_values, param
                )
                params_to_check.append(param[:-1])

        # remove rows containing all NAs
        ddf = ddf.map_partitions(
            super().remove_all_NAs, params_to_check
        )

        # return standardized dataframe
        return super().standardize_data(ddf)

#------------------------------------------------------------------------------#
## Keep best values for each row
    def keep_best_values(self,df,param):
        """Keep the best observation available for each row

        Arguments:
        df -- a row or a partition of a pandas dataframe
        param -- name of the qc variable of the parameter

        Returns:
        df  --  updated dataframe

        """

        # GLODAP's quality control columns end with "f" (e.g. "nitratef")
        # and good values are 0 or 2
        condition = df[param].isin([0,2])

        # Find bad QC values
        df.loc[condition, param] = pd.NA
        df.loc[condition, param[:-1]] = pd.NA

        return df

##########################################################################
if __name__ == "__main__":
    ConverterGLODAP()
