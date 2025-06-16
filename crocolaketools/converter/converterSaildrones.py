#!/usr/bin/env python3

## @file converterSaildrones.py
#
#  Converter for Saildrone NetCDF data to TRITON-compliant Parquet format
#
## @author David Nady <davidnady4yad@gmail.com>
##         Adapted from Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Sat 18 Apr 2025

##########################################################################
import os
import warnings
import dask
import dask.dataframe as dd
from dask.distributed import Lock
import gsw
import numpy as np
import pandas as pd
from pandas import ArrowDtype
import pyarrow as pa
import xarray as xr
from crocolakeloader import params
from crocolaketools.converter.converter import Converter
##########################################################################

class ConverterSaildrones(Converter):
    """class ConverterSaildrones: methods to generate parquet schemas for
    Saildrones NetCDF files"""

    def __init__(self, config=None, db_type=None):
        if config is not None and config.get("db") != "Saildrones":
            raise ValueError("Database must be 'Saildrones'.")
        elif config is None and db_type is not None:
            config = {
                "db": "Saildrones",
                "db_type": db_type.upper()
            }

        super().__init__(config)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

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

        if lock is None:
            warnings.warn("No lock provided. This might lead to concurrency or segmentation fault errors.")

        results = []
        for fname in flist:
            if not fname.endswith(".nc"):
                raise ValueError(f"{fname} does not end with '.nc'.")
            read_result = self.read_to_df(fname, lock)
            proc_result = self.process_df(read_result[0], read_result[1])
            results.append(proc_result)

        # combine all results into a single dask dataframe
        ddf = dd.from_delayed(results)

        self.call_guess_schema = True

        return ddf

#------------------------------------------------------------------------------#
## Read file to convert into a pandas dataframe
    @dask.delayed(nout=2)
    def read_to_df(self, filename=None, lock=None):
        """Read file into a pandas dataframe

        Argument:
        filename -- file name, excluding relative path
        lock     -- dask lock to use for concurrency

        Returns
        df     -- pandas dataframe
        invars -- list of variables in df
        """

        if lock is None:
            lock = Lock()

        if filename is None:
            raise ValueError("No filename provided for Saildrone database.")

        input_fname = os.path.join(self.input_path, filename)
        print(f"Reading file: {input_fname}")

        lock.acquire(timeout=600)
        try:
            with xr.open_dataset(input_fname, engine="netcdf4", cache=False) as ds:
                invars = list(set(params.params["Saildrones"]) & set(ds.data_vars))
                df = ds[invars].to_dataframe().reset_index()

            # Assign depths based on metadata and known sensor installation
            depth_map = {
                "TEMP_DEPTH_HALFMETER_MEAN": 0.5,
                "TEMP_SBE37_MEAN": 1.7,
                "O2_CONC_SBE37_MEAN": 1.7,
                "SAL_SBE37_MEAN": 1.7,
                "BKSCT_RED_MEAN": 1.9,
                "CDOM_MEAN": 1.9,
                "CHLOR_WETLABS_MEAN": 1.9,
            }

            # Split data into depth-specific DataFrames for each variable
            df_list = []
            common_cols = ["time", "latitude", "longitude"]

            for var_name, assigned_depth in depth_map.items():
                if var_name in df.columns:
                    temp_df = df[df[var_name].notna()].copy()
                    if not temp_df.empty:
                        # Create a new dataframe with common columns, the specific variable, and the assigned depth
                        new_row_df = temp_df[common_cols + [var_name]].copy()
                        new_row_df["depth"] = assigned_depth
                        df_list.append(new_row_df)

            # Concatenate all the depth-specific dataframes
            df_combined = pd.concat(df_list, ignore_index=True)

            # For each unique (time, lat, lon, depth), keep the first non-null value per variable
            df = df_combined.groupby(common_cols + ["depth"]).first().reset_index()

        except Exception as e:
            print(f"Error reading file {input_fname}: {e}")
            raise

        finally: # always release lock in case of error in try block
            lock.release()

        return df, invars

#------------------------------------------------------------------------------#
## Process pandas dataframe to standardize it to CrocoLake schema
    @dask.delayed(nout=1)
    def process_df(self, df, invars):
        """Process pandas dataframe to standardize it to CrocoLake schema

        Arguments:
        df     -- pandas dataframe as generated from .nc file
        invars -- list of variables in df

        Returns:
        df    -- pandas dataframe with standardized schema
        """

        invars = invars + ["depth"]

        # Filter out rows where latitude or longitude are missing
        df = df[df["latitude"].notna() & df["longitude"].notna()]

        # make df consistent with CrocoLake schema
        df = self.standardize_data(df)

        # remove rows that are all NAs
        cols_to_check = ["TEMP", "PSAL", "PRES"]
        if self.db_type == "BGC":
            cols_to_check += ["DOXY", "CHLA", "CDOM", "BBP700"]
        cols_to_check = [col for col in cols_to_check if col in df.columns]

        df = super().remove_all_NAs(df, cols_to_check)

        return df

#------------------------------------------------------------------------------#
## Convert parquet schema to xarray
    def standardize_data(self, df):
        """Standardize xarray dataset to schema consistent across databases

        Argument:
        ds -- xarray dataset

        Returns:
        df -- homogenized dataframe
        """

        # convert depth to pressure using the Gibbs SeaWater (GSW) Oceanographic
        # Toolbox of TEOS-10
        df["PRES"] = gsw.p_from_z(-df["depth"], df["latitude"])
        df["PRES"] = df["PRES"].astype("float32[pyarrow]")

        # Merge temperature readings from multiple sensors into a unified 'TEMP' column.
        temp_sources = ["TEMP_SBE37_MEAN", "TEMP_DEPTH_HALFMETER_MEAN"]
        existing_temp_sources = [col for col in temp_sources if col in df.columns]
        df["TEMP"] = df[existing_temp_sources].bfill(axis=1).iloc[:, 0]

        # standardize data and generate schemas
        df = super().standardize_data(df)

        qc_vars = ["TEMP", "PSAL", "PRES"]
        if self.db_type == "BGC":
            qc_vars += ["DOXY", "CHLA", "CDOM", "BBP700"]

        # add QC flag = 1 for some variables that exist in the dataframe
        df = super().add_qc_flags(df, ["TEMP","PSAL","PRES"], 1)

        df = df[sorted(df.columns.tolist())]

        return df

#------------------------------------------------------------------------------#
## Convert file
    def convert(self, filenames=None, filepath=None):
        """Override convert to handle single files without Dask overhead, and delegate to base class for multiple files."""
        
        if filenames is None:
            guess_path = filepath or self.input_path
            filenames = os.listdir(guess_path)

        if isinstance(filenames, str):
            filenames = [filenames]

        # Handle single file to compute delayed results
        if len(filenames) == 1:
            print("Reading single file")
            df_delayed, invars_delayed = self.read_to_df(filenames[0])
            processed_delayed = self.process_df(df_delayed, invars_delayed)
            df = dask.compute(processed_delayed)[0]
            ddf = dd.from_pandas(df, npartitions=1)
            self.call_guess_schema = True
            
            if self.add_derived_vars:
                print("Adding derived variables")
                ddf = self.compute_derived_variables(ddf)
            ddf = self.reorder_columns(ddf)
            ddf = ddf.drop_duplicates()
            self.to_parquet(ddf)
        else: # Multiple files, delegate to base class for Dask processing
            super().convert(filenames=filenames, filepath=filepath)

##########################################################################
if __name__ == "__main__":
    ConverterSaildrones()