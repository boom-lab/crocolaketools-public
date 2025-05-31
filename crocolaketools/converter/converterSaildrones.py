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
import gsw
import numpy as np
import pandas as pd
from pandas import ArrowDtype
import pyarrow as pa
import xarray as xr
import dask
import dask.dataframe as dd
from dask.distributed import Lock
from crocolakeloader import params
from crocolaketools.converter.converter import Converter
##########################################################################

class ConverterSaildrones(Converter):
    """Converter for Saildrone NetCDF files to TRITON-compatible Parquet format."""

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

    def read_to_ddf(self, flist=None, lock=None):
        """Read list of NetCDF files and generate list of delayed objects with processed data

        Arguments:
        flist -- list of files to process
        lock  -- dask lock to use for concurrency

        Returns:
        ddf -- dask dataframe
        """
        if lock is None:
            warnings.warn("No lock provided. This might lead to concurrency or segmentation fault errors.")

        if flist is None:
            flist = os.listdir(self.input_path)
            print("List of files not provided, guessing from input path: ", self.input_path)

        # If there is only one file, ensure it's treated as a list
        if isinstance(flist, str):
            flist = [flist]

        results = []
        for fname in flist:
            read_result = self.read_to_df(fname, lock)
            proc_result = self.process_df(read_result[0], read_result[1])
            results.append(proc_result)

        ddf = dd.from_delayed(results)
        ddf = ddf.persist()
        self.call_guess_schema = True

        return ddf


    @dask.delayed(nout=2)
    def read_to_df(self, filename=None, lock=None):
        """Read a Saildrone NetCDF file into a standardized pandas DataFrame.

        Arguments:
        filename -- file name to read
        lock     -- dask lock to use for concurrency

        Returns:
        df     -- pandas dataframe
        invars -- list of variables in df
        """
        if filename is None:
            raise ValueError("No filename provided for Saildrone database.")

        input_fname = os.path.join(self.input_path, filename)
        print(f"Reading file: {input_fname}")

        if lock is not None:
            lock.acquire(timeout=600)

        try:
            ds = xr.open_dataset(input_fname, engine="netcdf4")
            invars = list(set(params.params["Saildrones"]) & set(ds.data_vars))
            df = ds[invars].to_dataframe().reset_index()

            if "time" in df.columns:
                df["time"] = pd.to_datetime(df["time"], errors="coerce").astype(ArrowDtype(pa.timestamp("ns")))

            # Assign depths based on metadata and known sensor installation
            depth_map = {
                "TEMP_SBE37_MEAN": 1.7,
                "PSAL_SBE37_MEAN": 1.7,
                "O2_CONC_SBE37_MEAN": 1.7,
                "TEMP_DEPTH_HALFMETER_MEAN": 0.5
            }

            # Update depth column where valid readings exist for each variable
            for var_name, assigned_depth in depth_map.items():
                if var_name in df.columns:
                    count = df[var_name].notna().sum()
                    df.loc[df[var_name].notna(), "depth"] = assigned_depth
                    print(f"Assigned depth {assigned_depth}m to {count} records from variable '{var_name}'")

        finally:
            if lock is not None:
                lock.release()

        return df, invars

    @dask.delayed(nout=1)
    def process_df(self, df, invars):
        """Process pandas dataframe to standardize it to CrocoLake schema

        Arguments:
        df     -- pandas dataframe as generated from .nc file
        invars -- list of variables in df

        Returns:
        df -- processed pandas dataframe
        """
        # Only keep variables in invars
        if "depth" not in invars:
            invars.append("depth")

        cols_to_drop = [item for item in df.columns.to_list() if item not in invars]
        df = df.drop(columns=cols_to_drop)

        # Standardize the data
        df = self.standardize_data(df)

        # Remove rows that are all NAs
        cols_to_check = ["TEMP", "PSAL"]
        if self.db_type == "BGC":
            cols_to_check += ["DOXY", "CHLA", "CDOM", "BBP700"]
        cols_to_check = [col for col in cols_to_check if col in df.columns]
        
        df = super().remove_all_NAs(df, cols_to_check)

        return df

    def standardize_data(self, df):
        """Standardize xarray dataset to schema consistent across databases
        Argument:
        ds -- xarray dataset
        Returns:
        df -- homogenized dataframe
        """

        if "latitude" not in df.columns or df["latitude"].isna().all():
            raise ValueError("Latitude is missing or NaN in the dataset. Cannot compute pressure.")

        # GSW expects depth to be negative (below sea level), so we negate it here
        df["PRES"] = gsw.p_from_z(-df["depth"], df["latitude"])
        df["PRES"] = df["PRES"].astype("float32[pyarrow]")

        df = super().standardize_data(df)

        qc_vars = ["TEMP", "PSAL", "PRES"]
        if self.db_type == "BGC":
            qc_vars += ["DOXY", "CHLA", "CDOM", "BBP700"]

        df = super().add_qc_flags(df, qc_vars, 1)

        return df

##########################################################################
if __name__ == "__main__":
    ConverterSaildrones()