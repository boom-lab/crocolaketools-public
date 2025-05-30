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

    def read_to_ddf(self, flist=None, lock=None):
        if lock is None:
            warnings.warn("No lock provided. Creating one internally for safe concurrent access.")
            lock = Lock("saildrone_read_lock")

        if flist is None:
            flist = os.listdir(self.input_path)
            print("List of files not provided, guessing from input path: ", self.input_path)

        # Ensure we have files to process
        if not flist:
            raise ValueError("No files found to process")

        # Read all files first
        read_results = [self.read_to_df(fname, lock) for fname in flist]
        
        # Process all files
        processed_results = [self.process_df(result) for result in read_results]
        
        # Compute meta from first processed result (handle single file case)
        try:
            meta = processed_results[0].compute().iloc[0:0]
        except Exception as e:
            raise ValueError(f"Failed to compute metadata from first file: {e}")

        ddf = dd.from_delayed(processed_results, meta=meta)
        ddf = ddf.persist()

        self.call_guess_schema = True

        return ddf



    @dask.delayed(nout=1)
    def read_to_df(self, filename=None, lock=None):
        if filename is None:
            raise ValueError("No filename provided for Saildrone database.")

        input_fname = os.path.join(self.input_path, filename)
        print(f"Reading file: {input_fname}")

        try:
            if lock is not None:
                with lock:
                    ds = xr.open_dataset(input_fname, engine="netcdf4", cache=False)
            else:
                ds = xr.open_dataset(input_fname, engine="netcdf4", cache=False)
        except Exception as e:
            raise ValueError(f"Failed to read file {input_fname}: {e}")

        invars = list(set(params.params["Saildrones"]) & set(ds.data_vars))
        if not invars:
            raise ValueError(f"No matching variables found in file {input_fname}")

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

        # Initialize depth column if it doesn't exist
        if "depth" not in df.columns:
            df["depth"] = np.nan

        # Update depth column where valid readings exist for each variable
        for var_name, assigned_depth in depth_map.items():
            if var_name in df.columns:
                count = df[var_name].notna().sum()
                df.loc[df[var_name].notna(), "depth"] = assigned_depth
                print(f"Assigned depth {assigned_depth}m to {count} records from variable '{var_name}'")

        df = self.standardize_data(df)
        return df

    @dask.delayed(nout=1)
    def process_df(self, df):
        """Process pandas dataframe to remove all null rows
        
        Arguments:
        df -- pandas dataframe as generated from .nc file
        
        Returns:
        df -- pandas dataframe with null rows removed
        """
        # Remove rows that are all NAs
        cols_to_check = [
            "TEMP",
            "PSAL",
            "PRES",
        ]
        if self.db_type == "BGC":
            cols_to_check.append("DOXY")
            cols_to_check.append("CHLA")
            cols_to_check.append("CDOM")
            cols_to_check.append("BBP700")
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