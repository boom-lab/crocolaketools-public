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

    def convert(self, filenames=None, filepath=None):
        """Custom implementation of convert method because ConverterSaildrones read_to_df() 
        return both the DataFrame and the variables list, while the base Converter class 
        expects read_to_df() to return just the DataFrame.

        Arguments:
        filenames -- list of files to process
        filepath -- path to files to process
        """
        if filenames is None:
            if filepath is None:
                guess_path = self.input_path
                warnings.warn("Filename(s) not provided, guessing from input path: " + guess_path)
            else:
                guess_path = filepath
                warnings.warn("Filename(s) not provided, guessing from provided file path: " + guess_path)
            filenames = os.listdir(guess_path)
        print("List of files to convert: ", filenames)

        # adapt for single filename input
        if isinstance(filenames, str):
            filenames = [filenames]

        lock = Lock()
        if len(filenames) > 1:
            print("reading reference files")
            ddf = self.read_to_ddf(
                flist=filenames,
                lock=lock
            )
        else:
            read_result = self.read_to_df(filenames[0], lock)
            df = self.process_df(read_result[0], read_result[1])
            ddf = dd.from_delayed([df])

        if self.add_derived_vars:
            print("adding derived variables")
            ddf = self.add_derived_variables(ddf)

        ddf = self.reorder_columns(ddf)

        ddf = ddf.drop_duplicates()

        print("repartitioning dask dataframe")
        ddf = ddf.repartition(partition_size="300MB")

        # Generate schema before saving to parquet
        if self.call_guess_schema:
            self.schema_pq = self.guess_schema(ddf)
        else:
            self.generate_schema(ddf.columns.to_list())

        print("save to parquet")
        self.to_parquet(ddf)

        return

    def read_to_ddf(self, flist=None, lock=None):
        """Read list of NetCDF files and generate list of delayed objects with
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

        ddf = dd.from_delayed(results)

        self.call_guess_schema = True

        return ddf

    @dask.delayed(nout=2)
    def read_to_df(self, filename=None, lock=None):
        """Read a Saildrone NetCDF file into a standardized pandas DataFrame."""

        if filename is None:
            raise ValueError("No filename provided for Saildrone database.")

        input_fname = os.path.join(self.input_path, filename)
        print(f"Reading file: {input_fname}")

        if lock is not None:
            lock.acquire(timeout=600)

        try:
            ds = xr.open_dataset(input_fname, engine="netcdf4", cache=False)
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

        except Exception as e:
            print(f"Error reading file {input_fname}: {e}")
            raise

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
        df    -- pandas dataframe with standardized schema
        """

        # make df consistent with CrocoLake schema
        df = self.standardize_data(df)

        # remove rows that are all NAs
        cols_to_check = ["TEMP", "PSAL", "PRES"]
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