#!/usr/bin/env python3

## @file converterCPR.py
#
# @brief Converts CPR (Continuous Plankton Recorder) data to the CrocoLake format.
#
# @author David Nady <davidnady4yad@gmail.com>
#
# @date Sat 1 Mar 2025

##########################################################################
import os
import warnings
import dask.dataframe as dd
from dask.distributed import Lock
import pandas as pd
import numpy as np
from crocolaketools.utils import params
from crocolaketools.converter.converter import Converter

##########################################################################

class ConverterCPR(Converter):
    """class ConverterCPR: methods to generate parquet schemas for CPR data."""

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, db=None, db_type=None, input_path=None, outdir_pq=None, outdir_schema=None, fname_pq=None, add_derived_vars=False, overwrite=False):
        if not db == "CPR":
            raise ValueError("Database must be CPR.")
        super().__init__(db, db_type, input_path, outdir_pq, outdir_schema, fname_pq, add_derived_vars, overwrite)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

    def read_to_df(self, filename=None, lock=None):
        """
        Read CPR data from a CSV file into a Pandas DataFrame.

        Arguments:
        filename -- path to the CPR data file
        lock     -- optional lock for thread-safe file reading

        Returns:
        df -- Pandas DataFrame containing the CPR data
        """
        if filename is None:
            filename = "765141_v5_cpr-plankton-abundance.csv"

        input_fname = os.path.join(self.input_path, filename)
        print(f"Reading CPR file: {input_fname}")

        df = pd.read_csv(input_fname)

        # Convert MidPoint_Date_UTC to a datetime object
        df["MidPoint_Date_UTC"] = pd.to_datetime(df["MidPoint_Date_UTC"])

        df = self.standardize_data(df)

        return df

    def standardize_data(self, df):
        """
        Standardize the CPR data to the TRITON format.

        Arguments:
        df -- Pandas DataFrame containing the CPR data

        Returns:
        df -- Standardized Pandas DataFrame
        """
        # Rename columns using the CPR2TRITON mapping
        rename_map = params.params["CPR2TRITON"]
        df = df.rename(columns=rename_map)

        # Ensure the data types are correct
        df["LATITUDE"] = df["LATITUDE"].astype("float64")
        df["LONGITUDE"] = df["LONGITUDE"].astype("float64")
        df["JULD"] = df["JULD"].dt.tz_localize(None)  # Remove timezone informationa
        df["JULD"] = df["JULD"].astype("datetime64[ns]")

        # Add derived variables (if needed)
        if self.add_derived_vars:
            df = self.add_derived_variables(df)

        return df

    def add_derived_variables(self, df):
        """
        Add derived variables to the DataFrame.

        Arguments:
        df -- Pandas DataFrame containing the CPR data

        Returns:
        df -- DataFrame with derived variables added
        """
        # Create a copy to avoid modifying the input DataFrame
        df_with_derived = df.copy()

        # Columns starting with PLANKTON_ contain individual plankton counts (id)
        plankton_columns = [col for col in df.columns if col.startswith("PLANKTON_")]
        df_with_derived["TOTAL_PLANKTON"] = df[plankton_columns].fillna(0).sum(axis=1)

        return df_with_derived

##########################################################################

if __name__ == "__main__":
    ConverterCPR()