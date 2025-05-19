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

    def read_to_df(self, filename=None, lock=None):
        """Read a Saildrone NetCDF file into a standardized pandas DataFrame."""

        if filename is None:
            raise ValueError("No filename provided for Saildrone database.")

        input_fname = os.path.join(self.input_path, filename)
        print(f"Reading file: {input_fname}")

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

        df = self.standardize_data(df)
        return df

    # ------------------------------------------------------------------ #
    ## Convert parquet schema to xarray

    def standardize_data(self, df):
        """Standardize xarray dataset to schema consistent across databases

        Argument:
        ds -- xarray dataset

        Returns:
        df -- homogenized dataframe
        """

        if "latitude" not in df.columns or df["latitude"].isna().all():
            raise ValueError("Latitude is missing or NaN in the dataset. Cannot compute pressure.")

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
