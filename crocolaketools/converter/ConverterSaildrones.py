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

    def __init__(self, db=None, db_type=None, input_path=None, outdir_pq=None, outdir_schema=None, fname_pq=None, add_derived_vars=False, overwrite=False, depth_default=0.5):
        if db != "Saildrones":
            raise ValueError("Database must be 'Saildrones'.")
        super().__init__(db, db_type, input_path, outdir_pq, outdir_schema, fname_pq, add_derived_vars, overwrite)
        self.depth_default = depth_default

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

    #------------------------------------------------------------------------------#
    ## Read file to convert into a pandas dataframe

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

        # Handle missing values in temperature
        if "TEMP_SBE37_MEAN" in df.columns:
            temp_series = df["TEMP_SBE37_MEAN"].copy()
        else:
            temp_series = pd.Series([np.nan] * len(df), index=df.index)

        # Fill missing values from the fallback column, if it exists
        if "TEMP_DEPTH_HALFMETER_MEAN" in df.columns:
            temp_series = temp_series.fillna(df["TEMP_DEPTH_HALFMETER_MEAN"])

        df["TEMP_SBE37_MEAN"] = temp_series

        df = df.reindex(columns=params.params["Saildrones"])
        df = self.standardize_data(df)

        return df


    #------------------------------------------------------------------------------#
    ## Convert parquet schema to xarray

    def standardize_data(self, df):
        """Standardize and align Saildrone data with TRITON schema."""

        # convert depth to pressure using the Gibbs SeaWater (GSW) Oceanographic
        # Toolbox of TEOS-10
        if "latitude" in df.columns:
            df["PRES"] = gsw.p_from_z(-self.depth_default, df["latitude"])
        else:
            warnings.warn("Latitude missing; cannot compute PRES. Setting to NaN.")
            df["PRES"] = np.nan
        df["PRES"] = df["PRES"].astype("float32[pyarrow]")

        # standardize data and generate schemas
        df = super().standardize_data(df)

        qc_vars = ["TEMP", "PSAL", "PRES"]
        if self.db_type == "BGC":
            qc_vars = ["DOXY", "CHLA", "CDOM", "BBP700"]

        # add qc flag = 1 for temperature and salinity
        df = super().add_qc_flags(df, [v for v in qc_vars if v in df.columns], 1)

        return df

##########################################################################
if __name__ == "__main__":
    ConverterSaildrones()