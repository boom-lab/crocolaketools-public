#!/usr/bin/env python3

import copy
import os
import glob
import importlib.resources
import logging
import random

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
import yaml
import xarray as xr

from crocolakeloader import params
from crocolaketools.utils.logger_configurator import configure_logging

####################################################################################################
class TestData:
#------------------------------------------------------------------------------#
# Set of tests that verify that data in the original dataset is in the parquet
# conversion. Note that because of the size of the data, the integrity tests
# are often performed on a subset of the whole database.

#------------------------------------------------------------------------------#
    def _check_variables(self,db_name,db_type):
        """Pick a random variable in a random original netCDF file and find it
        in the parquet version, check that they are equal"""

        config_path = importlib.resources.files("crocolaketools.config").joinpath("config.yaml")
        config = yaml.safe_load(open(config_path))
        config = config[db_name + "_" + db_type]

        nc_path = config["input_path"]
        pq_path = config["outdir_pq"]

        # get list of original nc files
        if os.path.isdir(nc_path):
            nc_path = os.path.join(nc_path, "**", "*.nc")
        nc_files = glob.glob(nc_path, recursive=True)

        params_db2crocolake = params.params[db_name + "2CROCOLAKE"]
        # remove PLATFORM_NUMBER from params_db2crocolake because it needs to be dealt with separately for Spray Gliders
        params_in_crocolake = [k for k, v in params_db2crocolake.items()
                               if v != "PLATFORM_NUMBER"]

        params_crocolake2db = params.params[ "CROCOLAKE2" + db_name]
        lat_name = params_crocolake2db["LATITUDE"]
        lon_name = params_crocolake2db["LONGITUDE"]

        for j in range(1000):
            nc_file = random.choice( nc_files )

            if config["db"]=="ARGO":
                ds = xr.open_dataset(nc_file, engine="argo")
            else:
                ds = xr.open_dataset(nc_file, engine="h5netcdf")

            #only test variables that are preserved in crocolake
            ds_vars = list(ds.data_vars)
            logging.info(f"ds_vars:{ds_vars}")
            logging.info(f"params_crocolake:{params_in_crocolake}")

            variables = list(
                set(list(ds.data_vars)) & set(params_in_crocolake)
            )

            logging.info(f"variables:{variables}")
            random_var = random.choice(variables)
            var_data = ds[random_var]
            indices = {dim: random.randint(0, size - 1) for dim, size in var_data.sizes.items()}
            nc_value = var_data.isel(**indices).item()

            shared_indices = {dim: idx for dim, idx in indices.items() if dim in ds[lat_name].dims}
            nc_lat = ds[lat_name].isel(**shared_indices).item()
            shared_indices = {dim: idx for dim, idx in indices.items() if dim in ds[lon_name].dims}
            nc_lon = ds[lon_name].isel(**shared_indices).item()

            # Some Spray Gliders data have nan for lat and lon, the target
            # variable seems to be nan too in that case; it doesn't hurt to keep
            # them in the parquet database for now, but we could filter them out
            # if np.isnan(nc_lat) or np.isnan(nc_lon):
            #     continue

            logging.info(f"nc_file: {nc_file}")
            logging.info(f"random_var: {random_var}")
            logging.info(f"indices: {indices}")
            logging.info(f"coordinates:")
            for k,v in indices.items():
                logging.info(f"{k} : {ds[k][v].item()}")
            logging.info(f"nc_value: {nc_value}")

            # get coords and variable names in crocolake
            var_pq = params_db2crocolake[random_var]
            cols_pq = [var_pq]
            indices_pq = {}
            for k, v in indices.items():
                indices_pq[ params_db2crocolake[k] ] = ds[k][v].item()
            indices_pq[ "LATITUDE" ] = nc_lat
            indices_pq[ "LONGITUDE" ] = nc_lon
            cols_pq.extend(indices_pq.keys())

            logging.info(f"var_pq: {var_pq}")
            logging.info(f"indices_pq: {indices_pq}")
            pq_filters = [(column, "==", value) for column, value in indices_pq.items()]
            logging.info(f"filters: {pq_filters}")

            ddf = dd.read_parquet(
                pq_path,
                columns=[var_pq],
                filters=pq_filters,
            )
            ddf = ddf.compute() # this should be one row or an empty dataframe
                                # (if the was missing and the whole row it ended
                                # up into contained missing data that was thus
                                # discarded)

            if ddf.shape[0] == 0:
                # if the original data ended in a row with all observations as
                # pd.NAs the row was dropped as it did not contain relevant info
                logging.info("pq_value was pd.NA and discarded")
                assert pd.isna(nc_value)
                continue

            # otherwise ddf has at most one row
            assert ddf.shape[0] == 1

            pq_value = ddf[var_pq].values[0] # this should be a scalar or a pd.NA
            logging.info(f"pq_value: {pq_value}")

            if pd.isna(pq_value):
                # check that also original source is NaN or pd.NA
                assert pd.isna(nc_value)

            elif np.isscalar(pq_value):
                # CrocoLake measured variables are float32, but original dataset
                # might have float64 precision
                assert pq_value == np.float32(nc_value)

            else:
                assert False, "value in CrocoLake is not a scalar nor a pd.NA"


#------------------------------------------------------------------------------#
    def test_data_integrity_spraygliders_phy(self):
        self._check_variables(
            db_type="PHY",
            db_name="SprayGliders"
        )

#------------------------------------------------------------------------------#
    def test_data_integrity_spraygliders_bgc(self):

        self._check_variables(
            db_type="BGC",
            db_name="SprayGliders"
        )
