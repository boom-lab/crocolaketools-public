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
    def _get_scalar_from_ds(self, selected_scalar):
        """Get a scalar value from a dataset, it handles the case where the a
        value is of np datetime64 type

        Arguments:
        selected_scalar -- scalar from xarray data array for given variable and
                           indices

        Returns:
        value -- the value of the selected data, as a scalar or numpy datetime
                 depending on the original dtype

        """

        if np.issubdtype(selected_scalar.dtype, np.datetime64):
            return selected_scalar.values
        else:
            return selected_scalar.item()


#------------------------------------------------------------------------------#
    def _check_profiles(self,db_name,db_type,db_name_config=None,nc_pattern=None):

        """Pick a random variable in a random original netCDF file and find it
        in the parquet version, check that they are equal"""

#------------------------------------------------------------------------------#
    def _check_variables_nc(self,db_name,db_type,db_name_config=None,nc_pattern=None):

        """Pick a random variable in a random original netCDF file and find it
        in the parquet version, check that they are equal

        Arguments:
        db_name   --  database name as in params.py
        db_type   --  phy or bgc
        db_name_config  -- database name as in config.yaml if different from db_name
        nc_pattern      -- specific file name patterns for original netCDF files

        """

        if db_name_config is None:
            db_name_config = db_name
        if nc_pattern is None:
            nc_pattern = "*.nc"

        config_path = importlib.resources.files("crocolaketools.config").joinpath("config.yaml")
        config = yaml.safe_load(open(config_path))
        config = config[db_name_config + "_" + db_type]

        nc_path = config["input_path"]
        pq_path = config["outdir_pq"]

        # get list of original nc files
        if os.path.isdir(nc_path):
            nc_path = os.path.join(nc_path, "**", nc_pattern)
        nc_files = glob.glob(nc_path, recursive=True)

        logging.info(f"nc_path: {nc_path}")
        logging.info(f"Files found: {nc_files}")

        params_db2crocolake = params.params[db_name + "2CROCOLAKE"]
        # remove PLATFORM_NUMBER from params_db2crocolake because it needs to be dealt with separately
        # (in general it is not unique given lat, lon, profile)

        multi = ["CYCLE_NUMBER", "PLATFORM_NUMBER", "DATA_MODE",
                 "DIRECTION", "JULD_QC", "LATITUDE", "LONGITUDE", "POSITION_QC", "JULD"]

        # PLATFORM_NUMBER needs to be handled differently for spray gliders
        # because it's not 1:1 conversion but there is some extra step (not
        # implemented yet)
        if db_name == "SprayGliders":
            params_in_crocolake = [k for k, v in params_db2crocolake.items()
                                   if v not in ["PLATFORM_NUMBER"]]
        else:
            params_in_crocolake = params_db2crocolake.keys()

        params_crocolake2db = params.params[ "CROCOLAKE2" + db_name]
        lat_name = params_crocolake2db["LATITUDE"]
        lon_name = params_crocolake2db["LONGITUDE"]

        for j in range(1000):
            nc_file = random.choice( nc_files )

            if db_name == "Argo":
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
            nc_value = self._get_scalar_from_ds(var_data.isel(**indices))
            # nc_value = var_data.isel(**indices).item()

            shared_indices = {dim: idx for dim, idx in indices.items() if dim in ds[lat_name].dims}
            nc_lat = self._get_scalar_from_ds(ds[lat_name].isel(**shared_indices))
            # nc_lat = ds[lat_name].isel(**shared_indices).item()
            shared_indices = {dim: idx for dim, idx in indices.items() if dim in ds[lon_name].dims}
            nc_lon = self._get_scalar_from_ds(ds[lon_name].isel(**shared_indices))

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
                logging.info(f"{k} : {self._get_scalar_from_ds(ds[k][v])}")
            logging.info(f"nc_value: {nc_value}")

            # get coords and variable names in crocolake
            var_pq = params_db2crocolake[random_var]
            cols_pq = [var_pq]
            indices_pq = {}
            for k, v in indices.items():
                indices_pq[ params_db2crocolake[k] ] = self._get_scalar_from_ds(ds[k][v])
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

            # otherwise ddf has multiple rows only for the variables in multi,
            # but all rows should be identical
            if var_pq in multi:
                ddf = ddf.drop_duplicates()

            # otherwise has at most one row
            assert ddf.shape[0] == 1


            pq_value = ddf[var_pq].values[0] # this should be a scalar or a pd.NA
            logging.info(f"pq_value: {pq_value}")

            if pd.isna(pq_value):
                # check that also original source is NaN or pd.NA
                assert pd.isna(nc_value)

            elif np.isscalar(pq_value):
                # CrocoLake measured variables are float32, but original dataset
                # might have float64 precision
                if np.issubdtype(type(pq_value), np.integer):
                    pq_value = np.int32(pq_value)
                    nc_value = np.int32(nc_value)
                elif np.issubdtype(type(pq_value), np.floating):
                    pq_value = np.float32(pq_value)
                    nc_value = np.float32(nc_value)
                assert pq_value == nc_value

            else:
                assert False, "value in CrocoLake is not a scalar nor a pd.NA"

#------------------------------------------------------------------------------#
    def test_data_integrity_spraygliders_phy(self):
        self._check_variables_nc(
            db_type="PHY",
            db_name="SprayGliders"
        )

#------------------------------------------------------------------------------#
    def test_data_integrity_spraygliders_bgc(self):

        self._check_variables_nc(
            db_type="BGC",
            db_name="SprayGliders"
        )

#------------------------------------------------------------------------------#
    def test_data_integrity_argoqc_phy(self):
        self._check_variables_nc(
            db_type="PHY",
            db_name="Argo",
            db_name_config="ARGO-GDAC",
            nc_pattern="*_prof.nc"
        )

#------------------------------------------------------------------------------#
    def test_data_integrity_argoqc_bgc(self):
        self._check_variables_nc(
            db_type="BGC",
            db_name="Argo",
            db_name_config="ARGO-GDAC",
            nc_pattern="*_Sprof.nc"
        )
