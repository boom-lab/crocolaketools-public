#!/usr/bin/env python3

## @file test_converter.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Fri 22 Nov 2024
#
##########################################################################
import glob
import os
import random
from pprint import pprint
import shutil
terminal_width = shutil.get_terminal_size().columns

import dask.dataframe as dd
from dask.distributed import Client, Lock
import pandas as pd
import pytest

import xarray as xr

##########################################################################

class TestNC:

    def read_nc(self, input_fname, lock):

        input_fname = "/vortexfs1/share/boom/users/enrico.milanese/originalDatabases/SprayGliders/" + input_fname

        lock.acquire(timeout=600)
        ds = xr.open_dataset(
            input_fname,
            cache=False,
        )
        invars = ["time","lat","lon","depth","temp","temperature"]
        ds_vars = list(ds.data_vars) + list(ds.coords)
        invars = list(set(invars) & set(ds_vars))
        ddf = ds[invars].to_dataframe()
        #        ddf = ddf.compute()
        lock.release()

        if "temp" in ddf.columns.to_list():
            ddf = ddf.rename(columns={
                "temp": "temperature",
            })
            invars = invars + ["temperature"]

        print("columns in ddf pre flatten:")
        print(ddf.columns.to_list())

        print("index in ddf pre flatten:")
        print(ddf.index)

        ddf = ddf.reset_index(drop=True)

        print("columns in ddf post reset:")
        print(ddf.columns.to_list())

        print("index in ddf post reset:")
        print(ddf.index)

        cols_to_drop = [item for item in ddf.columns.to_list() if item not in invars]

        ddf = ddf.drop(columns=cols_to_drop)

        print("columns in ddf post drop:")
        print(ddf.columns.to_list())

        print("index in ddf post drop:")
        print(ddf.index)

        ddf = ddf.reset_index(drop=True)

        print("columns in ddf post 2nd reset:")
        print(ddf.columns.to_list())

        print("index in ddf post 2nd reset:")
        print(ddf.index)

        return ddf

    def test_nc2(self):

        client = Client(
            threads_per_worker=2,
            n_workers=1,
            memory_limit='100GB',
            dashboard_address=':8787',
        )

        # Define the paths to the NetCDF files
        spray_path = "/vortexfs1/share/boom/users/enrico.milanese/originalDatabases/SprayGliders/"
        netcdf_files = [
            spray_path+"Calypso.nc",
            spray_path+"GoM.nc"
        ],

        # Function to read and convert NetCDF to Dask DataFrame with a lock
        def netcdftodaskdataframe(netcdffile, lock):
            with lock:
                # Read the NetCDF file using xarray
                ds = xr.open_dataset(
                    netcdffile,
                    engine="netcdf4",
                    cache=False,
                    chunks={}
                )

                # Convert to Dask DataFrame
                ddf = ds.to_dask_dataframe()

                # Reset the index to ensure it is properly set
                ddf = ddf.reset_index()

            return ddf

        # Create a Dask distributed lock
        lock = Lock("netcdf_lock")

        # Create a Dask DataFrame using frommap with the lock
        ddf = dd.from_map(netcdftodaskdataframe, netcdf_files, lock=lock)

        # Define the output directory
        outputdir = "./parquetoutput/"
        os.makedirs(outputdir, exist_ok=True)

        # Store the Dask DataFrame to Parquet
        ddf.to_parquet(
            outputdir,
            engine="pyarrow",
            write_index=False,
            overwrite=True,
            compression='snappy'
        )

        print("Parquet files written successfully.")

        # Shutdown the Dask client
        client.shutdown()

    def test_nc(self):
       """Test that SprayGliders conversion executes; this test does not use
        convert() but its internal steps to check the dataframe is never empty
        """
       client = Client(
           threads_per_worker=2,
           n_workers=1,
           memory_limit='100GB',
           dashboard_address=':8787',
       )

       spray_path = "/vortexfs1/share/boom/users/enrico.milanese/originalDatabases/SprayGliders/"
       outdir_spray_pqt = "/vortexfs1/share/boom/users/enrico.milanese/myDatabases/1200_PHY_SPRAY-DEV/tests/"

       from dask.distributed import Lock
       lock=Lock()

       ddf = dd.from_map(
           self.read_nc,
           ["Calypso.nc","GoM.nc"],
           lock=lock
       )

       print("Before repartitioning:")
       print(ddf.dtypes)
       print(ddf.head())

       assert not ddf.head().empty

       #ddf = ddf.repartition(partition_size="300MB")
       #

       #print("not persisting")
       #ddf = ddf.persist()
       print("After repartitioning:")
       print(ddf.dtypes)
       print(ddf.head())
       assert not ddf.head().empty

       print("ddf info:")
       print(ddf.info())

       # print("ddf.compute():")
       # ddfc = ddf.compute()
       # print(ddfc)

       output_dir = "./parquet_output/"
       ddf.to_parquet(
           output_dir,
           engine="pyarrow",
           write_index=False,
           overwrite=True
       )

       output_dir = "./test_pqt/"
       ddf.to_parquet(
           output_dir,
           engine="pyarrow",
           write_index=False,
           overwrite=True,
           append=False
       )

       client.shutdown()

    def load_data(self,filename):
        # Simulate loading data from a file
        data = {
            'A': [1, 2, 3],
            'B': [4, 5, 6],
            'C': [file_name] * 3
        }
        return pd.DataFrame(data)

    def test_tp(self):
        file_names = ['file1', 'file2', 'file3']

        ddf = dd.frommap(load_data, filenames)

        output_dir = "./parquet_output/"
        os.makedirs(output_dir, existok=True)

        ddf.to_parquet(
            output_dir,
            engine="pyarrow",
            write_index=False,
            overwrite=True,
            partitionon=['C'],  # Partition by column 'C'
            compression='snappy'  # Use snappy compression
        )

        print("Parquet files written successfully.")
