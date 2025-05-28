#!/usr/bin/env python3

## @file converterGLODAP.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 04 Feb 2025

##########################################################################
import os
import warnings
import dask.dataframe as dd
import gsw
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr
from crocolakeloader import params
from crocolaketools.converter.converter import Converter
##########################################################################

class ConverterGLODAP(Converter):

    """class ConverterGLODAP: methods to generate parquet schemas for
    GLODAP database

    """

    # ------------------------------------------------------------------ #
    # Constructors/Destructors                                           #
    # ------------------------------------------------------------------ #

    def __init__(self, config=None, db_type=None):

        if config is not None and not config['db'] == "GLODAP":
            raise ValueError("Database must be GLODAP.")
        elif ((config is None) and (db_type is not None)):
            config = {
                'db': 'GLODAP',
                'db_type': db_type.upper(),
            }

        Converter.__init__(self, config)

    # ------------------------------------------------------------------ #
    # Methods                                                            #
    # ------------------------------------------------------------------ #

#------------------------------------------------------------------------------#
## Read file to convert into a pandas dataframe
    def read_to_df(self, filename=None, lock=None):
        """Read file into a pandas dataframe

        Argument:
        filename -- file name, including relative path

        Returns
        df -- pandas dataframe
        """

        if filename is None:
            filename = "GLODAPv2.2023_Merged_Master_File.csv"
            print("Using default filename: ", filename)

        input_fname = self.input_path + filename
        print("Reading GLODAP file: ", input_fname)

        # low_memory=False as GLODAP is a small db
        ddf = dd.read_csv(
            input_fname,
            assume_missing=True,
            delimiter=",",
            header=0,
            low_memory=False,
            dtype_backend='pyarrow'
        )

        return self.standardize_data(ddf)

#------------------------------------------------------------------------------#
## Convert parquet schema to pandas
    def standardize_data(self,ddf):
        """Standardize dask dataframe to schema consistent across databases

        Argument:
        ddf -- dask dataframe

        Returns:
        ddf -- homogenized (dask) dataframe
        """

        ### Adding profile number
        # GLODAP has no profile number, so we create a temporary one. For each
        # expocode, it is unique given (cruise, station, region, cast). For each
        # group of these values, the profile number is a progressive integer
        # number
        ddf = self.add_profile_id(ddf)

        # convert GLODAP multiple time columns to one datetime
        print("Converting GLODAP multiple time columns to one datetime")
        rename_datetime = {
            "G2year": "year",
            "G2month": "month",
            "G2day": "day",
            "G2hour": "hour",
            "G2minute": "minute"
        }
        ddf = ddf.rename(columns=rename_datetime) #pd.to_datetime expects these column names
        ddf["JULD"] = dd.to_datetime(ddf[["year", "month", "day", "hour", "minute"]])
        ddf = ddf.drop(columns=rename_datetime.values())
        ddf = ddf.persist()

        # keep only good QC values
        params_to_check = []
        for param in params.params["GLODAP2CROCOLAKE"].keys():
            if param.startswith("G2") and param.endswith("f") and param in ddf.columns:
                ddf = ddf.map_partitions(
                    self.keep_best_values, param
                )
                params_to_check.append(param[:-1])
        ddf = ddf.persist()

        # remove rows containing all NAs
        ddf = ddf.map_partitions(
            super().remove_all_NAs, params_to_check
        )
        ddf = ddf.persist()

        # return standardized dataframe
        return super().standardize_data(ddf)

#------------------------------------------------------------------------------#
## Keep best values for each row
    def add_profile_id(self,ddf):
        """"""

        # sorting values to order closer to that of operations later
        ddf = ddf.sort_values(
            by=["G2expocode","G2cruise","G2station","G2region","G2cast","G2pressure"]
        )
        # persisting and repartitioning to minimize chances of empty partitions
        ddf = ddf.persist()
        ddf = ddf.repartition(partition_size="100MB")

        def compute_hash(df, cols, hash_col="hash"):
            # gives unique hash for each sequence of values of columns cols
            concat = df[cols].astype(str).agg('-'.join, axis=1)
            df[hash_col] = pd.util.hash_pandas_object(concat, index=False).astype('uint64')
            return df

        # add hash_0 to perform sorts and groupbys and merges on one unique
        # column instead of multiple columns at once
        cols = ["G2expocode", "G2cruise", "G2station", "G2region", "G2cast"]
        hash_col = "hash_0"
        meta = ddf._meta
        meta[hash_col] = 'uint64'
        ddf = ddf.map_partitions(
            lambda df: compute_hash(df, cols, hash_col=hash_col),
            meta=meta,
        )
        ddf = ddf.persist()

        # get combinations of "metadata" that unambiguosly describe all profiles
        # (casts)
        unique_by = ["G2expocode", "G2cruise", "G2station", "G2region", "G2cast", hash_col]
        unique_casts = ddf[ unique_by ].drop_duplicates()

        # generate hash_1 for each of set of "metadata" that contains 1 or more casts:
        # in GLODAP, cast number resets when any in ["G2expocode", "G2cruise",
        # "G2station", "G2region"] changes
        meta = unique_casts._meta
        meta["hash_1"] = "uint64"
        hash_by_cols = ["G2expocode", "G2cruise", "G2station", "G2region"]
        unique_casts = unique_casts.map_partitions(
            lambda df: compute_hash(df, hash_by_cols, hash_col="hash_1"),
            meta=meta,
        )
        unique_casts = unique_casts.persist()

        # count how many casts each hash_1 contains, and assign to each of them
        # a progressive integer ID starting from 1
        #
        # NB: we cannot operate on partitions (map_partitions) because
        # cumcount() would reset at each partition; we must operate on groups;
        # groupby passes a pd dataframe to cumcount() and we're good

        # we also need to set the index or dask generates "axis with duplicate
        # labels" in the line after this, failing to properly assign the new
        # column
        df_indexed = unique_casts.set_index("hash_1", sorted=False)
        df_indexed["cumul_count"] = df_indexed.groupby("hash_1").cumcount() + 1
        unique_casts = df_indexed.reset_index()  # moving index (hash_1) back
                                                 # into column with same name
        unique_casts = unique_casts.persist()

        # getting unique hash_1 values, repartitioning and persisting to prevent
        # empty partitions
        unique_hash1 = unique_casts[ ["G2expocode","hash_1"] ].drop_duplicates()
        unique_hash1 = unique_hash1.repartition(partition_size="100MB")
        unique_hash1 = unique_hash1.persist()

        max_cumul_count = unique_casts.groupby('hash_1')['cumul_count'].max().reset_index()
        max_cumul_count = max_cumul_count.persist()
        max_cumul_count = max_cumul_count.rename(columns={"cumul_count":"max_cc"})
        max_cumul_count = max_cumul_count.persist()

        unique_hash1 = unique_hash1.merge(max_cumul_count, on='hash_1', how='left')
        unique_hash1 = unique_hash1.persist()

        unique_expocodes = unique_casts['G2expocode'].drop_duplicates().compute().tolist()
        unique_hash1_expocode_partitions = [
            unique_hash1[unique_hash1['G2expocode'] == expocode]
            for expocode in unique_expocodes
        ]
        unique_hash1_expocode_partitions = [p.repartition(npartitions=1) for p in unique_hash1_expocode_partitions]
        unique_hash1_repartitioned = dd.concat(unique_hash1_expocode_partitions)
        unique_hash1_repartitioned = unique_hash1_repartitioned.persist()

        def shifting(df):
            df["sum_mcc"] = df["max_cc"].shift(1).fillna(0).cumsum().astype("uint64")
            return df

        meta = unique_hash1_repartitioned._meta
        meta["sum_mcc"] = "uint64"
        unique_hash1_repartitioned = unique_hash1_repartitioned.map_partitions( shifting, meta = meta )

        unique_casts = unique_casts[ ["G2expocode","hash_0","hash_1","cumul_count"] ]
        merged = unique_casts.merge(unique_hash1_repartitioned[ ["hash_1","sum_mcc"] ], on=['hash_1'], how="left")
        merged["profile_nb"] = merged["cumul_count"] + merged["sum_mcc"]
        merged["profile_nb"] = merged["profile_nb"].astype("uint64")
        merged = merged.persist()

        merged_final = ddf.merge(merged[ ["hash_0","profile_nb"] ], on="hash_0", how="left")
        merged_final = merged_final.drop(labels=["hash_0"],axis=1)

        return merged_final.persist()

#------------------------------------------------------------------------------#
## Keep best values for each row
    def keep_best_values(self,df,param):
        """Keep the best observation available for each row

        Arguments:
        df -- a row or a partition of a pandas dataframe
        param -- name of the qc variable of the parameter

        Returns:
        df  --  updated dataframe

        """

        # GLODAP's quality control columns end with "f" (e.g. "nitratef")
        # and good values are 0 or 2
        condition = df[param].isin([0,2])

        # Find bad QC values
        df.loc[condition, param] = pd.NA
        df.loc[condition, param[:-1]] = pd.NA

        return df

##########################################################################
if __name__ == "__main__":
    ConverterGLODAP()
