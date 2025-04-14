#!/usr/bin/env python3

## @file glodap2parquet.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Wed 30 Oct 2024

##########################################################################
import os
from warnings import simplefilter
from datetime import datetime

import argparse
import glob
import pandas as pd
# ignore pandas "educational" performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from dask.distributed import Client, Lock
from crocolaketools.converter.converterSprayGliders import ConverterSprayGliders

import functools
print = functools.partial(print, flush=True)
##########################################################################

def spray2parquet(spray_path, outdir_pqt, fname_pq_phy, fname_pq_bgc):

    if outdir_pqt[-1] != '/':
        outdir_pqt += '/'
    outdir_pqt_phy = outdir_pqt + fname_pq_phy
    if outdir_pqt_phy[-1] != '/':
        outdir_pqt_phy += '/'
    outdir_pqt_bgc = outdir_pqt + fname_pq_bgc
    if outdir_pqt_bgc[-1] != '/':
        outdir_pqt_bgc += '/'
    outdir_pqt_phy = outdir_pqt_phy + 'current/'
    outdir_pqt_bgc = outdir_pqt_bgc + 'current/'

    tmp_nc_path = outdir_pqt + '1201_SPRAY_tmp/'

    spray_files = glob.glob(os.path.join(spray_path, '*.nc'))
    spray_names = [os.path.basename(f) for f in spray_files]

    print('Spray files:')
    print(spray_names)

    spray_fpath = []
    for n in spray_names:
        spray_fpath.append(spray_path + n)

    print('Spray_fpath:')
    print(spray_fpath)

    # this set up works
    # it seems that more workers or threads raises memory issues
    client = Client(
        threads_per_worker=20,
        n_workers=1,
        memory_limit='110GB',
        processes=True,
        dashboard_address=':1111',
    )

    ConverterPHY = ConverterSprayGliders(
        db = "SprayGliders",
        db_type="PHY",
        input_path = spray_path,
        outdir_pq = outdir_pqt_phy,
        outdir_schema = './schemas/SprayGliders/',
        fname_pq = fname_pq_phy,
        add_derived_vars = True,
        tmp_path = tmp_nc_path
    )

    return

    print("Creating temporary files...")
    ConverterPHY.prepare_data(
        flist=spray_names,
        lock=Lock()
    )

    print("Temporary files created.")

    client.restart()

    flist = glob.glob(os.path.join(tmp_nc_path, '*.nc'))
    flist = [os.path.basename(f) for f in flist]
    print('Temporary files:')
    print(flist)

    print("Converting temporary files to parquet...")

    ConverterPHY.convert(
        filenames=flist,
    )

    print("Temporary files converted to parquet.")

    print("PHY files converted to parquet.")
    print("Working on BGC files...")
    # client.restart()
    print('Temporary files:')
    print(flist)

    ConverterBGC = ConverterSprayGliders(
        db = "SprayGliders",
        db_type="BGC",
        input_path = spray_path,
        outdir_pq = outdir_pqt_bgc,
        outdir_schema = './schemas/SprayGliders/',
        fname_pq = fname_pq_bgc,
        add_derived_vars = True,
        tmp_path = tmp_nc_path
    )

    print("Converting temporary files to parquet...")
    ConverterBGC.convert(
        filenames=flist
    )
    print("BGC files converted to parquet.")

    client.shutdown()

    return

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to convert Spray Gliders database to parquet')
    parser.add_argument('-i', help="Path to Spray Gliders data", required=True)
    parser.add_argument('-o', help="Destination path", required=False, default="None")
    parser.add_argument('--phy', help="Basename for phy-variables database", required=True)
    parser.add_argument('--bgc', help="Basename for bgc-variables database", required=True)

    args = parser.parse_args()

    spray2parquet(args.i,args.o,args.phy,args.bgc)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("spray2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
