#!/usr/bin/env python3

## @file glodap2parquet.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Wed 30 Oct 2024

##########################################################################
import argparse
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

def spray2parquet(spray_path=None, outdir_pqt=None, fname_pq=None, use_config_file=None):

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

    if not use_config_file:
        print("Using user-defined configuration")
        config = {
            'db': 'GLODAP',
            'db_type': 'PHY',
            'input_path': spray_path,
            'outdir_pq': outdir_pqt,
            'outdir_schema': './schemas/SprayGliders/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
        }
        ConverterPHY = ConverterSprayGliders(config)

    else: # reads from file
        print("Using configuration from config.yaml")
        ConverterPHY = ConverterSprayGliders(db_type='phy')

    spray_files = glob.glob(os.path.join(ConverterPHY.input_path, '*.nc'))
    spray_names = [os.path.basename(f) for f in spray_files]

    print('Spray files:')
    pprint(spray_names)

    spray_fpath = []
    for n in spray_names:
        spray_fpath.append(spray_path + n)

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
    parser.add_argument('-o', help="Destination path for parquet format database", required=True)
    parser.add_argument("-f", help="Basename for output files", required=False, default="1200_PHY_SPRAY-DEV.parquet")
    parser.add_argument('--config', action='store_true', help="Use config files instead of parsing arguments", required=False, default=None)

    args = parser.parse_args()

    spray2parquet(args.i,args.o,args.f,args.config)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("spray2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
