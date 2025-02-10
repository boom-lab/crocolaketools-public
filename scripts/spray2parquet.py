#!/usr/bin/env python3

## @file glodap2parquet.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Wed 30 Oct 2024

##########################################################################
import os
from pprint import pprint
from warnings import simplefilter
from datetime import datetime
import glob
import pandas as pd
# ignore pandas "educational" performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from dask.distributed import Client
from crocolaketools.converter.converterSprayGliders import ConverterSprayGliders
##########################################################################

def spray2parquet(spray_path, outdir_pqt, fname_pq):

    spray_files = glob.glob(os.path.join(spray_path, '*.nc'))
    spray_names = [os.path.basename(f) for f in spray_files]

    print('Spray files:')
    pprint(spray_names)

    spray_fpath = []
    for n in spray_names:
        spray_fpath.append(spray_path + n)

    # this set up works
    # it seems that more workers or threads raises memory issues
    client = Client(
        threads_per_worker=2,
        n_workers=1,
        memory_limit='110GB',
        processes=True
    )

    ConverterPHY = ConverterSprayGliders(
        db = "SprayGliders",
        db_type="PHY",
        input_path = spray_path,
        outdir_pq = outdir_pqt,
        outdir_schema = './schemas/SprayGliders/',
        fname_pq = fname_pq,
        add_derived_vars = True
    )

    ConverterPHY.convert(spray_names)

    client.shutdown()

    return

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to convert Spray Gliders database to parquet')
    parser.add_argument('-i', help="Path to Spray Gliders data", required=True)
    parser.add_argument('-o', help="Destination path for parquet format database", required=True)
    parser.add_argument("-f", help="Basename for output files", required=False, default="1200_PHY_SPRAY-DEV.parquet")

    args = parser.parse_args()

    spray2parquet(args.i,args.o,args.f)

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("spray2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
