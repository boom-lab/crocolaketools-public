#!/usr/bin/env python3

## @file saildrones2parquet.py
#
# Script to convert Saildrone NetCDF files to CROCOLAKE-compliant Parquet format
#

import os
import glob
import argparse
from datetime import datetime
from warnings import simplefilter
import pandas as pd

# Ignore pandas performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

from crocolaketools.converter.ConverterSaildrones import ConverterSaildrones

##########################################################################

def saildrones2parquet(saildrones_path, outdir_pqt, fname_pq, db_type):
    """Convert Saildrone NetCDF files to parquet format"""

    # Get all NetCDF files in the input directory
    saildrones_files = glob.glob(os.path.join(saildrones_path, '*.nc'))
    saildrones_names = [os.path.basename(f) for f in saildrones_files]

    print(f'Saildrone {db_type} files to process:')
    for f in saildrones_names:
        print(f'  - {f}')

    # Create converter instance for the selected data type (BGC or PHY)
    converter = ConverterSaildrones(
        db="Saildrones",
        db_type=db_type,
        input_path=saildrones_path,
        outdir_pq=outdir_pqt,
        outdir_schema='./schemas/Saildrones/',
        fname_pq=fname_pq,
        add_derived_vars=True
    )

    # Convert the files
    converter.convert(saildrones_names)

    del converter

##########################################################################

def main():
    parser = argparse.ArgumentParser(description='Convert Saildrone NetCDF files to Parquet format')
    parser.add_argument('-i', help="Path to Saildrone NetCDF files", required=True)
    parser.add_argument('-o', help="Destination path for Parquet output", required=True)
    parser.add_argument('-f', help="Basename for output files", default="1300_SAILDRONES")
    parser.add_argument('-t', '--type', help="Data type: BGC or PHY", choices=["BGC", "PHY"], default="BGC")

    args = parser.parse_args()

    fname_full = f"{args.f}_{args.type.upper()}"
    saildrones2parquet(args.i, args.o, fname_full, args.type.upper())

##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("saildrones2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
