#!/usr/bin/env python3

## @file saildrones2parquet.py
#
# Script to convert Saildrone NetCDF files to CROCOLAKE-compliant Parquet format
#
## @author David Nady <davidnady4yad@gmail.com>
## @date Mon 19 May 2025

##########################################################################
import argparse
import os
import glob
from datetime import datetime
from warnings import simplefilter
import pandas as pd
# Ignore pandas performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from crocolaketools.converter.converterSaildrones import ConverterSaildrones

import functools
print = functools.partial(print, flush=True)
##########################################################################

def saildrones2parquet(saildrones_path=None, outdir_pqt_phy=None, outdir_pqt_bgc=None, fname_pq=None, use_config_file=False):
    """Convert Saildrone NetCDF files to Parquet format"""

    if not use_config_file:
        print("Using user-defined configuration")
        config_phy = {
            'db': 'Saildrones',
            'db_type': 'PHY',
            'input_path': saildrones_path,
            'outdir_pq': outdir_pqt_phy,
            'outdir_schema': './schemas/Saildrones/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
        }
        converter_phy = ConverterSaildrones(config_phy)

        # Get list of NetCDF files
        saildrones_files = glob.glob(os.path.join(saildrones_path, '*.nc'))
        saildrones_names = [os.path.basename(f) for f in saildrones_files]

        if not saildrones_names:
            raise ValueError(f"No NetCDF files found in '{saildrones_path}'.")

        print("Saildrone files to process:")
        for f in saildrones_names:
            print(f"  - {f}")

        print("Converting PHY files to parquet...")
        converter_phy.convert(saildrones_names)
        print("PHY files converted to parquet.")
        del converter_phy
    else:
        print("Using configuration from config.yaml")
        converter_phy = ConverterSaildrones(db_type='phy')
        print("Converting PHY files to parquet...")
        converter_phy.convert()
        print("PHY files converted to parquet.")
        del converter_phy

    if not use_config_file:
        print("Using user-defined configuration")
        config_bgc = {
            'db': 'Saildrones',
            'db_type': 'BGC',
            'input_path': saildrones_path,
            'outdir_pq': outdir_pqt_bgc,
            'outdir_schema': './schemas/Saildrones/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': False,
        }
        converter_bgc = ConverterSaildrones(config_bgc)

        # Reuse list of NetCDF files
        saildrones_files = glob.glob(os.path.join(saildrones_path, '*.nc'))
        saildrones_names = [os.path.basename(f) for f in saildrones_files]

        print("Saildrone files to process:")
        for f in saildrones_names:
            print(f"  - {f}")

        print("Converting BGC files to parquet...")
        converter_bgc.convert(saildrones_names)
        print("BGC files converted to parquet.")
        del converter_bgc
    else:
        print("Using configuration from config.yaml")
        converter_bgc = ConverterSaildrones(db_type='bgc')
        print("Converting BGC files to parquet...")
        converter_bgc.convert()
        print("BGC files converted to parquet.")
        del converter_bgc

    return

##########################################################################
def main():
    parser = argparse.ArgumentParser(description='Convert Saildrone NetCDF files to Parquet format')
    parser.add_argument('-i', help='Path to Saildrone NetCDF files (if not using config.yaml)', required=False, default=None)
    parser.add_argument('--phy', help='Destination path for physical-variables database', required=False, default=None)
    parser.add_argument('--bgc', help='Destination path for bgc-variables database', required=False, default=None)
    parser.add_argument('-f', help='Basename for output files', required=False, default='1300_SAILDRONES.parquet')
    parser.add_argument('--config', help='Use config file instead of command-line arguments', action='store_true')
    # When --config is provided, uses config.yaml; otherwise, uses command-line arguments

    args = parser.parse_args()

    # Validate input path if provided
    if args.i and not os.path.isdir(args.i):
        raise ValueError(f"Input path '{args.i}' is not a valid directory.")

    saildrones2parquet(
        saildrones_path=args.i,
        outdir_pqt_phy=args.phy,
        outdir_pqt_bgc=args.bgc,
        fname_pq=args.f,
        use_config_file=args.config
    )

##########################################################################
if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("saildrones2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")