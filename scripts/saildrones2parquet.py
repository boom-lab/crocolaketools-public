#!/usr/bin/env python3

## @file saildrones2parquet.py
#
# Script to convert Saildrone NetCDF files to CROCOLAKE-compliant Parquet format
#
## @author <your name>
## @date 2025-05-04

##########################################################################
import argparse
from datetime import datetime
from warnings import simplefilter
import pandas as pd
import os
import glob

# Ignore pandas performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

from crocolaketools.converter.converterSaildrones import ConverterSaildrones
##########################################################################

def saildrones2parquet(saildrones_path=None, outdir_pqt_phy=None, outdir_pqt_bgc=None, fname_pq=None, use_config_file=False):
    """Convert Saildrone NetCDF files to Parquet format"""

    if not use_config_file:
        print("Using user-defined configuration")
        
        # Process physical variables
        config_phy = {
            'db': 'Saildrones',
            'db_type': 'PHY',
            'input_path': saildrones_path,
            'outdir_pq': outdir_pqt_phy,
            'outdir_schema': './schemas/Saildrones/',
            'fname_pq': f"{fname_pq}_PHY" if fname_pq else None,
            'add_derived_vars': True,
            'overwrite': True
        }
        converter_phy = ConverterSaildrones(config_phy)
        
        # Process BGC variables
        config_bgc = {
            'db': 'Saildrones',
            'db_type': 'BGC',
            'input_path': saildrones_path,
            'outdir_pq': outdir_pqt_bgc,
            'outdir_schema': './schemas/Saildrones/',
            'fname_pq': f"{fname_pq}_BGC" if fname_pq else None,
            'add_derived_vars': True,
            'overwrite': True
        }
        converter_bgc = ConverterSaildrones(config_bgc)

        saildrones_files = glob.glob(os.path.join(saildrones_path, '*.nc'))
        saildrones_names = [os.path.basename(f) for f in saildrones_files]

        print(f"Saildrone files to process:")
        for f in saildrones_names:
            print(f"  - {f}")

        converter_phy.convert(saildrones_names)
        converter_bgc.convert(saildrones_names)

        del converter_phy, converter_bgc
    else:
        print("Using configuration from config.yaml")
        converter_phy = ConverterSaildrones(db_type='phy')
        converter_bgc = ConverterSaildrones(db_type='bgc')
        converter_phy.convert()
        converter_bgc.convert()
        del converter_phy, converter_bgc

    return

##########################################################################
def main():
    parser = argparse.ArgumentParser(description='Convert Saildrone NetCDF files to Parquet format')
    parser.add_argument('-i', help='Path to Saildrone NetCDF files (if not using config.yaml)', required=False, default=None)
    parser.add_argument('--phy', help='Destination path for physical-variables database', required=False, default=None)
    parser.add_argument('--bgc', help='Destination path for bgc-variables database', required=False, default=None)
    parser.add_argument('-b', help='Basename for output files', required=False, default='1300_SAILDRONES')
    parser.add_argument('--config', help='Use config.yaml instead of command-line arguments', action='store_true', required=False)

    args = parser.parse_args()

    saildrones2parquet(
        saildrones_path=args.i,
        outdir_pqt_phy=args.phy,
        outdir_pqt_bgc=args.bgc,
        fname_pq=args.b,
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