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
import yaml

# Ignore pandas performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

from crocolaketools.converter.ConverterSaildrones import ConverterSaildrones
##########################################################################

def saildrones2parquet(saildrones_path=None, outdir_pqt=None, fname_pq=None, db_type="BGC", config_key=None):
    """Convert Saildrone NetCDF files to Parquet format"""

    if config_key is None:
        print("Using user-defined configuration")

        config = {
            'db': 'Saildrones',
            'db_type': db_type.upper(),
            'input_path': saildrones_path,
            'outdir_pq': outdir_pqt,
            'outdir_schema': './schemas/Saildrones/',
            'fname_pq': fname_pq,
            'add_derived_vars': True,
            'overwrite': True
        }

    else:
        print(f"Using configuration from config.yaml: {config_key}")
        with open('../crocolaketools/config/config.yaml', 'r') as f:
            full_config = yaml.safe_load(f)

        if config_key not in full_config:
            raise ValueError(f"Config key '{config_key}' not found in config.yaml")

        config = full_config[config_key]

    saildrones_files = glob.glob(os.path.join(config["input_path"], '*.nc'))
    saildrones_names = [os.path.basename(f) for f in saildrones_files]

    print(f"Saildrone {config['db_type']} files to process:")
    for f in saildrones_names:
        print(f"  - {f}")

    converter = ConverterSaildrones(config=config)
    converter.convert(saildrones_names)

    return

##########################################################################
def main():
    parser = argparse.ArgumentParser(description='Convert Saildrone NetCDF files to Parquet format')
    parser.add_argument('-i', help='Path to Saildrone NetCDF files (if not using config.yaml)', required=False, default=None)
    parser.add_argument('--out', help='Destination path for Parquet output (if not using config.yaml)', required=False, default=None)
    parser.add_argument('-b', help='Basename for output files', required=False, default='1300_SAILDRONES')
    parser.add_argument('-t', '--type', help='Data type: BGC or PHY', choices=['BGC', 'PHY'], default='BGC')
    parser.add_argument('--config', help='Use config.yaml with specified key (e.g., SAILDRONES_PHY)', required=False, default=None)

    args = parser.parse_args()

    saildrones2parquet(
        saildrones_path=args.i,
        outdir_pqt=args.out,
        fname_pq=f"{args.b}_{args.type.upper()}",
        db_type=args.type,
        config_key=args.config
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
