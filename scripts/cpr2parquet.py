#!/usr/bin/env python3

## @file cpr2parquet.py
#
#
## @author David Nady <davidnady4yad@gmail.com>
#
# @date Fri 21 Mar 2025

##########################################################################
import os
import argparse
from pprint import pprint
from warnings import simplefilter
from datetime import datetime
import pandas as pd
# ignore pandas "educational" performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from crocolaketools.converter.converterCPR import ConverterCPR
##########################################################################

def cpr2parquet(cpr_path, cpr_name, outdir_pqt_phy, outdir_pqt_bgc, fname_pq):
    """Convert CPR data to parquet format"""

    ConverterPHY = ConverterCPR(
        db = "CPR",
        db_type="PHY",
        input_path = cpr_path,
        outdir_pq = outdir_pqt_phy,
        outdir_schema = './schemas/CPR/',
        fname_pq = fname_pq,
        add_derived_vars = True
    )

    ConverterPHY.convert(cpr_name)

    del ConverterPHY

    ConverterBGC = ConverterCPR(
        db = "CPR",
        db_type="BGC",
        input_path = cpr_path,
        outdir_pq = outdir_pqt_bgc,
        outdir_schema = './schemas/CPR/',
        fname_pq = fname_pq,
        add_derived_vars = True
    )

    ConverterBGC.convert(cpr_name)

    del ConverterBGC

    return

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to convert CPR csv file to parquet')
    parser.add_argument('-i', help="Path to CPR csv file", required=True)
    parser.add_argument('-n', help="Name of CPR csv file", required=True)
    parser.add_argument('--phy', help="Destination path for physical-variables database", required=True)
    parser.add_argument('--bgc', help="Destination path for bgc-variables database", required=True)
    parser.add_argument('-b', help="Basename for output files", required=False, default="CPR")

    args = parser.parse_args()

    cpr2parquet(args.i, args.n, args.phy, args.bgc, args.b)

##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("cpr2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")