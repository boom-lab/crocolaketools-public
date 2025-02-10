#!/usr/bin/env python3

## @file glodap2parquet.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Wed 30 Oct 2024

##########################################################################
from datetime import datetime
from warnings import simplefilter
import pandas as pd
# ignore pandas "educational" performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from crocolaketools.converter.converterGLODAP import ConverterGLODAP
##########################################################################

def glodap2parquet(glodap_path, glodap_name, outdir_pqt_phy, outdir_pqt_bgc, fname_pq):
    """Convert GLODAP data to parquet format"""

    ConverterPHY = ConverterGLODAP(
        db = "GLODAP",
        db_type="PHY",
        input_path = glodap_path,
        outdir_pq = outdir_pqt_phy,
        outdir_schema = './schemas/GLODAP/',
        fname_pq = fname_pq,
        add_derived_vars = True
    )

    ConverterPHY.convert(glodap_name)

    del ConverterPHY

    ConverterBGC = ConverterGLODAP(
        db = "GLODAP",
        db_type="BGC",
        input_path = glodap_path,
        outdir_pq = outdir_pqt_bgc,
        outdir_schema = './schemas/GLODAP/',
        fname_pq = fname_pq,
        add_derived_vars = True
    )

    ConverterBGC.convert(glodap_name)

    del ConverterBGC

    return

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to convert GLODAP csv master file to parquet')
    parser.add_argument('-i', help="Path to GLODAP csv master file", required=True)
    parser.add_argument('-n', help="Name of GLODAP csv master file", required=False, default="GLODAPv2.2023_Merged_Master_File.csv")
    parser.add_argument('--phy', help="Destination path for physical-variables database", required=True)
    parser.add_argument('--bgc', help="Destination path for bgc-variables database", required=True)
    parser.add_argument('-b', help="Basename for output files", required=False, default="None")

    args = parser.parse_args()

    if args.b == "None" and args.n == "GLODAPv2.2023_Merged_Master_File.csv":
        basename = args.n[:-4]
    else:
        raise ValueError("Please provide a basename for the output files.")

    glodap2parquet(args.i, args.n, args.phy, args.bgc, args.b)

##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("glodap2parquet.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
