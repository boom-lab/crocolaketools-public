#!/usr/bin/env python3

## @file main.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 03 Sep 2024

##########################################################################
import sys
import argparse
from crocolaketools.downloader.downloaderArgoGDAC import DownloaderArgoGDAC
from crocolaketools.converter.converterArgoGDAC import ConverterArgoGDAC
import importlib.metadata
import time
##########################################################################

def argogdac2parquet():

    start_time = time.time()

    # Ensure stdout and stderr are unbuffered
    sys.stdout = open(sys.stdout.fileno(), 'w', buffering=1)
    sys.stderr = open(sys.stderr.fileno(), 'w', buffering=1)

    parser = argparse.ArgumentParser(description='Package to create parquet copy of Argo databases.')
    parser.add_argument('--version', action='store_true', help="Show version and exit")

    parser.add_argument(
        "-d", "--download",
        type=str,
        default="true",
        help=" If true (default), the Argo databases are updated (see --db option to specify only one of them)"
    )

    parser.add_argument(
        "-c", "--convert",
        type=str,
        default="true",
        help=" If true (default), the Argo databases are converted to parquet (see --db option to specify only one of them)"
    )

    parser.add_argument(
        "--gdac_index",
        type=str,
        default="./data/",
        help=" Path to profiles index files."
    )

    parser.add_argument(
        "--db_nc",
        type=str,
        default="./data/GDAC/dac/",
        help=" Root folder where databases will be downloaded to."
    )

    parser.add_argument(
        "--db_parquet",
        type=str,
        default="./data/parquet/",
        help=" Folder where parquet database will be stored to."
    )

    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help=" If not specified, bot Argo Core and BGC profiles are downloaded and/or converted. If 'phy' or 'bgc', only the Core or BGC profiles are downloaded and/or converted."
    )

    args = parser.parse_args()

    if args.version:
        version = importlib.metadata.version(__package__)
        print(f"argo2parquet version: {version}")
        return

    download_dbs = args.download
    convert_dbs = args.convert
    gdac_path = args.gdac_index
    outdir_nc = args.db_nc
    outdir_parquet = args.db_parquet
    db = args.db
    if db is None:
        db = ["phy","bgc"]
    elif isinstance(db, str):
        db = [db]

    if download_dbs.lower()=="true":
        dl_start_time = time.time()
        print("Updating the Argo databases...")
        print("Destination folder: " + outdir_nc)
        downloaderArgo = DownloaderArgoGDAC()
        flist_phy, flist_bgc, metadata_phy, metadata_bgc = downloaderArgo.argo_download(gdac_path, outdir_nc, db, False)
        dl_elapsed_time = time.time() - dl_start_time
        print("Download elapsed time: " + str(dl_elapsed_time))

    else:
        print("Retrieving list of files from the Argo database(s)...")
        print("Looking into folder: " + outdir_nc)
        flist_phy = []
        flist_bgc = []
        metadata_phy = []
        metadata_bgc = []

        if "phy" in db:
            downloaderArgo = DownloaderArgoGDAC()
            flist_phy, _, metadata_phy, _ = downloaderArgo.argo_download(gdac_path, outdir_nc, ["phy"], True)

        if "bgc" in db:
            downloaderArgo = DownloaderArgoGDAC()
            _, flist_bgc, _, metadata_bgc = downloaderArgo.argo_download(gdac_path, outdir_nc, ["bgc"], True)

    if convert_dbs.lower()=="true":
        conv_start_time = time.time()
        print("Converting the databases...")
        print("Destination folder: " + outdir_parquet)

        ConverterArgoGDAC.convert_dask_tools(
            [flist_phy, flist_bgc],
            [metadata_phy, metadata_bgc],
            db,
            outdir_parquet,
            "./schemas/"
        )

        conv_elapsed_time = time.time() - conv_start_time
        print("Conversion elapsed time: " + str(conv_elapsed_time))

    elapsed_time = time.time() - start_time
    print("Total elapsed time: " + str(elapsed_time))

##########################################################################

if __name__ == "__main__":
    argogdac2parquet()
