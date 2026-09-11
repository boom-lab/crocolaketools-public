#!/usr/bin/env python3

## @file glodap2parquet.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Tue 22 Apr 2025

##########################################################################
import argparse
import ast
from datetime import datetime
from warnings import simplefilter
import pandas as pd
# ignore pandas "educational" performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from crocolaketools.downloader import argo_tools as at
from pprint import pprint
##########################################################################

def main():
    parser = argparse.ArgumentParser(description='Script to download Argo GDAC data to local mirror')
    parser.add_argument(
        '--index', type=str,
        help="Path to GDAC Argo Core and Argo BGC indeces",
        required=False, default="./data/GDAC/dac/"
    )
    parser.add_argument(
        '--db', type=str,
        help="bgc or phy",
        required=False, default="phy"
    )
    parser.add_argument(
        '--lat', type=str,
        help="Latitude range as [lat0,lat1]",
        required=False, default=None
    )
    parser.add_argument(
        '--lon', type=str,
        help="Longitude range as [lon0,lon1]",
        required=False, default=None
    )
    parser.add_argument(
        '--tstart', type=str,
        help="Start date ISO 8601 format, already in UTC (e.g., '2023-10-01T15:30:00')",
        required=False, default=None
    )
    parser.add_argument(
        '--tend', type=str,
        help="End date ISO 8601 format, already in UTC (e.g., '2023-10-01T15:30:00')",
        required=False, default=None
    )
    parser.add_argument(
        '--floats', type=int,
        help="Float(s) WMO number (int or list of ints)",
        required=False, default=None
    )
    parser.add_argument(
        '--save_to', type=str,
        help="Root folder where databases will be downloaded to",
        required=False, default=None
    )
    parser.add_argument(
        '--dryrun', action='store_true',
        help="If set, no files are downloaded"
    )
    parser.add_argument(
        '--checktime', action='store_true',
        help="If set, only download files if newer than those on disk"
    )
    parser.add_argument(
        '--verbose', action='store_true',
        help="If set, print additional information"
    )
    parser.add_argument(
        '--dac_url', type=str,
        help="URL of the GDAC server",
        required=False, default="https://usgodae.org/pub/outgoing/argo/dac/"
    )
    parser.add_argument(
        '--overwrite', action='store_true',
        help="If set, overwrite existing files"
    )
    parser.add_argument(
        '--nproc', type=int,
        help="Number of processes to use",
        required=False, default=1
    )

    args = parser.parse_args()

    # Parse the lat and lot ranges
    def parse_range(coords_range):
        if coords_range is not None:
            try:
                coords_range = ast.literal_eval(coords_range)
            except ValueError as e:
                print(f"Invalid lat or lon format: {e}")
        return coords_range

    args.lat = parse_range(args.lat)
    args.lon = parse_range(args.lon)

    # Parse the datetime string into a datetime object
    def parse_time(time):
        if time is not None:
            try:
                time = datetime.fromisoformat(time)
                time = pd.Timestamp(time).tz_localize(None)
            except ValueError as e:
                print(f"Invalid datetime format: {e}")
        return time

    t0 = parse_time(args.tstart)
    t1 = parse_time(args.tend)

    if args.save_to is None:
        args.save_to = args.index

    config = {
        'index': args.index,
        'db': args.db,
        'lat': args.lat,
        'lon': args.lon,
        'floats': args.floats,
        'tstart': t0,
        'tend': t1,
        'save_to': args.save_to,
        'dryrun': args.dryrun,
        'checktime': args.checktime,
        'verbose': args.verbose,
        'dac_url': args.dac_url,
        'overwrite': args.overwrite,
        'nproc': args.nproc
    }

    print("Calling argo_gdac with the following configuration:")
    pprint(config)

    wmos, _, wmos_fp = at.argo_gdac(
        gdac_path=config['index'],
        dataset=config['db'],
        lat_range=config['lat'],
        lon_range=config['lon'],
        start_date=config['tstart'],
        end_date=config['tend'],
        floats=config['floats'],
        save_to=config['save_to'],
        download_individual_profs=False,
        skip_downloads=False,
        dryrun=config['dryrun'],
        overwrite_profiles=config['overwrite'],
        NPROC=config['nproc'],
        verbose=config['verbose'],
        checktime=config['checktime'],
        dac_url_root=config['dac_url'],
    )

    print()
    print("Downloaded files:")
    pprint(wmos_fp)

    pprint("Total files to be downloaded: " + str(len(wmos_fp)))

##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("download_argo_gdac.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
