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
import importlib.metadata
##########################################################################

def main():

    # Ensure stdout and stderr are unbuffered
    sys.stdout = open(sys.stdout.fileno(), 'w', buffering=1)
    sys.stderr = open(sys.stderr.fileno(), 'w', buffering=1)

    parser = argparse.ArgumentParser(description='Package to create parquet copy of Argo databases.')
    parser.add_argument('--version', action='store_true', help="Show version and exit")

    args = parser.parse_args()

    if args.version:
        try:
            version = importlib.metadata.version('crocolaketools')
            print(f"crocolaketools version: {version}")
        except importlib.metadata.PackageNotFoundError:
            print("crocolaketools version: not installed")

    else:
        parser.print_help()

##########################################################################

if __name__ == "__main__":
    main()
