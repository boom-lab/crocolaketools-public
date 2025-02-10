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
from dask.distributed import Client
import pandas as pd
# ignore pandas "educational" performance warnings
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
from crocolaketools.converter.converterArgoQC import ConverterArgoQC
##########################################################################

def argo2argoqc_phy(argo_path,outdir_pqt,fname_pq):
    """Subset ARGO to QC-ed only data"""

    # this set up works
    # it seems that more workers or threads raises memory issues
    client = Client(
        threads_per_worker=1,
        n_workers=18,
        memory_limit='auto',
        processes=True
    )

    print("Client dashboard address: ", client.dashboard_link)
    print(client.scheduler.address)

    ConverterPHY = ConverterArgoQC(
        db = "ARGO",
        db_type="PHY",
        input_path = argo_path,
        outdir_pq = outdir_pqt,
        outdir_schema = './schemas/ArgoQC/',
        fname_pq = fname_pq,
        add_derived_vars=True
    )

    ConverterPHY.convert()

    client.shutdown()

#------------------------------------------------------------------------------#
def main():
    parser = argparse.ArgumentParser(description='Script to convert ARGO PHY database to QC-ed only data')
    parser.add_argument('-i', help="Path to physical Argo parquet database", required=True)
    parser.add_argument('-o', help="Destination path for best-quality physical Argo parquet database", required=True)
    parser.add_argument('-f', help="Basename for output files", required=False, default="1002_PHY_ARGO-QC-DEV")

    args = parser.parse_args()

    argo2argoqc_phy(args.i,args.o,args.f)

##########################################################################

if __name__ == "__main__":
    print(datetime.now())
    print()
    main()
    print("argo2argoqc_phy.py executed successfully")
    print()
    print(datetime.now())
    print(" ")
