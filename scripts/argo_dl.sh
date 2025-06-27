#!/usr/bin/env sh

download_argo_gdac \
    --db phy \
    --tstart 2021-04-01T00:00:00Z \
    --tend 2021-04-02T00:00:00Z \
    --checktime \
    --verbose \
    --nproc 4 \
    --lat [15,60] \
    --lon [-85,-30]

download_argo_gdac \
    --db bgc \
    --tstart 2024-10-31T00:00:00Z \
    --tend 2024-11-01T00:00:00Z \
    --checktime \
    --verbose \
    --nproc 4 \
    --lat [15,60] \
    --lon [-85,-30]

download_argo_gdac \
    --db phy \
    --floats 4903798 \
    --checktime \
    --verbose \
    --nproc 1

download_argo_gdac \
    --db bgc \
    --floats 4903798 \
    --checktime \
    --verbose \
    --nproc 1


# --float 1901730 \
