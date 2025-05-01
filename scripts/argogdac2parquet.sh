#!/usr/bin/env sh

DB_NAME="PHY_ARGO-GDAC"
OUT_PATH="./data/parquet/${DB_NAME}/$(date +%Y-%m-%d)/"

## executing code
argogdac2parquet \
    --db phy \
    --db_parquet "$OUT_PATH" \
    --gdac_index "../crocolaketools/demo/demo_ARGO_GDAC/GDAC/dac/" \
    --db_nc "../crocolaketools/demo/demo_ARGO_GDAC/GDAC/dac/" \
    --download "false"
