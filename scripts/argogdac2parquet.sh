#!/usr/bin/env sh

DB_NAME="BGC_ARGO-GDAC"
OUT_PATH="./data/parquet/${DB_NAME}/$(date +%Y-%m-%d)/"

## executing code
argogdac2parquet \
    --db bgc \
    --db_parquet "$OUT_PATH" \
    --gdac_index "./data/GDAC/dac/" \
    --db_nc "./data/GDAC/dac/" \
    --download "false"
