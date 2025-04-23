#!/usr/bin/env sh

DB_NAME="PHY_ARGO-QC"
IN_PATH="./data/parquet/PHY_ARGO-GDAC/2025-04-23/"
OUT_PATH="./data/parquet/${DB_NAME}/$(date +%Y-%m-%d)/"

## executing code
argo2argoqc_phy \
    -i "$IN_PATH" \
    -o "$OUT_PATH"
