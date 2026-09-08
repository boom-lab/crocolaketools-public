#!/usr/bin/env python3

## @file parquet_filters.py
#
#  Helpers to build parquet filters that dask/pyarrow can evaluate exactly
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Mon 07 Sep 2026

##########################################################################
import datetime

import numpy as np
import pandas as pd
##########################################################################

def _to_datetime64_ns(value):
    """Rebuild a datetime-like filter value as a nanosecond numpy datetime64.

    pyarrow infers microsecond resolution from a pandas Timestamp or a python
    datetime, so a predicate built from one silently matches nothing against a
    timestamp[ns] column whose value has a sub-microsecond part (e.g. JULD
    2006-12-06 20:39:14.999995904 is truncated to ...999995).
    """

    if isinstance(value, (list, tuple, set)):  # "in" / "not in" operators
        return type(value)(_to_datetime64_ns(v) for v in value)

    if not isinstance(value, (pd.Timestamp, np.datetime64, datetime.datetime)):
        return value
    if pd.isna(value):
        return value

    return np.datetime64(pd.Timestamp(value).value, "ns")

#------------------------------------------------------------------------------#
def normalize_filters(filters):
    """Make filters exact on nanosecond timestamps.

    Arguments:
    filters -- a filter compatible with dask.dataframe.read_parquet(), i.e. a
               list of tuples or a list of lists of tuples

    Returns:
    filters -- the same filters with every datetime-like value rebuilt at
               nanosecond resolution
    """

    if not filters:
        return filters

    if isinstance(filters[0], tuple):
        return [(col, op, _to_datetime64_ns(val)) for col, op, val in filters]

    return [
        [(col, op, _to_datetime64_ns(val)) for col, op, val in group]
        for group in filters
    ]
