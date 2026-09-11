#!/usr/bin/env python3

"""Registry of golden-test targets for tests/test_golden.py.

Add an entry to registry when a new converter is created.

Each entry names a (converter, db_type) pair already exercised by
tests/fixtures/, the dask_cluster.yaml key to use for its dask_client, and
which output columns are derived via gsw (compared with
numpy.testing.assert_allclose(atol=1e-10, rtol=1e-10)) rather than exact match.

The three gsw-computed columns (ABS_SAL_COMPUTED/CONSERVATIVE_TEMP_COMPUTED/
SIGMA1_COMPUTED, added by Converter.compute_derived_variables) and PRES (when
computed from DEPTH with gsw) are checked with tolerance; golden tests should be
run with add_derived_vars = True in datasets.yaml when possible (exception eg:
OleanderXBT has no salinity measurement and derived variable cannot be
estimated)

"""

from typing import NamedTuple, List, Optional, Type

from crocolaketools.converter.converterArgoQC import ConverterArgoQC
from crocolaketools.converter.converterGLODAP import ConverterGLODAP
from crocolaketools.converter.converterSprayGliders import ConverterSprayGliders
from crocolaketools.converter.converterSaildrones import ConverterSaildrones
from crocolaketools.converter.converterOleanderXBT import ConverterOleanderXBT

GSW_COLUMNS = ["ABS_SAL_COMPUTED", "CONSERVATIVE_TEMP_COMPUTED", "SIGMA1_COMPUTED"]


class GoldenTarget(NamedTuple):
    name: str                  # tests/golden/<name>/ directory
    converter_cls: Type
    db_type: str                # "PHY" or "BGC"
    cluster_key: str            # key into tests/config/dask_cluster.yaml
    tolerant_columns: List[str] # compared with atol=1e-10, rtol=1e-10; all others exact
    # SprayGliders only: it has no convert() override, so the base class's
    # generic convert() would try to read straight from tmp_path -- it needs
    # prepare_data() run first to chunk input_path's files into tmp_path, then
    # convert(filenames=<chunk files>). Set to the chunk_profile to use when
    # this two-phase flow is required; leave None everywhere else.
    chunk_profile: Optional[int] = None
    # Alias to run the same converter under a different cluster_key and compare
    # against another entry's golden files; golden_name points at the directory
    # under tests/golden/ to compare against.
    golden_name: Optional[str] = None


GOLDEN_REGISTRY = [
    GoldenTarget("ARGO-QC_PHY", ConverterArgoQC, "PHY", "TESTS", GSW_COLUMNS),
    GoldenTarget("ARGO-QC_BGC", ConverterArgoQC, "BGC", "TESTS", GSW_COLUMNS),
    GoldenTarget("GLODAP_PHY", ConverterGLODAP, "PHY", "TESTS", GSW_COLUMNS),
    GoldenTarget("GLODAP_BGC", ConverterGLODAP, "BGC", "TESTS", GSW_COLUMNS),
    GoldenTarget("SPRAY_PHY", ConverterSprayGliders, "PHY", "TESTS", ["PRES"] + GSW_COLUMNS, chunk_profile=20),
    GoldenTarget("SPRAY_BGC", ConverterSprayGliders, "BGC", "TESTS", ["PRES"] + GSW_COLUMNS, chunk_profile=20),
    GoldenTarget("Saildrones_PHY", ConverterSaildrones, "PHY", "TESTS", ["PRES"] + GSW_COLUMNS),
    GoldenTarget("Saildrones_BGC", ConverterSaildrones, "BGC", "TESTS", ["PRES"] + GSW_COLUMNS),
    GoldenTarget("OleanderXBT_PHY", ConverterOleanderXBT, "PHY", "TESTS", ["PRES"]),

    # Golden tests run on different multiworker and multithread configurations
    GoldenTarget("Saildrones_BGC@2x2", ConverterSaildrones, "BGC", "TESTS_MULTIWORKER",
                 ["PRES"] + GSW_COLUMNS, golden_name="Saildrones_BGC"),
    GoldenTarget("ARGO-QC_BGC@2x2", ConverterArgoQC, "BGC", "TESTS_MULTIWORKER",
                 GSW_COLUMNS, golden_name="ARGO-QC_BGC"),
]


class DataTarget(NamedTuple):
    """A parquet dataset tests/test_data.py reads.

    Unlike GoldenTarget, output goes to the datasets.yaml-declared outdir_pq
    (tests/fixtures/parquet/, gitignored) because test_data.py resolves its
    paths from datasets.yaml. conftest.generated_parquet builds all of these
    once per session, so CI has them and a local run never tests stale output.
    """
    config_key: str                 # crocolaketools/config/datasets.yaml key
    db_type: str                    # "PHY" or "BGC"
    converter_cls: Optional[Type]   # None: built with daskTools, not a Converter
    chunk_profile: Optional[int] = None  # see GoldenTarget.chunk_profile
    nc_suffix: Optional[str] = None      # daskTools targets only


DATA_REGISTRY = [
    DataTarget("ARGO_PHY", "PHY", ConverterArgoQC),
    DataTarget("ARGO_BGC", "BGC", ConverterArgoQC),
    DataTarget("ARGO-GDAC_PHY", "PHY", None, nc_suffix="_prof.nc"),
    DataTarget("ARGO-GDAC_BGC", "BGC", None, nc_suffix="_Sprof.nc"),
    DataTarget("GLODAP_PHY", "PHY", ConverterGLODAP),
    DataTarget("GLODAP_BGC", "BGC", ConverterGLODAP),
    DataTarget("SprayGliders_PHY", "PHY", ConverterSprayGliders, chunk_profile=20),
    DataTarget("SprayGliders_BGC", "BGC", ConverterSprayGliders, chunk_profile=20),
    DataTarget("Saildrones_PHY", "PHY", ConverterSaildrones),
    DataTarget("Saildrones_BGC", "BGC", ConverterSaildrones),
    DataTarget("OleanderXBT_PHY", "PHY", ConverterOleanderXBT),
]
