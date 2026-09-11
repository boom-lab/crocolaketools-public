#!/usr/bin/env python3

"""Shared pytest fixtures for crocolaketools test suite.

This module provides common fixtures used across all test categories.
"""

import os
from pathlib import Path
from typing import Dict, Any

import pytest
import yaml
from dask.distributed import Client

TEST_CONFIG_DIR = Path(__file__).parent / "config"
TEST_CONFIG_CLUSTER_FILE = TEST_CONFIG_DIR / "dask_cluster.yaml"

# Set before any test module reads configuration, and unconditionally: a
# CROCOLAKE_CONFIG_DIR exported in the developer's shell would otherwise run the
# suite, golden tests included, against that data.
os.environ["CROCOLAKE_CONFIG_DIR"] = str(TEST_CONFIG_DIR)

import crocolaketools.config.config_paths as cfgp  # noqa: E402

# ============================================================================
# Dask Fixtures
# ============================================================================

@pytest.fixture
def dask_client(request):
    """Client built from the named key's settings in
    tests/config/dask_cluster.yaml (small, CI-safe settings -- not
    production's crocolaketools/config/dask_cluster.yaml).

    Indirect fixture: parametrize with the dask_cluster.yaml key to use,
    e.g. @pytest.mark.parametrize("dask_client", ["TESTS"], indirect=True)
    """
    config_cluster = cfgp.get_config_cluster_db_dict(request.param, config_file=TEST_CONFIG_CLUSTER_FILE)
    client = Client(**config_cluster)
    yield client
    client.close()

# ============================================================================
# Network guardrail
# ============================================================================

def pytest_collection_modifyitems(config, items):
    """Fail any downloader test that opens a socket.

    The tests in test_downloader* are meant to be fully mocked and should never
    reach the network, so a socket here means a mock missed the method the code
    actually calls. Fix the mock, do not allow the socket. Scoped by filename.

    """
    for item in items:
        if item.path.name.startswith("test_downloader"):
            item.add_marker(pytest.mark.disable_socket)

# ============================================================================
# Path Fixtures
# ============================================================================

def pytest_addoption(parser):
    """Add custom command-line flag for golden tests."""
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Update the golden reference files with current test outputs",
    )

@pytest.fixture
def update_golden(request):
    """Fixture to check if the --update-golden flag is present."""
    return request.config.getoption("--update-golden")

# ============================================================================
# Generated parquet fixtures
# ============================================================================

@pytest.fixture(scope="session")
def generated_parquet(tmp_path_factory):
    """Build every parquet dataset in DATA_REGISTRY into its datasets.yaml
    outdir_pq, once per session.

    tests/fixtures/parquet/ is gitignored converter output, so CI has none of
    it and test_data.py fails with FileNotFoundError; locally it may predate
    the converter code under test. Generating it here makes both cases the
    same run.
    """
    import shutil

    from dask.distributed import Lock

    from crocolaketools.config.config_paths import get_config_paths_field
    from crocolaketools.converter.converterArgoGDAC import ConverterArgoGDAC
    from tests.golden_registry import DATA_REGISTRY

    gdac_dac_dir = Path(__file__).parent / "fixtures" / "demo_ARGO_GDAC" / "GDAC" / "dac"
    # convert_dask_tools regenerates the schema rather than reading the
    # committed one; test_golden_schema is what checks the two agree
    schemas_dir = tmp_path_factory.mktemp("gdac_schemas")

    config_cluster = cfgp.get_config_cluster_db_dict("TESTS", config_file=TEST_CONFIG_CLUSTER_FILE)
    client = Client(**config_cluster)
    try:
        for target in DATA_REGISTRY:
            if target.converter_cls is None:
                # ARGO-GDAC has no Converter subclass: it is the raw netCDF ->
                # PQT stage, reached through ConverterArgoGDAC (which is what
                # production runs) rather than the daskTools it wraps.
                out_dir = Path(str(get_config_paths_field(target.config_key, "outdir_pq")))
                if out_dir.exists():
                    shutil.rmtree(out_dir)
                out_dir.mkdir(parents=True)
                flist = sorted(str(p) for p in gdac_dac_dir.glob(f"*/*/*{target.nc_suffix}"))
                assert flist, f"no {target.nc_suffix} files under {gdac_dac_dir}"
                flists = ([flist, []] if target.db_type == "PHY" else [[], flist])
                ConverterArgoGDAC.convert_dask_tools(
                    flists,
                    [[], []],
                    [target.db_type],
                    out_dir,
                    schemas_dir,
                    client=client,
                )
                continue

            converter = target.converter_cls(db_type=target.db_type)
            if target.chunk_profile is None:
                converter.convert()
            else:
                # prepare_data refuses to run into an existing tmp_path
                if converter.tmp_path.is_dir():
                    shutil.rmtree(converter.tmp_path)
                input_files = [f.name for f in converter.input_path.glob("*.nc")]
                converter.prepare_data(flist=input_files, lock=Lock(), chunk_profile=target.chunk_profile)
                chunk_files = sorted(f.name for f in converter.tmp_path.glob("*.nc"))
                converter.convert(filenames=chunk_files)
    finally:
        client.close()
