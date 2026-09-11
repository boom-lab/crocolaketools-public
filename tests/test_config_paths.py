#!/usr/bin/env python3

## @file test_config_paths.py
#
# Tests for the config/path resolution layer.
#
## @date Mon 07 Sep 2026

##########################################################################
import os
from pathlib import Path

import pytest

import crocolaketools.config.config_paths as cfgp
from crocolaketools.downloader.downloader import Downloader

TEST_CONFIG_CLUSTER_FILE = Path(__file__).parent / "config" / "dask_cluster.yaml"

# a db/db_type pair that exists in the packaged datasets.yaml
DB = "GLODAP"
DB_TYPE = "PHY"
DB_KEY = f"{DB}_{DB_TYPE}"
##########################################################################


class TestConfigPaths:
    """Tests for crocolaketools/config/config_paths.py"""

    def test_config_path_is_the_packaged_config_dir(self):
        """get_config_path() points at the installed config package."""
        base = Path(str(cfgp.get_config_path()))
        assert base.is_dir()
        assert (base / "datasets.yaml").is_file()

    def test_config_paths_file(self):
        """get_config_paths_file() resolves to datasets.yaml inside it."""
        path = Path(str(cfgp.get_config_paths_file()))
        assert path.name == "datasets.yaml"
        assert path.is_file()

    def test_db_dict_has_required_keys(self):
        """A db block carries at least db, db_type and input_path."""
        cfg = cfgp.get_config_paths_db_dict(DB_KEY)
        assert cfg["db"] == DB
        assert cfg["db_type"] == DB_TYPE
        assert "input_path" in cfg

    def test_db_dict_unknown_key_raises(self):
        """An unknown db key raises KeyError rather than returning a default."""
        with pytest.raises(KeyError):
            cfgp.get_config_paths_db_dict("NOT_A_DB_PHY")

    def test_field_resolves_against_the_config_dir(self):
        """get_config_paths_field() joins the relative value onto the config dir.

        The result is absolute and normalised: datasets.yaml's values are written
        relative to the config dir ("../../tests/fixtures/..."), and the ".."
        segments are collapsed so consumers can compare paths by equality.
        """
        raw = cfgp.get_config_paths_db_dict(DB_KEY)["input_path"]
        resolved = cfgp.get_config_paths_field(DB_KEY, "input_path")
        assert resolved == Path(os.path.abspath(cfgp.get_config_path() / raw))
        assert resolved.is_absolute()
        assert ".." not in resolved.parts

    def test_absolute_field_is_left_alone(self):
        """An absolute config value is not joined onto the config dir.

        This is what lets a site config name paths outside the package.
        """
        assert cfgp.resolve_config_path("/srv/crocolake/phy") == Path("/srv/crocolake/phy")

    def test_symlinks_are_not_resolved(self, tmp_path):
        """Resolution normalises but does not follow symlinks.

        Published datasets are addressed through a `current` symlink; resolving
        it would record whichever snapshot it points at today.
        """
        real = tmp_path / "snapshots" / "2026-09-09"
        real.mkdir(parents=True)
        link = tmp_path / "current"
        link.symlink_to(real)
        assert cfgp.resolve_config_path(link) == link

    def test_cluster_file(self):
        """get_config_cluster_file() resolves to dask_cluster.yaml."""
        path = Path(str(cfgp.get_config_cluster_file()))
        assert path.name == "dask_cluster.yaml"
        assert path.is_file()

    def test_package_ships_templates_and_no_loadable_config(self):
        """Only *.example.yaml ship, so there is nothing to load implicitly."""
        pkg = cfgp.get_packaged_config_path()
        for name in cfgp.REQUIRED_CONFIG_FILES:
            assert not (pkg / name).exists(), f"{name} must not ship; use {name[:-5]}.example.yaml"
            assert (pkg / f"{name[:-5]}.example.yaml").is_file()

    def test_templates_carry_no_relative_paths(self):
        """Template paths are absolute placeholders.

        A relative path in a template would resolve against wherever the user
        copied it and appear to work; /path/to/... cannot be mistaken for a
        real location.
        """
        import yaml as _yaml
        path_fields = {
            "input_path", "outdir_pq", "outdir_schema", "tmp_path",
            "ln_path", "download_path",
        }
        cfg = _yaml.safe_load(
            (cfgp.get_packaged_config_path() / "datasets.example.yaml").read_text()
        )
        found = [
            (db, field, value)
            for db, fields in cfg.items()
            for field, value in (fields or {}).items()
            if field in path_fields and isinstance(value, str)
        ]
        assert found, "no path fields found -- have they been renamed?"
        for db, field, value in found:
            assert value.startswith("/"), f"{db}.{field} is relative: {value}"


class TestDatasetFields:
    """Invariants the config files must hold for the shell layer to work."""

    @staticmethod
    def _linked_datasets():
        """Datasets that generate_crocolake_symlinks.sh links into CrocoLake."""
        import yaml as _yaml
        cfg = _yaml.safe_load(cfgp.get_config_paths_file().read_text())
        return {
            k: v for k, v in cfg.items()
            if isinstance(v, dict) and "outdir_pq" in v and not k.startswith("CROCOLAKE_")
        }

    def test_every_linked_dataset_declares_a_codename(self):
        """db_codename names the symlink, so CrocoLakeLoader can find it."""
        missing = [k for k, v in self._linked_datasets().items() if not v.get("db_codename")]
        assert not missing, f"db_codename missing for {missing}"

    def test_codenames_are_unique_per_db_type(self):
        """Two datasets of one type cannot claim the same link name."""
        from collections import Counter
        for db_type in ("PHY", "BGC"):
            names = [v["db_codename"] for v in self._linked_datasets().values()
                     if v.get("db_type") == db_type]
            clashes = [n for n, c in Counter(names).items() if c > 1]
            assert not clashes, f"{db_type}: duplicate db_codename {clashes}"

    def test_template_declares_the_same_dataset_keys(self):
        """The template must not drift from the config the suite runs against."""
        import yaml as _yaml
        template = _yaml.safe_load(
            (cfgp.get_packaged_config_path() / "datasets.example.yaml").read_text()
        )
        assert set(template) == set(
            _yaml.safe_load(cfgp.get_config_paths_file().read_text())
        )


class TestConfigDirEnvVar:
    """CROCOLAKE_CONFIG_DIR resolution."""

    @staticmethod
    def _write_config_dir(path, cluster=True, paths_yaml=True):
        path.mkdir(parents=True, exist_ok=True)
        if paths_yaml:
            (path / "datasets.yaml").write_text(
                "GLODAP_PHY:\n"
                "  db: GLODAP\n"
                "  db_type: PHY\n"
                "  input_path: /srv/site/in\n"
                "  outdir_pq: relative/out\n"
            )
        if cluster:
            (path / "dask_cluster.yaml").write_text(
                "GLODAP:\n  n_workers: 3\n  threads_per_worker: 1\n"
            )
        return path

    def test_unset_raises(self, monkeypatch):
        """There is no default. The package ships templates, not a config."""
        monkeypatch.delenv(cfgp.CONFIG_DIR_ENV_VAR, raising=False)
        with pytest.raises(cfgp.ConfigDirError, match="is not set"):
            cfgp.get_config_path()

    def test_set_directory_wins(self, monkeypatch, tmp_path):
        site = self._write_config_dir(tmp_path / "site")
        monkeypatch.setenv(cfgp.CONFIG_DIR_ENV_VAR, str(site))
        assert cfgp.get_config_path() == site
        assert cfgp.get_config_paths_db_dict("GLODAP_PHY")["db"] == "GLODAP"

    def test_paths_resolve_against_the_site_dir(self, monkeypatch, tmp_path):
        """Absolute site values pass through; relative ones join the site dir."""
        site = self._write_config_dir(tmp_path / "site")
        monkeypatch.setenv(cfgp.CONFIG_DIR_ENV_VAR, str(site))
        assert cfgp.get_config_paths_field("GLODAP_PHY", "input_path") == Path("/srv/site/in")
        assert cfgp.get_config_paths_field("GLODAP_PHY", "outdir_pq") == site / "relative/out"

    def test_missing_directory_raises(self, monkeypatch, tmp_path):
        """A set-but-broken value is fatal, not a reason to use a default."""
        monkeypatch.setenv(cfgp.CONFIG_DIR_ENV_VAR, str(tmp_path / "not-mounted"))
        with pytest.raises(cfgp.ConfigDirError, match="not a directory"):
            cfgp.get_config_path()

    def test_directory_without_cluster_config_raises(self, monkeypatch, tmp_path):
        """A directory must supply both files, so paths and cluster sizing
        always come from the same place."""
        site = self._write_config_dir(tmp_path / "half", cluster=False)
        monkeypatch.setenv(cfgp.CONFIG_DIR_ENV_VAR, str(site))
        with pytest.raises(cfgp.ConfigDirError, match="dask_cluster.yaml"):
            cfgp.get_config_path()

    def test_directory_without_paths_config_raises(self, monkeypatch, tmp_path):
        site = self._write_config_dir(tmp_path / "half2", paths_yaml=False)
        monkeypatch.setenv(cfgp.CONFIG_DIR_ENV_VAR, str(site))
        with pytest.raises(cfgp.ConfigDirError, match="datasets.yaml"):
            cfgp.get_config_path()

    def test_empty_value_is_treated_as_unset(self, monkeypatch):
        """An exported-but-empty variable is a shell artefact, not a config."""
        monkeypatch.setenv(cfgp.CONFIG_DIR_ENV_VAR, "")
        with pytest.raises(cfgp.ConfigDirError, match="is not set"):
            cfgp.get_config_path()

    def test_user_home_is_expanded(self, monkeypatch, tmp_path):
        site = self._write_config_dir(tmp_path / "home" / "cfg")
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.setenv(cfgp.CONFIG_DIR_ENV_VAR, "~/cfg")
        assert cfgp.get_config_path() == site


class TestConfigDirArgument:
    """--config-dir, the alternative to exporting the variable."""

    @staticmethod
    def _parse(argv):
        import argparse
        parser = argparse.ArgumentParser()
        cfgp.add_config_dir_argument(parser)
        args = parser.parse_args(argv)
        cfgp.apply_config_dir_argument(args)
        return args

    def test_flag_sets_the_config_dir(self, monkeypatch, tmp_path):
        site = TestConfigDirEnvVar._write_config_dir(tmp_path / "site")
        monkeypatch.delenv(cfgp.CONFIG_DIR_ENV_VAR, raising=False)
        self._parse(["--config-dir", str(site)])
        assert cfgp.get_config_path() == site

    def test_flag_wins_over_the_variable(self, monkeypatch, tmp_path):
        exported = TestConfigDirEnvVar._write_config_dir(tmp_path / "exported")
        asked_for = TestConfigDirEnvVar._write_config_dir(tmp_path / "asked-for")
        monkeypatch.setenv(cfgp.CONFIG_DIR_ENV_VAR, str(exported))
        self._parse(["--config-dir", str(asked_for)])
        assert cfgp.get_config_path() == asked_for

    def test_absent_flag_leaves_the_variable_alone(self, monkeypatch, tmp_path):
        exported = TestConfigDirEnvVar._write_config_dir(tmp_path / "exported")
        monkeypatch.setenv(cfgp.CONFIG_DIR_ENV_VAR, str(exported))
        self._parse([])
        assert cfgp.get_config_path() == exported

    def test_flag_is_stored_absolute(self, monkeypatch, tmp_path):
        """A relative --config-dir must survive a child process's cwd.

        The scripts shell out to parse_yaml_argo_gdac.sh and friends, which may
        run from anywhere.
        """
        TestConfigDirEnvVar._write_config_dir(tmp_path / "site")
        monkeypatch.delenv(cfgp.CONFIG_DIR_ENV_VAR, raising=False)
        monkeypatch.chdir(tmp_path)
        self._parse(["--config-dir", "site"])
        stored = os.environ[cfgp.CONFIG_DIR_ENV_VAR]
        assert Path(stored).is_absolute()
        assert Path(stored) == tmp_path / "site"

    def test_flag_expands_user_home(self, monkeypatch, tmp_path):
        site = TestConfigDirEnvVar._write_config_dir(tmp_path / "home" / "cfg")
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.delenv(cfgp.CONFIG_DIR_ENV_VAR, raising=False)
        self._parse(["--config-dir", "~/cfg"])
        assert cfgp.get_config_path() == site

    def test_flag_is_validated_like_the_variable(self, monkeypatch, tmp_path):
        monkeypatch.delenv(cfgp.CONFIG_DIR_ENV_VAR, raising=False)
        self._parse(["--config-dir", str(tmp_path / "nope")])
        with pytest.raises(cfgp.ConfigDirError):
            cfgp.get_config_path()


class TestSuiteIsPinnedToTestConfig:
    """The suite reads tests/config/, whatever the shell exports."""

    def test_env_var_points_at_the_test_config_dir(self):
        from tests.conftest import TEST_CONFIG_DIR
        assert os.environ[cfgp.CONFIG_DIR_ENV_VAR] == str(TEST_CONFIG_DIR)
        assert cfgp.get_config_path() == TEST_CONFIG_DIR

    def test_fixture_tree_is_reachable_through_config(self):
        """The test config points at the committed fixtures."""
        fixtures = Path(__file__).parent / "fixtures"
        resolved = cfgp.get_config_paths_field("GLODAP_PHY", "input_path")
        assert resolved.is_relative_to(fixtures)
        assert resolved.is_dir()


class TestConfigPathsCluster:
    def test_cluster_db_dict_default_file(self):
        """With no config_file, the packaged dask_cluster.yaml is read."""
        cfg = cfgp.get_config_cluster_db_dict("GLODAP")
        assert "n_workers" in cfg
        assert "threads_per_worker" in cfg

    def test_cluster_db_dict_explicit_file(self):
        """An explicit config_file overrides the packaged one."""
        cfg = cfgp.get_config_cluster_db_dict(
            "TESTS", config_file=TEST_CONFIG_CLUSTER_FILE
        )
        assert cfg["n_workers"] == 1
        assert cfg["processes"] is True


class TestDownloaderConfigResolution:
    """Tests for Downloader.__init__ -- config merge and path normalisation."""

    def test_no_config_raises(self):
        """A missing config is rejected explicitly."""
        with pytest.raises(ValueError, match="No config argument"):
            Downloader()

    def test_unknown_db_raises(self):
        """An unknown db/db_type pair fails on the datasets.yaml lookup."""
        with pytest.raises(KeyError):
            Downloader(config={"db": "NOT_A_DB", "db_type": "PHY"})

    def test_input_path_is_an_absolute_path_object(self, tmp_path):
        """input_path is a resolved, absolute Path -- never a string."""
        target = tmp_path / "original"
        d = Downloader(config={
            "db": DB,
            "db_type": DB_TYPE,
            "input_path": str(target),
        })
        assert isinstance(d.input_path, Path)
        assert d.input_path.is_absolute()
        assert d.input_path == target

    def test_input_path_directory_is_created(self, tmp_path):
        """The destination directory is created if absent."""
        target = tmp_path / "does" / "not" / "exist"
        assert not target.exists()
        Downloader(config={
            "db": DB,
            "db_type": DB_TYPE,
            "input_path": str(target),
        })
        assert target.is_dir()

    def test_db_and_db_type_are_stored(self, tmp_path):
        """db and db_type land on the instance, db_type upper-cased.

        A lower-case db_type also trips the mismatch warning, because the
        constructor compares the user's raw value against datasets.yaml's
        after upper-casing only its own copy.
        """
        with pytest.warns(UserWarning, match="not matching at key db_type"):
            d = Downloader(config={
                "db": DB,
                "db_type": DB_TYPE.lower(),
                "input_path": str(tmp_path),
            })
        assert d.db == DB
        assert d.db_type == DB_TYPE

    def test_absent_keys_are_filled_from_config_yaml(self, tmp_path):
        """Keys the user omits are read from the db's datasets.yaml block."""
        disk = cfgp.get_config_paths_db_dict(DB_KEY)
        config = {"db": DB, "db_type": DB_TYPE, "input_path": str(tmp_path)}
        d = Downloader(config=config)

        # GLODAP_PHY declares overwrite in datasets.yaml; the user did not
        assert "overwrite" in disk
        assert d.overwrite == disk["overwrite"]
        # the merge writes back into the caller's dict -- a side effect worth
        # pinning, since it makes the constructor unsafe to call twice with
        # the same dict
        assert config["fname_pq"] == disk["fname_pq"]

    def test_user_values_win_over_config_yaml(self, tmp_path):
        """A key the user supplies is not overwritten by the disk value."""
        disk = cfgp.get_config_paths_db_dict(DB_KEY)
        assert disk["overwrite"] is True, "fixture assumes datasets.yaml sets True"
        d = Downloader(config={
            "db": DB,
            "db_type": DB_TYPE,
            "input_path": str(tmp_path),
            "overwrite": False,
        })
        assert d.overwrite is False

    def test_download_option_defaults(self, tmp_path):
        """Options absent from both user config and datasets.yaml take defaults."""
        disk = cfgp.get_config_paths_db_dict(DB_KEY)
        assert "num_threads" not in disk and "dryrun" not in disk
        d = Downloader(config={
            "db": DB,
            "db_type": DB_TYPE,
            "input_path": str(tmp_path),
        })
        assert d.num_threads == 4
        assert d.dryrun is False


##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
