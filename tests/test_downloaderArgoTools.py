#!/usr/bin/env python3

## @file test_downloaderArgoTools.py
#
# Tests for crocolaketools/downloader/argo_tools.py
#
## @date Thu 10 Sep 2026

##########################################################################
from unittest.mock import patch

import pytest
import requests

import gzip

from crocolaketools.downloader import argo_tools as at

#: the shared helper issues the request, so that is where it must be patched
HEAD = "crocolaketools.downloader.downloader.requests.head"
##########################################################################


class TestGetGdacUrl:
    """get_gdac_url() falls over to the next mirror."""

    @staticmethod
    def _response(status):
        return type("R", (), {"status_code": status, "ok": status < 400})()

    def test_returns_the_first_reachable_mirror(self):
        with patch(HEAD, return_value=self._response(200)) as head:
            assert at.get_gdac_url() == at.GDAC_MIRRORS[0]
        assert head.call_count == 1

    def test_falls_over_when_the_first_mirror_errors(self):
        responses = [requests.ConnectionError("down"), self._response(200)]
        with patch(HEAD, side_effect=responses):
            assert at.get_gdac_url() == at.GDAC_MIRRORS[1]

    def test_falls_over_on_an_http_error_status(self):
        responses = [self._response(503), self._response(200)]
        with patch(HEAD, side_effect=responses):
            assert at.get_gdac_url() == at.GDAC_MIRRORS[1]

    def test_raises_when_no_mirror_answers(self):
        with patch(HEAD, side_effect=requests.Timeout("slow")):
            with pytest.raises(RuntimeError, match="None of the URLs are reachable"):
                at.get_gdac_url()

    def test_tries_every_mirror_before_giving_up(self):
        with patch(HEAD, side_effect=requests.Timeout("slow")) as head:
            with pytest.raises(RuntimeError):
                at.get_gdac_url()
        assert head.call_count == len(at.GDAC_MIRRORS)

    def test_mirror_list_is_not_a_single_point_of_failure(self):
        """The whole point: more than one host, and no bare usgodae literal."""
        assert len(at.GDAC_MIRRORS) > 1
        hosts = {url.split("/")[2] for url in at.GDAC_MIRRORS}
        assert len(hosts) == len(at.GDAC_MIRRORS), f"duplicate hosts in {hosts}"

    def test_every_mirror_root_ends_with_a_slash(self):
        """Callers append 'dac/' and index filenames directly."""
        for url in at.GDAC_MIRRORS:
            assert url.endswith("/"), url


class TestDownloadFileDecodesContent:
    """download_file must write decoded bytes.

    The preferred mirror serves the profile index with
    Content-Encoding: gzip, so writing response.raw would leave a gzip stream
    on disk under a .txt name and pd.read_csv would choke on it.
    """

    def test_gzipped_response_lands_decoded(self, tmp_path):
        plain = b"# Title : Profile directory file\nfile,date\n"

        class FakeResponse:
            status_code = 200
            raw = gzip.compress(plain)

            def iter_content(self, chunk_size=None):
                yield plain

        with patch.object(at, "get_func", return_value=FakeResponse()), \
             patch.object(at, "get_time_url", return_value=None):
            at.download_file((
                "https://example.invalid/", "index.txt", tmp_path,
                True, False, False, None,
            ))

        written = (tmp_path / "index.txt").read_bytes()
        assert written == plain
        assert written[:2] != b"\x1f\x8b", "a gzip stream was written verbatim"
