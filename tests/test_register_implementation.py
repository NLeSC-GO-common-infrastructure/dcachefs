import fsspec
from fsspec.implementations.http import HTTPFileSystem

from dcachefs import register_implementation, dCacheFileSystem


def test_register_implementation():
    with register_implementation():
        assert fsspec.get_filesystem_class('https') == dCacheFileSystem
    assert fsspec.get_filesystem_class('https') == HTTPFileSystem
