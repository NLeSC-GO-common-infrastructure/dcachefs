import fsspec

from dcachefs import dCacheFileSystem


def test_dcachefs_is_registered_as_dcache_protocol():
    assert fsspec.get_filesystem_class('dcache') == dCacheFileSystem
    assert isinstance(fsspec.filesystem('dcache'), dCacheFileSystem)
