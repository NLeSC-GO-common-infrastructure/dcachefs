import contextlib
import fsspec

from .dcachefs import dCacheFileSystem


@contextlib.contextmanager
def register_implementation(protocol='https'):
    fsspec.register_implementation(protocol, dCacheFileSystem, clobber=True)
    try:
        yield
    finally:
        fsspec.registry.target.pop(protocol)
