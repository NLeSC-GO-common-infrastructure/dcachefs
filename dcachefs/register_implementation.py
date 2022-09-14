import contextlib
import fsspec

from .dcachefs import dCacheFileSystem


@contextlib.contextmanager
def register_implementation(protocol='https'):
    """
    Register dCacheFileSystem as fsspec backend.

    :param protocol: (str) URLs with this protocol will be open using
        dCacheFileSystem by fsspec
    """
    fsspec.register_implementation(protocol, dCacheFileSystem, clobber=True)
    try:
        yield
    finally:
        fsspec.registry.target.pop(protocol)
