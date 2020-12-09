import fsspec

from contextlib import contextmanager
from copy import deepcopy
from fsspec import register_implementation as _register_implementation

from .dcachefs import dCacheFileSystem


@contextmanager
def register_implementation():
    _registry = deepcopy(fsspec.registry)
    _register_implementation('https', dCacheFileSystem, clobber=True)
    try:
        yield
    finally:
        fsspec.registry = _registry
