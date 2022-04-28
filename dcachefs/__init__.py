import logging

import fsspec

from .__version__ import __version__
from .dcachefs import dCacheFileSystem
from .register_implementation import register_implementation

logging.getLogger(__name__).addHandler(logging.NullHandler())

__author__ = "Francesco Nattino"
__email__ = 'f.nattino@esciencecenter.nl'

fsspec.register_implementation("dcache", dCacheFileSystem, clobber=True)
