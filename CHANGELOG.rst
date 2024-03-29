###########
Change Log
###########

All notable changes to this project will be documented in this file.
This project adheres to `Semantic Versioning <http://semver.org/>`_.


[Unreleased]

[0.1.7]

Added
-----

* fsspec GUI via panel now tested

Changed
-------

* register_implementation is no longer supported (use only "dcache" protocol)

Fixed
-----

* implements encode_url to solve compatibility issue with HTTPFileSystem

[0.1.6]

Added
-----
* added abstract to CITATION.cff
* included README file in documentation
* improved description in README file

Changed
-------
* updated .zenodo.json
* updated notebook for documentation


[0.1.5]

Added
-----
* added request_kwargs to provide specific arguments to request calls
* added pipe_file method to dCacheFileSystem, which enables the usage in fsspec.get_mapper 

Fixed
-----
* Fixed naming collision that prevented timeout to be set on ClientSessions or requests


[0.1.4]

Added
-----
* dCacheFS registers itself as fsspec implementation for "dcache" protocol

[0.1.3]

Fixed
-----
* adapt to changes in AsyncFileSystem in fsspec version 0.9.0

[0.1.1]

Changed
-------
* block_size is an argument for the filesystem initialization
* when opening a file in stream mode, dcache API url is not needed

Fixed
-----
* bug in recursive remove 

[0.1.0]

Added
-----

* Empty Python project directory structure
