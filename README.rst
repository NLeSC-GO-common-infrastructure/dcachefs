.. list-table::
   :widths: 25 25
   :header-rows: 1

   * - fair-software.nl recommendations
     - Badges
   * - \1. Code repository
     - |GitHub Badge|
   * - \2. License
     - |License Badge|
   * - \3. Community Registry
     - |PyPI Badge|
   * - \4. Enable Citation
     - |Zenodo Badge|
   * - \5. Checklist
     - |CII Best Practices Badge|
   * - **Other best practices**
     -
   * - Continuous integration
     - |Python Build| |PyPI Publish|


.. |GitHub Badge| image:: https://img.shields.io/badge/github-repo-000.svg?logo=github&labelColor=gray&color=blue
   :target: https://github.com/NLeSC-GO-common-infrastructure/dcachefs
   :alt: GitHub Badge

.. |License Badge| image:: https://img.shields.io/github/license/NLeSC-GO-common-infrastructure/dcachefs
   :target: https://github.com/NLeSC-GO-common-infrastructure/dcachefs
   :alt: License Badge

.. |PyPI Badge| image:: https://img.shields.io/pypi/v/dcachefs.svg?colorB=blue
   :target: https://pypi.python.org/project/dcachefs/
   :alt: PyPI Badge

.. |Zenodo Badge| image:: https://zenodo.org/badge/DOI/10.5281/zenodo.4436720.svg
   :target: https://doi.org/10.5281/zenodo.4436720
   :alt: Zenodo Badge

.. |CII Best Practices Badge| image:: https://bestpractices.coreinfrastructure.org/projects/4585/badge
   :target: https://bestpractices.coreinfrastructure.org/projects/4585
   :alt: CII Best Practices Badge

.. |Python Build| image:: https://github.com/NLeSC-GO-common-infrastructure/dcachefs/workflows/Build/badge.svg
   :target: https://github.com/NLeSC-GO-common-infrastructure/dcachefs/actions?query=workflow%3A%22Build%22
   :alt: Python Build

.. |PyPI Publish| image:: https://github.com/NLeSC-GO-common-infrastructure/dcachefs/workflows/Publish/badge.svg
   :target: https://github.com/NLeSC-GO-common-infrastructure/dcachefs/actions?query=workflow%3A%22Publish%22
   :alt: PyPI Publish

################################################################################
dCacheFS
################################################################################

dCacheFS provides a file-system interface for a `dCache storage system`_, such as the `instance provided at SURF`_. 
dCacheFS builds on the `Filesystem Spec`_ (`fsspec`) library and it can be used as an independent library or via the 
more general `fsspec` functions.

.. _dCache storage system: https://www.dcache.org
.. _instance provided at SURF: http://doc.grid.surfsara.nl/en/stable/Pages/Service/system_specifications/dcache_specs.html
.. _Filesystem Spec: https://filesystem-spec.readthedocs.io

Installation
------------

To install dcachefs, do:

.. code-block:: console
  
  pip install dcachefs


or 

.. code-block:: console

  git clone https://github.com/NLeSC-GO-common-infrastructure/dcachefs.git
  cd dcachefs
  pip install .


Run tests (including coverage) with:

.. code-block:: console

  python setup.py test


Documentation
-------------

The project's full documentation can be found `here`_.

.. _here: https://dcachefs.readthedocs.io

Contributing
------------

If you want to contribute to the development of dCacheFS,
have a look at the `contribution guidelines <CONTRIBUTING.rst>`_.

License
-------

Copyright (c) 2020, Netherlands eScience Center

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.



Credits
-------

This package was created with `Cookiecutter <https://github.com/audreyr/cookiecutter>`_ and the `NLeSC/python-template <https://github.com/NLeSC/python-template>`_.
