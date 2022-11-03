.. pyNEID documentation master file, created by
   sphinx-quickstart on Fri Feb 19 10:20:20 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

pyNEID: API Access to the NEID Archive at NExScI
================================================

This package is designed to be an API interface to the
`NEID Data Archive <https://neid.ipac.caltech.edu>`_.

With pyNEID you can:
   - log in using your NEID Archive credentials, or use without login for access to public data only
   - query the archive for tables of L0, L1, and L2 metadata
   - download FITS files of any level
   - more features to come...

The package may be installed via pip:

.. code-block:: bash
		
   $ pip install git+https://github.com/Caltech-IPAC/pyNEID.git

Or by downloading directly from the `GitHub repository <https://github.com/Caltech-IPAC/pyNEID>`_.

The :ref:`Archive class <archive>` contains many useful methods for working with the NIED archive.

This package is under active development by the NASA Exoplanet Science Institute.

Please report any bugs, issues, or feature requests to the `NEID Help Desk <mailto: neid-help@ipac.caltech.edu>`_.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   api


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
