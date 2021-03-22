NEID Table Access Protocol (TAP) Client
======================================

[![Build Status](https://www.travis-ci.com/Caltech-IPAC/pyNEID.svg?branch=master)](https://www.travis-ci.com/Caltech-IPAC/pyNEID)
[![Documentation Status](https://readthedocs.org/projects/pyneid/badge/?version=latest)](https://pyneid.readthedocs.io/en/latest/?badge=latest)
[![Coverage Status](https://coveralls.io/repos/github/Caltech-IPAC/pyNEID/badge.svg?branch=master)](https://coveralls.io/github/Caltech-IPAC/pyNEID?branch=master)
[![PyPI version](https://badge.fury.io/py/pyneid.svg)](https://badge.fury.io/py/pyneid)

**This repository is currently in alpha release. Features may be broken or missing. Please report any bugs to neid-help@ipac.caltech.edu**

Documentation is available on [ReadTheDocs](https://pyneid.readthedocs.io/en/latest/)

This package provides API access to the NEID Archive: neid.ipac.caltech.edu

The package can be installed via pip: `pip install git+https://github.com/Caltech-IPAC/pyNEID.git`

With pyNEID you can:
  - log in using your NEID Archive credentials, or use without login for access to public data only
  - query the archive for tables of L0, L1, and L2 metadata
  - download FITS files of any level
  - more features to come...

Contributions are welcome! If you wish to contribute code please create a fork and submit a pull request with a good description of the proposed changes.


The NEID Table Access Protocol service is a general tool for making 
SQL queries against NEID database tables (which reside in an Oracle
DBMS). TAP is a standard defined by the International Virtual 
Astronomy Alliance (IVOA).

There are multiple TAP client packages in existence (notably TAPPlus
from the AstroQuery Python package.  This client does not try to 
reproduce that functionality but rather provide simple-to-use custom
query support for a few common use cases (e.g. query by proposal ID).
In addition, it supports proprietary access using standard session
cookies and a separate login service.

This software is written in Python 3.
