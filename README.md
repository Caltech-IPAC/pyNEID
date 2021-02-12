NEID Table Access Protocol (TAP) Client
======================================

**This repository is currently in alpha release. Features may be broken or missing. Please report any bugs to neid-help@ipac.caltech.edu**

Contributions are welcome! If you wish to contribute code please create a fork and submit a pull request with a good description of the proposed changes.

The NEID Table Access Protocol service is a general tool for making 
SQL queries against NEID database tables (which reside in an Oracle
DBMS).  TAP is a standard defined by the International Virtual 
Astronomy Alliance (IVOA).

There are multiple TAP client packages in existence (notably TAPPlus
from the AstroQuery Python package.  This client does not try to 
reproduce that functionality but rather provide simple-to-use custom
query support for a few common use cases (e.g. query by proposal ID).
In addition, it supports proprietary access using standard session
cookies and a separate login service.

The package can be installed from PiPy: `pip install pyneid`

This software is written in pure Python 3.
