import os
import sys
import io
import filecmp
import pytest 
from pathlib import Path

from pyneid.neid import Neid 
from astropy.table import Table,Column

# These tests are designed to be run inside the 
# Docker container built with the Dockerfile
# at the top level of the repo.

# dummy user pyneidprop with limited access

userdict = {
   "pyneidprop_pielemonquietyellow":"Successfully login as pyneidprop",
   "xxpyneidprop_pielemonquietyellow":"Failed to login: invalid userid = xxpyneidprop",
   "pyneidprop_xxpielemonquietyellow":"Failed to login: invalid password"
}

#
#    test login method: correctly, wrong userid, and wrong password
#
@pytest.mark.parametrize ("user, expected", list(userdict.items()), \
    ids=list(userdict.keys()))  
 
def test_login (user, expected, capsys):
   
    ind = user.index('_')
    userid = user[0:ind]
    password = user[ind+1:]

    Neid.login (cookiepath='./neidtestcookie.txt', \
        userid=userid, \
        password=password)

    out, err = capsys.readouterr()
    assert out.startswith (expected)


#
#    test query_datetime method for all datalevel; 
#    but currently only l0 and l1 contains data for the test user.
#
#    returned metadata files are compared with the truth data for validation.
#
datetimedict = {
    "l0":"2021-01-16 06:10:55/2021-01-16 23:59:59", \
    "l1":"2021-01-16 06:10:55/2021-01-16 23:59:59"
}

@pytest.mark.parametrize ("datalevel,datetime", list(datetimedict.items()), \
    ids=list(datetimedict.keys()))
 
def test_query_datetime (datalevel, datetime, capsys):

    outpath = './datetime.' + datalevel + '.tbl'
    datapath = './truth_data/datetime.' + datalevel + '.tbl'

    Neid.query_datetime (datalevel, \
        datetime, \
        cookiepath='./neidtestcookie.txt', \
        format='ipac', \
        outpath=outpath)

    assert os.path.exists(outpath), \
        f'Result not downloaded to file [{outpath:s}]'
    
    if (datalevel == 'l0'):
        assert (filecmp.cmp (outpath, datapath, shallow=False))

    elif (datalevel == 'l1'):
        astropytbl = None
        astropytbl = Table.read (outpath, format='ascii.ipac')
        assert (astropytbl is not None), \
            "f{outpath:s} cannot be read by astropy"

        astropytbl_truth = None
        astropytbl_truth = Table.read (datapath, format='ascii.ipac')
        assert (astropytbl_truth  is not None), \
            "f{datapath:s} cannot be read by astropy"

        assert (len(astropytbl) == len(astropytbl_truth)), \
            f"Number of records in {outpath:s} is incorrect"

#
#    test query_position method for all datalevel; 
#    but currently only l0 and l1 contains data for the test user.
#
#    returned metadata files are compared with the truth data for validation.
#
posdict = {
    "l0": "circle 23.634 68.95 1.0", \
    "l1": "circle 23.634 68.95 1.0"
}

@pytest.mark.parametrize ("datalevel,pos", list(posdict.items()), \
    ids=list(posdict.keys()))
 
def test_query_position (datalevel, pos, capsys):

    outpath = './pos.' + datalevel + '.tbl'
    datapath = './truth_data/pos.' + datalevel + '.tbl'

    Neid.query_position (datalevel, \
        pos, \
        cookiepath='./neidtestcookie.txt', \
        format='ipac',
        outpath=outpath)

    assert os.path.exists(outpath), \
        f'Result not downloaded to file [{outpath:s}]'
    #assert (filecmp.cmp (outpath, datapath, shallow=False))

    astropytbl = None
    astropytbl = Table.read (outpath, format='ascii.ipac')
    assert (astropytbl is not None), \
        "f{outpath:s} cannot be read by astropy"

    astropytbl_truth = None
    astropytbl_truth = Table.read (datapath, format='ascii.ipac')
    assert (astropytbl_truth  is not None), \
        "f{datapath:s} cannot be read by astropy"

    assert (len(astropytbl) >= len(astropytbl_truth)), \
        f"Number of records in {outpath:s} is incorrect"


#
#    test query_object method using l1 data
#
def test_query_object():

    outpath = './object.l1.tbl'
    datapath = './truth_data/object.l1.tbl'

    Neid.query_object ('l1', \
        'HD 9407', \
        cookiepath='./neidtestcookie.txt', \
        format='ipac', \
        outpath=outpath)

    assert os.path.exists(outpath), \
        f'Result not downloaded to file [{outpath:s}]'
    #assert (filecmp.cmp (outpath, datapath, shallow=False))

    astropytbl = None
    astropytbl = Table.read (outpath, format='ascii.ipac')
    assert (astropytbl is not None), \
        "f{outpath:s} cannot be read by astropy"

    astropytbl_truth = None
    astropytbl_truth = Table.read (datapath, format='ascii.ipac')
    assert (astropytbl_truth  is not None), \
        "f{datapath:s} cannot be read by astropy"

    assert (len(astropytbl) >= len(astropytbl_truth)), \
        f"Number of records in {outpath:s} is incorrect"


#
#    test query_qobject method using l1 data
#
def test_query_qobject():

    outpath = './qobject.l1.tbl'
    datapath = './truth_data/qobject.l1.tbl'

    Neid.query_qobject ('l1', \
        'Gaia DR2', \
        cookiepath='./neidtestcookie.txt', \
        format='ipac', \
        outpath=outpath)

    assert os.path.exists(outpath), \
        f'Result not downloaded to file [{outpath:s}]'
    #assert (filecmp.cmp (outpath, datapath, shallow=False))

    astropytbl = None
    astropytbl = Table.read (outpath, format='ascii.ipac')
    assert (astropytbl is not None), \
        "f{outpath:s} cannot be read by astropy"

    astropytbl_truth = None
    astropytbl_truth = Table.read (datapath, format='ascii.ipac')
    assert (astropytbl_truth  is not None), \
        "f{datapath:s} cannot be read by astropy"

    assert (len(astropytbl) >= len(astropytbl_truth)), \
        f"Number of records in {outpath:s} is incorrect"

#
#    test query_program method using l1 data
#
def test_query_program():

    outpath = './program.l1.tbl'
    datapath = './truth_data/program.l1.tbl'

    Neid.query_program ('l1', \
        '2021A-2014', \
        cookiepath='./neidtestcookie.txt', \
        format='ipac', \
        outpath=outpath)

    assert os.path.exists(outpath), \
        f'Result not downloaded to file [{outpath:s}]'
    #assert (filecmp.cmp (outpath, datapath, shallow=False))

    astropytbl = None
    astropytbl = Table.read (outpath, format='ascii.ipac')
    assert (astropytbl is not None), \
        "f{outpath:s} cannot be read by astropy"

    astropytbl_truth = None
    astropytbl_truth = Table.read (datapath, format='ascii.ipac')
    assert (astropytbl_truth  is not None), \
        "f{datapath:s} cannot be read by astropy"

    assert (len(astropytbl) >= len(astropytbl_truth)), \
        f"Number of records in {outpath:s} is incorrect"


#
#    test query_criteria method using l1 data
#
def test_query_criteria():

    outpath = './criteria.l1.tbl'
    datapath = './truth_data/criteria.l1.tbl'

    param = dict()
    param['datalevel'] = 'l1'
    param['datetime'] = '2021-01-01 00:00:00/2021-04-19 23:59:59'
    param['object'] = 'HD 9407'

    Neid.query_criteria (param, \
        cookiepath='./neidtestcookie.txt', \
        format='ipac', \
        outpath=outpath)

    assert os.path.exists(outpath), \
        f'Result not downloaded to file [{outpath:s}]'
    assert (filecmp.cmp (outpath, datapath, shallow=False))


#
#    test query_adql method using l1 data
#
def test_qeury_adql():

    outpath = './adql.l1.tbl'
    datapath = './truth_data/adql.l1.tbl'

    query = "select l1filename, l1filepath, l1propint, qobject, object, qra, qdec, to_char(obsdate,'YYYY-MM-DD HH24:MI:SS.FF3') as date_obs, exptime, obsmode, obstype, program, piname, datalvl, seeing, airmass, moonagl, qrad as ra, qdecd as dec from neidl1 where ((obsdate >= to_date('2020-01-01 06:10:55', 'yyyy-mm-dd HH24:MI:SS') and obsdate <= to_date('2021-04-19 23:59:59', 'yyyy-mm-dd HH24:MI:SS')) and (qdecd >= -90.)) order by obsdate"

    Neid.query_adql (query, \
        cookiepath='./neidtestcookie.txt', \
        format='ipac', \
        outpath=outpath)

    assert os.path.exists(outpath), \
        f'Result not downloaded to file [{outpath:s}]'
    #assert (filecmp.cmp (outpath, datapath, shallow=False))

    astropytbl = None
    astropytbl = Table.read (outpath, format='ascii.ipac')
    assert (astropytbl is not None), \
        "f{outpath:s} cannot be read by astropy"

    astropytbl_truth = None
    astropytbl_truth = Table.read (datapath, format='ascii.ipac')
    assert (astropytbl_truth  is not None), \
        "f{datapath:s} cannot be read by astropy"

    assert (len(astropytbl) >= len(astropytbl_truth)), \
        f"Number of records in {outpath:s} is incorrect"

#
#    test query_adql method: 
#    download the first two files from metadata file criteria.l1.tbl
#
dnloaddict = {
    "l0":"./datetime.l0.tbl", \
    "l1":"./criteria.l1.tbl"
}

@pytest.mark.parametrize ("datalevel,metatbl", list(dnloaddict.items()), \
    ids=list(dnloaddict.keys()))

def test_download(datalevel, metatbl, capsys):
#
#    Check if metadata file contains datalevel + 'filepath' column
#
    astropytbl = Table.read (metatbl, format='ascii.ipac')
    len_col = len(astropytbl.colnames)

    ind_filepathcol = -1
    for i in range (0, len_col):
   
        colname = datalevel + 'filepath'
        if (astropytbl.colnames[i].lower() == colname):
            ind_filepathcol = i

    assert (ind_filepathcol >= 0), \
        "filepath column doesn't exit in metadata table"

#
#    Make sure ./dnload_dir is empty
#
    dnloaddir = './dnload_dir'
    srow = 0
    erow = 1

    if (os.path.exists (dnloaddir)):
        
        files = os.listdir(dnloaddir)
        
        for f in files: 
            os.remove(dnloaddir + '/'+f)

    Neid.download(metatbl, \
        datalevel, \
        'ipac', \
        dnloaddir, \
        cookiepath='./neidtestcookie.txt', \
        start_row=srow, \
        end_row=erow)
   
    for i in range (srow, erow):

        filepath = astropytbl[i][ind_filepathcol]
        ind = filepath.rindex ('/')
        filename = filepath[ind+1:]
        
        print (f'filename= {filename:s}') 
        print (f'filepath= {filepath:s}') 
    
        dnloaded = dnloaddir + '/' + filename 
        assert (os.path.exists (dnloaded))
        
        filesize = Path (dnloaded).stat().st_size
        assert (filesize > 100000)

