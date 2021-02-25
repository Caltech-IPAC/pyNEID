import sys
import io
from pyneid.neid import Neid 
from astropy.table import Table,Column

# These tests are designed to be run inside the 
# Docker container built with the Dockerfile
# at the top level of the repo.

def test_login():
    # dummy user with limited access
    Neid.login (userid='pyneidprop', \
        password='pielemonquietyellow', \
        cookiepath='./neidadmincookie.txt', 
        debugfile='./archive.debug')

def test_l0_qeury():
    #   by_adql: neidl0 returned 11071 records
    #
    query = "select * from neidl0"
    Neid.query_adql (query, \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./adql.l0.tbl')

def test_l1_query():
    #
    #   by_adql: neidl1 returned 10662 records
    #
    query = "select * from neidl1"
    Neid.query_adql (query, \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./adql.l1.tbl')

def test_l2_query():
    #
    #   by_adql: neidl2 returned 461 records
    #
    query = "select * from neidl2"
    Neid.query_adql (query, \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./adql.l2.tbl')

def test_query_eng():
    #
    #   by_adql: neideng returned 3321 records
    #
    query = "select * from neideng"
    Neid.query_adql (query, \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./adql.eng.tbl')

def test_query_solar0():
    #
    #   by_adql: neidsolarl0 returned 8421 records
    #
    query = "select * from neidsolarl0"
    Neid.query_adql (query, \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./adql.solarl0.tbl')

def test_query_solar1():
    #
    #   by_adql: neidsolarl1 returned 4027 records
    #
    query = "select * from neidsolarl1"
    Neid.query_adql (query, \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./adql.solarl1.tbl')


def test_query_solar2():
    #
    #   by_adql: neidsolarl2 returned 3759 records
    #
    query = "select * from neidsolarl2"
    Neid.query_adql (query, \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./adql.solarl2.tbl')

def test_query_solareng():
    #
    #   by_adql: neidsolareng returned 6 records
    #
    query = "select * from neidsolareng"
    Neid.query_adql (query, \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./adql.solareng.tbl')

def test_query_datetime_l0():
    #
    #    search by datetime l0: returned 94 recs
    #
    Neid.query_datetime ('l0', \
        '2021-01-22 00:00:00/2021-01-22 23:59:59', \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./datetime.ops.1.tbl')

def test_query_datetime_l2():
    #
    #    search by datetime l2: returned 21 recs
    #
    Neid.query_datetime ('l2', \
        '2021-01-01 0:00:00/2021-01-31 23:59:59', \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./datetime.l2.tbl')

def test_query_datetime_solar0():
    #
    #    search by datetime solarl0: returned 19 recs
    #
    Neid.query_datetime ('solarl0', \
        '2021-01-01 00:00:00/2021-01-20 23:59:59', \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./datetime.solarl0.tbl')

def test_pos_cookie():
    #
    #    search by pos (with cookie): 56 records returned 
    #
    Neid.query_position ('l0', \
        'circle 26.0214 -15.9395 1.0', \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./pos.l0.tbl')
        
    astropytbl = Table.read ('./pos.l0.tbl', format='ascii.ipac')
    print ('pos.l0.tbl read successfully by astropy')

def test_query_obj_cookie():
    #
    #    search by object (with cookie): 56 records returned 
    #
    Neid.query_object ('l0', \
        'HD 10700', \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./object.l0.tbl')

    astropytbl = Table.read ('./object.l0.tbl', format='ascii.ipac')
    print ('object.l0.tbl read successfully by astropy')

def test_query_qobj_cookie():
    #
    #    search by qobject (with cookie): 56 records returned 
    #
    Neid.query_qobject ('l0', \
        'Gaia DR2', \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./qobject.l0.tbl')

    astropytbl = Table.read ('./qobject.l0.tbl', format='ascii.ipac')
    print ('qobject.l0.tbl read successfully by astropy')

def test_query_piname_cookie():
    #
    #    search by piname (with cookie): 93 records returned 
    #
    Neid.query_piname ('l0', \
        'Mahadevan', \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./piname.l0.tbl')

    astropytbl = Table.read ('./piname.l0.tbl', format='ascii.ipac')
    print ('piname.l0.tbl read successfully by astropy')

def test_query_program_cookie():
    #
    #    search by program (with cookie): 93 records returned 
    #
    Neid.query_program ('l0', \
        '2021B-6666', \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./program.l0.tbl')

    astropytbl = Table.read ('./program.l0.tbl', format='ascii.ipac')
    print ('program.l0.tbl read successfully by astropy')

def test_query_criteria_cookie():
    #
    #    following three examples search_criteria on the same location (with cookie)
    #
    #    1. input: object name, Sci data, datetime: 56 records returned 
    #
    param = dict()
    param['datalevel'] = 'l0'
    param['datetime'] = '2021-01-14 00:00:00/2021-01-14 23:59:59'
    param['object'] = 'HD 95735'

    Neid.query_criteria (param, \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./criteria.sci.tbl')

    astropytbl = Table.read ('./criteria.sci.tbl', format='ascii.ipac')
    print ('criteria.sci.tbl read successfully by astropy')

    #
    #    2. input: qobject name, Cal data, datetime: 6 records returned 
    #
    param = dict()
    param['datalevel'] = 'l0'
    param['datetime'] = '2021-01-14 12:00:00/2021-01-14 23:59:59'
    param['qobject'] = 'Gaia DR2'
    param['obstype'] = 'sci'

    Neid.query_criteria (param, \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./criteria.sci.2.tbl')

    astropytbl = Table.read ('./criteria.sci.2.tbl', format='ascii.ipac')
    print ('criteria.sci.2.tbl read successfully by astropy')

    #
    #    3. input: qobject name, Eng data, datetime: 12 records returned 
    #
    param = dict()
    param['datalevel'] = 'l0'
    param['datetime'] = '2021-01-14 12:00:00/2021-01-14 23:59:59'
    param['qobject'] = 'Gaia DR2'
    param['obstype'] = 'cal'

    Neid.query_criteria (param, \
        cookiepath='./neidadmincookie.txt', \
        format='ipac', \
        outpath='./criteria.eng.tbl')

    astropytbl = Table.read ('./criteria.eng.tbl', format='ascii.ipac')
    print ('criteria.eng.tbl read successfully by astropy')

# disabled for now since public users have no data to download yet

# def test_downloafd():
#     #
#     #    download FITS: first time when dnload_dir is empty or created the 
#     #    following script downloaded 3 FITS files, 3 calibration list, and
#     #    30 more FITS files from the calibration list.
#     #
#     Neid.download('./criteria.sci.tbl', 
#         'l0', \
#         'ipac', \
#         '.', \
#         cookiepath='./neidadmincookie.txt', \
#         start_row=0, \
#         end_row=1)
