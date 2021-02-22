#!/bin/sh

python3 << EOF

import sys
import io
from pyneid.neid import Neid 
from astropy.table import Table,Column

Neid.login (userid='neidadmin', \
    password='neidisawesome!', \
    cookiepath='./neidadmincookie.txt', 
    debugfile='./archive.debug')

#
#
#   by_adql: neidl0 returned 11071 records
#
query = "select * from neidl0"
Neid.query_adql (query, \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/adql.l0.tbl')

#
#   by_adql: neidl1 returned 10662 records
#
query = "select * from neidl1"
Neid.query_adql (query, \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/adql.l1.tbl')

#
#   by_adql: neidl2 returned 461 records
#
query = "select * from neidl2"
Neid.query_adql (query, \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/adql.l2.tbl')


#
#   by_adql: neideng returned 3321 records
#
query = "select * from neideng"
Neid.query_adql (query, \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/adql.eng.tbl')


#
#   by_adql: neidsolarl0 returned 8421 records
#
query = "select * from neidsolarl0"
Neid.query_adql (query, \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/adql.solarl0.tbl')


#
#   by_adql: neidsolarl1 returned 4027 records
#
query = "select * from neidsolarl1"
Neid.query_adql (query, \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/adql.solarl1.tbl')


#
#   by_adql: neidsolarl2 returned 3759 records
#
query = "select * from neidsolarl2"
Neid.query_adql (query, \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/adql.solarl2.tbl')


#
#   by_adql: neidsolareng returned 6 records
#
query = "select * from neidsolareng"
Neid.query_adql (query, \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/adql.solareng.tbl')


#
#    search by datetime l0: returned 94 recs
#
Neid.query_datetime ('l0', \
    '2021-01-22 00:00:00/2021-01-22 23:59:59', \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/datetime.ops.1.tbl')

#
#    search by datetime l2: returned 21 recs
#
Neid.query_datetime ('l2', \
    '2021-01-01 0:00:00/2021-01-31 23:59:59', \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/datetime.l2.tbl')

#
#    search by datetime solarl0: returned 19 recs
#
Neid.query_datetime ('solarl0', \
    '2021-01-01 00:00:00/2021-01-20 23:59:59', \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/datetime.solarl0.tbl')


#
#    search by pos (with cookie): 56 records returned 
#
Neid.query_position ('l0', \
    'circle 26.0214 -15.9395 1.0', \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/pos.l0.tbl')

astropytbl = Table.read ('./pos.l0.tbl', format='ascii.ipac')
print ('pos.l0.tbl read successfully by astropy')

#
#    search by object (with cookie): 56 records returned 
#
Neid.query_object ('l0', \
    'HD 10700', \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/object.l0.tbl')

astropytbl = Table.read ('./object.l0.tbl', format='ascii.ipac')
print ('object.l0.tbl read successfully by astropy')

#
#    search by qobject (with cookie): 56 records returned 
#
Neid.query_qobject ('l0', \
    'Gaia DR2', \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/qobject.l0.tbl')

astropytbl = Table.read ('./qobject.l0.tbl', format='ascii.ipac')
print ('qobject.l0.tbl read successfully by astropy')

#
#    search by piname (with cookie): 93 records returned 
#
Neid.query_piname ('l0', \
    'Mahadevan', \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/piname.l0.tbl')

astropytbl = Table.read ('./piname.l0.tbl', format='ascii.ipac')
print ('piname.l0.tbl read successfully by astropy')

#
#    search by program (with cookie): 93 records returned 
#
Neid.query_program ('l0', \
    '2021B-6666', \
    cookiepath='./neidadmincookie.txt', \
    format='ipac', \
    outpath='./dnload_dir/program.l0.tbl')

astropytbl = Table.read ('./program.l0.tbl', format='ascii.ipac')
print ('program.l0.tbl read successfully by astropy')

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
    outpath='./dnload_dir/criteria.sci.tbl')

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
    outpath='./dnload_dir/criteria.sci.2.tbl')

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
    outpath='./dnload_dir/criteria.eng.tbl')

astropytbl = Table.read ('./criteria.eng.tbl', format='ascii.ipac')
print ('criteria.eng.tbl read successfully by astropy')


#
#    download FITS: first time when dnload_dir is empty or created the 
#    following script downloaded 3 FITS files, 3 calibration list, and
#    30 more FITS files from the calibration list.
#
Neid.download ('./criteria.sci.tbl', 
    'l0', \
    'ipac', \
    'dnload_dir', \
    cookiepath='./neidadmincookie.txt', \
    start_row=0, \
    end_row=1)

EOF