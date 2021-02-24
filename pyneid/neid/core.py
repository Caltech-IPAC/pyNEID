import os
import sys
import io
import getpass
import logging
import json

import time
import xmltodict
import tempfile
import bs4 as bs

import requests
import urllib
import http.cookiejar

from astropy.table import Table, Column

from . import conf


class Archive:
    """ 
        'Archive' class provides NEID archive access functions for searching 
        NEID data via TAP interface.  
    
        The user's NEID credentials (given at login as a cookie file or a token 
        string) are used to search the proprietary data.

        Arguments:
            debugfile: a file path for the debug output

        In the following examples, "neid" will represent an instance of the 
        Archive class:

        Examples:
            >>> import os
            >>> import sys

            >>> from pyneid.neid import Neid

            >>> neid = Archive()
    """

    tap = None

    parampath = ''
    outpath = ''
    format = 'ipac'
    maxrec = -1 
    query = ''
    
    content_type = ''
    outdir = ''
    astropytbl = None

    ndnloaded = 0

    cookiepath = ''
    token = ''
    status = ''
    msg = ''
    
    debugfname = './archive.debug'    
    debug = 0    

    def __init__(self, **kwargs):
        
        if ('debugfile' in kwargs):
            
            self.debug = 1
            self.debugfname = kwargs.get ('debugfile')

            if (len(self.debugfname) > 0):
      
                logging.basicConfig (filename=self.debugfname, \
                    level=logging.DEBUG)
    
                with open (self.debugfname, 'w') as fdebug:
                    pass
 
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter Archive.init:')

        """retrieve baseurl from conf class;
        during dev or test, baseurl will be a keyword input
        """

        self.baseurl = conf.server

        self.baseurl = ''
        
        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

            if self.debug:
                logging.debug ('')
                logging.debug (f'baseurl= {self.baseurl:s}')

        """urls for nph-tap.py, nph-neidLogin, nph-neidMakeQyery, 
            nph-neidDownload
        """

        self.tap_url = self.baseurl + '/TAP'
        self.login_url = self.baseurl + 'cgi-bin/NeidAPI/nph-neidLogin.py?'
        self.makequery_url = self.baseurl + 'cgi-bin/NeidAPI/nph-neidMakequery.py?'
        self.getneid_url = self.baseurl + 'cgi-bin/NeidAPI/nph-neidDownload.py?'

        if self.debug:
            logging.debug ('')
            logging.debug (f'login_url= [{self.login_url:s}]')
            logging.debug (f'tap_url= [{self.tap_url:s}]')
            logging.debug (f'makequery_url= [{self.makequery_url:s}]')
            logging.debug (f'self.getneid_url= {self.getneid_url:s}')
      
        return
    
    def login (self, **kwargs):
        """
        login method validates a user has a valid NEID account; 
        it takes two 'keyword' arguments: userid and password. If the inputs 
        are not provided in the keyword, the auth method prompts for inputs.

        login method returns both cookie header and a token string in 
        the returned message file.  
        
        If cookiepath is provided, the cookie will be saved to the cookiepath.
        
        The token string will be saved in the variable "token" in memory to 
        be used for other Archive methods in the current session.

        Arguments:
	        userid (string): a valid user id  in the NEID's user table.
            password (string): a valid password in the NEID's user table.   
            cookiepath (string): (optional) a file path provided by the user to
                    save returned cookie in a local file for use anytime in the future data search. 
        
        Examples:    
            # without prompt
            >>> neid.login(userid='xxxx', password='xxxxxx', cookiepath=cookiepath)
            # or
            >>> neid.login(userid='xxxx', password='xxxxxx', token=token_string)
            # or
            # prompt for userid and password
            >>> neid.login(cookiepath=cookiepath) 
            >>> neid.login()
        """

        if (self.debug == 0):

            if ('debugfile' in kwargs):
            
                self.debug = 1
                self.debugfname = kwargs.get ('debugfile')

                if (len(self.debugfname) > 0):
      
                    logging.basicConfig (filename=self.debugfname, \
                        level=logging.DEBUG)
    
                    with open (self.debugfname, 'w') as fdebug:
                        pass

            if self.debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
 
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter login:')


        userid= ''
        password = ''
        if ('userid' in kwargs):
            userid = kwargs.get ('userid')

        if ('password' in kwargs):
            password = kwargs.get ('password')
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'userid= [{userid:s}]')
            logging.debug (f'password= [{password:s}]')

        url = ''
        response = ''
        jsondata = ''

        self.status = ''
        self.msg = ''


        """get userid and password via keyboard input
        """
        
        if (len(userid) == 0):
            userid = input ("Userid: ")

        if (len(password) == 0):
            password = getpass.getpass ("Password: ")

        """hide debug password printout
        password = urllib.parse.quote (password)
        if self.debug:
            logging.debug ('')
            logging.debug (f'password= {password:s}')
        """

        """retrieve baseurl from conf class;
        """ 
        
        self.baseurl = conf.server

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl (from conf)= {self.baseurl:s}')
        

        """retrieve cookiepath
        """
        
        if ('cookiepath' in kwargs):
            self.cookiepath = kwargs.get ('cookiepath')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {self.cookiepath:s}')

        """construct full url for login
        """

        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')

        self.login_url = self.baseurl + 'cgi-bin/NeidAPI/nph-neidLogin.py?'
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'login_url= [{self.login_url:s}]')

        param = dict()
        param['userid'] = userid
        param['password'] = password
    
        data_encoded = urllib.parse.urlencode (param)
    
        url = self.login_url + data_encoded

        if self.debug:
            logging.debug ('')
            logging.debug (f'url= [{url:s}]')


        """cookiejar declared and linked to cookiepath
        """

        if self.debug:
            logging.debug ('')
            logging.debug ('declare request session with cookie')
        
        session = requests.Session()
        session.cookies = http.cookiejar.MozillaCookieJar (self.cookiepath)
        cookiejar = session.cookies

        response = None
        try:
            response = session.get (url, cookies=cookiejar)
        
        except Exception as e:

            self.msg = str(e)
            print (self.msg)
            return

        if self.debug:
            logging.debug ('')
            logging.debug ('response.text: ')
            logging.debug (response.text)
            logging.debug ('response.headers: ')
            logging.debug (response.headers)
       
        """check content-type in response header: 
        it should be an 'application/json' structure, parse for returned 
        status and message
        """

        contenttype = response.headers['Content-type']
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'contenttype= {contenttype:s}')

        jsondata = json.loads (response.text);
   
        for key,val in jsondata.items():
                
            if (key == 'status'):
                self.status = val
                
            if (key == 'msg'):
                self.msg =  val
		
            if (key == 'token'):
                self.token =  val
       

        if self.debug:
            logging.debug ('')
            logging.debug (f'status= {self.status:s}')
            logging.debug (f'msg= {self.msg:s}')
            logging.debug (f'token= {self.token:s}')
            logging.debug (f'cookiepath= {self.cookiepath:s}')


        if (self.status == 'ok'):
            
            
            self.msg = 'Successfully login as ' + userid

            if (len(self.cookiepath) > 0):
                
                cookiejar.save ()
                self.cookie_loaded = 1

                """print out cookie values in debug file
                """

                if self.debug:
                    for cookie in cookiejar:
                        logging.debug ('')
                        logging.debug ('cookie saved:')
                        logging.debug (cookie)
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')
 
        else:       
            self.msg = 'Failed to login: ' + self.msg

        print (self.msg)
        return
    
    def query_datetime (self, datalevel, datetime, **kwargs):
        
        """'query_datetime' method search NEID data by 'datetime' range
        
            Arguments:
                datalevel (string): l0, l1, l2, eng, solarl0, solarl1, solarl2, solareng 
                datetime (string): a datetime string in the format of 
                    datetime1/datetime2 where '/' separates the two datetime values` 
                    of format 'yyyy-mm-dd hh:mm:ss'
                outpath (string): (optional) a full output filepath of the returned metadata 
                    table
                cookiepath (string): (optional) a full cookie file path saved from login for 
                    querying the proprietary NEID data.
                token (string): (optional) a token string save in memory from login for querying 
                    the proprietary NEID data; the token is only valid for the 
                    current session.
                format (string): (optional) Output format: votable, ipac, csv, tsv 
                            (default: votable)
                maxrec (integer): (optional) maximum records to be returned 
                    default: -1 or not specified will return all requested records

            Note:
                The following inputs are acceptable for the datetime parameter:

                'datetime1/': will search data with datetime later than (>=) datetime1

                '/datetime2': will search data with datetime earlier than (<=) datetime2

                'datetime1': will search data with datetime equal to (=) datetime1 (this is not recommended)

            Examples:
                >>> datalevel = 'l0',
                >>> datetime = '2020-11-16 06:10:55/2020-11-18 00:00:00' 

                >>> datalevel = 'l1',
                >>> datetime = '2020-11-16 06:10:55/' 

                >>> datalevel = 'l2',
                >>> datetime = '/2020-112-18 00:00:00' 

        """
 
        if (self.debug == 0):

            if ('debugfile' in kwargs):
            
                self.debug = 1
                self.debugfname = kwargs.get ('debugfile')

                if (len(self.debugfname) > 0):
      
                    logging.basicConfig (filename=self.debugfname, \
                        level=logging.DEBUG)
    
                    with open (self.debugfname, 'w') as fdebug:
                        pass

            if self.debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_datetime:')
       
        datalevel = str(datalevel)

        if (len(datalevel) == 0):
            print ('Failed to find required parameter: datalevel')
            return

        datetime = str(datetime)

        if (len(datetime) == 0):
            print ('Failed to find required parameter: datetime')
            return

        self.datalevel = datalevel
        self.datetime = datetime

        if self.debug:
            logging.debug ('')
            logging.debug (f'datalevel= {self.datalevel:s}')
            logging.debug (f'datetime= {self.datetime:s}')

        """send url to server to construct the select statement
        """
       
        param = dict()
        param['datalevel'] = self.datalevel
        param['datetime'] = self.datetime
        
        if self.debug:
            logging.debug ('')
            logging.debug ('call query_criteria')

        self.query_criteria (param, **kwargs)

        return

    def query_position (self, datalevel, position, **kwargs):
    
        """
        'query_position' method search NEID data by 'position' 
        
        Arguments:
            datalevel (string): l0, l1, l2, eng, solarl0, solarl1, solarl2, solareng 
            datetime (string): a datetime string as specified in `qurey_datetime`
            position (string): a position string in the format of 
	        outpath (string): (optional) a full output filepath of the returned metadata 
                table
            cookiepath (string): (optional) a full cookie file path saved from login for 
                querying the proprietary NEID data.
            token (string): (optional) a token string save in memory from login for querying 
                the proprietary NEID data; the token is only valid for the 
                current session.
            format (string): (optional) votable, ipac, csv, tsv  (default: ipac)
	        maxrec (integer): (optional) maximum records to be returned 
	            default: -1 or not specified will return all requested records

        Note:
            Position can be a string in the following formats
                1.  circle ra dec radius;
                2.  polygon ra1 dec1 ra2 dec2 ra3 dec3 ra4 dec4;
                3.  box ra dec width height;
        
            All ra dec should be specified in J2000 coordinates.

        Examples:  
            >>> datalevel = 'l1',
            >>> position = 'circle 230.0 45.0 0.5'

        """
   
        if (self.debug == 0):

            if ('debugfile' in kwargs):
            
                self.debug = 1
                self.debugfname = kwargs.get ('debugfile')

                if (len(self.debugfname) > 0):
      
                    logging.basicConfig (filename=self.debugfname, \
                        level=logging.DEBUG)
    
                    with open (self.debugfname, 'w') as fdebug:
                        pass

            if self.debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_position:')
      
        
        datalevel = str(datalevel)

        if (len(datalevel) == 0):
            print ('Failed to find required parameter: datalevel')
            return
 
        if (len(position) == 0):
            print ('Failed to find required parameter: position')
            return


        self.datalevel = datalevel
        self.position = position
 
        if self.debug:
            logging.debug ('')
            logging.debug (f'datalevel=  {self.datalevel:s}')
            logging.debug (f'position=  {self.position:s}')

        """send url to server to construct the select statement
        """
        
        param = dict()
        param['datalevel'] = self.datalevel
        param['position'] = self.position

        self.query_criteria (param, **kwargs)

        return
    
    def query_object (self, datalevel, object, **kwargs):
        
        """
        'query_object' method search NEID data by 'object name'.
        This method resolves the object name into coordiates to be used as the
        center of the circle position search with default radius of 0.5 deg.
 
        Arguments:
            datalevel: l0, l1, l2, eng, solarl0, solarl1, solarl2, solareng 
            object (string): an object name resolvable by SIMBAD, NED, and
                ExoPlanet's name_resolve; 
	        outpath (string): (optional) a full output filepath of the returned metadata 
                table
            cookiepath (string): (optional) a full cookie file path saved from login for 
                querying the proprietary NEID data.
            token (string): (optional) a token string save in memory from login for querying 
                the proprietary NEID data; the token is only valid for the 
                current session.
            format (string): (optional) votable, ipac, csv, tsv  (default: ipac)
	        maxrec (integer): (optional) maximum records to be returned 
	            default: -1 or not specified will return all requested records

        Examples: 
            datalevel = 'l1',
            object = 'WD 1145+017'
        """
   
        if (self.debug == 0):

            if ('debugfile' in kwargs):
            
                self.debug = 1
                self.debugfname = kwargs.get ('debugfile')

                if (len(self.debugfname) > 0):
      
                    logging.basicConfig (filename=self.debugfname, \
                        level=logging.DEBUG)
    
                    with open (self.debugfname, 'w') as fdebug:
                        pass

            if self.debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_object_name:')

        datalevel = str(datalevel)

        if (len(datalevel) == 0):
            print ('Failed to find required parameter: datalevel')
            return
 
        if (len(object) == 0):
            print ('Failed to find required parameter: object')
            return

        self.datalevel = datalevel
        self.object = object

        if self.debug:
            logging.debug ('')
            logging.debug (f'datalevel= {self.datalevel:s}')
            logging.debug (f'object= {self.object:s}')

        radius = 0.5 
        if ('radius' in kwargs):
            radiusi_str = kwargs.get('radius')
            radius = float(radius_str)

        if self.debug:
            logging.debug ('')
            logging.debug (f'radius= {radius:f}')

        lookup = None
        try:
            if self.debug:
                lookup = objLookup (object, debug=1)
            else:
                lookup = objLookup (object)
        
            if self.debug:
                logging.debug ('')
                logging.debug ('objLookup run successful and returned')
        
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'objLookup error: {str(e):s}')
            
            print (str(e))
            return 

        if (lookup.status == 'error'):
            
            self.msg = 'Input object [' + object + '] lookup error: ' + \
                lookup.msg
            
            print (self.msg)
            return

        if self.debug:
            logging.debug ('')
            logging.debug (f'source= {lookup.source:s}')
            logging.debug (f'objname= {lookup.objname:s}')
            logging.debug (f'objtype= {lookup.objtype:s}')
            logging.debug (f'objdesc= {lookup.objdesc:s}')
            logging.debug (f'parsename= {lookup.parsename:s}')
            logging.debug (f'ra2000= {lookup.ra2000:s}')
            logging.debug (f'dec2000= {lookup.dec2000:s}')
            logging.debug (f'cra2000= {lookup.cra2000:s}')
            logging.debug (f'cdec2000= {lookup.cdec2000:s}')

       
        ra2000 = lookup.ra2000
        dec2000 = lookup.dec2000

        self.position = 'circle ' + ra2000 + ' ' + dec2000 + ' ' + str(radius)
	
        if self.debug:
            logging.debug ('')
            logging.debug (f'position= {self.position:s}')
       
        print (f'object name resolved: ra2000= {ra2000:s}, de2000c={dec2000:s}')
 
 
        """send url to server to construct the select statement
        """
       
        param = dict()
        
        param['datalevel'] = self.datalevel
        param['position'] = self.position

        self.query_criteria (param, **kwargs)

        return
    
    def query_qobject (self, datalevel, qobject, **kwargs):
        """
        'query_qobject' method search NEID data for 'qobject' column value. 
        This method resolves the object name into coordiates to be used as the
	    center of the circle position search with default radius of 0.5 deg.

        Arguments:    
            datalevel: l0, l1, l2, eng, solarl0, solarl1, solarl2, solareng 
            qobject (string): an object name as specified in the QOBJECT column.
                This is usually the Gaia DR2 ID
	        outpath (string): (optional) a full output filepath of the returned metadata 
                table
            cookiepath (string): (optional) a full cookie file path saved from login for 
                querying the proprietary NEID data.
            token (string): (optional) a token string save in memory from login for querying 
                the proprietary NEID data; the token is only valid for the 
                current session.
            format (string): (optional) votable, ipac, csv, tsv  (default: ipac)
	        maxrec (integer): (optional) maximum records to be returned 
	            default: -1 or not specified will return all requested records

        """
   
        if (self.debug == 0):

            if ('debugfile' in kwargs):
            
                self.debug = 1
                self.debugfname = kwargs.get ('debugfile')

                if (len(self.debugfname) > 0):
      
                    logging.basicConfig (filename=self.debugfname, \
                        level=logging.DEBUG)
    
                    with open (self.debugfname, 'w') as fdebug:
                        pass

            if self.debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_object_name:')

        datalevel = str(datalevel)

        if (len(datalevel) == 0):
            print ('Failed to find required parameter: datalevel')
            return
 
        if (len(qobject) == 0):
            print ('Failed to find required parameter: qobject')
            return

        self.datalevel = datalevel
        self.qobject = qobject

        if self.debug:
            logging.debug ('')
            logging.debug (f'datalevel= {self.datalevel:s}')
            logging.debug (f'qobject= {self.qobject:s}')

        radius = 0.5 
        if ('radius' in kwargs):
            radiusi_str = kwargs.get('radius')
            radius = float(radius_str)

        if self.debug:
            logging.debug ('')
            logging.debug (f'radius= {radius:f}')

 
        """send url to server to construct the select statement
        """
       
        param = dict()
        
        param['datalevel'] = self.datalevel
        param['qobject'] = self.qobject

        self.query_criteria (param, **kwargs)

        return

    def query_piname (self, datalevel, piname, **kwargs):
        """
        'query_piname' method search NEID data by PI name 
        
        Arguments:    
            datalevel (string): l0, l1, l2, eng, solarl0, solarl1, solarl2, solareng 
            datetime (string): a datetime string in the format of 
            piname (string): PI name as formated in the project's catalog 
       	    outpath (string): (optional) a full output filepath of the returned metadata 
                table
            cookiepath (string): (optional) a full cookie file path saved from login for 
                querying the proprietary NEID data.
            token (string): (optional) a token string save in memory from login for querying 
                the proprietary NEID data; the token is only valid for the 
                current session.
            format (string): (optional) votable, ipac, csv, tsv  (default: ipac)
	        maxrec (integer): (optional) maximum records to be returned 
	            default: -1 or not specified will return all requested records
        """
   
        if (self.debug == 0):

            if ('debugfile' in kwargs):
            
                self.debug = 1
                self.debugfname = kwargs.get ('debugfile')

                if (len(self.debugfname) > 0):
      
                    logging.basicConfig (filename=self.debugfname, \
                        level=logging.DEBUG)
    
                    with open (self.debugfname, 'w') as fdebug:
                        pass

            if self.debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_piname:')

        datalevel = str(datalevel)

        if (len(datalevel) == 0):
            print ('Failed to find required parameter: datalevel')
            return
 
        if (len(piname) == 0):
            print ('Failed to find required parameter: piname')
            return

        self.datalevel = datalevel
        self.piname = piname 

        if self.debug:
            logging.debug ('')
            logging.debug (f'datalevel= {self.datalevel:s}')
            logging.debug (f'piname= {self.piname:s}')

        
        """send url to server to construct the select statement
        """
       
        param = dict()
        
        param['datalevel'] = self.datalevel
        param['piname'] = self.piname

        self.query_criteria (param, **kwargs)

        return

    def query_program (self, datalevel, program, **kwargs):
        """
        'query_program' method search NEID data by 'program' 
        
        Arguments:
            datalevel (string): l0, l1, l2, eng, solarl0, solarl1, solarl2, solareng 
            datetime (string): datetime string
            program (string): program ID in the project's catalog 
            outpath (string): (optional) a full output filepath of the returned metadata 
                table
            cookiepath (string): (optional) a full cookie file path saved from login for 
                querying the proprietary NEID data.
            token (string): (optional) a token string save in memory from login for querying 
                the proprietary NEID data; the token is only valid for the 
                current session.
            format (string): (optional) votable, ipac, csv, tsv  (default: ipac)
	        maxrec (integer): (optional) maximum records to be returned 
	            default: -1 or not specified will return all requested records
        """
   
        if (self.debug == 0):

            if ('debugfile' in kwargs):
            
                self.debug = 1
                self.debugfname = kwargs.get ('debugfile')

                if (len(self.debugfname) > 0):
      
                    logging.basicConfig (filename=self.debugfname, \
                        level=logging.DEBUG)
    
                    with open (self.debugfname, 'w') as fdebug:
                        pass

            if self.debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_program:')

        datalevel = str(datalevel)

        if (len(datalevel) == 0):
            print ('Failed to find required parameter: datalevel')
            return
 
        if (len(program) == 0):
            print ('Failed to find required parameter: program')
            return

        self.datalevel = datalevel
        self.program = program 

        if self.debug:
            logging.debug ('')
            logging.debug (f'datalevel= {self.datalevel:s}')
            logging.debug (f'program= {self.program:s}')

        
        """send url to server to construct the select statement
        """
       
        param = dict()
        
        param['datalevel'] = self.datalevel
        param['program'] = self.program

        self.query_criteria (param, **kwargs)

        return
    
    def query_criteria (self, param, **kwargs):
        """
        'query_criteria' method allows the search of NEID data by multiple
        the parameters specified in a dictionary (param).

        Arguments:
            param (dict): a dictionary containing the following keys; datalevel, datetime, position, target.
                See individual methods for input formating. 
            outpath (string): (optional) a full output filepath of the returned metadata 
                table
            cookiepath (string): (optional) a full cookie file path saved from login for 
                querying the proprietary NEID data.
            token (string): (optional) a token string save in memory from login for querying 
                the proprietary NEID data; the token is only valid for the 
                current session.
            format (string): (optional) votable, ipac, csv, tsv  (default: ipac)
	        maxrec (integer): (optional) maximum records to be returned 
	            default: -1 or not specified will return all requested records

        """

        if (self.debug == 0):

            if ('debugfile' in kwargs):
            
                self.debug = 1
                self.debugfname = kwargs.get ('debugfile')

                if (len(self.debugfname) > 0):
      
                    logging.basicConfig (filename=self.debugfname, \
                        level=logging.DEBUG)
    
                    with open (self.debugfname, 'w') as fdebug:
                        pass

            if self.debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_criteria')
        
        
        """retrieve keyword parameters
        """
       
        if ('outpath' in kwargs): 
            self.outpath = kwargs.get('outpath')

        if self.debug:
            logging.debug ('')
            logging.debug (f'outpath= {self.outpath:s}')
        
        if ('cookiepath' in kwargs): 
            self.cookiepath = kwargs.get('cookiepath')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {self.cookiepath:s}')

        if ('token' in kwargs): 
            self.token = kwargs.get('token')

        if self.debug:
            logging.debug ('')
            logging.debug (f'token= {self.token:s}')

        len_param = len(param)

        if self.debug:
            logging.debug ('')
            logging.debug (f'len_param= {len_param:d}')

            for k,v in param.items():
                logging.debug (f'k, v= {k:s}, {str(v):s}')

        """send url to server to construct the select statement
        """
      
        self.format ='votable'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        self.maxrec = -1 
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')
        
        try:
            self.maxrec = float(self.maxrec)
            self.maxrec = int(self.maxrec)
        except Exception as e:
            print (f'Failed to convert maxrec: ' + str(self.maxrec) + \
                ' to integer.')
            return

        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:d}')

        data = urllib.parse.urlencode (param)

        """retrieve baseurl from conf class;

        during dev or test, baseurl will be a keyword input
        """
       
        self.baseurl = conf.server

        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')

        """urls for nph-tap.py, nph-neidLogin, nph-neidMakeQyery, 
        nph-neidDownload
        """

        self.tap_url = self.baseurl + 'TAP'
        self.makequery_url = self.baseurl + 'cgi-bin/NeidAPI/nph-neidMakequery.py?'

        if self.debug:
            logging.debug ('')
            logging.debug (f'tap_url= [{self.tap_url:s}]')
            logging.debug (f'makequery_url= [{self.makequery_url:s}]')

        url = self.makequery_url + data            

        if self.debug:
            logging.debug ('')
            logging.debug (f'url= {url:s}')

        query = ''
        try:
            query = self.__make_query (url) 

            if self.debug:
                logging.debug ('')
                logging.debug ('returned __make_query')
  
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'Error: {str(e):s}')
            
            print (str(e))
            return 
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'query= {query:s}')
       
        self.query = query


        """send tap query
        """

        self.tap = None
        if (len(self.cookiepath) > 0):
            
            if self.debug:
                logging.debug ('')
                logging.debug ('xxx0')
                logging.debug (f'cookiepath= {self.cookiepath:s}')
       
            if self.debug:
                
                try:
                    self.tap = NeidTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        cookiefile=self.cookiepath, \
	                debug=1)
                
                except Exception as e:
            
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 

            else:
                try:
                    self.tap = NeidTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        cookiefile=self.cookiepath)
                
                except Exception as e:
            
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 
        
        elif (len(self.token) > 0):
            
            if self.debug:
                logging.debug ('')
                logging.debug ('xxx1')
                logging.debug (f'token= {self.token:s}')
       
            if self.debug:
                
                try:
                    self.tap = NeidTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        token=self.token, \
	                debug=1)
                
                except Exception as e:
            
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 

            else:
                try:
                    self.tap = NeidTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        token=self.token)
                
                except Exception as e:
            
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 
        
        else: 
            if self.debug:
                try:
                    self.tap = NeidTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec, \
	                debug=1)
                
                except Exception as e:
            
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 
        
            else:
                try:
                    self.tap = NeidTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec)
        
                except Exception as e:
            
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 
        
        if self.debug:
            logging.debug('')
            logging.debug('NeidTap initialized')
            logging.debug('')
            logging.debug(f'query= {query:s}')

        print ('submitting request...')

        if self.debug:
            logging.debug('')
            logging.debug('call self.tap.send_async with debug')
            
            retstr = self.tap.send_async (query, \
                outpath=self.outpath, \
                format=self.format, \
                maxrec=self.maxrec, debug=1)
        else:
            logging.debug('')
            logging.debug('call self.tap.send_async NO debug')
            
            retstr = self.tap.send_async (query, \
                outpath=self.outpath, \
                format=self.format, \
                maxrec=self.maxrec)
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'return self.tap.send_async:')
            logging.debug (f'retstr= {retstr:s}')

        retstr_lower = retstr.lower()

        indx = retstr_lower.find ('error')
    
        if (indx >= 0):
            print (retstr)
            sys.exit()

        """no error: 
        """
       
        print (retstr)
        return
    

    def query_adql (self, query, **kwargs):
       
        """
        'query_adql' method receives a qualified ADQL query string from
	    user input.
        
        Arguments:
            query (string):  a ADQL query
            outpath (string): (optional) a full output filepath of the returned metadata 
                table
            cookiepath (string): (optional) a full cookie file path saved from login for 
                querying the proprietary NEID data.
            token (string): (optional) a token string save in memory from login for querying 
                the proprietary NEID data; the token is only valid for the 
                current session.
            format (string): (optional) votable, ipac, csv, tsv  (default: ipac)
	        maxrec (integer): (optional) maximum records to be returned 
	            default: -1 or not specified will return all requested records

        """
   
        if (self.debug == 0):

            if ('debugfile' in kwargs):
            
                self.debug = 1
                self.debugfname = kwargs.get ('debugfile')

                if (len(self.debugfname) > 0):
      
                    logging.basicConfig (filename=self.debugfname, \
                        level=logging.DEBUG)
    
                    with open (self.debugfname, 'w') as fdebug:
                        pass

            if self.debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_adql:')
        
        if (len(query) == 0):
            print ('Failed to find required parameter: query')
            return
        
        self.query = query
 
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug (f'query= {self.query:s}')
       
        if ('cookiepath' in kwargs): 
            self.cookiepath = kwargs.get('cookiepath')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {self.cookiepath:s}')

        self.outpath = ''
        if ('outpath' in kwargs): 
            self.outpath = kwargs.get('outpath')

        self.format = 'ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        self.maxrec = -1 
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')

        if self.debug:
            logging.debug ('')
            logging.debug (f'outpath= {self.outpath:s}')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:d}')

        """retrieve baseurl from conf class;
        """
        
        self.baseurl = conf.server

        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')

        """urls for nph-tap.py
        """

        self.tap_url = self.baseurl + 'TAP'

        if self.debug:
            logging.debug ('')
            logging.debug (f'tap_url= [{self.tap_url:s}]')

        """send tap query
        """

        self.tap = None

        if (len(self.cookiepath) > 0):
           
            if self.debug:
                self.tap = NeidTap (self.tap_url, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    cookiefile=self.cookiepath, \
	            debug=1)
            else:
                self.tap = NeidTap (self.tap_url, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    cookiefile=self.cookiepath)
        else: 
            if self.debug:
                self.tap = NeidTap (self.tap_url, \
                    format=self.format, \
                    maxrec=self.maxrec, \
	            debug=1)
            else:
                self.tap = NeidTap (self.tap_url, \
                    format=self.format, \
                    maxrec=self.maxrec)
        
        if self.debug:
            logging.debug('')
            logging.debug('NeidTap initialized')
            logging.debug(f'query= {query:s}')
            logging.debug('call self.tap.send_async')

        print ('submitting request...')

        if self.debug:
            if (len(self.outpath) > 0):
                retstr = self.tap.send_async (query, \
                    outpath=self.outpath, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    debug=1)
            else:
                retstr = self.tap.send_async (query, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    debug=1)
        else:
            if (len(self.outpath) > 0):
                retstr = self.tap.send_async (query, \
                    outpath=self.outpath, \
                    format=self.format, \
                    maxrec=self.maxrec)
            else:
                retstr = self.tap.send_async (query, \
                    format=self.format, \
                    maxrec=self.maxrec)
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'return self.tap.send_async:')
            logging.debug (f'retstr= {retstr:s}')

        retstr_lower = retstr.lower()

        indx = retstr_lower.find ('error')
    
        if (indx >= 0):
            print (retstr)
            sys.exit()

        """no error: 
        """

        print (retstr)
        return
    

    def print_data (self):


        if self.debug:
            logging.debug ('')
            logging.debug ('Enter neid.print_data:')

        try:
            self.tap.print_data ()
        except Exception as e:
                
            self.msg = 'Error print data: ' + str(e)
            print (self.msg)
        
        return
    
    def download (self, metapath, datalevel, format, outdir, **kwargs):
        """
            The download method allows users to download FITS files shown in 
            the retrieved metadata file. The column 'filepath' must be included
            in the metadata file columns in order to download files.
        
        Arguments:
            metapath (string): a full path metadata table obtained from running query methods
            datalevel (string): l0, l1, l2, eng, solarl0, solarl1, solarl2, solareng
            datetime (string): a datetime range string, see `query_datetime`
            format (string): metasata table's format: ipac, votable, csv, or tsv.
            outdir (string): the directory for depositing the returned files      
            cookiepath (string): (optional) cookie file path for downloading the proprietary NEID data.
            token (string): (optional) token string obtained from login.
            start_row (integer): (optional) starting row
            end_row (integer): (optional) ending row
            calibfile (bool): whether to download the associated calibration files (0/1); default is False.
        """
        
        if (self.debug == 0):

            if ('debugfile' in kwargs):
            
                self.debug = 1
                self.debugfname = kwargs.get ('debugfile')

                if (len(self.debugfname) > 0):
      
                    logging.basicConfig (filename=self.debugfname, \
                        level=logging.DEBUG)
    
                    with open (self.debugfname, 'w') as fdebug:
                        pass

            if self.debug:
                logging.debug ('')
                logging.debug ('debug turned on')
    
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter download:')
        
        if (len(metapath) == 0):
            print ('Failed to find required input parameter: metapath')
            return

        if (len(format) == 0):
            print ('Failed to find required input parameter: format')
            return

        if (len(outdir) == 0):
            print ('Failed to find required input parameter: outdir')
            return

        self.metapath = metapath
        self.format = format
        self.outdir = outdir

        if self.debug:
            logging.debug ('')
            logging.debug (f'metapath= {self.metapath:s}')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'outdir= {self.outdir:s}')

        self.token = ''
        if ('token' in kwargs): 
            self.token = kwargs.get('token')

        if self.debug:
            logging.debug ('')
            logging.debug (f'token= {self.token:s}')

        self.cookiepath = ''
        if ('cookiepath' in kwargs): 
            self.cookiepath = kwargs.get('cookiepath')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {self.cookiepath:s}')

        """token take precedence: only load cookie if token doesn't exist
        """

        cookiejar = None
        
        if (len(self.token) == 0):

            """{ load cookie to cookiejar 
            """

            if (len(self.cookiepath) > 0):
   
                cookiejar = http.cookiejar.MozillaCookieJar (self.cookiepath)

                try: 
                    cookiejar.load (ignore_discard=True, ignore_expires=True)
    
                    if self.debug:
                        logging.debug (\
                            f'cookie loaded from file: {self.cookiepath:s}')
        
                    for cookie in cookiejar:
                    
                        if self.debug:
                            logging.debug ('')
                            logging.debug ('cookie=')
                            logging.debug (cookie)
                            logging.debug (f'cookie.name= {cookie.name:s}')
                            logging.debug (f'cookie.value= {cookie.value:s}')
                            logging.debug (f'cookie.domain= {cookie.domain:s}')

                except Exception as e:
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'loadCookie exception: {str(e):s}')
                    pass

        """} end load cookie to cookiejar 
        """

        fmt_astropy = self.format
        if (self.format == 'tsv'):
            fmt_astropy = 'ascii.tab'
        if (self.format == 'csv'):
            fmt_astropy = 'ascii.csv'
        if (self.format == 'ipac'):
            fmt_astropy = 'ascii.ipac'

        """read metadata to astropy table
        """

        self.astropytbl = None
        try:
            self.astropytbl = Table.read (self.metapath, format=fmt_astropy)
        
        except Exception as e:
            self.msg = 'Failed to read metadata table to astropy table:' + \
                str(e) 
            print (self.msg)
            sys.exit()

        self.len_tbl = len(self.astropytbl)

        if self.debug:
            logging.debug ('')
            logging.debug ('self.astropytbl read')
            logging.debug (f'self.len_tbl= {self.len_tbl:d}')

        if (self.len_tbl == 0):
            print ('There is no data in the metadata table.')
            sys.exit()
   
        
        self.colnames = self.astropytbl.colnames

        if self.debug:
            logging.debug ('')
            logging.debug ('self.colnames:')
            logging.debug (self.colnames)
  
        self.len_col = len(self.colnames)

        if self.debug:
            logging.debug ('')
            logging.debug (f'self.len_col= {self.len_col:d}')

        filenamecol = datalevel + 'filename'
        filepathcol = datalevel + 'filepath'

        if self.debug:
            logging.debug ('')
            logging.debug (f'filenamecol= {filenamecol:s}')
            logging.debug (f'filepathcol= {filepathcol:s}')


        ind_filenamecol = -1
        ind_filepathcol = -1
        for i in range (0, self.len_col):

            if (self.colnames[i].lower() == filenamecol):
                ind_filenamecol = i

            if (self.colnames[i].lower() == filepathcol):
                ind_filepathcol = i
             
        if self.debug:
            logging.debug ('')
            logging.debug (f'ind_filenamecol= {ind_filenamecol:d}')
            logging.debug (f'ind_filepathcol= {ind_filepathcol:d}')
      
        if (ind_filenamecol == -1):

            msg = "Cannot find the necessary column: [" + filenamecol + \
                "] in the metadata table for downloading data."
            raise Exception (msg)

        
        if (ind_filepathcol == -1):

            msg = "Cannot find the necessary column: [" + filepathcol + \
                "] in the metadata table for downloading data."
            raise Exception (msg)

    
        calibfile = 0 
        
        """
        if ('calibfile' in kwargs): 
            calibfile = kwargs.get('calibfile')
         
        if self.debug:
            logging.debug ('')
            logging.debug (f'calibfile= {calibfile:d}')
        """

        srow = 0;
        erow = self.len_tbl - 1

        if ('start_row' in kwargs): 
            srow = kwargs.get('start_row')

        if self.debug:
            logging.debug ('')
            logging.debug (f'srow= {srow:d}')
     
        if ('end_row' in kwargs): 
            erow = kwargs.get('end_row')
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'erow= {erow:d}')
     
        if (srow < 0):
            srow = 0 
        if (erow > self.len_tbl - 1):
            erow = self.len_tbl - 1 
 
        if self.debug:
            logging.debug ('')
            logging.debug (f'srow= {srow:d}')
            logging.debug (f'erow= {erow:d}')
     

        """create outdir if it doesn't exist

        decimal mode work for both python2.7 and python3;

        0755 also works for python 2.7 but not python3
  
        convert octal 0775 to decimal: 493 
        """

        d1 = int ('0775', 8)

        if self.debug:
            logging.debug ('')
            logging.debug (f'd1= {d1:d}')
     
        try:
            os.makedirs (self.outdir, mode=d1, exist_ok=True) 

        except Exception as e:
            
            self.msg = 'Failed to create {self.outdir:s}:' + str(e) 
            print (self.msg)
            sys.exit()

        if self.debug:
            logging.debug ('')
            logging.debug ('returned os.makedirs') 


        """retrieve baseurl from conf class;
        """

        self.baseurl = conf.server

        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')

        """urls for nph-neidDownload.py
        """

        self.getneid_url = self.baseurl + 'cgi-bin/NeidAPI/nph-neidDownload.py?'

        if self.debug:
            logging.debug ('')
            logging.debug (f'self.getneid_url= {self.getneid_url:s}')


        filename = ''
        filepath = ''
        self.ndnloaded = 0
        self.ndnloaded_calib = 0
        self.ncaliblist = 0
     
        nfile = erow - srow + 1   
        
        print (f'Start downloading {nfile:d} FITS data you requested;')
        print (f'please check your outdir: {self.outdir:s} for  progress.')
 
        """{ download srow to erow
        """

        for l in range (srow, erow+1):
       
            if self.debug:
                logging.debug ('')
                logging.debug (f'l= {l:d}')
                logging.debug ('')
                logging.debug ('self.astropytbl[l]= ')
                logging.debug (self.astropytbl[l])

            filename = self.astropytbl[l][ind_filenamecol]
            filepath = self.astropytbl[l][ind_filepathcol]
	    
            if self.debug:
                logging.debug ('')
                logging.debug ('type(datalevel)= ')
                logging.debug (type(datalevel))
                logging.debug (type(datalevel) is bytes)
            
            if (type (filename) is bytes):
                
                if self.debug:
                    logging.debug ('')
                    logging.debug ('bytes: decode')

                filename = filename.decode("utf-8")
                filepath = filepath.decode("utf-8")
           
            if self.debug:
                logging.debug ('')
                logging.debug (f'l= {l:d} filename= {filename:s}')
                logging.debug (f'filepath= {filepath:s}')

            """get data files
            """

            url = self.getneid_url + 'datalevel=' + datalevel + \
                '&filepath=' + '/' + filepath + '&debug=1'
            
            filepath = self.outdir + '/' + filename 
                
            if self.debug:
                logging.debug ('')
                logging.debug (f'filepath= {filepath:s}')
                logging.debug (f'url= {url:s}')

            """if file doesn't exist: download
            """

            isExist = os.path.exists (filepath)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'isExist= {isExist:d}')

            if (not isExist):

                try:
                    self.__submit_request (url, filepath, cookiejar)
                    self.ndnloaded = self.ndnloaded + 1

                    self.msg =  'Returned file written to: ' + filepath   
           
                    if self.debug:
                        logging.debug ('')
                        logging.debug ('returned __submit_request')
                        logging.debug (f'self.msg= {self.msg:s}')
            
                except Exception as e:
                    print (f'File [{filename:s}] download: {str(e):s}')

        if self.debug:
            logging.debug ('')
            logging.debug (f'{self.len_tbl:d} files in the table;')
            logging.debug (f'{self.ndnloaded:d} files downloaded.')
            logging.debug (f'{self.ncaliblist:d} calibration list downloaded.')


        print (f'A total of new {self.ndnloaded:d} FITS files downloaded.')
 
        return

    def __submit_request(self, url, filepath, cookiejar):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter database.__submit_request:')
            logging.debug (f'url= {url:s}')
            logging.debug (f'filepath= {filepath:s}')
       
            if not (cookiejar is None):  
            
                for cookie in cookiejar:
                    
                    if self.debug:
                        logging.debug ('')
                        logging.debug ('cookie saved:')
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')
            
        try:
            self.response =  requests.get (url, cookies=cookiejar, \
                stream=True)

            if self.debug:
                logging.debug ('')
                logging.debug ('request sent')
        
        except Exception as e:
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: {str(e):s}')

            self.status = 'error'
            self.msg = 'Failed to submit the request: ' + str(e)
	    
            raise Exception (self.msg)
            return
                       
        if self.debug:
            logging.debug ('')
            logging.debug ('status_code:')
            logging.debug (self.response.status_code)
      
      
        if (self.response.status_code == 200):
            self.status = 'ok'
            self.msg = ''
        else:
            self.status = 'error'
            self.msg = 'Failed to submit the request'
	    
            raise Exception (self.msg)
            return
                       
            
        if self.debug:
            logging.debug ('')
            logging.debug ('headers: ')
            logging.debug (self.response.headers)
      
      
        self.content_type = ''
        try:
            self.content_type = self.response.headers['Content-type']
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception extract content-type: {str(e):s}')

        if self.debug:
            logging.debug ('')
            logging.debug (f'content_type= {self.content_type:s}')


        if (self.content_type == 'application/json'):
            
            if self.debug:
                logging.debug ('')
                logging.debug (\
                    'return is a json structure: might be error message')
            
            jsondata = json.loads (self.response.text)
          
            if self.debug:
                logging.debug ('')
                logging.debug ('jsondata:')
                logging.debug (jsondata)

 
            self.status = ''
            try: 
                self.status = jsondata['status']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'self.status= {self.status:s}')

            except Exception as e:

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'get status exception: e= {str(e):s}')

            self.msg = '' 
            try: 
                self.msg = jsondata['msg']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'self.msg= {self.msg:s}')

            except Exception as e:

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract msg exception: e= {str(e):s}')

            errmsg = ''        
            try: 
                errmsg = jsondata['error']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'errmsg= {errmsg:s}')

                if (len(errmsg) > 0):
                    self.status = 'error'
                    self.msg = errmsg

            except Exception as e:

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'get error exception: e= {str(e):s}')


            if self.debug:
                logging.debug ('')
                logging.debug (f'self.status= {self.status:s}')
                logging.debug (f'self.msg= {self.msg:s}')


            if (self.status == 'error'):
                raise Exception (self.msg)
                return

        """save to filepath
        """

        if self.debug:
            logging.debug ('')
            logging.debug ('save_to_file:')
       
        try:
            with open (filepath, 'wb') as fd:

                for chunk in self.response.iter_content (chunk_size=1024):
                    fd.write (chunk)
            
            self.msg =  'Returned file written to: ' + filepath   
            
            if self.debug:
                logging.debug ('')
                logging.debug (self.msg)
	
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: {str(e):s}')

            self.status = 'error'
            self.msg = 'Failed to save returned data to file: %s' % filepath
            
            raise Exception (self.msg)
            return

        return
    
    def __make_query (self, url):
       
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter __make_query:')
            logging.debug (f'url= {url:s}')

        response = None
        try:
            response = requests.get (url, stream=True)

            if self.debug:
                logging.debug ('')
                logging.debug ('request sent')

        except Exception as e:
           
            self.msg = 'Error: ' + str(e)

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)


        content_type = response.headers['content-type']

        if self.debug:
            logging.debug ('')
            logging.debug (f'content_type= {content_type:s}')
      
        query = ''
        if (content_type == 'application/json'):
                
            if self.debug:
                logging.debug ('')
                logging.debug (f'response.text: {response.text:s}')

            """error message
            """

            try:
                jsondata = json.loads (response.text)
                 
                if self.debug:
                    logging.debug ('')
                    logging.debug ('jsondata loaded')
                
                self.status = jsondata['status']
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'status: {self.status:s}')


                if (self.status == 'ok'):
                    query = jsondata['query']
                    
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'query: {self.query:s}')

                else:
                    self.msg = jsondata['msg']
                    
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'msg: {self.msg:s}')

                    raise Exception (self.msg)

            except Exception:
                self.msg = 'returned JSON object parse error'
                
                if self.debug:
                    logging.debug ('')
                    logging.debug ('JSON object parse error')
      
                
                raise Exception (self.msg)
            
        return (query)
    
class objLookup(object):
    """
        objLookup wraps ExoPlanet's web name resolver into a python class; 
        the exoLookup checks the exoplanet archive database and if that fails 
        it checks with the Sesame web service at CDS.  Sesame checks the CDS
        database and if that fails it checks NED.  So this class covers
        SIMBAD, NED, and ExoPlanet search.

    Arguments:
        object (char):  object name to be resolved
    """


    lookupurl = 'https://exoplanetarchive.ipac.caltech.edu/cgi-bin/Lookup/nph-lookup?'
    msg = ''
    status = ''

    url = ''
    response = None 

    source = ''
    input = ''
    objname = ''
    objtype = ''
    parsename= ''
    objdesc = ''
    ra2000= ''
    dec2000 = ''
    cra2000 = ''
    cdec2000 = ''

    debug = 0

    """{ objLookup.init
    """
    def __init__ (self, object, **kwargs):

        self.object = object

        if ('debug' in kwargs):
            self.debug = kwargs['debug']

        self.url = self.lookupurl + 'location=' + self.object

        if self.debug:
            logging.debug ('')
            logging.debug (f'url={self.url:s}')


        self.response = None 
        try:
            self.response = requests.get (self.url, stream=True)

            if self.debug:
                logging.debug ('')
                logging.debug (f'response:')
                logging.debug (self.response)

        except Exception as e:
            self.msg = f'submit request exception: {str(e):s}'
            raise Exception (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug (
                f'response.statu_code= {self.response.status_code:d}')

            logging.debug ('response.headers:')
            logging.debug (self.response.headers)

            logging.debug ('response.text:')
            logging.debug (self.response.text)


        content_type = ''
        try:
            content_type = self.response.headers['Content-type']
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'content_type= {content_type:s}')

        except Exception as e:
            self.msg = f'extract content_type exception: {str(e):s}'
            raise Exception (self.msg)


        jsondata = None
        try:
            jsondata = json.loads (self.response.text)

        except Exception as e:
            self.msg = f'load jsondata exception: {str(e):s}'
            raise Exception (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug ('jsondata:')
            logging.debug (jsondata)

        
        self.status = ''
        try:
            self.status = jsondata['stat']
            if self.debug:
                logging.debug ('')
                logging.debug (f'self.status= {self.status:s}')

        except Exception as e:

            self.msg = f'extract stat exception: {str(e):s}'
            if self.debug:
                logging.debug ('')
                logging.debug (f'self.msg= {self.msg:s}')
            
            raise Exception (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug (f'got here: status= {self.status:s}')
       
    
        if (self.status.lower() == 'ok'):
        
            """{  objLookup OK, extract parameters
            """

            if self.debug:
                logging.debug ('')
                logging.debug ('xxx1')
       
            try:
                self.source = jsondata['source']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract source exception: {str(e):s}')
    
            try:
                self.objname = jsondata['objname']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract objname exception: {str(e):s}')
                
            try:
                self.objtype = jsondata['objtype']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract objtype exception: {str(e):s}')
                
            try:
                self.objdesc = jsondata['objdesc']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract objdesc exception: {str(e):s}')
                
            try:
                self.parsename = jsondata['parsename']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract parsename exception: {str(e):s}')
                
            try:
                self.ra2000 = jsondata['ra2000']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract ra2000 exception: {str(e):s}')
                
            try:
                self.dec2000 = jsondata['dec2000']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract dec2000 exception: {str(e):s}')
                
            try:
                self.cra2000 = jsondata['cra2000']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract cra2000 exception: {str(e):s}')
                
            try:
                self.cdec2000 = jsondata['cdec2000']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract cdec20000 exception: {str(e):s}')
                
            if self.debug:
                logging.debug ('')
                
                logging.debug (f'dec2000= {self.dec2000:s}')
                logging.debug (f'source= {self.source:s}')
                logging.debug (f'objname= {self.objname:s}')
                logging.debug (f'objtype= {self.objtype:s}')
                logging.debug (f'objdesc= {self.objdesc:s}')
                logging.debug (f'parsename= {self.parsename:s}')
                logging.debug (f'ra2000= {self.ra2000:s}')
                logging.debug (f'dec2000= {self.dec2000:s}')
                logging.debug (f'cra2000= {self.cra2000:s}')
                logging.debug (f'cdec2000= {self.cdec2000:s}')

            """}  end objLookup OK, extract parameters
            """

        else:
            
            """{  objLookup Error, extract errmsg
            """

            if self.debug:
                logging.debug ('')
                logging.debug ('xxx2')
       
            self.status = 'error'
            try:
                self.msg = jsondata['msg']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'errmsg= {self.msg:s}')
        
            except Exception as e:

                self.msg = f'extract msg exception: {str(e):s}'
                raise Exception (self.msg)

            """}  end extract errmsg
            """

        return
    
class NeidTap(object):
    """
        NeidTap class provides client access to NEID's TAP service.
        Public data doesn't not require user login, optional NEID login via 
        NeidLogin class are used to search a user's proprietary data.

    Arguments:
        query (string): a SQL statement in specified query language
	    request (string): (optional) default 'doQuery'
	    lang (string): (optional) default 'ADQL'
	    phase (string): (optional) default 'RUN'
	    format (string): (optional) default 'votable'
	    maxrec (int): (optional) default '2000'
        cookiefile (string): a full path cookie file containing user info        
        debug (bool): default False

    Examples:
        >>> service = NeidTap(url, cookiefile=cookiepath)
        # or
        >>> service = NeidTap(url)
        # or
        >>> job = service.send_async (query, format='votable', request='doQuery', ...)
        # or    
        >>> job = service.send_sync (query, format='votable', request='doQuery', ...)

    """
    def __init__ (self, url, **kwargs):

        self.url = url 
        
        self.cookiename = ''
        self.cookiepath = ''
        
        self.async_job = 0 
        self.sync_job = 0
        
        self.response = None 
        self.response_result = None 
              
        self.outpath = ''
        
        self.debug = 0  
 
        self.datadict = dict()
        
        self.status = ''
        self.msg = ''

        """tapjob contains async job's status;
        resulttbl is the result of sync saved an astropy table 
        """

        self.tapjob = None
        self.astropytbl = None
        
        if ('debug' in kwargs):
            self.debug = kwargs.get('debug') 
 
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter neidtap.init (debug on)')
                                
        if ('cookiefile' in kwargs):
            self.cookiepath = kwargs.get('cookiefile')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {self.cookiepath:s}')


        self.request = 'doQuery'
        if ('request' in kwargs):
            self.request = kwargs.get('request')

        self.lang = 'ADQL'
        if ('lang' in kwargs):
            self.lang = kwargs.get('lang')

        self.phase = 'RUN'
        if ('phase' in kwargs):
           self.phase = kwargs.get('phase')

        self.format = 'votable'
        if ('format' in kwargs):
           self.format = kwargs.get('format')

        self.maxrec = '0'
        if ('maxrec' in kwargs):
           self.maxrec = kwargs.get('maxrec')

        self.propflag = 1 
        if ('propflag' in kwargs):
            self.propflag = kwargs.get('propflag')
            
        if self.debug:
            logging.debug ('')
            logging.debug (f'url= {self.url:s}')
            logging.debug (f'cookiepath= {self.cookiepath:s}')
            logging.debug (f'propflag= {self.propflag:d}')

        """turn on server debug
        """

        pid = os.getpid()
        self.datadict['request'] = self.request              
        self.datadict['lang'] = self.lang              
        self.datadict['phase'] = self.phase              
        self.datadict['format'] = self.format              
        self.datadict['maxrec'] = self.maxrec              
        self.datadict['propflag'] = self.propflag              

        for key in self.datadict:

            if self.debug:
                logging.debug ('')
                logging.debug (f'key= {key:s} val= {str(self.datadict[key]):s}')
    
        
        self.cookiejar = http.cookiejar.MozillaCookieJar (self.cookiepath)
         
        if self.debug:
            logging.debug ('')
            logging.debug ('cookiejar')
            logging.debug (self.cookiejar)
   
        if (len(self.cookiepath) > 0):
        
            try:
                self.cookiejar.load (ignore_discard=True, ignore_expires=True);
            
                if self.debug:
                    logging.debug (
                        'cookie loaded from %s' % self.cookiepath)
        
                    for cookie in self.cookiejar:
                        logging.debug ('cookie:')
                        logging.debug (cookie)
                        
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')
            except:
                if self.debug:
                    logging.debug ('NeidTap: loadCookie exception')
 
                self.msg = 'Error: failed to load cookie file.'
                raise Exception (self.msg) 

        return 
    
    def send_async (self, query, **kwargs):

        debug = 0

        if ('debug' in kwargs):
            debug = kwargs.get('debug') 

        if debug:
            logging.debug ('')
            logging.debug ('Enter send_async:')
 
        self.async_job = 1
        self.sync_job = 0

        url = self.url + '/async'

        if debug:
            logging.debug ('')
            logging.debug (f'url= {url:s}')
            logging.debug (f'query= {query:s}')

        self.datadict['query'] = query 

        """for async query, there is no maxrec limit
        """

        self.maxrec = '0'

        if ('format' in kwargs):
            
            self.format = kwargs.get('format')
            self.datadict['format'] = self.format              

            if debug:
                logging.debug ('')
                logging.debug (f'format= {self.format:s}')
            
        if ('maxrec' in kwargs):
            
            self.maxrec = kwargs.get('maxrec')
            self.datadict['maxrec'] = self.maxrec              
            
            if debug:
                logging.debug ('')
                logging.debug (f'maxrec= {self.maxrec:d}')
        
        for key in self.datadict:

            if self.debug:
                logging.debug ('')
                logging.debug (f'key= {key:s} val= {str(self.datadict[key]):s}')
    
        self.oupath = ''
        if ('outpath' in kwargs):
            self.outpath = kwargs.get('outpath')
  
        try:

            if (len(self.cookiepath) > 0):
        
                self.response = requests.post (url, data= self.datadict, \
	            cookies=self.cookiejar, allow_redirects=False)
            else: 
                self.response = requests.post (url, data= self.datadict, \
	            allow_redirects=False)

            if debug:
                logging.debug ('')
                logging.debug ('request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)

     
        self.statusurl = ''

        if debug:
            logging.debug ('')
            logging.debug (f'status_code= {self.response.status_code:d}')
            logging.debug ('self.response: ')
            logging.debug (self.response)
            logging.debug ('self.response.headers: ')
            logging.debug (self.response.headers)
            
        if debug:
            logging.debug ('')
            logging.debug (f'status_code= {self.response.status_code:d}')
            
        """if status_code != 303: probably error message
        """

        if (self.response.status_code != 303):
            
            if debug:
                logging.debug ('')
                logging.debug ('case: not re-direct')
       
            self.content_type = self.response.headers['Content-type']
            self.encoding = self.response.encoding
        
            if debug:
                logging.debug ('')
                logging.debug (f'content_type= {self.content_type:s}')
                logging.debug ('encoding= ')
                logging.debug (self.encoding)


            data = None
            self.status = ''
            self.msg = ''
           
            if (self.content_type == 'application/json'):
        
                """error message
                """

                if debug:
                    logging.debug ('')
                    logging.debug ('self.response:')
                    logging.debug (self.response.text)
      
                try:
                    data = self.response.json()
                    
                except Exception as e:
                
                    if debug:
                        logging.debug ('')
                        logging.debug (f'JSON object parse error: {str(e):s}')
      
                    self.status = 'error'
                    self.msg = 'JSON parse error: ' + str(e)
                
                    if debug:
                        logging.debug ('')
                        logging.debug (f'status= {self.status:s}')
                        logging.debug (f'msg= {self.msg:s}')

                    return (self.msg)

                self.status = data['status']
                self.msg = data['msg']
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'status= {self.status:s}')
                    logging.debug (f'msg= {self.msg:s}')

                if (self.status == 'error'):
                    self.msg = 'Error: ' + data['msg']
                    return (self.msg)

        """retrieve statusurl
        """

        self.statusurl = ''
        if (self.response.status_code == 303):
            self.statusurl = self.response.headers['Location']

        if debug:
            logging.debug ('')
            logging.debug (f'statusurl= {self.statusurl:s}')

        if (len(self.statusurl) == 0):
            self.msg = 'Error: failed to retrieve statusurl from re-direct'
            return (self.msg)

        """create tapjob to save status result
        """
        try:
            if (debug):
                self.tapjob = TapJob (\
                    self.statusurl, debug=1)
            else:
                self.tapjob = TapJob (\
                    self.statusurl)
        
            if debug:
                logging.debug ('')
                logging.debug (f'tapjob instantiated')
                logging.debug (f'phase= {self.tapjob.phase:s}')
       
       
        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)    
        
        """loop until job is complete and download the data
        """
        
        phase = self.tapjob.phase
        
        if debug:
            logging.debug ('')
            logging.debug (f'phase: {phase:s}')
            
        if ((phase.lower() != 'completed') and (phase.lower() != 'error')):
            
            while ((phase.lower() != 'completed') and \
                (phase.lower() != 'error')):
                
                time.sleep (2)
                phase = self.tapjob.get_phase()
        
                if debug:
                    logging.debug ('')
                    logging.debug ('here0-1')
                    logging.debug (f'phase= {phase:s}')
            
        if debug:
            logging.debug ('')
            logging.debug ('here0-2')
            logging.debug (f'phase= {phase:s}')
            
        """phase == 'error'
        """
        if (phase.lower() == 'error'):
	   
            self.status = 'error'
            self.msg = self.tapjob.errorsummary
        
            if debug:
                logging.debug ('')
                logging.debug (f'returned get_errorsummary: {self.msg:s}')
            
            return (self.msg)

        if debug:
            logging.debug ('')
            logging.debug ('here2: phase is completed')
            
        """phase == 'completed' 
        """
        self.resulturl = self.tapjob.resulturl
        if debug:
            logging.debug ('')
            logging.debug (f'resulturl= {self.resulturl:s}')

        """send resulturl to retrieve result table
        """
        try:
            self.response_result = requests.get (self.resulturl, stream=True)
        
            if debug:
                logging.debug ('')
                logging.debug ('resulturl request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
       
        """save table to file
        """
        if (len(self.outpath) > 0):
           
            if debug:
                logging.debug ('')
                logging.debug ('write data to outpath:')

            self.msg = self.save_data (self.outpath)
            
            if debug:
                logging.debug ('')
                logging.debug (f'returned save_data: msg= {self.msg:s}')

            return (self.msg)

    def send_sync (self, query, **kwargs):

       
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter send_sync:')
            logging.debug (f'query= {query:s}')
 
        url = self.url + '/sync'

        if self.debug:
            logging.debug ('')
            logging.debug (f'url= {url:s}')

        self.sync_job = 1
        self.async_job = 0
        self.datadict['query'] = query
    
        """optional parameters: format, maxrec, self.outpath
        """

        self.maxrec = '0'

        if ('format' in kwargs):
            
            self.format = kwargs.get('format')
            self.datadict['format'] = self.format              

        
            if self.debug:
                logging.debug ('')
                logging.debug (f'format= {self.format:s}')
            
        if ('maxrec' in kwargs):
            
            self.maxrec = kwargs.get('maxrec')
            self.datadict['maxrec'] = self.maxrec              
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'maxrec= {self.maxrec:s}')
        
        self.outpath = ''
        if ('outpath' in kwargs):
            self.outpath = kwargs.get('outpath')
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'outpath= {self.outpath:s}')
	
        try:
            if (len(self.cookiepath) > 0):
        
                self.response = requests.post (url, data= self.datadict, \
                    cookies=self.cookiejar, allow_redirects=False, stream=True)
            else: 
                self.response = requesrs.post (url, data= self.datadict, \
                    allow_redicts=False, stream=True)

            if self.debug:
                logging.debug ('')
                logging.debug ('request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)

        """re-direct case not implemented for send_sync
    
    	if (self.response.status_code == 303):
                self.resulturl = self.response.headers['Location']
        """

        self.content_type = self.response.headers['Content-type']
        self.encoding = self.response.encoding

        if self.debug:
            logging.debug ('')
            logging.debug (f'content_type= {self.content_type:s}')
       
        data = None
        self.status = ''
        self.msg = ''
        if (self.content_type == 'application/json'):
    
            """error message
            """
            try:
                data = self.response.json()
            except Exception:
                if self.debug:
                    logging.debug ('')
                    logging.debug ('JSON object parse error')
      
                self.status = 'error'
                self.msg = 'Error: returned JSON object parse error'
                
                return (self.msg)
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'status= {self.status:s}')
                logging.debug (f'msg= {self.msg:s}')
     
        """save table to file
        """

        if self.debug:
            logging.debug ('')
            logging.debug ('got here')

        self.msg = self.save_data (self.outpath)
            
        if self.debug:
            logging.debug ('')
            logging.debug (f'returned save_data: msg= {self.msg:s}')

        return (self.msg)

    """} end NeidTap.send_sync
    """

    
    """{ NeidTap.save_data: save data to astropy table
    """
    def save_data (self, outpath):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter save_data:')
            logging.debug (f'outpath= {outpath:s}')
      
        tmpfile_created = 0

        fpath = ''
        if (len(outpath) >  0):
            fpath = outpath
        else:
            fd, fpath = tempfile.mkstemp(suffix='.xml', dir='./')
            tmpfile_created = 1 
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'tmpfile_created = {tmpfile_created:d}')

        if self.debug:
            logging.debug ('')
            logging.debug (f'fpath= {fpath:s}')
     
        fp = open (fpath, "wb")
            
        for data in self.response_result.iter_content(4096):
                
            len_data = len(data)            
        
            if (len_data < 1):
                break

            fp.write (data)
        
        fp.close()

        if self.debug:
            logging.debug ('')
            logging.debug (f'data written to file: {fpath:s}')
                
        if (len(self.outpath) >  0):
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx1')
                
            self.msg = 'Result downloaded to file [' + self.outpath + ']'
        else:
    
            """read temp outpath to astropy table
            """

            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx2')
                
            self.astropytbl = Table.read (fpath, format='votable')	    
            self.msg = 'Result saved in memory (astropy table).'
      
        if self.debug:
            logging.debug ('')
            logging.debug (f'{self.msg:s}')
     
        if (tmpfile_created == 1):
            os.remove (fpath)
            
            if self.debug:
                logging.debug ('')
                logging.debug ('tmpfile {fpath:s} deleted')

        return (self.msg)
    
    """} end NeidTap.save_data
    """


    """{ NeidTap.print_data: use astropy function to print data
    """
    def print_data (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter print_data:')

        try:

            """
            len_table = len (self.astropytbl)
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'len_table= {len_table:d}')
       
            for i in range (0, len_table):
	    
                row = self.astropytbl[i]
                print (row)
            """

            self.astropytbl.pprint()

        except Exception as e:
            
            raise Exception (str(e))

        return

    """} end NeidTap.print_data
    """


    """{ NeidTap.get_data
    """
    def get_data (self, resultpath):
    
        """ loop until job is complete, then download the data to the 
        given resultpath
        """
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_data:')
            logging.debug (f'async_job = {self.async_job:d}')
            logging.debug (f'resultpath = {resultpath:s}')



        if (self.async_job == 0):
    
            """sync data is in astropytbl
            """
            self.astropytbl.write (resultpath)

            if self.debug:
                logging.debug ('')
                logging.debug ('astropytbl written to resultpath')

            self.msg = 'Result written to file: [' + resultpath + ']'
        
        else:
            phase = self.tapjob.get_phase()
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'returned tapjob.get_phase: phase= {phase:s}')

            while ((phase.lower() != 'completed') and \
	        (phase.lower() != 'error')):
                time.sleep (2)
                phase = self.tapjob.get_phase()
        
                if self.debug:
                    logging.debug ('')
                    logging.debug (\
                        f'returned tapjob.get_phase: phase= {phase:s}')

            """ phase == 'error'
            """
            if (phase.lower() == 'error'):
	   
                self.status = 'error'
                self.msg = self.tapjob.errorsummary
        
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'returned get_errorsummary: {self.msg:s}')
            
                return (self.msg)

            """job completed write table to disk file
            """
            try:
                self.tapjob.get_result (resultpath)

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'returned tapjob.get_result')
        
            except Exception as e:
            
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
            
                return (self.msg)    
        
            if self.debug:
                logging.debug ('')
                logging.debug ('got here: download result successful')

            self.status = 'ok'
            self.msg = 'Result downloaded to file: [' + resultpath + ']'

        if self.debug:
            logging.debug ('')
            logging.debug (f'self.msg = {self.msg:s}')
       
        return (self.msg) 
    
class TapJob:
    """
    TapJob class is used internally by TapClient class to store a Tap job's 
    parameters and returned job status and result urls.
    """

    def __init__ (self, statusurl, **kwargs):
        self.debug = 0 
        
        self.statusurl = statusurl

        self.status = ''
        self.msg = ''
        
        self.statusstruct = ''
        self.job = ''

        self.jobid = ''
        self.processid = ''
        self.ownerid = 'None'
        self.quote = 'None'
        self.phase = ''
        self.starttime = ''
        self.endtime = ''
        self.executionduration = ''
        self.destruction = ''
        self.errorsummary = ''
        
        self.parameters = ''
        self.resulturl = ''

        if ('debug' in kwargs):
           
            self.debug = kwargs.get('debug')
           
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter Tapjob (debug on)')
                                
        try:
            self.__get_statusjob()
         
            if self.debug:
                logging.debug ('')
                logging.debug ('returned __get_statusjob')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
        
        if self.debug:
            logging.debug ('')
            logging.debug ('done TapJob.init:')

        return     
    
    def get_status (self):
        
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_status')
            logging.debug (f'phase= {self.phase:s}')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        return (self.statusstruct)
    
    def get_resulturl (self):
        
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_resulturl')
            logging.debug (f'phase= {self.phase:s}')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        return (self.resulturl)
    
    def get_result (self, outpath):
        
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_result')
            logging.debug (f'resulturl= {self.resulturl:s}')
            logging.debug (f'outpath= {outpath:s}')

        if (len(outpath) == 0):
            self.status = 'error'
            self.msg = 'Output file path is required.'
            return

        
        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned __get_statusjob')
                    logging.debug (f'resulturl= {self.resulturl:s}')

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                
                raise Exception (self.msg)    
    

        if (len(self.resulturl) == 0):
  
            self.get_resulturl()            
            self.msg = 'Failed to retrieve resulturl from status structure.'
            raise Exception (self.msg)    
	    
        try:
            response = requests.get (self.resulturl, stream=True)
        
            if self.debug:
                logging.debug ('')
                logging.debug ('resulturl request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
        """retrieve table from response
        """
        with open (outpath, "wb") as fp:
            
            for data in response.iter_content(4096):
                
                len_data = len(data)            

                """comment block
                if debug:
                    logging.debug ('')
                    logging.debug (f'len_data= {len_data:d}')
                """

                if (len_data < 1):
                    break

                fp.write (data)
        fp.close()
        
        self.resultpath = outpath
        self.status = 'ok'
        self.msg = 'returned table written to output file: ' + outpath
        
        if self.debug:
            logging.debug ('')
            logging.debug ('done writing result to file')
            
        return        
    
    def get_parameters (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_parameters')
            logging.debug ('parameters:')
            logging.debug (self.parameters)

        return (self.parameters)
    

    def get_phase (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_phase')
            logging.debug (f'self.phase= {self.phase:s}')

        if ((self.phase.lower() != 'completed') and \
	    (self.phase.lower() != 'error')):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

            if self.debug:
                logging.debug ('')
                logging.debug (f'phase= {self.phase:s}')

        return (self.phase)
    
    
    
    def get_jobid (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_jobid')

        if (len(self.jobid) == 0):
            self.jobid = self.job['uws:jobId']

        if self.debug:
            logging.debug ('')
            logging.debug (f'jobid= {self.jobid:s}')

        return (self.jobid)
    
    
    def get_processid (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_processid')

        if (len(self.processid) == 0):
            self.processid = self.job['uws:processId']

        if self.debug:
            logging.debug ('')
            logging.debug (f'processid= {self.processid:s}')

        return (self.processid)
    

    def get_starttime (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_starttime')

        if (len(self.starttime) == 0):
            self.starttime = self.job['uws:startTime']

        if self.debug:
            logging.debug ('')
            logging.debug (f'starttime= {self.starttime:s}')

        return (self.starttime)
    

    def get_endtime (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_endtime')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        self.endtime = self.job['uws:endTime']

        if self.debug:
            logging.debug ('')
            logging.debug (f'endtime= {self.endtime:s}')

        return (self.endtime)
    


    def get_executionduration (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_executionduration')

        
        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        self.executionduration = self.job['uws:executionDuration']

        if self.debug:
            logging.debug ('')
            logging.debug (f'executionduration= {self.executionduration:s}')

        return (self.executionduration)


    def get_destruction (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_destruction')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        self.destruction = self.job['uws:destruction']

        if self.debug:
            logging.debug ('')
            logging.debug (f'destruction= {self.destruction:s}')

        return (self.destruction)
    
    def get_errorsummary (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_errorsummary')

        if ((self.phase.lower() != 'error') and \
	    (self.phase.lower() != 'completed')):
        
            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = 'Error: ' + str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   
	
        if ((self.phase.lower() != 'error') and \
	    (self.phase.lower() != 'completed')):
        
            self.msg = 'The process is still running.'
            if self.debug:
                logging.debug ('')
                logging.debug (f'msg= {self.msg:s}')

            return (self.msg)
	
        elif (self.phase.lower() == 'completed'):
            
            self.msg = 'Process completed without error message.'
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'msg= {self.msg:s}')

            return (self.msg)
        
        elif (self.phase.lower() == 'error'):

            self.errorsummary = self.job['uws:errorSummary']['uws:message']

            if self.debug:
                logging.debug ('')
                logging.debug (f'errorsummary= {self.errorsummary:s}')

            return (self.errorsummary)
    
    def __get_statusjob (self):

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter __get_statusjob')
            logging.debug (f'statusurl= {self.statusurl:s}')

        """ self.status doesn't exist, call get_status
        """
        try:
            self.response = requests.get (self.statusurl, stream=True)
            
            if self.debug:
                logging.debug ('')
                logging.debug ('statusurl request sent')

        except Exception as e:
           
           
            self.msg = 'Error: ' + str(e)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
        if self.debug:
            logging.debug ('')
            logging.debug ('response returned')
            logging.debug (f'status_code= {self.response.status_code:d}')

        if self.debug:
            logging.debug ('')
            logging.debug ('response.text= ')
            logging.debug (self.response.text)
        
        self.statusstruct = self.response.text

        if self.debug:
            logging.debug ('')
            logging.debug ('statusstruct= ')
            logging.debug (self.statusstruct)
        
        """ parse returned status xml structure for parameters
        """
        soup = bs.BeautifulSoup (self.statusstruct, 'lxml')
            
        if self.debug:
            logging.debug ('')
            logging.debug ('soup initialized')
        
        self.parameters = soup.find('uws:parameters')
        
        if self.debug:
            logging.debug ('')
            logging.debug ('self.parameters:')
            logging.debug (self.parameters)
        
        
        """convert status xml structure to dictionary doc 
        """
        doc = xmltodict.parse (self.response.text)
        self.job = doc['uws:job']

        self.phase = self.job['uws:phase']
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'self.phase.lower():{ self.phase.lower():s}')
        
       
        if (self.phase.lower() == 'completed'):

            if self.debug:
                logging.debug ('')
                logging.debug ('xxx1: got here')
            
            results = self.job['uws:results']
        
            if self.debug:
                logging.debug ('')
                logging.debug ('results')
                logging.debug (results)
            
            result = self.job['uws:results']['uws:result']
        
            if self.debug:
                logging.debug ('')
                logging.debug ('result')
                logging.debug (result)
            

            self.resulturl = \
                self.job['uws:results']['uws:result']['@xlink:href']
        
        elif (self.phase.lower() == 'error'):
            self.errorsummary = self.job['uws:errorSummary']['uws:message']


        if self.debug:
            logging.debug ('')
            logging.debug ('self.job:')
            logging.debug (self.job)
            logging.debug (f'self.phase.lower(): {self.phase.lower():s}')
            logging.debug (f'self.resulturl: {self.resulturl:s}')

        return
    

Neid = Archive()
print ('Neid instantiated')
