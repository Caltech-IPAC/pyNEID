"""
The pyneid package is NEID's (.....) python client interface for querying 
NEID's database and downloading NEID data.
"""
from astropy import config as _config

class Conf(_config.ConfigNamespace):
    
    """
    Configuration parameters for 'astroquery.koa'.
    """
    server = _config.ConfigItem (
        ['https://neid.ipac.caltech.edu/cgi-bin/'],
        'Name of the NEID server to use.') 

    timeout = _config.ConfigItem (
        60,
        'Time limit for connecting to NEID server.')

conf = Conf()
from .neid import Archive, NeidTap, TapJob

__all__ = ['Archive', 'NeidTap', 'TapJob', 
           'Conf', 'conf',
           ] 