#!/usr/bin/env python
#********************************************************************
# A script to continuously download ACE data during the day to keep 
# the files up-to-date.
# Currently running every 15 mins - :07,:22,:37,:52.
#
# Created 2015-02-17 by RLB. Updated 2020-12-23 by RL.
# Adapted from /home/leon/CronScripts/saturn_wikactivity.py
# Activated as cronjob on saturn 2015-03-02.
#
#********************************************************************

from magpy.stream import read
from magpy.database import * 
import magpy.opt.cred as mpcred
from pyproj import Geod
import getopt
import pwd
import socket


coredir = os.path.abspath(os.path.join('/home/cobs/MARTAS', 'core'))
coredir = os.path.abspath(os.path.join('/home/leon/Software/MARTAS', 'core'))
sys.path.insert(0, coredir)
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, getstringdate

def downloadACEdata():


def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    debug=False
    endtime = datetime.utcnow()
    joblist = ['NEIC','AT']
    stb = DataStream()
    sta = DataStream()
    errorcntAT = 0
    errorcntNE = 0
    uploadcheck = 1
    path = '/tmp/neic_quakes.d' 

    try:
        opts, args = getopt.getopt(argv,"hc:e:j:p:s:o:D",["config=","endtime=","joblist=","debug="])
    except getopt.GetoptError:
        print ('neic_download.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- neic_download.py will determine the primary instruments --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python neic_download.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-e            : endtime, default is now')
            print ('-j            : joblist: NEIC, AT')
            print ('-p            : path for neic data')
            print ('-------------------------------------')
            print ('Application:')
            print ('python neic_download.py -c /etc/marcos/analysis.cfg -p /home/cobs/ANALYSIS/Seismo/neic_quakes.d')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-e", "--endtime"):
            # get an endtime
            endtime = arg.split(',')
        elif opt in ("-j", "--joblist"):
            # get an endtime
            joblist = arg.split(',')
        elif opt in ("-p", "--path"):
            path = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running flagging version {}".format(version))
    print ("--------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname='mm-dp-flagging.log', debug=debug)

    namea = "{}-quakes-AT".format(config.get('logname'))
    nameb = "{}-quakes-NEIC".format(config.get('logname'))
    currentvaluepath = config.get('currentvaluepath')


# Download and then sync with cobs
curl ftp://ftp.swpc.noaa.gov/pub/lists/ace/ace_swepam_1m.txt -s > test.txt







import os, sys, pytz
from magpy.stream import *
from magpy.transfer import *
import magpy.mpplot as mp
from ace_download_daily import *
from dateutil.tz import tzutc



#--------------------------------------------------------------------
# General
#--------------------------------------------------------------------

localpath = '/srv/archive/external/esa-nasa/ace'
ftppath = '/pub/lists/ace/'
filestr_swe = 'ace_swepam_1m.txt'
filestr_mag = 'ace_mag_1m.txt'
filestr_sis = 'ace_sis_5m.txt'
filestr_epa = 'ace_epam_5m.txt'
myproxy = "138.22.156.44"
port = 8021
login = 'anonymous@ftp.swpc.noaa.gov'
passwd = 'anonymous'

now = datetime.utcnow()
utc_pytz = pytz.timezone('UTC')
now = datetime(now.year, now.month, now.day, now.hour, now.minute, tzinfo=utc_pytz)

logging.basicConfig(level = logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s - %(message)s')
logger_ace = logging.getLogger('ACE-DD')
handler = logging.FileHandler(os.path.join(localpath,'ace_min_processing.log'))
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger_ace.addHandler(handler)


def merge_ACE(streama, streamb, keys):
    # Merge streamb into streama without interpolation

    for key in keys:
        a = streamb._get_column(key)
        streama._put_column(a, key)
        streama.header['col-'+key] = streamb.header['col-'+key]
        streama.header['unit-col-'+key] = streamb.header['unit-col-'+key]

    return streama


def process_ACE(ace_P, ace_types, merge_variables, logger_ace, skipcompression=False):
    """
    Processes new data and adds it to old stream.
    
    INPUT:
    ace_P:             (str) String describing data type: '1m' or '5m'
    ace_types   :       (list) List of type for type, e.g. ["swepam" and "mag"]
    marge_variables:    (list) Variables to merge from type 2 into 1, e.g.:
                        ['x','y','z','f','t1','t2']
    """
    
    print("Processing %s data..." % ace_P)
    newday = False
    
    # Read current data
    ace_stream1 = read(os.path.join(localpath,'raw','ace_%s_%s.txt' % (ace_types[0], ace_P)))
    ace_stream2 = read(os.path.join(localpath,'raw','ace_%s_%s.txt' % (ace_types[1], ace_P)))

    lastval = num2date(ace_stream1.ndarray[0][-1])
    today = datetime.strftime(lastval, "%Y-%m-%d")
    yesterday = datetime.strftime(lastval-timedelta(days=1), "%Y-%m-%d")

    ace_file = os.path.join(localpath,'collected','ace_%s_%s.cdf' % (ace_P, today))
    ace_lastfile = os.path.join(localpath,'collected','ace_%s_%s.cdf' % (ace_P, yesterday))

    lastfile = True
    if os.path.exists(ace_file):
        try:
            ace_last = read(ace_file)
        except:
            lastfile = False
    else:
        try:
            ace_last = read(ace_lastfile)
            newday = True
        except:
            lastfile = False

    if lastfile:
        if len(ace_last.ndarray[KEYLIST.index("var1")]) == 0:
            lastfile = False

    # Merging streams wrt time with no interpolation:
    ace_data = merge_ACE(ace_stream1, ace_stream2, merge_variables)

    if lastfile:
         append == True
         for key in merge_variables+['var1','var2','var3']:
             keyind = KEYLIST.index(key)
             if len(ace_last.ndarray[keyind]) == 0:
                 print keyind, len(ace_last.ndarray[keyind])
                 print("Error in data - not appending.")
                 append == False
         if append:
             ace_data = appendStreams([ace_last, ace_data])
  
    if newday == True:
        logger_ace.info("Created new %s file. Writing last data to yesterday." % ace_P)
        ace_data.write(os.path.join(localpath,'collected'),filenamebegins="ace_%s_" % ace_P,
                format_type='PYCDF', skipcompression=skipcompression)
    else:
        ace_data = ace_data.trim(starttime=today+"T00:00:00")
        ace_data.write(os.path.join(localpath,'collected'),filenamebegins="ace_%s_" % ace_P,
                format_type='PYCDF', skipcompression=skipcompression) # XXX

#####################################################################
# Job #1: Keep updated ACE data 
#####################################################################

# Download most recent data (last two hours):
try:
    FTPGET(myproxy,login,passwd,ftppath,os.path.join(localpath,'raw'),filestr_swe,port)
    FTPGET(myproxy,login,passwd,ftppath,os.path.join(localpath,'raw'),filestr_mag,port)
    FTPGET(myproxy,login,passwd,ftppath,os.path.join(localpath,'raw'),filestr_epa,port)
    FTPGET(myproxy,login,passwd,ftppath,os.path.join(localpath,'raw'),filestr_sis,port)
except:
    logger_ace.error("Error downloading files from ftp!")

#--------------------------------------------------------------------
# PROCESS 1-MIN DATA
#--------------------------------------------------------------------

process_ACE('1m', ['swepam', 'mag'], ['x','y','z','f','t1','t2'], logger_ace)

#--------------------------------------------------------------------
# PROCESS 5-MIN DATA
#--------------------------------------------------------------------

process_ACE('5m', ['epam', 'sis'], ['x','y'], logger_ace, skipcompression=True)


