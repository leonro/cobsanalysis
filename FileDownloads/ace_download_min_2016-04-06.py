#!/usr/bin/env python
#********************************************************************
# A script to continuously download ACE data during the day to keep 
# the files up-to-date.
# Currently running every 15 mins - :07,:22,:37,:52.
# NOTE:
# This routine has a twin sister running on Saturn. Any changes
# here should be duplicated on Saturn.
#
# Created 2015-02-17 by RLB.
# Adapted from /home/leon/CronScripts/saturn_wikactivity.py
# Activated as cronjob on saturn 2015-03-02.
#
#********************************************************************

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
#today = datetime.strftime(now,'%Y-%m-%d')
#beforenow = now - timedelta(days=1)
#yesterday = datetime.strftime(beforenow,'%Y-%m-%d')
#yesterday2 = datetime.strftime(beforenow,'%Y%m%d')

filenamebegins1 = 'ace_1m_'
filenamebegins5 = 'ace_5m_'
fileformat = 'PYCDF'
process1min = True
process5min = True

logging.basicConfig(level = logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s - %(message)s')
logger_ace = logging.getLogger('ACE-DD')
handler = logging.FileHandler(os.path.join(localpath,'ace_min_processing.log'))
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger_ace.addHandler(handler)

def mergeACE(streama, streamb, keys):
    # Merge streamb into streama without interpolation

    for key in keys:
        a = streamb._get_column(key)
        streama._put_column(a, key)
        streama.header['col-'+key] = streamb.header['col-'+key]
        streama.header['unit-col-'+key] = streamb.header['unit-col-'+key]

    return streama

#####################################################################
# Job #1: Keep updated ACE data 
#####################################################################

# Download most recent data (last two hours):
try:
    FTPGET(myproxy,login,passwd,ftppath,os.path.join(localpath,'raw'),filestr_swe,port)
    FTPGET(myproxy,login,passwd,ftppath,os.path.join(localpath,'raw'),filestr_mag,port)
    FTPGET(myproxy,login,passwd,ftppath,os.path.join(localpath,'raw'),filestr_epa,port)
    FTPGET(myproxy,login,passwd,ftppath,os.path.join(localpath,'raw'),filestr_sis,port)
    #ftpget(localpath=os.path.join(localpath,'raw'), ftppath=ftppath, filestr=filestr_swe,
	#	myproxy=myproxy, port=port, login=login, passwd=passwd)
    #ftpget(localpath=os.path.join(localpath,'raw'), ftppath=ftppath, filestr=filestr_mag,
	#	myproxy=myproxy, port=port, login=login, passwd=passwd)
    #ftpget(localpath=os.path.join(localpath,'raw'), ftppath=ftppath, filestr=filestr_epa,
	#	myproxy=myproxy, port=port, login=login, passwd=passwd)
    #ftpget(localpath=os.path.join(localpath,'raw'), ftppath=ftppath, filestr=filestr_sis,
	#	myproxy=myproxy, port=port, login=login, passwd=passwd)
except:
    logger_ace.error("Error downloading files from ftp!")

#--------------------------------------------------------------------
# PROCESS 1-MIN DATA
#--------------------------------------------------------------------

if process1min:
    print("Processing 1-min data...")
    ace_swe = read(os.path.join(localpath,'raw',filestr_swe))
    ace_mag = read(os.path.join(localpath,'raw',filestr_mag),dataformat='NOAAACE')

    lastval = num2date(ace_swe.ndarray[0][-1])
    today = datetime.strftime(lastval, "%Y-%m-%d")
    yesterday = datetime.strftime(lastval-timedelta(days=1), "%Y-%m-%d")

    ace1_file = os.path.join(localpath,'collected',filenamebegins1+today+'.cdf')
    ace1_lastfile = os.path.join(localpath,'collected',filenamebegins1+yesterday+'.cdf')

    if os.path.exists(ace1_file):
        try:
            ace1_last = read(ace1_file)
            newday = False
        except:
            logger_ace.warning("Cannot read 1m file from today. Using 2015-10-27.")
            print(os.path.join(localpath,'collected','ace_1m_2015-10-27.cdf'))
            ace1_last = read(os.path.join(localpath,'collected','ace_1m_2015-10-27.cdf'))
            newday = True
    else:
        newday = True
        try:
            ace1_last = read(ace1_lastfile)
        except:
            logger_ace.warning("No 1m file from yesterday available. Using 2015-10-27.")
            ace1_last = read(os.path.join(localpath,'collected','ace_1m_2015-10-27.cdf'))
    lasttime = num2date(ace1_last.ndarray[0][-1])
    firsttime = num2date(ace1_last.ndarray[0][0])
    print firsttime
    print lasttime
    print now.replace(tzinfo=tzutc())

    print "1m-1", len(ace1_last.ndarray[0]), len(ace_swe.ndarray[0]), len(ace_mag.ndarray[0])
    ace_swe = ace_swe.trim(starttime=lasttime, endtime=now)
    ace_mag = ace_mag.trim(starttime=lasttime, endtime=now)

    print "1m-2", len(ace1_last.ndarray[0]), len(ace_swe.ndarray[0]), len(ace_mag.ndarray[0])

    # Merging streams wrt time with no interpolation:
    ace_1min = mergeACE(ace_swe,ace_mag,['x','y','z','f','t1','t2'])

    print "1m-3", len(ace_1min.ndarray[0])

    for key in ['time','x','y','z','f','t1','t2','var1','var2','var3','str1']:
        ind = KEYLIST.index(key)
        ace_1min.ndarray[ind] = np.asarray(np.hstack((ace1_last.ndarray[ind], ace_1min.ndarray[ind])))

    print "1m-4", len(ace_1min.ndarray[0])

    if newday == True:
        logger_ace.info("Created new 1m file. Writing last data to yesterday.")
        ace_1min.write(os.path.join(localpath,'collected'),filenamebegins=filenamebegins1,
		format_type=fileformat)
    else:
        ace_1m = ace_1min.trim(starttime=yesterday+"T23:59:29")
        ace_1min.write(os.path.join(localpath,'collected'),filenamebegins=filenamebegins1,
		format_type=fileformat)

#--------------------------------------------------------------------
# PROCESS 5-MIN DATA
#--------------------------------------------------------------------

if process5min:
    print("Processing 5-min data...")
    ace_epa = read(os.path.join(localpath,'raw',filestr_epa))
    ace_sis = read(os.path.join(localpath,'raw',filestr_sis),dataformat='NOAAACE')

    lastval = num2date(ace_epa.ndarray[0][-1])
    today = datetime.strftime(lastval, "%Y-%m-%d")
    yesterday = datetime.strftime(lastval-timedelta(days=1), "%Y-%m-%d")

    ace5_file = os.path.join(localpath,'collected',filenamebegins5+today+'.cdf')
    ace5_lastfile = os.path.join(localpath,'collected',filenamebegins5+yesterday+'.cdf')

    if os.path.exists(ace5_file):
        ace5_last = read(ace5_file)
        newday = False
    else:
        newday = True
        try:
            ace5_last = read(ace5_lastfile)
        except:
            logger_ace.info("No 5m file from yesterday available. Using 2015-12-10.")
            ace5_last = read(os.path.join(localpath,'collected','ace_5m_2015-12-10.cdf'))
    lasttime = num2date(ace5_last.ndarray[0][-1])
    firsttime = num2date(ace5_last.ndarray[0][0])
    print firsttime
    print lasttime
    print now.replace(tzinfo=tzutc())

    print "1", len(ace5_last.ndarray[0]), len(ace_sis.ndarray[0]), len(ace_epa.ndarray[0])

    ace_sis = ace_sis.trim(starttime=lasttime, endtime=now)
    ace_epa = ace_epa.trim(starttime=lasttime, endtime=now)

    print "2", len(ace5_last.ndarray[0]), len(ace_sis.ndarray[0]), len(ace_epa.ndarray[0])

    # Merging streams wrt time with no interpolation:
    ace_5min = mergeACE(ace_epa,ace_sis,['x','y'])

    print "3", len(ace_5min.ndarray[0])

    for key in ['time','x','y','z','f','var1','var2','var3','var4','var5','str1']:
        ind = KEYLIST.index(key)
        ace_5min.ndarray[ind] = np.asarray(np.hstack((ace5_last.ndarray[ind], ace_5min.ndarray[ind])))

    print "4", len(ace_5min.ndarray[0])

    if newday == True:
        logger_ace.info("Created new 5m file. Writing last data to yesterday.")
        ace_5min.write(os.path.join(localpath,'collected'),filenamebegins=filenamebegins5,
		format_type=fileformat)
    else:
        ace_5min = ace_5min.trim(starttime = yesterday+"T23:59:29")
        ace_5min.write(os.path.join(localpath,'collected'),filenamebegins=filenamebegins5,
		format_type=fileformat)

#mp.plot(ace_1min)
#mp.plot(ace_5min)

