#!/usr/bin/env python

"""
Uploading RCS data to ZAMG FTP Server
"""

from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred


import itertools
from threading import Thread

## New Logging features 
from martas import martaslog as ml
logpath = '/var/log/magpy/mm-dp-rcsupload.log'
#import socket
#sn = socket.gethostname().upper()
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
name = "{}-DataProducts-RCSupload".format(sn)

dbpasswd = mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print "Connecting to primary MARCOS..."
    db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")
    print db
except:
    print "... failed"
    try:
        # Test MARCOS 2
        print "Connecting to secondary MARCOS..."
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
        print db
    except:
        print "... failed -- aborting"
        sys.exit()

endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=1),"%Y-%m-%d")
date = datetime.strftime(endtime,"%Y-%m-%d")
dbdateformat = "%Y-%m-%d %H:%M:%S.%f"
today1 = datetime.strftime(endtime,"%Y%m%d")
yesterd1 = datetime.strftime(endtime-timedelta(days=1),"%Y%m%d")
yesterd2 = datetime.strftime(endtime-timedelta(days=1),"%Y-%m-%d")
weekago = datetime.strftime(endtime-timedelta(days=6),"%Y-%m-%d")

rawt7path = '/srv/archive/SGO/RCST7_20160114_0001/raw/'
rawg0path = '/srv/archive/SGO/RCSG0_20160114_0001/raw/'

zamgcred = 'zamg'
zamgaddress=mpcred.lc(zamgcred,'address')
zamguser=mpcred.lc(zamgcred,'user')
zamgpasswd=mpcred.lc(zamgcred,'passwd')
zamgport=mpcred.lc(zamgcred,'port')


part1 = True
if part1:
    """
    Upload rcs raw data to zamg server
    """
    try:
        p1start = datetime.utcnow()

        for da in [date,yesterd2]:
            # Send in background mode
            Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(rawt7path,'RCS-T7-'+da+'_00-00-00.txt'),'ftppath':'/data/environment/sgo','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/rawt7-transfer.log'}).start()
            Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(rawg0path,'RCS-G0anw-'+da+'_00-00-00.txt'),'ftppath':'/data/environment/sgo','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/rawg0-transfer.log'}).start()
        p1end = datetime.utcnow()
        print "-----------------------------------"
        print "Part1 needs", p1end-p1start
        print "-----------------------------------"
        statusmsg[name] = 'RCS upload successful'
    except:
        statusmsg[name] = 'RCS upload failed'

part2 = False
if part2:
    """
    Get data from tethys ???
    """
    # Done by senddata.py on MARTAS

part3 = False
if part3:
    """
    Transform Environment data
    """
    # Done by senddata.py on MARTAS

martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)

