#!/usr/bin/env python
#********************************************************************
# A script to download ACE finished files once a day from FTP server.
# Works in conjunction with ace_download.py.
#
# Created by RLB on 2015-02-13.
# Activated as cronjob on 2015-02-13.
#
#********************************************************************

import os, sys
import logging
from magpy.stream import *
from magpy.transfer import *

#--------------------------------------------------------------------
# General
#--------------------------------------------------------------------

localpath = '/srv/archive/external/esa-nasa/ace'
ftppath = '/pub/lists/ace/'
myproxy = "138.22.156.44"
port = 8021
login = 'anonymous@ftp.swpc.noaa.gov'
passwd = 'anonymous'
now = datetime.utcnow()
yesterday = now - timedelta(days=1)
date1 = datetime.strftime(yesterday,'%Y-%m-%d')
date2 = datetime.strftime(yesterday,'%Y%m%d')
filelist = ['swepam_1m', 'epam_5m', 'mag_1m', 'sis_5m']

logging.basicConfig(level = logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s - %(message)s')
logger_ace = logging.getLogger('ACE-DD')
handler = logging.FileHandler(os.path.join(localpath,'ace_processing.log'))
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger_ace.addHandler(handler)

def FTPGET(ftpaddress,login,passwd,remotepath,localpath,filestr,port):

    ftp = ftplib.FTP()
    ftp.connect(ftpaddress, port)
    ftp.login(login,passwd)      
    ftp.cwd(remotepath)
    downloadpath = os.path.normpath(os.path.join(localpath,filestr))
    try:
        ftp.retrbinary('RETR %s' % filestr, open(downloadpath,'wb').write)
    except:
        print "Could not download file!"
        pass
    ftp.quit()

#####################################################################
# Job #2: Keep updated ACE data (every 15 mins - :01,:16,:31,:46))
#####################################################################

if __name__ == '__main__':

    print date1

    for f in filelist:
        filestr = date2 + '_ace_' + f + '.txt'
        logger_ace.info("Starting download of %s from FTP server..." % filestr)
        print os.path.join(localpath,'raw',filestr)
        try:
            FTPGET(myproxy,login,passwd,ftppath,os.path.join(localpath,'raw'),filestr,port)
            logger_ace.info("Download successful.")
        except:
            logger_ace.error("Download failed.")



