#!/usr/bin/env python
#********************************************************************
# This is the main program for data analysis of Sparkling Geomagnetic
# Field data. Runs once per day and produces output in day files
# and plots.
#
# FUNCTIONS:
# 1. Download raw data files
#	- Save locally:
#		...	/srv/archive/magnetism/CODE/INST/raw
#	- Save to ftp server:
#		...	www.zamg.ac.at
# 2. Filter data
# 3. Do baseline correction
# 4. Plot all data
#	- Save locally:
#		...	/srv/archive/magnetism/CODE/INST/plots
# 5. Save data in IAGA-2002 min files
#	- Save locally:
#		...	/srv/archive/magnetism/CODE/INST/filtered
# 6. Upload files
#
# Adapted from tse_fetch.py script by RLB on 2014-04-15.
# Updated to work on Vega 2016-02-29.
#
#********************************************************************

import os, urllib, sys
import logging
from magpy.stream import *
from magpy.transfer import * 
import magpy.opt.cred as mpcred

#--------------------------------------------------------------------
# I. Preamble: General
#--------------------------------------------------------------------

#############
TSE = True  
IPL = False 
GQU = False 
#############

basepath = 	'/srv/archive'

IAGA_headers = ['StationInstitution','StationName','StationIAGAcode',
		'DataAcquisitionLatitude','DataAcquisitionLongitude',
		'DataElevation','DataSensorOrientation']

logging.basicConfig(level = logging.INFO)
formatter = 	logging.Formatter('%(asctime)s [%(name)s] %(levelname)s - %(message)s')

if __name__ == '__main__':
    today = datetime.utcnow()
    yesterday = today - timedelta(days=1)
    date = datetime.strftime(yesterday, "%Y-%m-%d")
    starttime = date+' 00:00:00'
    endtime = datetime.strftime(today, '%Y-%m-%d') +' 00:00:00'

#####################################################################
# A. TSE (Tamsweg-Sedna) DATA
#####################################################################

    if TSE:

        station = 'TSE'
        stationname = 'sedna'
        sensors = ['LEMI025_27_0001', 'POS1_N457_0001', 'ENV05_12_0001']
        decl = 3.02

	# Start logger
	logger_tse = logging.getLogger('TSE-DP')
	handler = logging.FileHandler(os.path.join(basepath,station,'%s_processing.log' % station))
	#handler = logging.StreamHandler(sys.stdout)
	handler.setLevel(logging.INFO)
	handler.setFormatter(formatter)
	logger_tse.addHandler(handler)

        # Correct for westerly orientation:
	# TODO: This is probably wrong and it actually needs a 180 degree rotation...
        factors =  {'x': -1,
	    	    'y': -1,
	    	    'z': +1}
        # Correct values with IGRF values:
        baseline = {'x': 21492.4,
	    	    'y': 1134.8,
	    	    'z': 42941.5}

        cred = 'zamg'
        myproxy = mpcred.lc(cred,'address')
        login = mpcred.lc(cred,'user')
        passwd = mpcred.lc(cred,'passwd')
        port = mpcred.lc(cred,'port')
        from_ftp = "/data/magnetism/spark/tamsweg"

	download = True
	
        for sensor in sensors:
            print sensor
            filename = sensor+'_'+date+'.bin'
            from_url = "http://193.170.245.145"+os.path.join("/srv/ws",stationname,sensor,filename)
            to_path = os.path.join(basepath,station,sensor,'raw')

            try:
	        logger_tse.info("Starting download of %s file from FTP server..." % filename)
                # TODO: Find out how to get FTP access from this PC?
                # (ftpaddress,ftpname,ftppasswd,remotepath,localpath,identifier,port=None,**kwargs)
                ftpget(myproxy, login, passwd, from_ftp, to_path, filename, port=port)
	        logger_tse.info("Download complete.")
            except:
		logger_tse.error("Download failed.")
                download = False


#####################################################################
# B. IPL (Innsbruck-Pluto) DATA
#####################################################################

    if IPL:
	station = 'IPL'
        sensors = ['lemi', 'pos1', 'Env05']
	logger_ipl = logging.getLogger('IPL')
        decl = 2.49

	# Start logger
	logger_tse = logging.getLogger('IPL-DP')
	handler = logging.FileHandler(os.path.join(basepath,station,'%s_processing.log' % station))
	handler.setLevel(logging.INFO)
	handler.setFormatter(formatter)
	logger_tse.addHandler(handler)

        # Correct for westerly orientation:
        factors =  {'x': -1,
	    	    'y': -1,
	    	    'z': -1}

        # Correct values with IGRF values:
        baseline = {'x': 21404.6,
	    	    'y': 932.2,
	    	    'z': 42731.5}

#####################################################################
# C. GQU (Graz-Quaoar) DATA
#####################################################################

    if GQU:
	station = 'GQU'
        sensors = ['cs1','env1']
	logger_gqu = logging.getLogger('GQU')
	print "So..."


