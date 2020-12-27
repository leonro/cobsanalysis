#!/usr/bin/env python
# coding=utf-8

"""
DESCRIPTION
   Analyses gamma measurements.
   Sources are SCA gamma and METEO data. Created standard project tables and a Webservice
   table version.

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"

APPLICATION
    PERMANENTLY with cron:
        python gamma_products.py -c /etc/marcos/analysis.cfg
    REDO analysis for a time range:
        (startime is defined by endtime - daystodeal as given in the config file 
        python gamma_products.py -c /etc/marcos/analysis.cfg -e 2020-11-22

"""

from magpy.stream import *
from magpy.database import *
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred
import getopt
import pwd
import sys  # for sys.version_info()
import socket

import itertools
from threading import Thread

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf


def CreateOldsProductsTables(config={}, statusmsg={}, start=datetime.utcnow()-timedelta(days=7), end=datetime.utcnow()):
    """
    prepare mean tables ready for analysis
    """

    rawdatapath = config.get('gammarawdata')
    rcsg0path = config.get('rcsg0rawdata')
    tablepath = config.get('gammaresults')
    name = "{}-projectstables".format(config.get('logname'))

    try:
        if debug:
             p2start = datetime.utcnow()
             print ("-----------------------------------")
             print ("Creating DataProducts for GAMMA")
        print ("  Reading SCA Gamma data...")
        gammasca = read(os.path.join(rawradpath,'COBSEXP_2_*'), starttime=start, endtime=end)
        print (gammasca.ndarray)
        if not debug:
            gammasca.write(tablepath, filenamebegins='sca-tunnel-1min_',dateformat='%Y',coverage='year', mode='replace',format_type='PYCDF')
        gammasca = gammasca.filter(filter_type='gaussian', resample_period=900 )
        if not debug:
            gammasca.write(tablepath, filenamebegins='sca-tunnel-15min_',dateformat='%Y', coverage='year', mode='replace',format_type='PYCDF')
        print ("...finished")
        p2end = datetime.utcnow()
        if debug:
            print ("-----------------------------------")
            print ("Finished in {} sec".format(p2end-p2start))
            print ("-----------------------------------")
        statusmsg[name] = 'SCA Radon step2 success'
    except:
        statusmsg[name] = 'SCA Radon step2 failed'

    """
    Get further additional data 
    """
    try:
        # Temperature from all positions within the SGO
        if debug:
            print ("-----------------------------------")
            print ("Extracting RCS tunnel temperature")
        print ("  Loading and filetering RCSG0temp data")
        tempsgo = read(os.path.join(rcsg0path,'*'), starttime=start, endtime=end)
        tempsgo = tempsgo.filter()
        if not debug:
            tempsgo.write(tablepath, filenamebegins='temp-sgo-1min_',dateformat='%Y', coverage='year', mode='replace',format_type='PYCDF')

        if debug:
            print ("-----------------------------------")
        statusmsg[name] = 'SCA Radon step4 success'
    except:
        statusmsg[name] = 'SCA Radon step4 failed'

    return statusmsg


def CreateWebserviceTable(config={}, statusmsg={}, start=datetime.utcnow()-timedelta(days=7), end=datetime.utcnow()):

    # 1. read data
    rawdatapath = config.get('gammarawdata')
    meteopath = config.get('meteoproducts')
    result = DataStream()
    rcsg0path = config.get('rcsg0rawdata')
    name = "{}-servicetables".format(config.get('logname'))

    try:
        if debug:
             print ("-----------------------------------")
             print ("Creating WebService database table")
        print ("  Reading SCA Gamma data...")
        gammasca = read(os.path.join(rawradpath,'COBSEXP_2_*'), starttime=start, endtime=end)
        print (gammasca._get_key_headers())

        print ("  Reading meteo data ...")
        meteo = read(os.path.join(meteopath,'meteo-1min_*'), starttime=start, endtime=end)
        if meteo.length()[0] > 0:
            print (meteo.length())
            meteo._drop_column('y')
            meteo._drop_column('t1')
            meteo._drop_column('var4')
            meteo._move_column('f','t2')
            meteo._drop_column('f')
            #meteo._move_column('f','var2')
            #meteo._drop_column('f')
            #meteo._move_column('x','var3')
            #meteo._drop_column('x')
            print (meteo.length())
            print (meteo.header)
            #meteo.header['col-t2'] = 'T'
            #meteo.header['unit-col-t2'] = 'deg C'
            #meteo.header['col-var2'] = 'P'
            #meteo.header['unit-col-var2'] = 'hPa'
            #meteo.header['col-var3'] = 'snowheight'
            #meteo.header['unit-col-var3'] = 'cm'
    except:
        pass

    # 2. join with other data from meteo
    result = mergeStreams(gammasca,meteo)
    # 3. add new meta information
    result.header['SensorID'] = 'GAMMASGO_adjusted_0001'
    result.header['DataID'] = 'GAMMASGO_adjusted_0001_0001'
    result.header['SensorGroup'] = 'services'
    print ("Results", result.length())

    # 4. export to DB as GAMMASGO_adjusted_0001_0001 in minute resolution
    if not debug:
        if result.lenght()[0] > 0:
            if len(connectdict) > 0:
                for dbel in connectdict:
                    dbw = connectdict[dbel]
                    # check if table exists... if not use:
                    writeDB(dbw,result)
                    # else use
                    #writeDB(dbw,datastream, tablename=...)
                    print ("  -> GAMMASGO_adjusted written to DB {}".format(dbel))

    return statusmsg


def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    joblist = ['default','service']
    debug=False
    endtime = None

    try:
        opts, args = getopt.getopt(argv,"hc:j:e:D",["config=","joblist=","endtime=","debug=",])
    except getopt.GetoptError:
        print ('gamma_products.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- gamma_products.py will analyse magnetic data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python gamma_products.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-j            : default, service')
            print ('-e            : endtime')
            print ('-------------------------------------')
            print ('Application:')
            print ('python gamma_products.py -c /etc/marcos/analysis.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-j", "--joblist"):
            # get a list of jobs (vario, scalar)
            joblist = arg.split(',')
        elif opt in ("-e", "--endtime"):
            endtime = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    if debug:
        print ("Running magnetism_checkadj - debug mode")
        print ("---------------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    if endtime:
        try:
            endtime = DataStream()._testtime(endtime)
        except:
            print ("Endtime could not be interpreted - Aborting")
            sys.exit(1)
    else:
        endtime = datetime.utcnow()

    print ("1. Read configuration data")
    config = GetConf(configpath)
    config = ConnectDatabases(config=config, debug=debug)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname='mm-dp-scaradon.log', debug=debug)

    if debug:
        print (" -> Config contents:")
        print (config)

    starttime = datetime.strftime(endtime-timedelta(days=7),"%Y-%m-%d")
    if 'default' in joblist:
        print ("3. Create standard data table")
        statusmsg = CreateOldsProductsTables(config=config, statusmsg=statusmsg, start=starttime, end=endtime)

    if 'service' in joblist:
        print ("4. Create Webservice table")
        statusmsg = CreateWebserviceTable(config=config, statusmsg=statusmsg, start=starttime, end=endtime)


    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])

