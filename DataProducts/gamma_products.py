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
from version import __version__


def CreateOldsProductsTables(config={}, statusmsg={}, start=datetime.utcnow()-timedelta(days=7), end=datetime.utcnow(), debug=False):
    """
    prepare mean tables ready for analysis
    """

    rawdatapath = config.get('gammarawdata')
    rcsg0path = config.get('rcsg0rawdata')
    tablepath = config.get('gammaresults')
    name1 = "{}-projectradontable".format(config.get('logname'))
    name2 = "{}-projecttempstable".format(config.get('logname'))

    try:
        if debug:
             print ("-----------------------------------")
             print ("Creating DataProducts for GAMMA")
        print ("  Reading SCA Gamma data...")
        gammasca = read(os.path.join(rawdatapath,'COBSEXP_2_*'), starttime=start, endtime=end)
        if not debug:
            gammasca.write(tablepath, filenamebegins='sca-tunnel-1min_',dateformat='%Y',coverage='year', mode='replace',format_type='PYCDF')
        gammasca = gammasca.filter(filter_type='gaussian', resample_period=900 )
        if not debug:
            gammasca.write(tablepath, filenamebegins='sca-tunnel-15min_',dateformat='%Y', coverage='year', mode='replace',format_type='PYCDF')
        if debug:
            print ("  -> Done")
            print ("-----------------------------------")
        statusmsg[name1] = 'radon tables created'
    except:
        statusmsg[name1] = 'radon tables failed'

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
            print ("  -> Done")
            print ("-----------------------------------")
        statusmsg[name2] = 'radon-temperature tables success'
    except:
        statusmsg[name2] = 'radon-temperature tables failed'

    return statusmsg


def CreateWebserviceTable(config={}, statusmsg={}, start=datetime.utcnow()-timedelta(days=7), end=datetime.utcnow(), debug=False):

    # 1. read data
    rawdatapath = config.get('gammarawdata')
    meteopath = config.get('meteoproducts')
    result = DataStream()
    rcsg0path = config.get('rcsg0rawdata')
    name = "{}-servicetables".format(config.get('logname'))
    connectdict = config.get('conncetedDB')

    statusmsg[name] = 'gamma webservice table successfully created'

    try:
        if debug:
             print (" -----------------------------------")
             print (" Creating WebService database table")
        print ("     -> Reading SCA Gamma data...")
        gammasca = read(os.path.join(rawdatapath,'COBSEXP_2_*'), starttime=start, endtime=end)
        if gammasca.length()[0] > 0:
            gammasca.header['col-t1'] = 'T(tunnel)'
            gammasca.header['unit-col-t1'] = 'degC'
        if debug:
             print (gammasca._get_key_headers())
        print ("     -> Done")
    except:
        statusmsg[name] = 'gamma table failed - critical'
        gammasca = DataStream()

    try:
        print ("     -> Reading meteo data ...")
        meteo = read(os.path.join(meteopath,'meteo-1min_*'), starttime=start, endtime=end)
        if debug:
            print (meteo._get_key_headers())
        if meteo.length()[0] > 0:
            if debug:
                print (meteo.length())
            meteo._move_column('y','var3')
            meteo._drop_column('y')        #rain - keep
            meteo._drop_column('t1')
            meteo._drop_column('var4')
            meteo._move_column('f','t2')   #temp - keep -> add unit and description
            meteo._drop_column('f')
            meteo._drop_column('var2')     # wind direction - remove
            meteo.header['col-t2'] = 'T(outside)'
            meteo.header['unit-col-t2'] = 'degC'
            meteo.header['col-var3'] = 'rain'
            meteo.header['unit-col-var3'] = 'mm/h'
            # dropping potential string columns
            meteo._drop_column('str2')
            print ("     -> Done")
        else:
            statusmsg[name] = 'no meteo data'
            print ("     -> Done - no data")
    except:
        statusmsg[name] = 'meteo table failed'
        meteo = DataStream()

    # 2. join with other data from meteo
    if gammasca.length()[0] > 0 and meteo.length()[0] > 0:
        #meteo = meteo.filter()
        result = mergeStreams(gammasca,meteo)
    elif gammasca.length()[0] > 0:
        result = gammasca.copy()
    else:
        result = DataStream()
    # 3. add new meta information
    result.header['StationID'] = 'SGO'
    result.header['SensorID'] = 'GAMMASGO_adjusted_0001'
    result.header['DataID'] = 'GAMMASGO_adjusted_0001_0001'
    result.header['SensorElements'] = 'Counts,Temp,OutsideTemp,Voltage,rain' 
    result.header['SensorKeys'] = 'x,t1,t2,var1,var3'
    result.header['SensorGroup'] = 'services'
    result.header['SensorName'] = 'GAMMASGO'
    result.header['SensorType'] = 'Radiometry'

    if debug:
        print ("    Results", result.length())
    if debug and config.get('testplot',False):
        mp.plot(result)

    # 4. export to DB as GAMMASGO_adjusted_0001_0001 in minute resolution
    if not debug:
        if result.length()[0] > 0:
            if len(connectdict) > 0:
                for dbel in connectdict:
                    dbw = connectdict[dbel]
                    # check if table exists... if not use:
                    name3 = "{}-toDB-{}".format(config.get('logname'),dbel)
                    statusmsg[name3] = 'gamma table successfully written to DB'
                    try:
                        writeDB(dbw,result)
                    except:
                        statusmsg[name3] = 'gamma table could not be written to DB - disk full?'
                    # else use
                    #writeDB(dbw,datastream, tablename=...)

                    print ("  -> GAMMASGO_adjusted written to DB {}".format(dbel))

    return statusmsg


def main(argv):
    try:
        version = __version__
    except:
        version = "1.0.0"
    configpath = ''
    statusmsg = {}
    joblist = ['default','service']
    debug=False
    endtime = None
    testplot=False

    try:
        opts, args = getopt.getopt(argv,"hc:j:e:DP",["config=","joblist=","endtime=","debug=","plot=",])
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
        elif opt in ("-P", "--plot"):
            # delete any / at the end of the string
            testplot = True

    if debug:
        print ("Running gamma_products version {} - debug mode".format(version))
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
    config['testplot'] = testplot

    starttime = datetime.strftime(endtime-timedelta(days=7),"%Y-%m-%d")
    if 'default' in joblist:
        print ("3. Create standard data table")
        statusmsg = CreateOldsProductsTables(config=config, statusmsg=statusmsg, start=starttime, end=endtime, debug=debug)

    if 'service' in joblist:
        print ("4. Create Webservice table")
        statusmsg = CreateWebserviceTable(config=config, statusmsg=statusmsg, start=starttime, end=endtime, debug=debug)


    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])

