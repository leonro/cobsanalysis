#!/usr/bin/env python
# coding=utf-8

"""
DESCRIPTION
   Creates DB Table with technical iGrav data.


PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"

APPLICATION
    PERMANENTLY with cron:
        python iGrav_log.py -c /etc/marcos/analysis.cfg


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




def CreateDBTable(config={}, statusmsg={}, start=datetime.utcnow()-timedelta(hours=2), end=datetime.utcnow(), debug=False):

    # 1. read data
    rawdatapath = config.get('gravrawdata')
    result = DataStream()
    name = "{}-servicetables".format(config.get('logname'))
    connectdict = config.get('conncetedDB')
    
    try:
        print('Reading iGrav data between %s and %s' % (start,end))
        year,m,d=end.strftime('%Y-%m-%d').split('-')[0],end.strftime('%Y-%m-%d').split('-')[1],end.strftime('%Y-%m-%d').split('-')[2]
        y0,m0,d0=start.split(' ')[0].split('-')[0],start.split(' ')[0].split('-')[1],start.split(' ')[0].split('-')[2]
        if year==y0 and m==m0 and d==d0:
            grav_mon = read( os.path.join( rawdatapath,year,'Data_iGrav050_%s%s.tsf' % (m,d) ), starttime=start, endtime=end, channels='3,4,5,6,7,8,9,10,11,12,13,14,17,18,19')
        else:
            grav1 = read( os.path.join( rawdatapath,year,'Data_iGrav050_%s%s.tsf' % (m,d) ), starttime=start, endtime=end, channels='3,4,5,6,7,8,9,10,11,12,13,14,17,18,19')
            grav2 = read( os.path.join( rawdatapath,y0,'Data_iGrav050_%s%s.tsf' % (m0,d0) ), starttime=start, endtime=end, channels='3,4,5,6,7,8,9,10,11,12,13,14,17,18,19')
            grav_mon = joinStreams(grav2,grav1)       
        #grav_mon = grav_mon.resample(keys=['x', 'y', 'z', 'f', 't1', 't2', 'var1', 'var2', 'var3', 'var4', 'var5', 'dx', 'dy', 'dz', 'df'], period=60)
        grav_mon=grav_mon.filter(filter_type = 'gaussian', resample_period=60, filter_width = timedelta(minutes=2))
        grav_mon=grav_mon.trim( starttime=start, endtime=end+timedelta(minutes=1) )
        print(grav_mon._get_key_headers())
    except:
        statusmsg[name] = 'iGrav table failed - critical'
  
    #sys.exit()
    # 3. add new meta information
    result = grav_mon.copy()
    result.header['StationID'] = 'SGO'
    result.header['SensorID'] = 'iGrav_log_0001'
    result.header['DataID'] = 'iGrav_log_0001_0001'
    #result.header['SensorGroup'] = 'services'
    result.header['SensorName'] = 'iGrav_log'
    result.header['SensorType'] = 'gravity'
    #result.header['SensorElements'] = 'Grav-Bal,TiltX-Bal,TiltY-Bal,Temp-Bal,Grav-Ctrl,TiltX-Ctrl,TiltY-Ctrl,Temp-Ctrl,Neck-T1,Neck-T2,Body-T,Belly-T,Dewar-Pwr,Dewar-Press,He-Level' 
    #result.header['SensorKeys'] = 'x,y,z,f,t1,t2,var1,var2,var3,var4,var5,dx,dy,dz,df'

    if debug:
        print ("    Results", result.length())
    #if debug and config.get('testplot',False):
        #mp.plot(result)

    # 4. export to DB as GAMMASGO_adjusted_0001_0001 in minute resolution
    if not debug:
        if result.length()[0] > 0:
            if len(connectdict) > 0:
                for dbel in connectdict:
                    dbw = connectdict[dbel]
                    # check if table exists... if not use:
                    name3 = "{}-toDB-{}".format(config.get('logname'),dbel)
                    statusmsg[name3] = 'iGrav table successfully written to DB'
                    try:
                        writeDB(dbw,result)
                    except:
                        statusmsg[name3] = 'iGrav table could not be written to DB - disk full?'
                    # else use
                    #writeDB(dbw,datastream, tablename=...)

                    print ("  -> iGrav_log written to DB {}".format(dbel))

    return statusmsg


def main(argv):
    try:
        version = __version__
    except:
        version = "1.0.0"
    configpath = ''
    statusmsg = {}
    #joblist = ['default','service']
    joblist = ['db']
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
            print ('python gravity_products.py -c /etc/marcos/analysis.cfg')
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
        print ("Running gravity_products version {} - debug mode".format(version))
        print ("---------------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check gravity_products.py -h for more options and requirements')
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
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname='mm-info-igrav.log', debug=debug)
    config['testplot'] = testplot

    starttime = datetime.strftime(endtime-timedelta(hours=2),"%Y-%m-%d %H:%M:%S")
    if 'db' in joblist:
        print ("3. Create standard database table")
        statusmsg = CreateDBTable(config=config, statusmsg=statusmsg, start=starttime, end=endtime, debug=debug)
        


    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])

