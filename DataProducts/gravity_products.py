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

#prpare ini file for eterna
def prep_ini(fpath,inifile,date,span,samplerate=60):
    outfile='calctide.ini'
    fout=open('%s/%s' % (fpath,outfile), 'w')
    f=open('%s/%s' % (fpath,inifile),'r')
    for line in f:
        if line.startswith('SAMPLERATE'):
            line=line.replace('300',str(samplerate).rjust(3))
        if line.startswith('INITIALEPO'):
            line=line.replace('2024',date.split('-')[0])
            line=line.replace('04',date.split('-')[1])
            line=line.replace('16',date.split('-')[2])
        if line.startswith('PREDICSPAN'):
            line=line.replace('1512',str(span).rjust(4))
        fout.write(line)
    f.close()
    fout.close()

#read eterna results
def read_tides(fpath,date0,date1,dt):
    f=open('%s/%s' % (fpath,'calctide.prd'),'r')
    t=np.arange(np.datetime64(date0), np.datetime64(date1), np.timedelta64(dt, "s"))
    dat=[]
    rl=0
    for line in f:
        line=line.strip()
        line=line.split()
        if len(line)>0:
            if line[0]=='77777777':
                rl=1
            elif line[0]=='99999999':
                rl=0
            else:
                if rl==1:
                    dat.append([float(line[2]), float(line[3])])
    f.close()
    dat=np.array(dat)
    return t,dat[:,0]


def CreateOldsProductsTables(config={}, statusmsg={}, start=datetime.utcnow()-timedelta(days=7), end=datetime.utcnow(), debug=False):
    """
    prepare mean tables ready for analysis
    """

    rawdatapath = config.get('gammarawdata')
    eternapath = config.get('eternapath')
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


def CreateWebserviceTable(config={}, statusmsg={}, start=datetime.utcnow()-timedelta(hours=2), end=datetime.utcnow(), debug=False):

    # 1. read data
    rawdatapath = config.get('gravrawdata')
    #meteopath = config.get('meteoarchive')
    result = DataStream()
    #rcsg0path = config.get('rcsg0rawdata')
    name = "{}-servicetables".format(config.get('logname'))
    connectdict = config.get('conncetedDB')
    
    
    
    print('Reading iGrav data between %s and %s' % (start,end))

    try:
        year,m,d=end.strftime('%Y-%m-%d').split('-')[0],end.strftime('%Y-%m-%d').split('-')[1],end.strftime('%Y-%m-%d').split('-')[2]
        y0,m0,d0=start.split(' ')[0].split('-')[0],start.split(' ')[0].split('-')[1],start.split(' ')[0].split('-')[2]
        if year==y0 and m==m0 and d==d0:
            grav = read( os.path.join( rawdatapath,year,'Data_iGrav050_%s%s.tsf' % (m,d) ), starttime=start, endtime=end, channels='1,2')
        else:
            grav1 = read( os.path.join( rawdatapath,year,'Data_iGrav050_%s%s.tsf' % (m,d) ), starttime=start, endtime=end, channels='1,2')
            grav2 = read( os.path.join( rawdatapath,y0,'Data_iGrav050_%s%s.tsf' % (m0,d0) ), starttime=start, endtime=end, channels='1,2')
            grav = joinStreams(grav2,grav1)
        #grav = grav.resample(keys=['x','y'], period=60)
        grav=grav.filter(filter_type = 'gaussian', resample_period=60, filter_width = timedelta(minutes=2))
        grav = grav.trim( starttime=start, endtime=end+timedelta(minutes=1) )
        print(grav._get_key_headers())
    except:
        statusmsg[name] = 'grav table failed - critical'
    #sys.exit()

    #one could add earthquake data here
    #try:
        #print ("     -> Reading meteo data ...")
        #meteo = read(os.path.join(meteopath,'METEOSGO_*'), starttime=start, endtime=end)
        #if debug:
           # print (meteo._get_key_headers())
        #if meteo.length()[0] > 0:
            #if debug:
                #print (meteo.length())
            #meteo._move_column('y','var3')
            #meteo._drop_column('y')        #rain - keep
            #meteo._drop_column('t1')
            #meteo._drop_column('var4')
            #meteo._move_column('f','t2')   #temp - keep -> add unit and description
            #meteo._drop_column('f')
            #meteo._drop_column('var2')     # wind direction - remove
            #meteo.header['col-t2'] = 'T(outside)'
            #meteo.header['unit-col-t2'] = 'degC'
            #meteo.header['col-var3'] = 'rain'
            #meteo.header['unit-col-var3'] = 'mm/h'
            # dropping potential string columns
            #meteo._drop_column('str2')
            #print ("     -> Done")
        #else:
            #statusmsg[name] = 'no meteo data'
            #print ("     -> Done - no data")
    #except:
        #statusmsg[name] = 'meteo table failed'
        #meteo = DataStream()

    ## 2. join with other data from meteo
    #if gammasca.length()[0] > 0 and meteo.length()[0] > 0:
        ##meteo = meteo.filter()
        #result = mergeStreams(gammasca,meteo)
    #elif gammasca.length()[0] > 0:
        #result = gammasca.copy()
    #else:
        #result = DataStream()
    # 3. add new meta information
    result = grav.copy()
    result.header['StationID'] = 'SGO'
    result.header['SensorID'] = 'iGrav_adjusted_0001'
    result.header['DataID'] = 'iGrav_adjusted_0001_0001'
    result.header['SensorElements'] = 'g,p' 
    result.header['SensorKeys'] = 'x,y'
    result.header['SensorGroup'] = 'services'
    result.header['SensorName'] = 'iGrav_050'
    result.header['SensorType'] = 'gravity'

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
                    statusmsg[name3] = 'gravity table successfully written to DB'
                    try:
                        writeDB(dbw,result)
                    except:
                        statusmsg[name3] = 'gravity table could not be written to DB - disk full?'
                    # else use
                    #writeDB(dbw,datastream, tablename=...)

                    print ("  -> gravity written to DB {}".format(dbel))

    return statusmsg


def main(argv):
    try:
        version = __version__
    except:
        version = "1.0.0"
    configpath = ''
    statusmsg = {}
    #joblist = ['default','service']
    joblist = ['service']
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
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname='mm-dp-gravity.log', debug=debug)
    config['testplot'] = testplot

    starttime = datetime.strftime(endtime-timedelta(hours=2),"%Y-%m-%d %H:%M:%S")
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

