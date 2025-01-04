
#!/usr/bin/env python
"""
DESCRIPTION
   Create Meteo minute files as wished by Bruno
   Reads CDF files and creates csv files once a day to be delivered 
   to the FTP folder


APPLICATION
    PERMANENTLY with cron:
        python create_meteo_files.py -c /etc/marcos/analysis.cfg

"""
from magpy.stream import *
from magpy.database import *
from magpy.transfer import *
import json, os

import getopt
import pwd
import socket
import sys  # for sys.version_info()

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, getstringdate
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__

def main(argv):
    try:
        version = __version__
    except:
        version = "1.0.0"
    configpath = ''
    debug=False
    endtime = None
    starttime = None

    try:
        opts, args = getopt.getopt(argv,"hc:e:s:D",["config=","endtime=","starttime=","debug=",])
    except getopt.GetoptError:
        print ('create_meteo_files.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- create_meteo_files.py will determine the primary instruments --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python weather_products.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-e            : endtime - default is today 00:00')
            print ('-s            : starttime - default is day before yesterday')
            print ('-a            : create archive data - no plots, no DB inputs, no...')
            print ('-------------------------------------')
            print ('Application:')
            print ('python weather_products.py -c /etc/marcos/analysis.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-e", "--endtime"):
            # get an endtime
            endtime = arg
        elif opt in ("-s", "--starttime"):
            # get an starttime
            starttime = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running current_weather version {}".format(version))
    print ("--------------------------------")

    if endtime:
         try:
             endtime = DataStream()._testtime(endtime)
         except:
             print (" Could not interprete provided endtime. Please Check !")
             sys.exit(1)
    else:
         endtime = datetime.strptime(datetime.strftime(datetime.utcnow(), "%Y-%m-%d"), "%Y-%m-%d")

    if starttime:
         try:
             starttime = DataStream()._testtime(starttime)
         except:
             print (" Could not interprete provided endtime. Please Check !")
             sys.exit(1)
    else:
         starttime = endtime-timedelta(days=2)

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()


    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    meteoproductpath = config.get('meteoproducts')
    meteofilename = 'meteo-1min_*'

    print ("2. Connect databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
    except:
        db = None
        
    if debug:
        print (starttime, endtime)
    source = os.path.join(meteoproductpath,meteofilename)
    print ("3. Reading data ...")
    data = read(source, starttime=starttime, endtime=endtime)
    print ("Sampling rate", data.samplingrate())
    data = data._drop_nans('f')
    print ("Sampling rate", data.samplingrate())
    #data = data.resample(keys=['y','z','f','t1','t2','var1','var2','var4','var5'], period=60)
    #print ("Sampling rate", data.samplingrate())
    #data = readDB(db, 'METEOSGO_adjusted_0001_0001', starttime=starttime, endtime=endtime)
    destination = os.path.join(meteoproductpath,"daily-meteo")
    print ("4. writing data ...")
    if debug:
        print (" debug: not writing:", data._find_t_limits(), destination) 
    else:
        data.write(destination, filenamebegins='meteo_', mode='overwrite', format_type='CSV')
    print (" ->  success")


if __name__ == "__main__":
   main(sys.argv[1:])

