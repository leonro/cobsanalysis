#!/usr/bin/env python

"""
Radon DataTreatment
    - Prepare DataProducts (Tables)
    - Ideally prepare a single table with environment( tunnel and outside temperatures)
    - GAMMA_adjusted_0001
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

coredir = os.path.abspath(os.path.join('/home/cobs/MARTAS', 'core'))
coredir = os.path.abspath(os.path.join('/home/leon/Software/MARTAS', 'core'))
sys.path.insert(0, coredir)
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases


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
             print ("PART 2:")
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
            print ("Part2 needs", p2end-p2start)
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
            p4start = datetime.utcnow()
            print ("-----------------------------------")
            print ("PART 4:")
        print ("  Loading and filetering RCSG0temp data")
        tempsgo = read(os.path.join(rcsg0path,'*'), starttime=start, endtime=end)
        tempsgo = tempsgo.filter()
        if not debug:
            tempsgo.write(tablepath, filenamebegins='temp-sgo-1min_',dateformat='%Y', coverage='year', mode='replace',format_type='PYCDF')

        if debug:
            p4end = datetime.utcnow()
            print ("-----------------------------------")
            print ("Part4 needs", p4end-p4start)
            print ("-----------------------------------")
        statusmsg[name] = 'SCA Radon step4 success'
    except:
        statusmsg[name] = 'SCA Radon step4 failed'


def CreateWebserviceTable():

        # 1. read data
        gammasca = read(os.path.join(rawradpath,'COBSEXP_2_*'), starttime=start, endtime=end)
        # 2. join with other data from meteo
        # 3. add new meta information
        # 4. export to DB as GAMMA_adjusted_0001_0001 in minute resolution
        pass



def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    joblist = ['vario','scalar']
    debug=False
    endtime = None

    try:
        opts, args = getopt.getopt(argv,"hc:j:D",["config=","joblist=","endtime=","debug=",])
    except getopt.GetoptError:
        print ('magnetism_checkadj.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- magnetism_checkadj.py will analyse magnetic data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python magnetism_checkadj.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-j            : vario, scalar')
            print ('-e            : endtime')
            print ('-------------------------------------')
            print ('Application:')
            print ('python magnetism_checkadj.py -c /etc/marcos/analysis.cfg')
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
    print ("3. Create standard data table")
    statusmsg = CreateOldsProductsTables(config=config, statusmsg=statusmsg, start=starttime, end=endtime)

    print ("4. Create Webservice table")
    #statusmsg = CreateWebserviceTable(config=config, statusmsg=statusmsg, start=starttime, end=endtime)


    if not debug:
        #martaslog = ml(logfile=config.get('logfile'),receiver='telegram')
        #martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
        #martaslog.msg(statusmsg)
        pass
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])

