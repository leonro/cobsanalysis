#!/usr/bin/env python
# coding=utf-8

"""
DESCRIPTION
   Creates plots for a specific sensor.
PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods
PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -r range               :   days    :  default  2 (days)

APPLICATION
    PERMANENTLY with cron:
        python webpage_graph.py -c /etc/marcos/analysis.cfg
        python3 tilt_graph_new.py -c ../conf/wic.cfg -e 2019-01-15 -s GP20S3NSS2_012201_0001 -D

"""

from magpy.stream import *
from magpy.database import *
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred
import io, pickle
import getopt
import pwd
import sys  # for sys.version_info()
import socket

import itertools
from threading import Thread
from subprocess import check_output   # used for checking whether send process already finished

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, Quakes2Flags
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__

#git tag -a 1.0.0 -m 'version 1.0.0'
#vers=`git describe master`
#line="__version__ = '$vers'"
#echo $line > /home/leon/Software/cobsanalysis/core/version.py



def ReadDatastream(config={}, endtime=datetime.utcnow(), timerange=5, sensorid=None, keylist=[], revision="0001", debug=False):

    # Read seconds data and create plots
    starttime=endtime-timedelta(days=timerange)
    dataid = '{}_{}'.format(sensorid,revision)
    db = config.get('primaryDB')

    if starttime < datetime.utcnow()-timedelta(days=15):
        print (" Reading from archive files ...")
        path = os.path.join(config.get('archivepath'),sensorid,dataid,'*')
        print (path)
        stream = read(path, starttime=starttime, endtime=endtime) 
    else:
        print (" Reading from database ...")
        stream = readDB(db,dataid,starttime=starttime, endtime=endtime)

    fl = db2flaglist(db,stream.header.get('SensorID'),begin=starttime, end=endtime)
    stream = stream.flag(fl)
    stream = stream.remove_flagged()

    return stream



def CreateDiagram(stream=DataStream(), keylist=[], flaglist=[], debug=False):

    stream = stream.flag(flaglist)
    print (flaglist)

    mp.plotStreams([stream],[keylist], gridcolor='#316931',fill=[], padding=[], annotate=True, confinex=True, fullday=True, opacity=0.7, noshow=True)

    print ("Plot created .. saving now")
    if not debug:
        savepath = "/srv/products/graphs/tilt/tilt_%s.png" % date
        plt.savefig(savepath)
    else:
        plt.show()



def main(argv):
    version = __version__
    configpath = ''
    statusmsg = {}
    dayrange = 5
    debug=False
    endtime = None
    sensorid='LM_TILT01_0001'
    keylist=None

    try:
        opts, args = getopt.getopt(argv,"hc:r:s:k:e:D",["config=","range=","sensor=","keys=","endtime=","debug=",])
    except getopt.GetoptError:
        print ('job.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- magnetism_products.py will analyse magnetic data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python magnetism_products.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-r            : range in days')
            print ('-e            : endtime')
            print ('-------------------------------------')
            print ('Application:')
            print ('python job.py -c /etc/marcos/analysis.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-r", "--range"):
            # get a list of jobs (adjusted, quasidefinitive,upload,plots)
            dayrange = int(arg)
        elif opt in ("-s", "--sensor"):
            # get a list of jobs (adjusted, quasidefinitive,upload,plots)
            sensorid = arg
        elif opt in ("-k", "--keys"):
            # get a list of jobs (adjusted, quasidefinitive,upload,plots)
            keylist = arg.split(',')
        elif opt in ("-e", "--endtime"):
            # get a list of jobs (adjusted, quasidefinitive,upload,plots)
            endtime = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    if debug:
        print ("Running tilt graph creator in analysis version {}".format(version))

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

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "TestPlotter", job=os.path.basename(__file__), newname='mm-pp-tilt.log', debug=debug)

    print ("3. Connect to databases")
    config = ConnectDatabases(config=config, debug=debug)


    print ("4. Read datastream")
    stream = ReadDatastream(config=config, endtime=endtime, timerange=dayrange,sensorid=sensorid, keylist=keylist, revision="0001", debug=debug)
    if not keylist:
        keylist = stream._get_key_headers()
    print ("KEYS:", keylist)

    print ("5. Get flaglist from Quakes")
    print ("  -> constructing flaglist from QUAKES table")
    namecheck1 = "{}-extract-quakes".format(config.get('logname'))
    try:
        flaglist = Quakes2Flags(config=config, endtime=endtime, timerange=dayrange+1, sensorid=sensorid, keylist=keylist[0], debug=debug)
        statusmsg[namecheck1] = "success"
    except:
        flaglist = []
        statusmsg[namecheck1] = "failure"

    print ("6. Diagrams")
    diagram = CreateDiagram(stream=stream, keylist=keylist, flaglist=flaglist, debug=debug)


    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])



"""
# ##############################################################
#                     Configuration data for analysis jobs
# ##############################################################
# Observatory
# --------------------------------------------------------------
obscode                :      WIC
# Basic analysis parameters
# --------------------------------------------------------------
meteorange             :       3
daystodeal             :      2
#  - MAGNETISM
variometerinstruments  :      LEMI036_1_0002_0002,LEMI025_22_0003_0002,FGE_S0252_0001_0001
scalarinstruments      :      GSM90_14245_0002_0002,GSM90_6107631_0001_0001,GP20S3NSS2_012201_0001_0001
magnetismexports       :      IAGA,CDF,DBmin
qdstarthour            :      3
qdendhour              :      4
# analyze quasidefinitive data only on 5=Saturday
qdweekday              :      5
# baseline anaylsis
primarypier            :      A2
baselinedays           :      100
# Databases
# --------------------------------------------------------------
dbcredentials          :      list
# Paths and Directories
# --------------------------------------------------------------
#  - METEOROLOGY
sgopath                :       /srv/archive/SGO
meteoproducts          :       /srv/products/data/meteo
meteoimages            :       /srv/products/graphs/meteo
#  - MAGNETISM
variationpath          :       /srv/products/data/magnetism/variation/
quasidefinitivepath    :       /srv/products/data/magnetism/quasidefinitive/
dipath                 :       /srv/archive/WIC/DI/data
archivepath            :       /srv/archive/WIC
#  - GAMMA
rcsg0rawdata           :       /srv/archive/SGO/RCSG0temp_20161027_0001/raw/
gammarawdata           :       /srv/archive/SGO/GAMMA_SFB867_0001/raw/
gammaresults           :       /srv/projects/radon/tables/
#  - GENERAL
currentvaluepath       :       /srv/products/data/current.data
magfigurepath          :       /srv/products/graphs/magnetism/
# Logging and notification
# --------------------------------------------------------------
# Logfile (a json style dictionary, which contains statusmessages) 
loggingdirectory       :   /var/log/magpy
# Notifaction (uses martaslog class, one of email, telegram, mqtt, log) 
notification         :   telegram
# Configuration for notification type, e.g. /home/cobs/SCRIPTS/telegram_notify.conf
notificationconfig   :   /myconfpath/mynotificationtype.cfg
"""

