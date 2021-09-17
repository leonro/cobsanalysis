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
    -r range               :   int     :  default  2 (days)
    -s sensor              :   string  :  sensor or dataid
    -k keys                :   string  :  comma separated list of keys to be plotted
    -f flags               :   string  :  flags from other lists e.g. quakes, coil, etc
    -y style               :   string  :  plot style
    -l loggername          :   string  :  loggername e.g. mm-pp-tilt.log
    -e endtime             :   string  :  endtime (plots from endtime-range to endtime)

APPLICATION
    PERMANENTLY with cron:
        python webpage_graph.py -c /etc/marcos/analysis.cfg
    SensorID:
        python3 general_graph.py -c ../conf/wic.cfg -e 2019-01-15 -s GP20S3NSS2_012201_0001 -D
    DataID:
        python3 general_graph.py -c ../conf/wic.cfg -e 2019-01-15 -s GP20S3NSS2_012201_0001_0001 -D
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
from analysismethods import DefineLogger, ConnectDatabases, Quakes2Flags, combinelists
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__

#git tag -a 1.0.0 -m 'version 1.0.0'
#vers=`git describe master`
#line="__version__ = '$vers'"
#echo $line > /home/leon/Software/cobsanalysis/core/version.py



def Coil2Flags(config={}, endtime=datetime.utcnow(), timerange=5, sensorid=None, keylist=[], debug=False):
    """
    DESCRIPTION
        Creates a flaglist from Merrit Coil usage data.
    PARAMETER
        sensorid  : provide the sensorid to be used in the flagging output 
        keys      : provide the keys to be used in the flagging output 
        valuedict : extract specific components from COIL table

    TESTIT:
        Coil2Flags(config=config, endtime=datetime(2020,2,14), timerange=5, sensorid=None, keylist=[], debug=True)


    COMMENT:
        Only 2020-02-13 data in database- content unclear, many duplicates 
    """

    flaglist = []

    print ("  - extracting coil data and construct flaglist")
    db = config.get('primaryDB')

    if timerange == 0:
        timerange = 1

    if debug:
        print ("  - reading COIL table from database for selected time range between {} and {}".format(endtime-timedelta(days=timerange),endtime))
    st = datetime.strftime(endtime-timedelta(days=timerange), "%Y-%m-%d")
    et = datetime.strftime(endtime, "%Y-%m-%d")
    sqlstring = "SELECT * FROM COIL_Status_Log WHERE utc BETWEEN '{}' AND '{}'".format(st,et)


    #FIELD100MUX 	14703.9
    #2020-02-13 23:58:08 	FIELD100MUY 	15544.9
    #2020-02-13 23:58:08 	FIELD100MUZ 	0
    #2020-02-13 23:58:08 	COMPCOMPX 	0
    #2020-02-13 23:58:08 	COMPCOMPY 	0
    #2020-02-13 23:58:08 	COMPCOMPZ
    
    #flagdict = [{"starttime" : el[0], "endtime" : el[1], "components" : el[2].split(','), "flagid" : el[3], "comment" : el[4], "sensorid" : el[5], "modificationdate" : el[6]} for el in flaglist]

    if debug:
        print (sqlstring)
    # CREATE a flaglist from this data

    sys.exit()
    return flaglist


def CheckSensorID(sensorid, revision='0001', debug=False):

    # Check sensorid:
    senslength = sensorid.split('_')
    if len(senslength) == 4:
        # DataID provided
        revision = senslength[3]
        sensorid = "_".join(senslength[:3])
        if debug:
            print ("    -> DataID provided: using SensorID {} with datarevision {}".format( sensorid, revision))
    else:
        if debug:
            print ("    -> SensorID provided - adding revision {}".format(revision))

    return sensorid, revision

def ReadDatastream(config={}, endtime=datetime.utcnow(), timerange=5, sensorid=None, keylist=[], revision="0001", datapath='', dropflagged=True, debug=False):

    # Read seconds data and create plots
    starttime=endtime-timedelta(days=timerange)
    dataid = '{}_{}'.format(sensorid,revision)
    db = config.get('primaryDB')

    if debug:
        print ("READING data stream ...")
    if datapath:
        path = os.path.join(datapath,'*')
        print (" -> fixed path selected: {}".format(path))
        stream = read(path, starttime=starttime, endtime=endtime) 
    else:
        if starttime < datetime.utcnow()-timedelta(days=15):
            print (" -> reading from archive files ...")
            path = os.path.join(config.get('archivepath'),sensorid,dataid,'*')
            print (path)
            stream = read(path, starttime=starttime, endtime=endtime) 
        else:
            print (" -> reading from database ...")
            stream = readDB(db,dataid,starttime=starttime, endtime=endtime)

    fl = db2flaglist(db,stream.header.get('SensorID'),begin=starttime, end=endtime)
    print (" - obtained {} flags in db".format(len(fl)))
    if dropflagged:
        print (" - dropping flagged data")    
        stream = stream.flag(fl)
        stream = stream.remove_flagged()

    return stream, fl



def CreateDiagram(stream=DataStream(), keylist=[], flaglist=[], style='magpy', debug=False):

    if debug:
        print (" CREATING DIAGRAM ...")
        print (" applying flags")
    if len(flaglist) > 0:
        stream = stream.flag(flaglist)
        if debug:
            print (flaglist)

    if style in ['magpy','MagPy','MAGPY']:
        mp.plotStreams([stream],[keylist], gridcolor='#316931',fill=[], padding=[], annotate=True, confinex=True, fullday=True, opacity=0.7, noshow=True)
    else:
        print (" -> unkown plot style ... doing nothing")
        return False

    print ("Plot created ...")
    if not debug:
        print (" ... saving now")
        savepath = "/srv/products/graphs/tilt/tilt_%s.png" % date
        plt.savefig(savepath)
    else:
        print (" ... debug mode - showing plot")
        plt.show()
    return True


def main(argv):
    version = __version__
    configpath = ''
    statusmsg = {}
    path=''
    dayrange = 5
    debug=False
    endtime = None
    sensorid='LM_TILT01_0001'
    revision='0001'
    keylist=None
    flagsources = ['flagdb','quakes','coil']
    flagsources = []
    flaglist = []
    plotstyle = 'magpy' # one in magpy, xxx
    newloggername = 'mm-pp-tilt.log'
    dropflagged = False


    try:
        opts, args = getopt.getopt(argv,"hc:r:p:s:k:f:y:e:l:D",["config=","range=","path=","sensor=","keys=","flags=","style=","endtime=","loggername=","debug=",])
    except getopt.GetoptError:
        print ('job.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- general_graph.py will plot sensor data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python general_graph.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-r            : range in days')
            print ('-p            : provide a fixed path')
            print ('-s            : sensor')
            print ('-k            : keys')
            print ('-f            : flags from other lists e.g. quakes, coil, etc')
            print ('-y            : plot style')
            print ('-l            : loggername e.g. mm-pp-tilt.log')
            print ('-e            : endtime')
            print ('-------------------------------------')
            print ('Application:')
            print ('python general_graph.py -c /etc/marcos/analysis.cfg')
            print ('python general_graph.py -c /etc/marcos/analysis.cfg')
            print ('# debug run on my machine')
            print ('python3 general_graph.py -c ../conf/wic.cfg -s debug -k x,y,z -f none -D')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-r", "--range"):
            # range in days
            dayrange = int(arg)
        elif opt in ("-p", "--path"):
            # delete any / at the end of the string
            path = os.path.abspath(arg)
        elif opt in ("-s", "--sensor"):
            # sensor name
            sensorid = arg
        elif opt in ("-f", "--flags"):
            # get a list of tables with flagging sources (quakes, coil)
            flagsources = arg.split(',')
        elif opt in ("-k", "--keys"):
            # select keys
            keylist = arg.split(',')
        elif opt in ("-y", "--style"):
            # define a plotstyle
            plotstyle = arg
        elif opt in ("-e", "--endtime"):
            # endtime of the plot
            endtime = arg
        elif opt in ("-l", "--loggername"):
            # loggername
            newloggername = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    if debug:
        print ("Running graph creator in analysis version {}".format(version))

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check general_graph.py -h for more options and requirements')
        sys.exit()

    if endtime:
        try:
            endtime = DataStream()._testtime(endtime)
        except:
            print ("Endtime could not be interpreted - Aborting")
            sys.exit(1)
    else:
        endtime = datetime.utcnow()

    # basic flagging treatment
    if not 'flagdb' in flagsources:
         # Then drop flagged data directly after reading data
         print (" will drop data with existing -remove- flags in DB") 
         dropflagged = True
        
    # general test environment:
    if debug and sensorid == 'travis':
        print (" basic code test successful")
        sys.exit(0)


    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "TestPlotter", job=os.path.basename(__file__), newname=newloggername, debug=debug)

    print ("3. Connect to databases")
    config = ConnectDatabases(config=config, debug=debug)

    if debug and sensorid == 'debug':
        print ("DEBUG run with sensor test selected ---")
        config['archivepath'] = '/home/leon/Cloud/Daten'
        sensorid = 'LEMI036_1_0002_0002'
        endtime = datetime(2020,12,17)

    print ("4. Check SensorID - or whether DataID provided")
    sensorid, revision = CheckSensorID(sensorid, revision, debug=debug)

    print ("5. Read datastream")
    stream, fl = ReadDatastream(config=config, endtime=endtime, timerange=dayrange,sensorid=sensorid, keylist=keylist, revision=revision, dropflagged=dropflagged, datapath=path, debug=debug)
    if 'flagdb' in flagsources:
        # Using flags from DB for annotation
        flaglist = fl
    if not keylist:
        keylist = stream._get_key_headers()
    print ("KEYS:", keylist)

    print ("6. Getting additional flags")
    if 'quakes' in flagsources:
        print ("  6.1 Get flaglist from Quakes")
        print ("     -> constructing flaglist from QUAKES table")
        namecheck1 = "{}-extract-quakes".format(config.get('logname'))
        try:
            fl = Quakes2Flags(config=config, endtime=endtime, timerange=dayrange+1, sensorid=sensorid, keylist=keylist[0], debug=debug)
            statusmsg[namecheck1] = "success"
        except:
            fl = []
            statusmsg[namecheck1] = "failure"
        flaglist = combinelists(flaglist,fl)


    print ("7. Diagrams")
    diagram = CreateDiagram(stream=stream, keylist=keylist, flaglist=flaglist, style=plotstyle, debug=debug)


    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])


