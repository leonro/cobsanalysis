#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DESCRIPTION
   Creates plots for specific sensor(s).
PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods
PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -i input               :   jsonfile:  sensordefinitions
    -o output              :   dir/file:  save graph to this one
    -y style               :   string  :  plot style (i.e. magpy, crazynice
    -l loggername          :   string  :  loggername e.g. mm-pp-tilt.log
    -s starttime           :   string  :  starttime (plots from endtime-range to endtime)
    -e endtime             :   string  :  endtime (plots from endtime-range to endtime)
    -r range               :   int     :  use range in days. A given starttime AND endtime will overwrite range 

INPUT fiel example: see bottom

APPLICATION
    Standard - will save the file to tmp with 'magnetism_ENDDATE.png' as filename:
        python3 general_graph.py -c ../conf/wic.cfg -i ../conf/magnetism_plot.json -e 2020-12-17
    SensorID:
        python3 general_graph.py -c ../conf/wic.cfg -e 2019-01-15 -s GP20S3NSS2_012201_0001 -D
    Testing:
        python3 general_graph.py -c ../conf/wic.cfg -i ../conf/sensordef_plot.json -e 2020-12-17

python3 general_graph.py -c ../conf/wic.cfg -e 2020-12-17 -D

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
import json

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


debugsensor = { 'LEMI036_1_0002_0002' : { 'keys' : ['x','y','z'], 
                            'plotstyle' : 'line',
                            'source' : '/home/leon/Cloud/Daten/',
                            'filenamebegins' : 'LEMI036_1_0002_0002',
                            'color' : ['k','r','k'], 
                            'flags' : 'flag,quake',
                            'fill' : ['y'],
                            'quakekey' : 'z',
                            'padding' : [0.2,100.0,0.0], 
                            'annotate' : [True,False,True],
                            'columns' : ['H','E','Z'],
                            'units' : ['nT','nT','nT']
                          }
              }


"""
sensordefs = { 'dataid' : { 'keys' : ['x','y','z'], 
                            'plotsytle' : 'line (or point or bar)', 
                            'color' : ['r','g','b'], 
                            'flags' : 'drop',  #('' - no flags, 'flag' - show flags from db, 'drop' - load flags and drop flagged, 'coil' - create flags from coil, 'quake' - create flags from quakes)
                            'fill' : ['y'],
                            'columns' : ['H','E','Z'],
                            'units' : ['nT','nT','nT']
                          },
               'anotherdataid' : { 'keys' : ['t1'], 
                            'plotsytle' : 'line (or point or bar)', 
                            'color' : ['r'], 
                            'source' : '/srv/projects/gravity/tilt/', #(if source == db always use  dbgetlines(db,inst,70000)
                                                                       lasthour = lasthour.trim(endtime=datetime.utcnow())
                            'padding' : [[0.2,0.5,0.0],[0.0]], 
                            'annotate' : [[False,False,True],[False]],
                            'confinex' : True, 
                            'fullday' : True,
                            'opacity' : 0.7, 
                            'plottitle' : 'Tilts'  
             }
                          

in e.g. tiltplots.cfg

defaultconfig like wic.cfg contains:

gridcolor='#316931',
fill=['t1','var2'], 
padding=[[0.2,0.5,0.0],[0.0]], annotate=[[False,False,True],[False]], confinex=True, fullday=True, opacity=0.7, plottitle='Tilts (until %s)'

currentvaluepath = '/srv/products/data/current.data'
"""

def WriteMemory(memorypath, memdict):
        """
        DESCRIPTION
             write memory
        """
        try:
            with open(memorypath, 'w', encoding='utf-8') as f:
                json.dump(memdict, f, ensure_ascii=False, indent=4)
        except:
            return False
        return True

def ReadMemory(memorypath,debug=False):
        """
        DESCRIPTION
             read memory
        -> Same function as used for imbot (imbotcore)
        """
        memdict = {}
        if os.path.isfile(memorypath):
            if debug:
                print ("Reading memory: {}".format(memorypath))
            with open(memorypath, 'r') as file:
                memdict = json.load(file)
        else:
            print ("Memory path not found - please check (first run?)")
        if debug:
            print ("Found in Memory: {}".format([el for el in memdict]))
        return memdict

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

def ReadDatastream(config={}, endtime=datetime.utcnow(), starttime=datetime.utcnow()-timedelta(days=5), sensorid=None, keylist=[], revision="0001", datapath='', filenamebegins='', flags=False, dropflagged=False, columns=[], units=[], debug=False):

    # Read seconds data and create plots
    dataid = '{}_{}'.format(sensorid,revision)
    db = config.get('primaryDB')
    fl=[]

    if debug:
        print ("READING data stream ...")
    if datapath and os.path.isdir(datapath):
        path = os.path.join(datapath,'{}*'.format(filenamebegins))
        if debug:
            print (" -> fixed path selected: {} and timerange from {} to {}".format(path,starttime,endtime))
        stream = read(path, starttime=starttime, endtime=endtime) 
    elif datapath and os.path.isfile(datapath):
        print (" -> fixed file selected: {}".format(path))
        stream = read(path, starttime=starttime, endtime=endtime) 
    elif datapath in ['db','DB','database']:
        if starttime < datetime.utcnow()-timedelta(days=15):
            print (" -> reading from archive files ...")
            path = os.path.join(config.get('archivepath'),sensorid,dataid,'*')
            print (path)
            stream = read(path, starttime=starttime, endtime=endtime) 
        else:
            print (" -> reading from database ...")
            stream = readDB(db,dataid,starttime=starttime, endtime=endtime)
    if columns and len(columns) == len(keylist):
        for ind,key in enumerate(keylist):
            coln = 'col-{}'.format(key)
            stream.header[coln] = columns[ind]
    if units and len(units) == len(keylist):
        for ind,key in enumerate(keylist):
            coln = 'unit-col-{}'.format(key)
            stream.header[coln] = units[ind]
    if debug:
        print (" -> obtained {} datapoints".format(stream.length()[0]))

    if flags:
        fl = db2flaglist(db,stream.header.get('SensorID'),begin=starttime, end=endtime)
        print (" - obtained {} flags in db".format(len(fl)))
    if dropflagged:
        print (" - dropping flagged data")    
        stream = stream.flag(fl)
        stream = stream.remove_flagged()

    return stream, fl



def CreateDiagram(streamlist,keylist, filllist=None, colorlist=None, paddinglist=None, annotatelist=None, gridcolor='#316931', confinex=True, fullday=True, opacity=0.7,style='magpy',show=False,fullplotpath='',debug=False):

    if debug:
        show=True
        
    if style in ['magpy','MagPy','MAGPY']:
        # TODO Union colorlist, etc
        mp.plotStreams(streamlist,keylist, fill=filllist, colorlist=colorlist, padding=paddinglist, annotate=annotatelist, gridcolor=gridcolor, confinex=confinex, fullday=fullday, opacity=opacity, noshow=True)
    else:
        print (" -> unkown plot style ... doing nothing")
        return False

    print ("  ... plotting done")
    if fullplotpath:
        print (" ... saving plot to {}".format(fullplotpath))
        if not debug:
            plt.savefig(fullplotpath)
        else:
            print (" ... debug selected : otherwise would save figure to {}".format(fullplotpath))
    if show:
        print (" ... debug mode or show selected - showing plot")
        plt.show()

    print ("  -> finished")
    return True



def main(argv):
    version = __version__
    configpath = ''
    statusmsg = {}
    outpath='/tmp'
    sensordefpath=''
    sensordefs = {}
    dayrange = 3
    plotstyle = 'magpy' # one in magpy, xxx
    starttime = None
    endtime = None
    newloggername = 'mm-pp-tilt.log'
    flaglist = []
    plotname = 'debug'
    debug=False

    dropflagged = False
    sensorid='LM_TILT01_0001'
    keylist=None


    try:
        opts, args = getopt.getopt(argv,"hc:i:o:y:s:e:r:l:D",["config=","input=","output=","style=","starttime=","endtime=","range=","loggername=","debug=",])
    except getopt.GetoptError:
        print ('try general_graph.py -h for instructions')
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
            print ('-i            : input json file for sensor information')
            print ('-o            : output directory (or file) to save the graph')
            print ('-y            : plot style')
            print ('-l            : loggername e.g. mm-pp-tilt.log')
            print ('-s            : starttime')
            print ('-e            : endtime')
            print ('-r            : range in days')
            print ('-------------------------------------')
            print ('Application:')
            print ('python3 general_graph.py -c ../conf/wic.cfg -i ../conf/sensordef_plot.json -e 2020-12-17')
            print ('# debug run on my machine')
            print ('python3 general_graph.py -c ../conf/wic.cfg -e 2020-12-17 -D')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-i", "--input"):
            # delete any / at the end of the string
            sensordefpath = os.path.abspath(arg)
        elif opt in ("-o", "--output"):
            # delete any / at the end of the string
            outpath = os.path.abspath(arg)
        elif opt in ("-y", "--style"):
            # define a plotstyle
            plotstyle = arg
        elif opt in ("-s", "--starttime"):
            # starttime of the plot
            starttime = arg
        elif opt in ("-e", "--endtime"):
            # endtime of the plot
            endtime = arg
        elif opt in ("-r", "--range"):
            # range in days
            dayrange = int(arg)
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

    if not os.path.exists(sensordefpath):
        print ('Sensordefinitions not found...')
        if debug:
            print (' ... but debug selected - using dummy values')
            sensordefs = debugsensor
            # creating a dummy sensordefs file in tmp        
            print (' ... and now creating an example in /tmp/sensordefinitions_default.json')
            WriteMemory('/tmp/sensordefinitions_default.json', sensordefs)
        else:
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

    if not starttime:
        starttime = endtime-timedelta(days=dayrange)
    else:
        try:
            starttime = DataStream()._testtime(starttime)
            dayrange = int((endtime-starttime).days)
        except:
            print ("Starttime could not be interpreted - Aborting")
            sys.exit(1)
        
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

    print ("4. Read sensordefinitions")
    if not sensordefs:
        sensordefs = ReadMemory(sensordefpath)
        plotname = os.path.basename(sensordefpath).replace('.json','').replace('_plot','')
        print ("Plotname : ", plotname)
        statname = "plot-{}".format(plotname)
        pass
    else:
        statname = "plot-{}".format('debug')

    print ("5. Cycle through sensordefinitions")
    streamlist = []
    keylist = []
    filllist = []
    paddinglist=[]
    annotatelist =[]
    colorlist=[]
    flaglist = []
    for cnt,dataid in enumerate(sensordefs):
        processname = "{}-{}".format(statname,dataid.replace("_","-"))
        statusmsg[processname] = "failure"
        useflags = False
        dropflagged = False
        sensdict = sensordefs[dataid]
        revision = sensdict.get('revision','0001')
        print ("5.{}.1 Check SensorID - or whether DataID provided for {}".format(cnt+1,dataid))
        sensorid, revision = CheckSensorID(dataid, revision, debug=debug)
        keys = sensdict.get('keys',[])
        columns = sensdict.get('columns',[])
        units = sensdict.get('units',[])
        path = sensdict.get('source','')
        flagtreatment = sensdict.get('flags','')
        if 'flag' in flagtreatment or 'drop' in flagtreatment:
            useflags = True
            if 'drop' in flagtreatment:
                dropflagged = True
        filenamebegins = sensdict.get('filenamebegins','')
        if keys:
            print ("5.{}.2 Read datastream for {}".format(cnt+1,dataid))
            try:
                stream, fl = ReadDatastream(config=config, starttime=starttime, endtime=endtime, sensorid=sensorid, keylist=keys, revision=revision, flags=useflags, dropflagged=dropflagged, datapath=path, filenamebegins=filenamebegins, columns=columns, units=units, debug=debug)
                if stream and stream.length()[0]>1:
                    print ("5.{}.3 Check out flagging and annotation".format(cnt+1))
                    if 'flag' in flagtreatment:
                        print ("       -> eventuallyadding existing standard flags from DB")
                        flaglist = fl
                    if 'quake' in flagtreatment:
                        quakekey = sensdict.get('quakekey',keys[0])
                        print ("       -> eventually adding QUAKES to column {}".format(quakekey))
                        fl = Quakes2Flags(config=config, endtime=endtime, timerange=dayrange+1, sensorid=sensorid, keylist=quakekey, debug=debug)
                        flaglist = combinelists(flaglist,fl)
                    if 'coil' in flagtreatment:
                        print ("       -> eventually adding COIL data to column xxx")
                        pass
                    if len(flaglist) > 0:
                        print ("       => total amount of {} flags added".format(len(flaglist)))
                        stream = stream.flag(flaglist)
                    print ("5.{}.4 Creating plot configuration lists".format(cnt+1))
                    streamlist.append(stream)
                    keylist.append(keys)
                    padding = sensdict.get('padding',[])
                    if not padding:
                        padding = [0.0 for el in keys]
                    paddinglist.append(padding)
                    annotate = sensdict.get('annotate',[])
                    if not annotate:
                        annotate = [False for el in keys]
                    annotatelist.append(annotate)
                    color = sensdict.get('color',[])
                    if not color:
                        color = ['k' for el in keys]
                    colorlist.extend(color)
                    fill = sensdict.get('fill',[])
                    filllist.extend(fill)
                    print ("  ==> section 5.{} done".format(cnt+1))
                    statusmsg[processname] = "success"
            except:
                print (" -- seviere error in data treatment")
                pass
        else:
            print ("  -- no keys defined - skipping this sensor")
            pass

    if len(streamlist) > 0:
        #mp.plotStreams(streamlist,keylist, fill=filllist, colorlist=colorlist, padding=paddinglist, annotate=annotatelist, gridcolor='#316931', confinex=True, opacity=0.7)
        print ("6. Creating plot")
        if os.path.isdir(outpath):
             # creating file name from sensorsdef input file
             fullplotpath = os.path.join(outpath,"{}_{}.png".format(plotname,datetime.strftime(endtime,"%Y-%m-%d")))
             print (" -> Saving graph to {}".format(fullplotpath))
        elif os.path.isfile(outpath):
             fullplotpath = outpath
        else:
             fullplotpath = ''
        show = True
        CreateDiagram(streamlist,keylist, filllist=filllist, colorlist=colorlist, paddinglist=paddinglist, annotatelist=annotatelist, gridcolor='#316931', confinex=True, opacity=0.7, show=show, fullplotpath=fullplotpath, debug=debug)

    debug = True
    
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
Example for a sensordefinitions json-file
Please use filenames like tilt_plot.json, or spaceweather_plot.json 
{
    "LEMI036_1_0002_0002": {
        "keys": [
            "x",
            "y",
            "z"
        ],
        "plotstyle": "line",
        "source": "/home/leon/Cloud/Daten/",
        "filenamebegins": "LEMI036_1_0002_0002",
        "color": [
            "k",
            "r",
            "k"
        ],
        "flags": "flag,quake",
        "fill": [
            "y"
        ],
        "quakekey": "z",
        "padding": [
            0.2,
            100.0,
            0.0
        ],
        "annotate": [
            true,
            false,
            true
        ],
        "columns": [
            "H",
            "E",
            "Z"
        ],
        "units": [
            "nT",
            "nT",
            "nT"
        ]
    },
    "GP20S3NSS2_012201_0001_0001": {
        "keys": [
            "f"
        ],
        "plotstyle": "line",
        "source": "/home/leon/Cloud/Daten/",
        "filenamebegins": "GP20S3NSS2_012201_0001_0001",
        "color": [
            "b"
        ],
        "flags": "flag",
        "padding": [
            100.0
        ],
        "annotate": [
            true
        ],
        "columns": [
            "S"
        ],
        "units": [
            "nT"
        ]
    }
}
"""
