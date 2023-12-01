#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DESCRIPTION
   Creates windose plots based on direction and strength.
   https://python-windrose.github.io/windrose/usage-output.html#A-stacked-histogram-with-normed-(displayed-in-percent)-results.
PREREQUISITES
   The following packegas are required:
      geomagpy >= 1.1.6
      windrose >= 1.9.0
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

INPUT field example: see bottom

APPLICATION
    Standard - will save the file to tmp with 'magnetism_ENDDATE.png' as filename:
        python3 windrose_graph.py -c ../conf/wic.cfg -i ../conf/magnetism_plot.json -e 2020-12-17
    Testing:
        python3 windrose_graph.py -c ../conf/wic.cfg -i ../conf/sensordef_plot.json -e 2020-12-17

WORKING EXAMPLES:

    Creating a Gamma plot of 10 days
        python3 general_graph.py -c /home/cobs/CONF/wic.cfg -i /home/cobs/CONF/plots/radon_plot.json -r 20 -o /srv/products/graphs/radon/ -l mm-pp-radon.log -D
    SpaceWeatherplot
        python3 general_graph.py -c /home/cobs/CONF/wic.cfg -i /home/cobs/CONF/plots/solarwindact_plot.json -r 5 -o /srv/products/graphs/spaceweather/ -l mm-pp-sw.log
    Tilt plot
        python3 general_graph.py -c /home/cobs/CONF/wic.cfg -i /home/cobs/CONF/plots/tilt_plot.json -r 5 -o /srv/products/graphs/tilt/ -l mm-pp-tilt.log -D
    Supergrad plot
          python3 general_graph.py -c /home/cobs/CONF/wic.cfg -i /home/cobs/CONF/plots/supergrad_plot.json -l mm-pp-supergrad.log

    Testing
          python3 general_graph.py -c ../conf/wic.cfg -i ../conf/supergrad_plot.json -l mm-pp-supergrad.log -D



EXAMPLE configuration file:

{
    # general plot parameters are defined within section "parameter"
    "parameter": {
        "show": "True",
        "confinex": "True"
    },
    # specific plot parameters for each data set are defined in subsection with dataid
    "Kp": {
        "keys": [
            "var1"
        ],
        # supported plotstyles are line and bar
        "plotstyle": "bar",
        # possible sources are directories, files or a database. if a database is selected, the section name is used to obtain the dataid
        "source": "/home/leon/Cloud/Daten/",
        "filenamebegins": "gfzkp",
        "color": [
            "k"
        ],
        "specialdict": {
            "var1" : [0,9]
            },
        # possible name attributes are "date" - will be replaced by date or any string like "latest"
        "savenameattribute": "date",
        "columns": [
            "Kp"
       ],
        "units": [
            ""
       ]
    },
    "ACESWEPAM": {
        "keys": [
            "var2",
            "var1"
        ],
        "plotstyle": "line",
        "source": "/home/leon/Cloud/Daten/",
        "filenameends": "_ace_swepam_1m.txt",
        # if a basesource is provided then the data of "source" is merged into the base. database access is not supported for basesource
        "basesource": "/home/leon/Cloud/Daten/",
        "basebegins": "DSCOVR_plasma_",
        "color": [
            "k",
            "k"
        ],
        "padding": [
            10.0,
            10.0
        ],
        "columns": [
            "Solar windspeed",
            "proton density"
        ],
        "units": [
            "km/s",
            "p/cc"
        ]
    },
    "ACEMAG": {
        "keys": [
            "z"
        ],
        "plotstyle": "line",
        "source": "/home/leon/Cloud/Daten/",
        "filenameends": "_ace_mag_1m.txt",
        "color": [
            "k"
        ],
        "padding": [
            0.1
        ],
        "columns": [
            "Bz"
        ],
        "units": [
            "nT"
        ]
    },
    "cobs_sop2": {
        "keys": [
            "x",
            "y"
        ],
        "plotstyle": "line",
        "source": "/home/leon/Cloud/Daten/Tilt",
        "filenamebegins": "cobs_sop2",
        "color": [
            "k",
            "k"
        ],
        "flags": "quake",    # Supported flags are 'flag' - flags from db applied, 'drop' - flags from db dropped, 'outlier' - outlier removed, 'quake' - flags taken from quake list
        "quakekey": "y",
        "padding": [
            0.01,
            0.01
        ],
        "annotate": [
            false,
            true
        ],
        "columns": [
            "X",
            "Y"
        ],
        "units": [
            "angle",
            "angle"
        ]
    },
    "cobs_meteo1": {
        "keys": [
            "x",
            "y",
            "t1",
            "var1",
            "var2"
        ],
        "plotstyle": "line",
        "source": "/home/leon/Cloud/Daten/Tilt",
        "filenamebegins": "cobs_meteo1",
        "color": [
            "r",
            "r",
            "r",
            "b",
            "c"
        ],
        "flags": "drop",
        "padding": [
            0.05,
            0.05,
            0.05,
            0.5,
            1.0
        ],
        "fill": [
            "var1",
            "var2"
        ],
        "columns": [
            "T (meteo1_1)",
            "T (meteo1_2)",
            "T (meteo1_3)",
            "rh",
            "P"
       ],
        "units": [
            "°C",
            "°C",
            "°C",
            "per",
            "hPa"
        ]
    }
}

"""
from matplotlib import cm
from windrose import WindroseAxes
from magpy.stream import *
from magpy.database import *
import getopt
import sys  # for sys.version_info()
import json

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, Quakes2Flags, combinelists, quakes2flags_new, ReadMemory
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__


debugsensor = { 'METEOSGO_adjusted_0001' : {
                            'directionkey' : ['var2'], # if len=2 then calculate with atan
                            'speedkey' : ['var1'],     # if len > 1 then caluclate vectorsum
                            'legend' : 'True',
                            'plottype' : 'bar', # can be bar or contour
                            'source' : '/Users/leon/GeoSphereCloud/Daten/CobsDaten/Meteo',
                            'filenamebegins' : 'METEOSGO_adjusted_0001',
                            'savenameattribute' : 'latest',
                            'transparent' : 'True',
                            'color' : ['k','r','k'],
                            'edgecolor' : 'white',
                            'facecolor' : 'white',
                            'plotname' : 'wind',
                            'flags' : 'flag',
                            'fill' : ['y'],
                            'columns' : ['H','E','Z'],
                            'units' : ['nT','nT','nT']
                          }
              }

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

def ReadDatastream(config={}, endtime=datetime.utcnow(), starttime=datetime.utcnow()-timedelta(days=5), sensorid=None, keylist=[], revision="0001", datapath='', filenamebegins='', filenameends='', flags=False, outlier=False, dropflagged=False, columns=[], units=[], debug=False):

    # Read seconds data and create plots
    dataid = '{}_{}'.format(sensorid,revision)
    db = config.get('primaryDB')
    fl=[]

    if debug:
        print ("READING data stream ...")
    if datapath and os.path.isdir(datapath):
        if debug:
            print("  - path provided")
        path = os.path.join(datapath,'{}*{}'.format(filenamebegins,filenameends))
        if debug:
            print ("  - fixed path selected: {} and timerange from {} to {}".format(path,starttime,endtime))
        stream = read(path, starttime=starttime, endtime=endtime)
    elif datapath and os.path.isfile(datapath):
        if debug:
            print ("  -  fixed file selected: {}".format(datapath))
        stream = read(datapath, starttime=starttime, endtime=endtime)
    elif datapath in ['db','DB','database']:
        if debug:
            print ("  -  database selected")
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
    if debug and stream:
        print (" -> obtained {} datapoints".format(stream.length()[0]))

    if flags:
        fl = db2flaglist(db,stream.header.get('SensorID'),begin=starttime, end=endtime)
        print (" - obtained {} flags in db".format(len(fl)))
    if outlier:
        ofl = stream.flag_outlier(threshold=3, timerange=timedelta(seconds=180))
        print (" - obtained {} flags in db".format(len(fl)))
    if dropflagged:
        print (" - dropping flagged data")
        stream = stream.flag(fl)
        stream = stream.remove_flagged()

    return stream, fl

def GetWdWsFromDatastream(datastream, dirkey, speedkey, debug=False):
    ws = np.array([])
    wd = np.array([])
    if not datastream:
        N = 500
        ws = np.random.random(N) * 6
        wd = np.random.random(N) * 360
    else:
        if len(speedkey)==1:
            speedkey = speedkey[0]
        if len(dirkey)==1:
            dirkey = dirkey[0]
        # drop nans
        print ("Length with NaN", datastream.length()[0])
        datastream = datastream._drop_nans(speedkey)
        datastream = datastream._drop_nans(dirkey)
        print ("Length without NaN", datastream.length()[0])
        if datastream.length()[0]>0:
            ws = datastream._get_column(speedkey)
            wd = datastream._get_column(dirkey)
    if debug:
        print ("Speed", ws)
        print ("Dir", wd)
    return wd, ws

def create_windrose(wd, ws, legend=True, normed=True, opening=0.8, edgecolor='white', facecolor='white', plottype='bar',fullplotpath='', transparent=False, show=False,debug=False):

    if debug:
        show=True

    ax = WindroseAxes.from_ax()
    ax.set_facecolor(facecolor)
    if plottype == 'contour':
        ax.contourf(wd, ws, bins=np.arange(0, 8, 1), cmap=cm.hot)
    else:
        ax.bar(wd, ws, normed=normed, opening=opening, edgecolor=edgecolor)
    if legend:
        ax.set_legend()

    print ("  ... plotting done")
    if transparent and debug:
        print ("   ... transparent mode selected for saving")
    if fullplotpath:
        if not debug:
            print(" ... saving plot to {}".format(fullplotpath))
            plt.savefig(fullplotpath, transparent=transparent)
        else:
            print (" ... debug selected : otherwise would save figure to {}".format(fullplotpath))
    if show:
        print (" ... debug mode or show selected - showing plot")
        plt.show()

    print ("  -> finished")
    return True

def create_savepath(outpath,plotname="dummy",savenameattribute='date', debug=False):
    fullplotpath = ''
    if os.path.isdir(outpath):
        if savenameattribute in ['date','DATE','Date']:
            # creating file name from sensorsdef input file
            fullplotpath = os.path.join(outpath,"{}_{}.png".format(plotname,datetime.strftime(endtime,"%Y-%m-%d")))
        else:
            fullplotpath = os.path.join(outpath,
                                            "{}_{}.png".format(plotname, savenameattribute))
    elif os.path.isfile(outpath):
        fullplotpath = outpath
    if debug:
        print(" - defining save path: {}".format(fullplotpath))
    return fullplotpath


def main(argv):
    version = __version__
    configpath = ''
    statusmsg = {}
    outpath='/tmp'
    sensordefpath=''
    sensordefs = {}
    range = 3600
    starttime = None
    endtime = None
    newloggername = 'mm-pp-rose.log'
    flaglist = []
    plotname = 'debug'
    succ = False
    debug=False

    opacity = 0.7
    fullday = False
    show = False
    bgcolor = 'white'

    sensorid='LM_TILT01_0001'


    try:
        opts, args = getopt.getopt(argv,"hc:i:o:s:e:r:l:D",["config=","input=","output=","starttime=","endtime=","range=","loggername=","debug=",])
    except getopt.GetoptError:
        print ('try windrose_graph.py -h for instructions')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- windrose_graph.py will plot sensor data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python windrose_graph.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-i            : input json file for sensor information')
            print ('-o            : output directory (or file) to save the graph')
            print ('-l            : loggername e.g. mm-pp-tilt.log')
            print ('-s            : starttime')
            print ('-e            : endtime')
            print ('-r            : range in seconds')
            print ('-------------------------------------')
            print ('Application:')
            print ('python3 windrose_graph.py -c ../conf/wic.cfg -i ../conf/sensordef_plot.json -e 2020-12-17')
            print ('# debug run on my machine')
            print ('python3 windrose_graph.py -c ../conf/wic.cfg -e 2023-11-17 -D')
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
        elif opt in ("-s", "--starttime"):
            # starttime of the plot
            starttime = arg
        elif opt in ("-e", "--endtime"):
            # endtime of the plot
            endtime = arg
        elif opt in ("-r", "--range"):
            # range in days
            range = int(arg)
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
        #if debug:
        print (' ... using default hard coded values')
        sensordefs = debugsensor

    if endtime:
        try:
            endtime = DataStream()._testtime(endtime)
        except:
            print ("Endtime could not be interpreted - Aborting")
            sys.exit(1)
    else:
        endtime = datetime.utcnow()

    if not starttime:
        starttime = endtime-timedelta(seconds=range)
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

    if debug:
        print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    if debug:
        print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "TestPlotter", job=os.path.basename(__file__), newname=newloggername, debug=debug)

    if debug:
        print ("3. Connect to databases")
    config = ConnectDatabases(config=config, debug=debug)

    if debug:
        print ("4. Read sensordefinitions")
    if not sensordefs:
        if debug:
            print ("Reading sensor and graph definitions from {}".format(sensordefpath))
        sensordefs = ReadMemory(sensordefpath)
        plotname = os.path.basename(sensordefpath).replace('.json','').replace('_rose','')
        if debug:
            print ("Plotname : ", plotname)
        statname = "rose-{}".format(plotname)
    else:
        if debug:
            print ("Using default sensor and graph definitions")
        statname = "rose-{}".format('debug')

    if debug:
        print ("5. Cycle through sensor definitions")
    defslen = [sen for sen in sensordefs]
    if len(defslen) > 0:
        useflags = False
        outlier = False
        dropflagged = False
        legend = False
        normed = False
        dataid = list(sensordefs.keys())[0]
        sensdict = sensordefs[dataid]
        if debug:
            print ("Got the following parameters for sensor {}: {}".format(dataid, sensdict))
        processname = "{}-{}".format(statname,dataid.replace("_","-"))
        statusmsg[processname] = "failure"
        revision = sensdict.get('revision','0001')
        if debug:
            print ("5.1.1 Check SensorID - or whether DataID provided for {}".format(dataid))
        sensorid, revision = CheckSensorID(dataid, revision, debug=debug)
        directionkey = sensdict.get('directionkey',[])
        speedkey = sensdict.get('speedkey',[])
        columns = sensdict.get('columns',[])
        units = sensdict.get('units',[])
        path = sensdict.get('source','')
        legend = sensdict.get('legend',False)
        if legend in ["True","true","TRUE", True]:
            legend = True
        normed = sensdict.get('normed',False)
        if normed in ["True","true","TRUE", True]:
            normed = True
        transparent = sensdict.get('transparent', False)
        if transparent in ["True","true","TRUE", True]:
            transparent = True
        flagtreatment = sensdict.get('flags','')
        if 'outlier' in flagtreatment:
            outlier = True
            dropflagged = True
        filenamebegins = sensdict.get('filenamebegins','')
        filenameends = sensdict.get('filenameends','')
        plotname = sensdict.get('plotname','noname')
        if debug:
            print ("5.1.2 Read datastream for {}".format(dataid))
            print (path,sensorid, revision)
        stream, fl = ReadDatastream(config=config, starttime=starttime, endtime=endtime, sensorid=sensorid, revision=revision, flags=useflags, outlier=outlier, dropflagged=dropflagged, datapath=path, filenamebegins=filenamebegins, filenameends=filenameends,columns=columns, units=units, debug=debug)
        if stream and stream.length()[0]>1:
            if debug:
                print ("5.1.3 Extracting speed and direction data")
            wd, ws = GetWdWsFromDatastream(stream, dirkey=directionkey, speedkey=speedkey, debug=debug)

            if debug:
                print ("5.1.4 Defining path")
            fulloutpath = create_savepath(outpath,plotname=plotname,savenameattribute=sensdict.get('savenameattribute','date'), debug=debug)

            if debug:
                print ("5.1.5 Creating plot")
            succ = create_windrose(wd, ws, legend=legend, normed=normed, opening=0.8, edgecolor=sensdict.get('edgecolor','white'), facecolor=sensdict.get('facecolor','white'), plottype=sensdict.get('plottype','bar'), transparent=transparent, fullplotpath=fulloutpath,
                           show=False, debug=debug)
            if succ:
                statusmsg[processname] = "success"

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])

