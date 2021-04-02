#!/usr/bin/env python
# coding=utf-8

"""
DESCRIPTION
   Create a BLV file from expected field values D, I,F. If no field values are provided
   then IGRF values will be obtained for the StationLocation from the obscode provided in
   the confguration file. Make sure that no current.data path is defined for stations.

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -v vector              :   int     :  only provided for testing, obtaining IGRF value otherwise
    -t time                :   string  :  default is 1. of month, at least after the fifth day (so that data should be present) 

APPLICATION
    Runtime:
        python3 baseline_generator.py -c ../conf/gam.cfg -o Figrf:-147.0
    Reconctruct baseline file:
        python3 baseline_generator.py -c ~/CONF/swz.cfg -t 2020-09-01T03:00:00 -o Figrf:-6.0
    Testing:
        python3 baseline_generator.py -c ../conf/gam.cfg -t 2018-08-08T07:41:00 -v 64.33397725500629,4.302646668706179,48621.993688723036 -D

#2018-08-08T07:41:00.000000,64.33397725500629,4.302646668706179,48621.993688723036,48621.993688723036,7.3,0.9979648645227842,4.604435241283524,2.101801590493833,57.766936972403585,5.217477545447175,57.62508321847123,25.206756810268416,4.25880815395662,-19.210346713720355,nan,Herzog,T010B_160391_07-2011_MAG01H_504_0911H_07-2011,180.1372,Fext,0000000000000000-,-,idff,nan
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
from analysismethods import DefineLogger, DoVarioCorrections, DoBaselineCorrection, DoScalarCorrections,ConnectDatabases, GetPrimaryInstruments, getcurrentdata, writecurrentdata
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__

import urllib.request
import json


def GetDIF(config={}, settime=datetime.utcnow(), offset=None,  debug=False):

    baseaddress = "http://{}".format(config.get('igrfmodel'))
    #baseaddress = 'http://geomag.bgs.ac.uk/web_service/GMModels/igrf/13/'  # take from config

    print (" Getting Reference DIF values for your location from IGRF and F record")
    offsets = {"D" : 0.0, "I": 0.0, "Figrf" : 0.0, "Flocal" : 0.0 }
    if offset:
        for off in offset:
            if off in offsets:
                offsets[off] = offset.get(off)
        print ("   Applying offsets: ", offsets)

    db = config.get('primaryDB',None)
    # 1. get stationcode from config
    stationid = config.get('obscode')
    # 2. obtain location data from StationsID
    try:
        longi = dbselect(db, 'StationLongitude', 'STATIONS', 'StationID = "{}"'.format(stationid))[0]
        lati = dbselect(db, 'StationLatitude', 'STATIONS', 'StationID = "{}"'.format(stationid))[0]
        elev = dbselect(db, 'StationElevation', 'STATIONS', 'StationID = "{}"'.format(stationid))[0]
    except:
        print ("    !!! Could not extract location from STATIONS")
        sys.exit(1)
    datum = datetime.strftime(settime,"%Y-%m-%d")
    # 3. feed location data to webservice
    url = "{}?latitude={}&longitude={}&altitude={}&date={}&format=json".format(baseaddress, lati, longi, float(elev)/1000., datum)
    print ("Requesting data: {}".format(url))
    # 4. Extract DIF from JSON
    with urllib.request.urlopen(url) as response:
        html = response.read()
    datadict = json.loads(html)
    maindict = datadict.get('geomagnetic-field-model-result')
    model = maindict.get('model')
    modelrevision = maindict.get('model_revision')
    fieldvalues = maindict.get('field-value')
    F = fieldvalues.get('total-intensity').get('value') + float(offsets.get("Figrf",0.0))
    D = fieldvalues.get('declination').get('value') + float(offsets.get("D",0.0))
    I = fieldvalues.get('inclination').get('value') + float(offsets.get("I",0.0))
    DIFsource = '{}{}'.format(model,modelrevision)
    print ("  -> Done: obtained D = {} deg, I = {} deg and F = {} nT from IGRF".format(D,I,F))
    # 5. read primaryScalar
    try:
        print ("  Trying to get reference F value from real measurement")
        scalar = config.get('primaryScalar')
        print (scalar)
        endtime = datetime.strptime(datetime.strftime(settime+timedelta(days=1),"%Y-%m-%d"),"%Y-%m-%d")
        revision = '0001'
        print ("   -> check sensor")
        scalar, revision = CheckSensorID(scalar, revision, debug=debug)
        print ("   -> read datastream")
        stream = ReadDatastream(config=config, endtime=endtime, timerange=1, sensorid=scalar, revision=revision, debug=debug)
        # subtract offset
        stream = DoScalarCorrections(db, stream, scalarsens=scalar, starttimedt=endtime-timedelta(days=1), debug=debug)
        idx, line =  stream.findtime(settime)
        F = stream.ndarray[4][idx] + float(offsets.get("Flocal",0.0))
        print ("   -> Done: using F = {} nT from scalar magnetometer {}".format(F,scalar))
    except:
        pass

    return [I,D,F], DIFsource


def IDF2HDZ(fieldvector):
    I = fieldvector[0]
    D = fieldvector[1]
    F = fieldvector[2]
    ang_fac = 1.
    dc = D*np.pi/(180.*ang_fac)
    ic = I*np.pi/(180.*ang_fac)
    X = F*np.cos(dc)*np.cos(ic)
    Y = F*np.sin(dc)*np.cos(ic)
    Z = F*np.sin(ic)
    H = np.sqrt(X**2 + Y**2)
    return [H,D,Z]


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


def GetDeltas(config={}, settime=datetime.utcnow(), fieldvector=[0,0,0], debug=False):

    db = config.get('primaryDB',None)
    sensorid = config.get('primaryVario')
    print (sensorid)
    #config['primaryVario'] = variosens
    #config['primaryScalar'] = scalasens
    endtime = datetime.strptime(datetime.strftime(settime+timedelta(days=1),"%Y-%m-%d"),"%Y-%m-%d")
    revision = '0002'
    print ("   -> check sensor")
    sensorid, revision = CheckSensorID(sensorid, revision, debug=debug)

    print ("   -> read datastream for {}".format(sensorid))
    stream = ReadDatastream(config=config, endtime=endtime, timerange=1, sensorid=sensorid, revision=revision, debug=debug)

    if stream.length()[0] > 0 and db:
        stream = DoVarioCorrections(db, stream, variosens=sensorid, starttimedt=endtime-timedelta(days=1))
    #if debug:
    #mp.plot(stream)
    stream = stream.xyz2hdz()
    idx, line =  stream.findtime(settime)
    Hv = stream.ndarray[1][idx]
    Dv = stream.ndarray[2][idx]
    Zv = stream.ndarray[3][idx]
    absvector = IDF2HDZ(fieldvector)
    dH = absvector[0]-Hv 
    dD = absvector[1]-Dv 
    dZ = absvector[2]-Zv
    if debug:
        print ("   -> obtained the following basevalues:")
        print (dH,dD,dZ)
    return [dH,dD,dZ]

def CreateOutputStream(config={}, settime=None, fieldvector=[], deltavector=[], observer="Robot", DIFsource="IGRF", debug=False):
    # 1. try : create stream and add too existing, write
    # if fails 2. try: read existing, add data, remove duplicates, write

    outname = "BLV_{}_{}_{}".format(config.get('primaryVario'),config.get('primaryScalar'),config.get('primarypier'))
    output = DataStream()
    output.header['DataPier'] =  config.get('primarypier') # :  A2
    output.header['DataComponents'] =  "IDFF"
    output.header['SensorID'] = outname # SensorID:  BLV_LEMI036_1_0002_GP20S3NSS2_012201_0001_A2
    output.header['DataID'] = outname # DataID:  BLV_LEMI036_1_0002_GP20S3NSS2_012201_0001_A2
    output.header['StationID'] = config.get('obscode')  # StationID:  WIC
    output.header['DataFormat'] =  "MagPyDI"
    inputlist = [date2num(settime),fieldvector[0],fieldvector[1],fieldvector[2],fieldvector[2],np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,deltavector[0],deltavector[1],deltavector[2],np.nan, observer,DIFsource,"None","Fext","0000000000000000-","-","idff","nan"]
    array = np.asarray([np.asarray([inputlist[idx]]) for idx,key in enumerate(KEYLIST)]).astype(object)
    output.ndarray = array
    output.header['col-time'] = 'Epoch'
    output.header['col-x'] = 'i'
    output.header['col-y'] = 'd'
    output.header['col-z'] = 'f'
    output.header['col-f'] = 'f'
    output.header['unit-col-x'] = 'deg'
    output.header['unit-col-y'] = 'deg'
    output.header['unit-col-z'] = 'nT'
    output.header['unit-col-f'] = 'nT'
    output.header['col-dx'] = 'H-base'
    output.header['col-dy'] = 'D-base'
    output.header['col-dz'] = 'Z-base'
    output.header['unit-col-dx'] = 'nT'
    output.header['unit-col-dy'] = 'deg'
    output.header['unit-col-dz'] = 'nT'
    output.header['col-str1'] = 'Person'
    output.header['col-str2'] = 'DI-Inst'
    output.header['col-str3'] = 'Mire'
    output.header['col-str4'] = 'F-type'

    filename = os.path.join(config.get('dipath'), "{}.txt".format(outname))

    if not debug:
        try:
            output.write(config.get('dipath'), coverage='all', filenamebegins=outname, format_type='PYSTR', mode='replace')
            print ("   -> absfile written to {}".format(filename))
            success = True
        except:
            success = False
    else:
        success = True
        print ("   -> Debug selected - skipping writing of data file")

    return success


def main(argv):
    try:
        version = __version__
    except:
        version = "1.0.0"
    configpath = ''
    statusmsg = {}
    debug=False
    settime = None
    fieldvector = None
    newloggername = 'mm-dp-gams.log'
    DIFsource="UserValue"
    off = ''
    offset = {}

    try:
        opts, args = getopt.getopt(argv,"hc:v:t:o:l:D",["config=","vector=","time=","offset=","loggername=","debug=",])
    except getopt.GetoptError:
        print ('baseline_generator.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- baseline_generator.py will analyse magnetic data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python baseline_generator.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-v            : define a vector (absolute field values for this time)')
            print ('              :  - if not provided, IGRF data will be used')
            print ('-t            : time')
            print ('-o            : offset  - e.g. -o Flocal:-15.51,Figrf:100,D:0.00001')
            print ('-l            : loggername')
            print ('-------------------------------------')
            print ('Application:')
            print ('python3 baseline_generator.py -c ../conf/gam.cfg')
            print ('python3 baseline_generator.py -c ../conf/gam.cfg -t 2018-08-08T07:41:00 -v 64.33397725500629,4.302646668706179,48621.993688723036')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-v", "--vector"):
            # define a vector (absolute field values for this time) - if not provided, IGRF data will be used
            vector = arg.split(',')
            fieldvector = [float(el) for el in vector]
        elif opt in ("-t", "--time"):
            # define a point in time for the current analysis
            settime = arg
        elif opt in ("-o", "--offset"):
            # define a point in time for the current analysis
            off = arg
        elif opt in ("-l", "--loggername"):
            # define an endtime for the current analysis - default is now
            newloggername = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running magpy_products version {}".format(version))

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    if off:
        try:
            offlist = off.split(',')
            for el in offlist:
                keyvalue = el.split(':')
                if len(keyvalue) == 2:
                    offset[keyvalue[0]] = float(keyvalue[1])
        except:
            offset = {}

    if settime:
        try:
            settime = DataStream()._testtime(settime)
        except:
            print ("Endtime could not be interpreted - Aborting")
            sys.exit(1)
    else:
        settime = datetime.utcnow() ### selected projected time at 1. of month # run job once every day only if settime day equals day
        y = settime.year
        m = settime.month
        d = settime.day
        if settime.day < 5:
            m = (settime-timedelta(days=10)).month
        settime = datetime(y,m,1,3)
        print ("   Current analysis date will be {}".format(settime))


    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname=newloggername, debug=debug)

    ##### IMPORTANT: for GAM and SWZ the primary Instruments are not contained in current.data
    print ("3. Loading current.data (not existing for SWZ and GAM) and getting primary instruments")
    config, statusmsg = GetPrimaryInstruments(config=config, statusmsg=statusmsg, debug=debug)

    print ("4. Connect to databases")
    config = ConnectDatabases(config=config, debug=debug)

    # Obtain DIF
    print ("5. Calculate projected IDF")
    name5 = "{}-getIDF".format(config.get('logname'))
    if not fieldvector:
        try:
            fieldvector, DIFsource = GetDIF(config=config, settime=settime, offset=offset, debug=debug)  # return [I,D,F]
            statusmsg[name5] = 'field vector obtained from IGRF model'
        except:
            statusmsg[name5] = 'field vector could not be obtained from IGRF model'
    else:
        statusmsg[name5] = 'field vector provided in call'

    print ("6. Read variometer")
    name6 = "{}-readVario".format(config.get('logname'))
    try:
        deltas = GetDeltas(config=config, settime=settime, fieldvector=fieldvector, debug=debug)
        statusmsg[name6] = 'Deltas successfully obtained'
    except:
        statusmsg[name6] = 'Obtaining deltas failed'

    print ("7. Creating output")
    name7 = "{}-writingAbsData".format(config.get('logname'))
    try:
        success = CreateOutputStream(config=config, settime=settime, fieldvector=fieldvector, deltavector=deltas, observer="BGSwebservice", DIFsource=DIFsource, debug=debug)
        statusmsg[name7] = 'Absdata file written'
        if not success:
            statusmsg[name7] = 'Writing of absdata file failed'
    except:
        statusmsg[name7] = 'Output method could not be performed -check'

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])


