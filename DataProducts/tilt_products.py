#!/usr/bin/env python
# coding=utf-8

"""
DESCRIPTION
   Tilt-products provides a filtered access to to tilt data 
   
   The output files contains the following data:
   x:   timeline of induction coil
   y:   main power GMO
   z:   entrance control (magnetic) GMO
   f:   CO2 value of GMO
   t1:  outside temperature at GMO
   t2:  lab temperatur at GMO
   var1: temperature GMO Seg 1
   var2: temperature GMO Seg 2
   var3: temperature GMO Seg 3
   var4: temperature GMO Seg 4
   var5: temperature GMO Seg 5
   dx:  temperature of cabinet GMO
   dy:  temperature of cabinet GMO
   dz:  pressure in tunnel GMO
   df:  nS/h GMO

SGO
   x:   timeline of induction coil
   y:   main power SGO
   z:   entrance control SGO
   f:   CO2 value of SGO
   t1:  outside temperature in SGO
   t2:  lab temperatur of the tunnel SGO
   var1: temperature SGO Seg 1
   var2: temperature SGO Seg 2
   var3: temperature SGO Seg 3
   dx:  temperature of cabinet SGO
   dy:  temperature of cabinet SGO
   dz:  pressure in tunnel SGO
   df:  nS/h SGO
   
PREREQUISITES
   The following packegas are required:
      geomagpy >= 1.0.0
      martas.martaslog
      martas.acquisitionsupport
      analysismethods
PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"
    -s starttime           :   date    :  new in version 1.0.1


APPLICATION
    PERMANENTLY with cron:
        python magnetism_products.py -c /etc/marcos/analysis.cfg
    REDO analysis for a time range:
        (startime is defined by endtime - daystodeal as given in the config file 
        python magnetism_products.py -c /etc/marcos/analysis.cfg -e 2020-11-22
    REDO quasidefinitve data analysis 
        python magnetism_products.py  -c /home/cobs/CONF/wic.cfg -j quasidefinitive -s 2021-07-13 -e 2021-07-20 -F -l test.log
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
from analysismethods import DefineLogger, DoVarioCorrections, DoBaselineCorrection, DoScalarCorrections,ConnectDatabases, GetPrimaryInstruments, getcurrentdata, writecurrentdata, load_current_data_sub
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__


# ################################################
#         Read Methods for specific data sources
# ################################################


tabledictionary = {'Barrier':'', 'GMO light':'', 'GMO CO2' : 'MQ135_20220214_0001_0001', 'GMO T1': , 


def combinelists(l1,l2):
    if len(l1) > 0 and len(l2) > 0:
        l1.extend(l2)
    elif not len(l1) > 0 and len(l2) > 0:
        l1 = l2
    return l1



def readTable(db, sourcetable="ULTRASONIC%", source='database', path='', starttime='', endtime='', debug=False):
    """
    DESCRIPTION:
        read data from database for all sensors matching sourcetabe names and timerange
        If archive is selected as source, then the database is searched for all sensors and
        the archive directory is scanned for all related raw files in related subdirectores
    """
    print ("  -----------------------")
    print ("  Reading source tables like {}".format(sourcetable))
    datastream = DataStream([],{},np.asarray([[] for key in KEYLIST]))
    if db and not starttime == '':
        search = 'SensorID LIKE "{}"'.format(sourcetable)
        senslist = dbselect(db, 'DataID', 'DATAINFO',search)
        sens=[]
        sens2=[]
        for sensor in senslist:
            print ("Found", sensor)
            if source == 'database':
                if debug:
                    print ("   -- checking sensor {}".format(sensor))
                last = dbselect(db,'time',sensor,expert="ORDER BY time DESC LIMIT 1")
                ### last2 should be the better alternative
                last2 = dbselect(db,'DataMaxTime','DATAINFO','DataID="{}"'.format(sensor))
                #print (last, starttime, last2)
                if not last2:
                    last2 = last
                if last and (getstringdate(last[0]) > starttime or getstringdate(last2[0]) > starttime):
                    #sens.append(sensor)
                    sens2.append([sensor,getstringdate(last[0])])
                    if debug:
                        print ("     -> valid data for sensor {}".format(sensor))
            else:
                sens.append(sensor)

        # sort sens2 so that the one with the latest record is last
        if not sens:
            sens = [el[0] for el in sorted(sens2, key=lambda x: x[1])]

        #print (sens)

        datastream = DataStream([],{},np.asarray([[] for key in KEYLIST]))
        if len(sens) > 0:
            for sensor in sens:
                print ("    -- getting data from {}".format(sensor))
                try:
                    if source == 'database':
                        datastream = readDB(db,sensor,starttime=starttime,endtime=endtime)
                    else:
                        raw = 'raw'
                        if sensor.startswith('BM35'):
                            raw = sensor[:-5]+'_0002'
                        print ("    -- reading from {}".format(os.path.join(path,sensor[:-5],raw)))
                        if debug:
                            print ("       starttime: {}, endtime: {}".format(starttime,endtime))
                        datastream = read(os.path.join(path,sensor[:-5],raw,'*'),starttime=starttime,endtime=endtime)
                except:
                    datastream = DataStream()
                if debug:
                    print ("      -> got data with range: {}".format(datastream._find_t_limits()))
                print ("      -> Done")

    if debug:
        print ("      -> obtained {}".format(datastream.length()[0]))
    print ("      -> readTable finished")
    print ("  -----------------------")

    return datastream


def transformDS(db, datastream, sourcekey='t1', destinationkey='t1', debug=False):
    """
    DESCRIPTION:
        filter and transform dallas sensor data to obtain filtered temperature records
        # Columns after: 
        pressure: 'var5'
    PARAMTER:
        dbupdate : if True then new flags will be written to DB
    """
    flaglist = []

    print ("  Transforming Dallas")
    if datastream.length()[0] > 0:
        if debug:
            print ("    -- getting existing flags ...")
        start, end = datastream._find_t_limits()
        flaglist = db2flaglist(db, datastream.header.get("SensorID"),begin=start,end=end)
        if len(flaglist) > 0:
            datastream = datastream.flag(flaglist)
            datastream = datastream.remove_flagged()        
            if debug:
                print ("    -- found and removed {} existing flags".format(len(flaglist)))
        if debug:
            print ("    -- flagging outliers ...")
        flaglist = datastream.flag_outlier(threshold=3,returnflaglist=True)
        if len(flaglist) > 0:
            print ("    -- found {} new outliers".format(len(flaglist)))
            #if not debug:
            #    print ("    -> writing {} new flags to DB".format(len(flaglist)))
            #    flaglist2db(db, flaglist)
            datastream = datastream.flag(flaglist)
            datastream = datastream.remove_flagged()
            
        print ("    -- filtering to minute")
        datastream = datastream.filter() # minute data

        if not sourcekey==destinationkey:
            datastream._move_column(sourcekey,destinationkey)
            datastream._drop_column(sourcekey)
    print ("      -> Done")
    return datastream, flaglist


def transformRCS(db, datastream, debug=False):
    """
    DESCRIPTION:
        filter and transform rcs t7 data to produce a general structure for combination
        # --------------------------------------------------------    
        # RCS - Get data from RCS (no version/revision control in rcs) 
        # Schnee: x, Temperature: y,  Maintainance: z, Pressure: f, Rain: t1,var1, Humidity: t2
        # 
        # Columns after: 
        pressure (if existing): 'var5'
        temperature: 'f'
        snowheight: 'z'
        rain: 'y'
        humidity: 't1'

    PARAMTER:
        dbupdate : if True then new flags will be written to DB
    """

    print ("  Transforming RCST7 data")

    filtdatastream = DataStream()
    flaglist = []
    if datastream.length()[0]>0:
        start, end = datastream._find_t_limits()
        print ("    -- getting existing flags for {} ...".format(datastream.header.get("SensorID")))
        flaglist = db2flaglist(db,datastream.header.get("SensorID"),begin=start, end=end)
        print ("    -- found {} flags for given time range".format(len(flaglist)))
        if len(flaglist) > 0:
            datastream = datastream.flag(flaglist)
        datastream = datastream.remove_flagged()
        datastream.header['col-y'] = 'T'
        datastream.header['unit-col-y'] = 'deg C'
        datastream.header['col-t2'] = 'rh'
        datastream.header['unit-col-t2'] = 'percent'
        datastream.header['col-f'] = 'P'
        datastream.header['unit-col-f'] = 'hPa'
        datastream.header['col-x'] = 'snowheight'
        datastream.header['unit-col-x'] = 'cm'

        flaglist = []
        print ("    -- cleanup snow height measurement - outlier")  # WHY NOT SAVED?? -- TOO MANY Flags -> needs another method
        removeimmidiatly = True
        if removeimmidiatly:
            datastream = datastream.flag_outlier(keys=['x'],timerange=timedelta(days=5),threshold=3)
            datastream = datastream.remove_flagged()
        else:
            flaglist = datastream.flag_outlier(keys=['x'],timerange=timedelta(days=5),threshold=3,returnflaglist=True)
        print ("      -> size of flaglist now {}".format(len(flaglist)))
        print ("    -- cleanup rain measurement")
        try:
            z = datastream._get_column('z')
            if np.mean('z') >= 0 and np.mean('z') <= 1:
                flaglist0 = datastream.bindetector('z',1,['t1','z'],datastream.header.get("SensorID"),'Maintanence switch for rain bucket',markallon=True)
            else:
                print ("      -> flagging of service switch rain bucket failed")
        except:
            flaglist0 = []
            print ("      -> flagging of service switch rain bucket failed")
        flaglist = combinelists(flaglist,flaglist0)
        print ("      -> size of flaglist now {}".format(len(flaglist)))
        print ("    -- cleanup temperature measurement")
        flaglist1 = datastream.flag_outlier(keys=['y'],timerange=timedelta(hours=12),returnflaglist=True)
        flaglist = combinelists(flaglist,flaglist1)
        print ("      -> size of flaglist now {}".format(len(flaglist)))
        print ("    -- cleanup pressure measurement") # not part of rcs any more -> flag only if mean is between 800 and 1000...
        if not np.isnan(datastream.mean('f')) and 800 < datastream.mean('f') and datastream.mean('f') < 1000:
            flaglist2 = datastream.flag_range(keys=['f'], flagnum=3, keystoflag=['f'], below=800,text='pressure below value range')
            flaglist = combinelists(flaglist,flaglist2)
            flaglist3 = datastream.flag_range(keys=['f'], flagnum=3, keystoflag=['f'], above=1000,text='pressure exceeding value range')
            flaglist = combinelists(flaglist,flaglist3)
            print ("      -> size of flaglist now {}".format(len(flaglist)))
        else:
            datastream._drop_column('f')
            print ("      -> no pressure data found")
        print ("    -- cleanup humidity measurement")
        flaglist4 = datastream.flag_range(keys=['t2'], flagnum=3, keystoflag=['t2'], above=100, below=0,text='humidity not valid')
        flaglist = combinelists(flaglist,flaglist4)
        print ("      -> size of flaglist now {}".format(len(flaglist)))
        datastream = datastream.flag(flaglist)
        print ("    -- found new flags: {}".format(len(flaglist)))
        datastream = datastream.remove_flagged()
        print ("    -- found and removed new flags: {}".format(len(flaglist)))

        # Now Drop flag and comment line - necessary because of later filling of gaps
        flagpos = KEYLIST.index('flag')
        commpos = KEYLIST.index('comment')
        datastream.ndarray[flagpos] = np.asarray([])
        datastream.ndarray[commpos] = np.asarray([])
        ## Now use missingvalue treatment
        print ("    -- interpolating missing values if less then 5 percent are missing within 2 minutes")
        datastream.ndarray[1] = datastream.missingvalue(datastream.ndarray[1],120,threshold=0.05,fill='interpolate')
        print ("    -- determine average rain")
        res = datastream.steadyrise('t1', timedelta(minutes=60),sensitivitylevel=0.002)
        print ("       -> RCST7 rain checked")
        datastream= datastream._put_column(res, 'var1', columnname='Percipitation',columnunit='mm/1h')
        print ("    -- filter all RCS data columns to 1 min")
        filtdatastream = datastream.filter(missingdata='interpolate')

        filtdatastream._move_column('f','var5')
        filtdatastream._move_column('y','f')
        filtdatastream._move_column('x','z')
        filtdatastream._drop_column('x')
        filtdatastream._move_column('var1','y')
        filtdatastream._move_column('t2','t1')
        filtdatastream._drop_column('t2')
        filtdatastream._drop_column('var1')
        filtdatastream._drop_column('var2')
        filtdatastream._drop_column('var3')
        filtdatastream._drop_column('var4')

    else:
        filtdatastream = DataStream()

    print ("      -> Done")

    return filtdatastream, flaglist


def transformMETEO(db, datastream, debug=False):
    """
    DESCRIPTION:
        filter and transform rcs t7 data to produce a general structure for combination
        # --------------------------------------------------------
        # METEO - Get data from RCS (no version/revision control in rcs)
        # Schnee: z, Temperature: f, Humidity: t1, Pressure: var5
    """

    print ("  Transforming METEO data")

    if datastream.length()[0] > 0:
        start, end = datastream._find_t_limits()
        flaglist = db2flaglist(db,datastream.header.get("SensorID"),begin=start, end=end)
        print ("    -- Found existing flags: {}".format(len(flaglist)))
        datastream = datastream.flag(flaglist)
        datastream = datastream.remove_flagged()
        datastream = datastream.flag_outlier(keys=['f','z'],timerange=timedelta(days=5),threshold=3)
        # meteo data is not flagged
        datastream = datastream.remove_flagged()
        print ("    -- Cleanup pressure measurement")
        if not np.isnan(datastream.mean('var5')) and 800 < datastream.mean('var5') and datastream.mean('var5') < 1000:
            flaglist1 = datastream.flag_range(keys=['var5'], flagnum=3, keystoflag=['var5'], below=800,text='pressure below value range')
            flaglist = combinelists(flaglist,flaglist1)
            flaglist15 = datastream.flag_range(keys=['var5'], flagnum=3, keystoflag=['var5'], above=1000,text='pressure exceeding value range')
            flaglist = combinelists(flaglist,flaglist15)
        else:
            print ("      -> no pressure data found")
            datastream._drop_column('var5')
        print ("    -- Cleanup humidity measurement")
        flaglist2 = datastream.flag_range(keys=['t1'], flagnum=3, keystoflag=['t1'], above=100, below=0)
        flaglist = combinelists(flaglist,flaglist2)
        meteost = datastream.flag(flaglist)
        meteost = datastream.remove_flagged()
        print ("    -- Determine average rain")
        res = datastream.steadyrise('dx', timedelta(minutes=60),sensitivitylevel=0.002)
        print ("       -> METEO rain checked")
        if np.isnan(np.sum(res)):
            print (" STEADYRISE: found a NAN value")
        datastream = datastream._put_column(res, 'y', columnname='Percipitation',columnunit='mm/1h')
        flagpos = KEYLIST.index('flag')
        commpos = KEYLIST.index('comment')
        datastream.ndarray[flagpos] = np.asarray([])
        datastream.ndarray[commpos] = np.asarray([])

        print ("    -- cleaning data stream")
        # Meteo
        datastream._drop_column('x')
        datastream._drop_column('t2')
        #datastream._drop_column('var1')   # remove after CheckRain
        datastream._drop_column('var2')
        datastream._drop_column('var3')
        datastream._drop_column('var4')
        datastream._drop_column('dx')
        datastream._drop_column('dy')
        datastream._drop_column('dz')
        datastream._drop_column('df')

    print ("      -> Done")

    return datastream

def ValidityCheckDirectories(config={},statusmsg={}, debug=False):
    """
    DESCRIPTION:
        Check availability of paths for saving data products
    """
    name0 = "{}-obligatory directories".format(config.get('logname','Dummy'))
    statusmsg[name0] = 'all accessible'
    successlist = [True,True]

    vpath = config.get('variationpath')
    qpath = config.get('quasidefinitivepath')
    figpath = config.get('magfigurepath')
    dipath = config.get('dipath')
    dbcreds = config.get('dbcredentials')
    if not isinstance(dbcreds,list):
        dbcreds = [dbcreds]
    try:
        # Asuming dbpasswf is also good for mounting
        dbcred = dbcreds[0] # only primary
        dbpasswd = mpcred.lc(dbcred,'passwd')
    except:
        dbpasswd=''


    def umount(path,pwd):
        """usage: umount("/srv/archive")"""
        cmd = 'umount ' + path
        print ("Sending umount command: {}".format(cmd))
        echo = 'echo {}|sudo -S {}'.format(pwd,cmd)
        subprocess.Popen(str(echo), shell=True, stdout=subprocess.PIPE)
        print ("Done")

    def mount(path,pwd):
        """usage: mount("/srv/archive")"""
        cmd = 'mount ' + path
        print ("Sending command: {}".format(cmd))
        echo = 'echo {}|sudo -S {}'.format(pwd,cmd)
        subprocess.Popen(str(echo), shell=True, stdout=subprocess.PIPE)

    if not os.path.isdir(vpath) and not os.path.isdir(qpath) and not os.path.isdir(figpath):
        print ("directory for products not accessible?")
        statusmsg[name0] = 'products unavailable'
        successlist[0] = False
        # all other jobs cannot be performed
        try:
            print ("unmounting...")
            umount("/srv/products",dbpasswd)
            time.sleep(10)
            print ("mounting products again...")
            mount("-a",dbpasswd)
            print ("success...")
            successlist[0] = True
            statusmsg[name0] = 'products unavailable - remounting successful'
        except:
            statusmsg[name0] = 'products unavailable - remounting failed'
    if not os.path.isdir(dipath):
        print ("archive not accessible?")
        statusmsg[name0] = 'archive unavailable'
        successlist[1] = False
        try:
            print ("unmounting...")
            umount("/srv/archive",dbpasswd)
            time.sleep(10)
            print ("mounting archive again...")
            mount("-a",dbpasswd)
            print ("success...")
            statusmsg[name0] = 'archive unavailable - remounting successful'
            successlist[1] = True
        except:
            statusmsg[name0] = 'archive unavailable - remounting failed'

    success = all([i for i in successlist])

    return success, statusmsg


def DoCombination(db, variostream, scalarstream, config={}, publevel=2, date='', debug=False):
    """
    DESCRIPTION:
        Combine vario and scalar data set and add meta information
    """

    print ("  -> Combinations and Meta info")
    prelim = mergeStreams(variostream,scalarstream,keys=['f'])
    print ("     -- preliminary data file after MERGE:", prelim.length()[0])
    prelim.header['DataPublicationLevel'] = str(publevel)
    prelim.header['DataPublicationDate'] = date
    # Eventually convert it to XYZ
    prelim = prelim.hdz2xyz()
    if len(prelim.header['DataComponents']) < 4:
        prelim.header['DataComponents'] += 'F'
    prelim = prelim._drop_column('flag')
    prelim = prelim._drop_column('comment')
    print ("     -- preliminary data finished")

    return prelim

def ExportData(datastream, config={}, publevel=2):
    """
    DESCRIPTION:
        Export data sets to selected directories and databases
    """

    success = True
    explist = config.get('magnetismexports')
    obscode = config.get('obscode')
    connectdict = config.get('conncetedDB')
    varioinst = config.get('primaryVarioInst')
    scalainst = config.get('primaryScalarInst')

    vpathsec = os.path.join(config.get('variationpath'),'sec')
    vpathmin = os.path.join(config.get('variationpath'),'min')
    vpathcdf = os.path.join(config.get('variationpath'),'cdf')

    if int(publevel)==2:
        pubtype = 'adjusted'
        pubshort = 'p'
    elif int(publevel)==3:
        pubtype = 'quasidefinitive'
        pubshort = 'q'
        vpathsec = os.path.join(config.get('quasidefinitivepath'),'sec')
        vpathmin = os.path.join(config.get('quasidefinitivepath'),'min')
        vpathcdf = os.path.join(config.get('quasidefinitivepath'),'cdf')
    else:
        pubtype = 'variation'
        pubshort = 'v'

    print ("  -> Exporting {} data ".format(pubtype))
    if 'IAGA' in explist:
        print ("     -- Saving one second data - IAGA - to {}".format(vpathsec))
        datastream.write(vpathsec,filenamebegins="wic",dateformat="%Y%m%d",filenameends="{}sec.sec".format(pubshort),format_type='IAGA')
        # supported keys of IMAGCDF -> to IMF format
        #supkeys = ['time','x','y','z','f','df']
        print ("       -> Done")
    if 'CDF' in explist:
        print ("     -- Saving one second data - CDF")
        datastream.write(vpathcdf,filenamebegins="wic_",dateformat="%Y%m%d_%H%M%S",format_type='IMAGCDF',filenameends='_'+datastream.header.get('DataPublicationLevel')+'.cdf')
        print ("       -> Done")
    if 'DBsec' in explist:
        print ("     -- Saving one second data - Database")
        oldDataID = datastream.header['DataID']
        oldSensorID = datastream.header['SensorID']
        datastream.header['DataID'] = "WIC_{}sec_0001_0001".format(pubtype)
        datastream.header['SensorID'] = "WIC_{}sec_0001".format(pubtype)
        # save
        for dbel in connectdict:
            db = connectdict[dbel]
            print ("     -- Writing {} data to DB {}".format(pubtype,dbel))
            writeDB(db,datastream,tablename="WIC_{}sec_0001_0001".format(pubtype))
        datastream.header['DataID'] = oldDataID
        datastream.header['SensorID'] = oldSensorID
    if 'IAGA' in explist:
        print ("     -- Saving one minute data - IAGA")
        prelimmin = datastream.filter()
        prelimmin.write(vpathmin,filenamebegins="wic",dateformat="%Y%m%d",filenameends="{}min.min".format(pubshort),format_type='IAGA')
        #mp.plot(prelimmin)
    if 'DBmin' in explist:
        print ("     -- Saving one minute {} data to database".format(pubtype))
        prelimmin.header['DataID'] = "WIC_{}min_0001_0001".format(pubtype)
        prelimmin.header['SensorID'] = "WIC_{}min_0001".format(pubtype)
        variocol = np.asarray([varioinst for el in prelimmin.ndarray[0]])
        scalacol = np.asarray([scalainst for el in prelimmin.ndarray[0]])
        prelimmin = prelimmin._put_column(variocol, 'str1')
        prelimmin = prelimmin._put_column(scalacol, 'str2')
        for dbel in connectdict:
            db = connectdict[dbel]
            print ("     -- Writing {} data to DB {}".format(pubtype,dbel))
            writeDB(db,prelimmin,tablename="WIC_{}min_0001_0001".format(pubtype))

    return prelimmin



def AdjustedData(config={},statusmsg = {}, endtime=datetime.utcnow(), starttime=None, debug=False):
    """
    DESCRIPTION
        Create and submit variation/adjusted data
        Use one specific (primary) variometer and scalar instrument with current data
        Done here to be as close as possible to data acquisition
    """
    prelim = DataStream()
    prelimmin = DataStream()
    success = True
    archivepath = config.get('archivepath')
    variosens = config.get('primaryVario') # LEMI036_1_0002
    scalarsens = config.get('primaryScalar')
    varioinst = config.get('primaryVarioInst') # LEMI036_1_0002_0001
    scalarinst = config.get('primaryScalarInst')
    primpier = config.get('primarypier')
    daystodeal = config.get('daystodeal')
    db = config.get('primaryDB')
    try:
        dbcoverage = int(config.get('dbcoverage',10))
    except:
        dbcoverage = 10 # amount of days covered in database (second resolution)

    if not starttime:
       st = endtime-timedelta(days=daystodeal)
       starttime = datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d")

    if debug:
        p1start = datetime.utcnow()

    if debug:
        print ("-------------------------------------------------------")
        print ("Adjusted Data")
        print ("-------------------------------------------------------")
    print ("  Primary variometer {}:".format(varioinst))
    print ("  ----------------------")

    name1b = "{}-AdjustedVarioData".format(config.get('logname'))
    if not varioinst == '':
        if st < datetime.utcnow()-timedelta(days=dbcoverage): 
            print ("  -> reading archive data")
            vario = read(os.path.join(archivepath,variosens,varioinst,'*'),starttime=starttime,endtime=endtime)
        else:
            print ("  -> reading data from database")
            vario = readDB(db,varioinst,starttime=starttime,endtime=endtime)
        db = config.get('primaryDB',None)
        if vario.length()[0] > 0 and db:
            vario = DoVarioCorrections(db, vario, variosens=config.get('primaryVario'), starttimedt=endtime-timedelta(days=daystodeal))
            statusmsg[name1b] = 'variometer data loaded'
        else:
            print ("  -> Did not find variometer data - aborting")
            statusmsg[name1b] = 'variometer data loading failed - aborting'
            #sys.exit()
        print ("  => got vario data from {}".format(vario._find_t_limits()))
    else:
        print ("  -> No variometer - aborting")
        statusmsg[name1b] = 'no variometer specified - aborting'
        sys.exit()

    print ("  Primary scalar magnetometer {}:".format(scalarinst))
    print ("  ----------------------")
    name1c = "{}-AdjustedScalarData".format(config.get('logname','Dummy'))
    if not scalarinst == '':
        if st < datetime.utcnow()-timedelta(days=dbcoverage):
            print ("  -> reading archive data")
            scalar = read(os.path.join(archivepath,scalarsens,scalarinst,'*'),starttime=starttime,endtime=endtime)
        else:
            print ("  -> reading data from database")
            scalar = readDB(db,scalarinst,starttime=starttime,endtime=endtime)
        if scalar.length()[0] > 0 and db:
            scalar = DoScalarCorrections(db, scalar, scalarsens=config.get('primaryScalar'), starttimedt=endtime-timedelta(days=daystodeal))
            statusmsg[name1c] = 'scalar data loaded'
        else:
            print ("  -> Did not find scalar data - aborting")
            statusmsg[name1c] = 'scalar data load faild - aborting'
            #sys.exit()
        print ("  => got scalar data from {}".format(scalar._find_t_limits()))
    else:
        print ("No scalar instrument - aborting")
        statusmsg[name1c] = 'no scalar instrument specified - aborting'
        #sys.exit()

    print ("  Baseline correction for pier {}".format(primpier))
    print ("  ----------------------")
    print ("  Instruments: {} and {}".format(varioinst, scalarinst))

    name1d = "{}-AdjustedBaselineData".format(config.get('logname','Dummy'))
    vario, msg = DoBaselineCorrection(db, vario, config=config, baselinemethod='simple',endtime=endtime)
    statusmsg[name1d] = msg
    if not msg == 'baseline correction successful':
        success = False
        # New update - if baseline fails then k will be wrong - better just stop
        # and return empty
        return DataStream(), statusmsg

    print ("  Combining all data sets")
    print ("  ----------------------")
    name1e = "{}-AdjustedDataCombination".format(config.get('logname','Dummy'))
    try:
        prelim = DoCombination(db, vario, scalar, config=config, publevel=2, date=datetime.strftime(endtime,"%Y-%m-%d"), debug=debug)
        statusmsg[name1e] = 'combination finished'
    except:
        print (" !!!!!!!!!!! Combination failed")
        statusmsg[name1e] = 'combination failed'

    print ("  Exporting data sets")
    print ("  ----------------------")
    name1f = "{}-AdjustedDataExport".format(config.get('logname','Dummy'))
    statusmsg[name1f] = 'export of adjusted data successful'
    if debug:
        print ("     for time range: {}".format(prelim._find_t_limits()))
    if not debug:
        #try:
        prelimmin = ExportData(prelim, config=config, publevel=2)
        #except:
        #    statusmsg[name1f] = 'export of adjusted data failed'
    else:
        print ("Debug selected: export disabled")

    if debug:
        p1end = datetime.utcnow()
        print ("  ---------------------------------")
        print ("  Adjusted data needed {}".format(p1end-p1start))
        print ("-----------------------------------")

    return prelimmin, statusmsg

def KValues(datastream,config={},statusmsg={}, debug=False):
    """
    DESCRIPTION
        Obtain K values and update current data structure
    """

    k9level = datastream.header.get('StationK9',500)
    name5b = "{}-KValueCurrentData".format(config.get('logname','Dummy'))
    currentvaluepath = config.get('currentvaluepath')
    connectdict = config.get('conncetedDB')
    success = True

    ## What about k values ??
    print ("  K values")
    print ("  ----------------------")
    print ("     -- K9 level in stream: {}".format(datastream.header.get('StationK9')))
    try:
        if datastream.length()[0] > 1600:
            # Only perform this job if enough minute data is available
            # Thus there won't be any calculation between 0.00 and 1:30 
            kvals = datastream.k_fmi(k9_level=k9level)
            kvals = kvals._drop_nans('var1')
            # use cut (0.4.6) to get last 70% of data
            try:
                kvals = kvals.cut(70)
            except:
                pass
            # then write kvals to DB
            # get index of last kval before now
            index = len(kvals)
            print ("     -- Checking kvals:")
            for idx,t in enumerate(kvals.ndarray[0]):
                print ("         -> Index: {}, time: {}, kval: {}".format(idx,num2date(t),kvals.ndarray[7][idx]))
                if num2date(t).replace(tzinfo=None) <= datetime.utcnow()+timedelta(hours=1):
                    index = idx
            kvaltime = datetime.strftime(num2date(kvals.ndarray[0][index]).replace(tzinfo=None),"%Y-%m-%d %H:%M")
            kval = kvals.ndarray[7][index]
            print ("     -- Expecting k of {} until {} + 1.5 hours UTC (current time = {})".format(kval,kvaltime,datetime.utcnow()))
            # Update K value in current value path
            print ("     -- updating current data contents")
            if os.path.isfile(currentvaluepath):
                fulldict, valdict = load_current_data_sub(currentvaluepath, 'magnetism')
                ## set k values and k time
                valdict['k'] = [int(kval),'']
                valdict['k-time'] = [kvaltime,'']
                fulldict[u'magnetism'] = valdict
                print ("       reading done, now writing")
                with open(currentvaluepath, 'w',encoding="utf-8") as file:
                    file.write(unicode(json.dumps(fulldict)))
                print ("     -- K value has been updated to {}".format(kval))
            if not debug:
                print ("     -- now writing kvals to database")
                for dbel in connectdict:
                    db = connectdict[dbel]
                    print ("     -- Writing k values to DB {}".format(dbel))
                    writeDB(db,kvals,tablename="WIC_k_0001_0001")
        statusmsg[name5b] = 'determinig k successfull'
    except:
        statusmsg[name5b] = 'determinig k failed'
        success = False

    return success, statusmsg

def CreateDiagram(datastream,config={},statusmsg={}, endtime=datetime.utcnow(), debug=False):
    """
    DESCRIPTION
        Create diagram of adjusted data
    """

    figpath = config.get("magfigurepath")
    daystodeal = config.get("daystodeal")
    date = datetime.strftime(endtime,"%Y-%m-%d")
    yesterd2 = datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d")
    success = True
    name5 = "{}-CreateDiagramVario".format(config.get('logname','Dummy'))

    # Create realtime diagram and upload to webpage  WDC
    try:
            print ("  Creating plot")
            pnd = datastream._select_timerange(starttime=datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d"))
            pst = DataStream([LineStruct()],datastream.header,pnd)
            pst = pst.xyz2hdz()
            mp.plotStreams([pst],[['x','y','z','f']], gridcolor='#316931',fill=['x','z','f'],confinex=True, fullday=True, opacity=0.7, plottitle='Geomagnetic variation (until %s)' % (datetime.utcnow().date()),noshow=True)
            print ("     -- Saving diagram to products folder")

            pltsavepath = os.path.join(figpath,"magvar_{}.png".format(date))
            plt.savefig(pltsavepath)
            statusmsg[name5] = 'creating and saving graph successful'
            print ("      -> Done")
    except:
            statusmsg[name5] = 'failed to save data - remount necessary?'
            success = False

    return success, statusmsg


def GetQDTimeslot(config={}, debug=False):
    """
    DESCRIPTION
        Check whether the current time slot justifies QD analysis
    RETURN
        True if valid
    """
    runqd = True
    # Get the current weekday
    qdstart = datetime.utcnow()
    weekday = qdstart.weekday()
    qdweekday = config.get("qdweekday")
    qdstarthour = config.get("qdstarthour")
    qdendhour = config.get("qdendhour")

    print ("  -> Testing for a valid time slot")
    print ("     -- Current weekday: {}".format(weekday))
    print ("     -- QD calculation will only be performed on day {} between {}:00 and {}:00".format(qdweekday,qdstarthour,qdendhour))
    # run the analysis only once at the scheduled weekday between starthour and endhour

    # update current.data if quasidefinite data has been calculated
    ldate = datetime(qdstart.year, qdstart.month, qdstart.day,qdstarthour)  # 2
    udate = datetime(qdstart.year, qdstart.month, qdstart.day,qdendhour)  # 3

    if ldate<qdstart<udate and weekday in [qdweekday,str(qdweekday)]:  # 5
        print ("  - Running Quasidefinitve data determinations - checking for new flags")
        runqd = True
    else:
        print ("  - Not time for Quasidefinitve data determinations - waiting for 2:00 am")
        runqd = False

    return runqd


def GetQDFlagcondition(db, lastQDdate='', variosens='', scalasens='', debug=False):
    """
    DESCRIPTION
        Check whether flags have been updated recently to justify QD analysis
    RETURN
        True if valid
    """
    newQDenddate = ''
    flaglist = []
    if not variosens or not scalasens:
        return False, lastQDdate

    runqd = True

    print ("  -> Checking actuality of flags")
    if debug:
        print ("     -- lastQDdate: {}, converted: {}".format(lastQDdate, datetime.strftime(datetime.strptime(lastQDdate,"%Y-%m-%d"),"%Y-%m-%d %H:%M:%S")))
    varioflag = db2flaglist(db,variosens,begin=datetime.strftime(datetime.strptime(lastQDdate,"%Y-%m-%d"),"%Y-%m-%d %H:%M:%S"))
    scalaflag = db2flaglist(db,scalasens,begin=datetime.strftime(datetime.strptime(lastQDdate,"%Y-%m-%d"),"%Y-%m-%d %H:%M:%S"))

    if len(varioflag) > 0:
        flaglist = varioflag
        if len(scalaflag) > 0:
            flaglist.extend(scalaflag)
    elif len(scalaflag) > 0:
        flaglist = scalaflag
    if len(flaglist) > 0:
        print ("     -- found flags: {}".format(len(flaglist)))
        # checking last input date (modifications dates)
        moddates = [el[-1] for el in flaglist if el[3] in [0,2,3,'0','2','3']]
        if len(moddates) > 0:
            print ("     -- last flag modification with IDs 0, 2 or 3 at {}".format(max(moddates)))
            newQDenddate = datetime.strftime(max(moddates)-timedelta(days=7),"%Y-%m-%d")
            # now get the last flag date and define lastflagdate -7 days as the new QD enddate
            print ("     -- found new {} flags set by an observer -> assuming QD conditions for the week before".format(len(moddates)))
        else:
            print ("     -- did not find any new flags with IDs 0, 2 or 3")
            newQDenddate = lastQDdate
            runqd = False
    else:
        print ("     -- did not find any new flags")
        newQDenddate = lastQDdate
        runqd = False

    return runqd, newQDenddate


def GetQDDonealready(lastQDdate='', QDenddate='', newQDenddate='', debug=False):
    """
    DESCRIPTION
        Check whether qd range is new and not already existing
    RETURN
        True if valid
    """
    date = datetime.strftime(datetime.utcnow(),"%Y-%m-%d")
    runqd = True

    print ("  -> checking whether QD determination has already been performed within the current time period")
    if debug:
        print ("     -- last analysis performed on {} (today: {})".format(lastQDdate, date))
    if lastQDdate == date:
        print ("       -> QD determination already performed (or tried) in this period")
        runqd = False

    if runqd:
        print ("     -- checking whether an analysis is already existing for the period in question")
        if debug:
            print ("     -- QDendate in currentvalues: {}, new QDenddate: {}".format(QDenddate, newQDenddate))
        if QDenddate == newQDenddate or newQDenddate == '':
            print ("       -> the projected period has already been analyzed")
            runqd = False

    return runqd


def QuasidefinitiveData(config={}, statusmsg={}, starttime=None, endtime=None, force=False, debug=False):
    """
    DESCRIPTION
        Create and submit quasidefinitive data
        Use one specific (primary) variometer and scalar instrument with current data
        Done here to be as close as possible to data acquisition
    """

    lastQDdate = config.get('lastQDdate')
    QDenddate = config.get('QDenddate')
    currentvaluepath = config.get('currentvaluepath')
    archivepath = config.get('archivepath')
    variosens = config.get('primaryVario')
    scalarsens = config.get('primaryScalar')
    varioinst = config.get('primaryVarioInst')
    scalarinst = config.get('primaryScalarInst')
    db = config.get('primaryDB',None)
    date=datetime.strftime(datetime.utcnow(),"%Y-%m-%d")
    dbcoverage = int(config.get('dbcoverage',10))

    rotangle = 0.0
    runqd = False
    forcecond = False

    if debug:
        print ("----------------------------------------------------------------")
        print ("Determine quasidefinite data")
        print ("----------------------------------------------------------------")
        p3start = datetime.utcnow()


    # Messages:
    # name3a: {}-QDanalysis
    # name3b: {}-QDanalysis-performance  -> general runtime notification
    # name3c: {}-QDdatasource
    # name3d: {}-QDvario
    # name3e: {}-QDscalar
    # name3f: {}-QDBaselineData
    # name3g: {}-QDDataCombination
    # name3h: {}-QDDataExport

    name3b = "{}-QDanalysis-performance".format(config.get('logname','Dummy'))

    if force and starttime and endtime:
        print ("  Force anaysis selected - running QD analysis")
        print ("  ----------------------")
        runqd = True
        forcecond = True
    else:
        print ("  Suitability test for QD analysis")
        print ("  ----------------------")

        name3a = "{}-QDanalysis".format(config.get('logname','Dummy'))
        statusmsg[name3a] = "last suitability-test for quasidefinitive finished"
        try:
            runqd = GetQDTimeslot(config=config, debug=debug)
            if runqd:
                runqd, newQDenddate = GetQDFlagcondition(db, lastQDdate=lastQDdate, variosens=variosens, scalasens=scalarsens, debug=debug)
            if runqd:
                runqd = GetQDDonealready(lastQDdate=lastQDdate, QDenddate=QDenddate, newQDenddate=newQDenddate, debug=debug)
        except:
            statusmsg[name3a] = "suitability-test for quasidefinitive failed"

    if runqd:
        print ("  Running QD analysis")
        print ("  ----------------------")
        try:
            if forcecond:
                qdstarttime = starttime
                qdendtime = endtime
            else:
                 # first time condition
                qdendtime = datetime.strptime(newQDenddate,"%Y-%m-%d") + timedelta(days=1)
                if not QDenddate == '':
                    # QDenddate is 8 days before newQDenddate
                    qdstarttime = datetime.strptime(QDenddate,"%Y-%m-%d") - timedelta(days=1)
                else:
                    qdstarttime = datetime.strptime(newQDenddate,"%Y-%m-%d") - timedelta(days=8)
                print ("  -> all conditions met - running QD analysis")
            print ("     -- Analyzing data between:")
            print ("     -- Start: {}".format(qdstarttime))
            print ("     -- End:   {}".format(qdendtime))

            if not debug and not forcecond:
                print ("  -> Updating current value file with new QD enddate {}".format(newQDenddate))
                # QDenddate should be updated already now with newQDenddate in current.data file to prevent restart of job in next schedule, if running the analysis is not yet finished
                if os.path.isfile(currentvaluepath):
                    with open(currentvaluepath, 'r') as file:
                        fulldict = json.load(file)
                        valdict = fulldict.get('magnetism')
                        valdict['QD enddate'] = [newQDenddate,'']
                        fulldict[u'magnetism'] = valdict
                    with open(currentvaluepath, 'w',encoding="utf-8") as file:
                        file.write(unicode(json.dumps(fulldict))) # use `json.loads` to 
                        print ("     -- QDenddate has been updated from {} to {}".format(QDenddate,newQDenddate))
            else:
                print ("  -> Debug (or Force) selected: Updating current value file with new QD enddate skipped...")

            # Running analysis
            # ##############################################
            print ("  -> Checking whether database has a suitable coverage")
            name3c = "{}-QDdatasource".format(config.get('logname','Dummy'))
            archive = False
            if qdstarttime < datetime.utcnow()-timedelta(days=dbcoverage):
                print ("     -- Eventually not enough data in database for full coverage")
                print ("       -> Accessing archive files instead")
                statusmsg[name3c] = 'using data from file archive for QD analysis'
                archive = True
            else:
                statusmsg[name3c] = 'using database for QD analysis'

            print ("  -> Reading raw vario data and applying corrections")
            name3d = "{}-QDvario".format(config.get('logname','Dummy'))
            if not archive:
                vario = readDB(db,varioinst,starttime=qdstarttime,endtime=qdendtime)
            else:
                print ("     -- getting vario file archive")
                vario = read(os.path.join(archivepath,variosens,varioinst,'*'),starttime=qdstarttime,endtime=qdendtime)
                # Get meta info
                vario.header = dbfields2dict(db,varioinst)
            if (vario.length()[0]) > 0:
                vario = DoVarioCorrections(db, vario, variosens=config.get('primaryVario'), starttimedt=qdstarttime)
                statusmsg[name3d] = 'variometer data loaded'
            else:
                print ("     -- Did not find variometer data - aborting")
                statusmsg[name3d] = 'variometer data load failed'
                sys.exit()

            print ("  -> Reading raw scalar data and applying corrections")
            name3e = "{}-QDscalar".format(config.get('logname','Dummy'))
            if not archive:
                scalar = readDB(db,scalarinst,starttime=qdstarttime,endtime=qdendtime)
            else:
                print ("     -- getting scalar file archive")
                scalar = read(os.path.join(archivepath,scalarsens,scalarinst,'*'),starttime=qdstarttime,endtime=qdendtime)
                # Get meta info
                scalar.header = dbfields2dict(db,scalarinst)
            if (scalar.length()[0]) > 0:
                scalar = DoScalarCorrections(db, scalar, scalarsens=scalarsens, starttimedt=qdstarttime)
                statusmsg[name3e] = 'scalar data loaded'
            else:
                print ("  -> Did not find scalar data - aborting")
                statusmsg[name3e] = 'scalar data load failed'

            print ("  -> Doing baseline correction")
            name3f = "{}-QDBaselineData".format(config.get('logname','Dummy'))
            vario, msg = DoBaselineCorrection(db, vario, config=config, baselinemethod='normal')
            statusmsg[name3f] = msg
            if msg == 'baseline correction failed':
                success = False

            print ("  -> Combining all data sets")
            name3g = "{}-QDDataCombination".format(config.get('logname','Dummy'))
            try:
                qddata = DoCombination(db, vario, scalar, config=config, publevel=3, date=datetime.strftime(datetime.utcnow(),"%Y-%m-%d"), debug=debug)
                statusmsg[name3g] = 'combination finished'
            except:
                print ("      !!!!!!!!!!! Combination failed")
                statusmsg[name3g] = 'combination failed'

            print ("  -> Exporting data sets")
            name3h = "{}-QDDataExport".format(config.get('logname','Dummy'))
            statusmsg[name3h] = 'export of adjusted data successful'
            if not debug:
                try:
                    qdmin = ExportData(qddata, config=config, publevel=3)
                except:
                    statusmsg[name3h] = 'export of adjusted data failed'
            else:
                print ("Debug selected: export disabled")

            if not debug and not forcecond:
                print ("  -> Updating current data")
                if os.path.isfile(currentvaluepath):
                    with open(currentvaluepath, 'r') as file:
                        fulldict = json.load(file)
                        valdict = fulldict.get('magnetism')
                        valdict['QD analysis date'] = [date,'']
                        fulldict[u'magnetism'] = valdict
                    with open(currentvaluepath, 'w',encoding="utf-8") as file:
                        file.write(unicode(json.dumps(fulldict)))
                        print ("    -- last QD analysis date has been updated from {} to {}".format(lastQDdate,date))
            else:
                print ("Debug (or Force) selected: current data remains unchanged")

            statusmsg[name3b] = "QD data between {} and {} calculated and published".format(qdstarttime, qdendtime)
        except:
            statusmsg[name3b] = "quasidefinitive calculation performed but failed - check current.data before redoing"

    if debug:
        p3end = datetime.utcnow()
        print ("-----------------------------------")
        print ("QDanalysis needed {}".format(p3end-p3start))
        print ("-----------------------------------")

    return statusmsg


def main(argv):
    try:
        version = __version__
    except:
        version = "1.0.1"
    configpath = ''
    statusmsg = {}
    debug=False
    force=False
    starttime = None
    endtime = None
    joblist = ["adjusted", "quasidefinitive", "addon"]
    newloggername = 'mm-dp-magnetism.log'

    try:
        opts, args = getopt.getopt(argv,"hc:j:s:e:l:FD",["config=","joblist=","starttime=","endtime=","loggername=","force=","debug=",])
    except getopt.GetoptError:
        print ('magnetism_products.py -c <config>')
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
            print ('-j            : adjusted, quasidefinitive,addon')
            print ('              : adjusted -> calculated adjusted data with constant')
            print ('              :          baseline approximation')
            print ('              : quasidefinitive -> calculated data with spline')
            print ('              :          baseline and only if observer flags are present')
            print ('              : addon -> requires adjusted, create diagtram and k values')
            print ('-l            : loggername')
            print ('-s            : starttime')
            print ('-e            : endtime')
            print ('-F            : force redo of quasidefinitve (not updating current.data)')
            print ('-------------------------------------')
            print ('Application:')
            print ('python magnetism_products.py -c /etc/marcos/analysis.cfg')
            print ('python magnetism_products.py -c /etc/marcos/analysisGAM.cfg -j adjusted -l mm-dp-magnetism-GAM')
            print ('python magnetism_products.py -c /etc/marcos/analysisGAM.cfg -j quasidefinitive -s starttime -e endtime -F -l mm-dp-qd-GAM')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-j", "--joblist"):
            # get a list of jobs e.g. "adjusted, quasidefinitive, addon"
            joblist = arg.split(',')
        elif opt in ("-s", "--starttime"):
            # define an endtime for the current analysis - default is now
            starttime = arg
        elif opt in ("-e", "--endtime"):
            # define an endtime for the current analysis - default is now
            endtime = arg
        elif opt in ("-l", "--loggername"):
            # define an endtime for the current analysis - default is now
            newloggername = arg
        elif opt in ("-F", "--force"):
            # delete any / at the end of the string
            force = True
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running magpy_products version {}".format(version))

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    if "addon" in joblist and not "adjusted" in joblist:
        print ("joblist input 'addon' requires 'adjusted' as well - therefore skipping 'addon' option" )    

    if starttime:
        try:
            starttime = DataStream()._testtime(starttime)
        except:
            print ("Starttime could not be interpreted - using None")

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
    success = ValidityCheckConfig(config)

    #if not success:
    #    sys.exit(1)
    # #############################################################
    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname=newloggername, debug=debug)

    if debug:
        print (" -> Config contents:")
        print (config)

    print ("3. Check all paths and eventually remount directories")
    success,statusmsg = ValidityCheckDirectories(config=config, statusmsg=statusmsg, debug=debug)

    print ("4. Loading current.data and getting primary instruments")
    config, statusmsg = GetPrimaryInstruments(config=config, statusmsg=statusmsg, debug=debug)

    print ("5. Connect to databases")
    config = ConnectDatabases(config=config, debug=debug)

    if "adjusted" in joblist:
        print ("6. Obtain adjusted data")
        mindata,statusmsg = AdjustedData(config=config, statusmsg=statusmsg, endtime=endtime, debug=debug)

        if mindata.length()[0]>0 and "addon" in joblist:
            print ("8. Diagrams")
            suc,statusmsg = CreateDiagram(mindata, config=config,statusmsg=statusmsg, endtime=endtime)
            print ("9. K Values")
            suc,statusmsg = KValues(mindata, config=config,statusmsg=statusmsg)

    if "quasidefinitive" in joblist:
        print ("10. Obtain quasidefinitive data")
        statusmsg = QuasidefinitiveData(config=config, statusmsg=statusmsg, starttime=starttime, endtime=endtime, force=force, debug=debug)


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
# amount of days covered by database (in second resolution)
dbcoverage             :      10
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

