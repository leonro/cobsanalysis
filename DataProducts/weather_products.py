
#!/usr/bin/env python
"""
DESCRIPTION
   Analyses environmental data from various different sensors providing weather information.
   Sources are RCS raw data, RCS analyzed meteo data, ultrasonic, LNM and BM35 pressure.
   Creates a general METEO table called METEOSGO_adjusted, by applying various flagging
   methods and synop codes (LNM). Additionally, a short term current condition table is created (30min mean),
   two day and one year plots are generated. Beside, different measurements are compared for similarity (e.g.
   rain from bucket and lnm)

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
        python weather_products.py -c /etc/marcos/analysis.cfg
    REDO analysis for a time range:
        (startime is defined by endtime - daystodeal as given in the config file
        python weather_products.py -c /etc/marcos/analysis.cfg -e 2020-11-22
    RECREATE archive files:
        python3 weather_products.py -c ~/CONF/wic.cfg -e 2020-05-18 -r 20 -a

"""
from magpy.stream import *
from magpy.database import *
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred
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

# IMPORT analysismethods


#sgopath                :       /srv/archive/SGO
#meteoproducts          :       /srv/products/data/meteo
#meteoimages            :       /srv/products/graphs/meteo
#meteorange             :       3

"""
# ################################################
#             SYNOP Definitions
# ################################################
"""

synopdict = {"-1":"Sensorfehler",
                 "41":"Leichter bis maessiger Niederschlag (nicht identifiziert)",
                 "42":"Starker Niederschlag (nicht identifiziert, unbekannt)",
                 "00":"Kein Niederschlag",
                 "51":"Leichter Niesel",
                 "52":"Maessiger Niesel",
                 "53":"Starker Niesel",
                 "57":"Leichter Niesel mit Regen",
                 "58":"Maessiger bis starker Niesel mit Regen",
                 "61":"Leichter Regen",
                 "62":"Maessiger Regen",
                 "63":"Starker Regen",
                 "67":"Leichter Regen",
                 "68":"Maessiger bis starker Regen",
                 "77":"Schneegriesel",
                 "71":"Leichter Schneefall",
                 "72":"Maessiger Schneefall",
                 "73":"Starker Schneefall",
                 "74":"Leichte Graupel",
                 "75":"Maessige Graupel",
                 "76":"Starke Graupel",
                 "89":"Hagel"}




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
        for sensor in senslist:
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
                    sens.append(sensor)
                    if debug:
                        print ("     -> valid data for sensor {}".format(sensor))
            else:
                sens.append(sensor)


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


def transformUltra(db, datastream, debug=False):
    """
    DESCRIPTION:
        transform utrasonic data to produce a general structure for combination
    """
    if datastream.length()[0] > 0:
        print ("  Transforming ultrasonic data")
        print ("    -- getting existing flags ...")
        start, end = datastream._find_t_limits()
        flaglist = db2flaglist(db, datastream.header.get("SensorID"),begin=start,end=end)
        print ("      -> found existing flags: {}".format(len(flaglist)))
        datastream = datastream.flag(flaglist)
        datastream = datastream.remove_flagged()

        print ("    -- resampling and reordering ...")
        datastream = datastream.resample(datastream._get_key_headers(),period=60,startperiod=60)

        datastream._move_column('t2','f')
        datastream._drop_column('t2')

    print ("      -> Done")
    return datastream


def getLastSynop(datastream, synopdict={}):
    """
    DESCRIPTION:
        transform lnm data to produce a general structure for combination
    """
    print ("  Extracting last synop code")
    trans = ''
    # Get latest synop data
    for index,elem in enumerate(datastream.ndarray):
        if len(elem) > 0:
            #print ("KEY: {} = {} ({})".format(KEYLIST[index],elem[-1],data.header.get('col-{}'.format(KEYLIST[index]))))
            if KEYLIST[index] == 'str1':
                synop = elem[-1]

    transtmp = synopdict.get(str(synop))
    try:
        if (datetime.utcnow()-num2date(datastream.ndarray[0][-1]).replace(tzinfo=None)).total_seconds() < 3600:
             print ("     -- Current Weather: {}".format(transtmp))
             trans = transtmp
        else:
             print ("     -- !!! No recent SYNOP data available - check LNM data") 
    except:
        pass

    print ("      -> Done")
    return trans

def transformLNM(datastream, debug=False):
    """
    DESCRIPTION:
        transform lnm data to produce a general structure for combination
        # Columns after: 
        temperature: 'f'
        synop: 'str1'
        visibility: 't2'
        rain: 'y'
        rain: 'df'
    """

    print ("  Transforming LNM data")
    if datastream.length()[0] > 0:
        print ("    -- extracting synop code into resampled stream")
        data = datastream.copy()
        syn = data._get_column('str1')
        syn = np.asarray([float(el) for el in syn])
        data._drop_column('var4')
        data._put_column(syn,'var4')
        print ("    -- determine average rain from LNM")
        res2 = datastream.steadyrise('x', timedelta(minutes=60),sensitivitylevel=0.002)
        datastream= datastream._put_column(res2, 'df', columnname='Percipitation',columnunit='mm/1h')
        print ("    -- resampling LNM")
        datastream= datastream.resample(datastream._get_key_headers(),period=60,startperiod=60)
        # Test merge to get synop data again
        print ("    -> Syn column looks like:", syn)
        datastream = datastream._drop_column('var4')
        if len(syn) > 0:
            if debug:
                print ("      -> found synop codes {}".format(len(syn)))
            datastream = mergeStreams(datastream,data, keys=['var4'])
        else:
            emp = [0]*datastream.length()[0]
            datastream = datastream._put_column(emp,'var4')
            print ("    -> no percipitation codes found within the covered time range")

        datastream._drop_column('x')
        datastream._move_column('y','t2')
        datastream._drop_column('z')
        datastream._move_column('t1','f')
        datastream._drop_column('t1')
        datastream._drop_column('var1')
        datastream._drop_column('var2')
        datastream._drop_column('var3')
        datastream._drop_column('var5')
        datastream._drop_column('dx')
        datastream._drop_column('dy')
        datastream._drop_column('dz')
        datastream._move_column('df','y')
        datastream._drop_column('str1')
        #datastream._drop_column('df')  # remove after CheckRain

    print ("      -> Done")
    return datastream


def transformBM35(db, datastream, debug=False):
    """
    DESCRIPTION:
        filter and transform bm35 data to produce a general structure for combination
        # Columns after: 
        pressure: 'var5'
    PARAMTER:
        dbupdate : if True then new flags will be written to DB
    """

    print ("  Transforming BM35 data")
    if datastream.length()[0] > 0:
        datastream = datastream.filter(filter_width=timedelta(seconds=3.33333333),resample_period=1)
        start, end = datastream._find_t_limits()
        print ("    -- getting existing flags ...")
        flaglist = db2flaglist(db, datastream.header.get("SensorID"),begin=start,end=end)
        print ("    -- found existing flags: {}".format(len(flaglist)))
        datastream = datastream.flag(flaglist)
        datastream = datastream.remove_flagged()
        flaglist2 = datastream.flag_range(keys=['var3'],above=1000, text='pressure exceeding value range',flagnum=3)
        flaglist3 = datastream.flag_range(keys=['var3'],below=750, text='pressure below value range',flagnum=3)
        flaglist2 = combinelists(flaglist2,flaglist3)
        print ("    -- removing flagged data")
        datastream = datastream.flag(flaglist2)
        datastream = datastream.remove_flagged()
        print ("    -- filtering to minute")
        datastream = datastream.filter() # minute data

        datastream._move_column('var3','var5')
        datastream._drop_column('var3')

    print ("      -> Done")
    return datastream, flaglist2


def transformRCST7(db, datastream, debug=False):
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


def CheckRainMeasurements(lnmdatastream, meteodatastream, dayrange=3, config={}, debug=False):
    """
    DESCRIPTION:
        compare rain measurements between LNM and Meteo bucket
        # --------------------------------------------------------    
        # RCS - Get data from RCS (no version/revision control in rcs) 
        # Schnee: x, Temperature: y,  Maintainance: z, Pressure: f, Rain: t1,var1, Humidity: t2
    """

    print (" -----------------------")
    print (" Compare rain data from bucket and LNM")
    istwert = -999
    # now get: filter data to 1 minute
    # extract similar time rangee from both datasets
    print ("    -- get similar time ranges")
    minlnm, maxlnm = lnmdatastream._find_t_limits()
    minmet, maxmet = meteodatastream._find_t_limits()
    mint = max([minlnm,minmet])
    maxt = min([maxlnm,maxmet])
    lnmdata = lnmdatastream.trim(starttime=mint,endtime=maxt)
    meteodata = meteodatastream.trim(starttime=mint,endtime=maxt)
    if debug:
        print ("      -> dealing with time range from {} to {}".format(mint,maxt))
    print ("    -- extract rain measurements")
    res = np.asarray([el for el in meteodata._get_column('y') if not np.isnan(el)])
    res2 = np.asarray([el for el in lnmdata._get_column('df') if not np.isnan(el)])  # limit to the usually shorter rcst7 timeseries
    print ("      -> cumulative rain t7={} and lnm={}".format(np.sum(res), np.sum(res2)))
    if not len(res) > 0:
        config['rainsource'] = 'laser' 
    if len(res) > 1440*int(dayrange*0.5) and not np.mean(res) == 0:
        istwert = np.abs((np.mean(res) - np.mean(res2))/np.mean(res))
        sollwert = 0.3
        if debug:
            print ("Obsevered difference of cumulative percipitioan: {} percent; (accepted difference: {} percent)".format(istwert,sollwert))
        if istwert > 0.3:
            print ("     !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print ("     Current Weather: large differences between rain measurements !!") # add this to consistency log
            print ("     Difference is {}".format(istwert))
            print ("     Means: T7 = {}; LNM = {}".format(np.mean(res), np.mean(res2)))
            print ("     !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            # you can switch to lnm data by activating the following two lines
            #data2 = data2._put_column(res2, 't2', columnname='Niederschlag',columnunit='mm/1h')
            #data = mergeStreams(data,data2,keys=['t2'],mode='replace')
    else:
        print ("    -> no rain in time range or sequence too short")

    print ("      -> Done")
    print (" -----------------------")

    return istwert, config


def CombineStreams(streamlist, debug=False):
    """
    DESCRIPTION:
        combine datastream to a single meteo data stream
    """
    result = DataStream()
    print ("  Joining stream") 
    for st in streamlist:
        print ("   -> dealing with {}".format(st.header.get("SensorID"))) 
        if debug:
            print ("    coverage before:", st._find_t_limits())
            #mp.plot(st)
        if not result.length()[0] > 0:
            result = st
        else:
            try:
                if debug:
                    print ("     - before join/merge: len {} and keys {}".format(result.length()[0], result._get_key_headers()))
                result = joinStreams(result,st)  # eventually extend the stream
                result = FloatArray(result)   # remove any unwanted string as occur in lnm
                if debug:
                    print ("     - after join: len {} and keys {}".format(result.length()[0], result._get_key_headers()))
                result = mergeStreams(result,st,mode='replace')  # then merge contents 
                if debug:
                    print ("     - after merge: len {} and keys {}".format(result.length()[0], result._get_key_headers()))
            except:
                print ("   -> problem when joining datastream")
        if debug:
            print ("    coverage after:", result._find_t_limits())

    result.header = {}
    result.header['col-y'] = 'rain'
    result.header['unit-col-y'] = 'mm/h'
    result.header['col-z'] = 'snow'
    result.header['unit-col-z'] = 'cm'
    result.header['col-f'] = 'T'
    result.header['unit-col-f'] = 'deg C'
    result.header['col-t1'] = 'rh'
    result.header['unit-col-t1'] = 'percent'
    result.header['col-t2'] = 'visibility'
    result.header['unit-col-t2'] = 'm'
    result.header['col-var1'] = 'windspeed'
    result.header['unit-col-var1'] = 'm/s'
    result.header['col-var2'] = 'winddirection'
    result.header['unit-col-var2'] = 'deg'
    result.header['col-var4'] = 'synop'
    result.header['unit-col-var4'] = '4680'
    result.header['col-var5'] = 'P'
    result.header['unit-col-var5'] = 'hPa'
    result.header['StationID'] = 'SGO'
    result.header['SensorID'] = 'METEOSGO_adjusted_0001'
    result.header['DataID'] = 'METEOSGO_adjusted_0001_0001'

    if debug:
        #mp.plot(result)
        print (result.ndarray)
        print (result.length()[0])
    return result


def ObjectArray(datastream):
    l = np.asarray([np.asarray(el).astype(object) for el in datastream.ndarray])
    return DataStream([],datastream.header,l)


def FloatArray(datastream):
    newnd = []
    for ar in datastream.ndarray:
        n = []
        for el in ar:
            try:
                n.append(float(el))
            except: 
                n.append(np.nan)
        newar = np.asarray(n).astype(float)
        newnd.append(newar)

    return DataStream([],datastream.header,np.asarray(newnd))


def AddSYNOP(datastream, synopdict={}, debug=False):
    # Add plain text synop descriptions
    print (" -----------------------")
    print (" Adding synop codes")
    syno = datastream._get_column('var4')
    txt= []
    for el in syno:
        try:
            itxt = str(int(el))
            txt.append(synopdict.get(itxt,''))
        except:
            txt.append('')
    txt = np.asarray(txt)
    datastream._put_column(txt,'str2')

    print ("      -> Done")
    print (" -----------------------")

    return datastream


def RainSource(datastream, diff=0.1, source='bucket', debug=False):
    """
    DESCRIPTION
        Select primary source of percipitation data
        Three possibilities:
          1) source "bucket" -> always use rain bucket data
          2) source "laser" -> always use laser disdrometer
          3) source "whatever" -> primarly use 'bucket', switch to 'laser' if difference exceeds threshold (e.g 0.3 = 30%)
        Background of "whatever": In very strong rain events, the bucket cannot measure rain amounts precisly any more  
    """

    print (" -----------------------")
    print (" Fixing source of rain measurements")
    threshold = 0.3
    if diff > threshold and not source == 'bucket':
        source = 'laser'
    if source=='bucket':
        datastream._drop_column('df')
    else:  # source=='laser':
        datastream._move_column('df','y')
        datastream._drop_column('df')

    print ("      -> Done: using {}".format(source))
    print (" -----------------------")

    return datastream


def ExportData(datastream, onlyarchive=False, config={}):

    meteofilename = 'meteo-1min_'
    connectdict = config.get('conncetedDB')
    meteoproductpath = config.get('meteoproducts','')
    print (" -----------------------")
    print (" Exporting data")
    try:
        if meteoproductpath:
            # save result to products
            datastream.write(meteoproductpath,filenamebegins=meteofilename,dateformat='%Y',coverage='year', mode='replace',format_type='PYCDF')
            print ("  -> METEO_adjusted written to File")

        if len(connectdict) > 0 and not onlyarchive:
            for dbel in connectdict:
                dbw = connectdict[dbel]
                ## Set some important header infos
                datastream.header['StationID'] = 'SGO'
                datastream.header['SensorID'] = 'METEOSGO_adjusted_0001'
                datastream.header['DataID'] = 'METEOSGO_adjusted_0001_0001'
                datastream.header['SensorGroup'] = 'services'
                # check if table exists... if not use:
                writeDB(dbw,datastream,tablename='METEOSGO_adjusted_0001_0001')
                # else use
                #writeDB(dbw,datastream, tablename=...)
                print ("  -> METEOSGO_adjusted written to DB {}".format(dbel))
        success = True
    except:
        success = False
    print (" -----------------------")

    return success


def WeatherAnalysis(db, config={},statusmsg={}, endtime=datetime.utcnow(), source='database', onlyarchive=False, debug=False):
    """
    DESCRIPTION:
        Main method to analyse and combine measurements from various different
        environmental sensors
    """

    name1a = "{}-lnm".format(config.get('logname'))
    name1b = "{}-ultra".format(config.get('logname'))
    name1c = "{}-bm35".format(config.get('logname'))
    name1d = "{}-rcs".format(config.get('logname'))
    name1e = "{}-meteo".format(config.get('logname'))
    name1f = "{}-rain".format(config.get('logname'))
    name1g = "{}-combination".format(config.get('logname'))
    name1h = "{}-synop".format(config.get('logname'))
    sgopath = config.get('sgopath')
    lnm = DataStream()
    ultra = DataStream()
    bm35 = DataStream()
    rcs = DataStream()
    meteo = DataStream()
    dayrange = int(config.get('meteorange',3))
    starttime = endtime-timedelta(days=dayrange)
    diff = 0.0
    succ = False
    trans = ''
    flaglistbm35 = []
    flaglistrcs = []

    if starttime < datetime.utcnow()-timedelta(days=20):
        print ("     -- Eventually not enough data in database for full coverage")
        print ("       -> Accessing archive files instead")
        source='archive'


    print (" A. Reading Laser Niederschlag")
    try:
        lnm = readTable(db, sourcetable="LNM%", source=source,  path=sgopath, starttime=starttime, endtime=endtime, debug=debug)
        try:
            trans = getLastSynop(lnm, synopdict=synopdict)
        except:
            pass
        lnm = transformLNM(lnm, debug=debug)
        if lnm.length()[0] > 0:
            statusmsg[name1a] = 'LNM data finished - data available'
        else:
            statusmsg[name1a] = 'LNM data finished - but no data available'
    except:
        if debug:
            print ("    -> LNM failed")
        statusmsg[name1a] = 'LNM data failed'
    config['lastSynop'] = trans

    print (" B. Reading Ultrasonic data")
    try:
        ultra = readTable(db, sourcetable="ULTRASONIC%", source=source,  path=sgopath, starttime=starttime, endtime=endtime, debug=debug)
        ultra = transformUltra(db, ultra, debug=debug)
        if ultra.length()[0] > 0:
            statusmsg[name1b] = 'ULTRA data finished - data available'
        else:
            statusmsg[name1b] = 'ULTRA data finished - but no data available'
    except:
        if debug:
            print ("    -> ULTRA failed")
        statusmsg[name1b] = 'ULTRA data failed'

    print (" C. Reading BM35 data")
    try:
        bm35 = readTable(db, sourcetable="BM35%", source=source, path=sgopath, starttime=starttime, endtime=endtime, debug=debug)
        bm35, flaglistbm35 = transformBM35(db, bm35, debug=debug)
        if bm35.length()[0] > 0:
            statusmsg[name1c] = 'BM35 data finished - data available'
        else:
            statusmsg[name1b] = 'BM35 data finished - but no data available'
    except:
        if debug:
            print ("    -> BM35 failed")
        statusmsg[name1c] = 'BM35 data failed'

    print (" D. Reading RCST7 data")
    try:
        rcs = readTable(db, sourcetable="RCST7%", source=source,  path=sgopath, starttime=starttime, endtime=endtime, debug=debug)
        rcs, flaglistrcs = transformRCST7(db, rcs, debug=debug)
        if rcs.length()[0] > 0:
            statusmsg[name1d] = 'RCST7 data finished - data available'
        else:
            statusmsg[name1d] = 'RCST7 data finished - but no data available'
    except:
        if debug:
            print ("    -> RCST7 failed")
        statusmsg[name1d] = 'RCST7 data failed'

    print (" E. Reading METEO data (realtime RCS T7 data)")
    try:
        meteo = readTable(db, sourcetable="METEO_T%", source=source,  path=sgopath, starttime=starttime, endtime=endtime, debug=debug)
        meteo = transformMETEO(db, meteo, debug=debug)
        if meteo.length()[0] > 0:
            statusmsg[name1e] = 'METEO data finished - data available'
        else:
            statusmsg[name1e] = 'METEO data finished - but no data available'
    except:
        if debug:
            print ("    -> METEO failed")
        statusmsg[name1e] = 'METEO data failed'

    if debug:
        print (" - Data contents and coverage:")
        print (" - Length LNM:",lnm.length()[0], lnm._find_t_limits())
        print (" - Length Ultra:",ultra.length()[0], ultra._find_t_limits())
        print (" - Length RCS:",rcs.length()[0], rcs._find_t_limits())
        print (" - Length METEO:", meteo.length()[0], meteo._find_t_limits())
        print (" - Length BM35:", bm35.length()[0], bm35._find_t_limits())


    print (" F. Check Rain measurements")
    try:
        diff, config = CheckRainMeasurements(lnm, meteo, dayrange=dayrange, config=config, debug=debug)
        statusmsg[name1f] = 'Rain analysis finished'
    except:
        statusmsg[name1f] = 'Rain analysis failed'
    # diff can be used to eventually switch from rain bucket to lnm

    print (" G. Combine measurements")
    try:
        result = CombineStreams([ultra, bm35, lnm, meteo, rcs], debug=debug)
        statusmsg[name1g] = 'combination of streams successful'
    except:
        statusmsg[name1g] = 'combination of streams failed'

    print (" H. Add Synop codes and I. Selecting Bucket or Laser")
    try:
        result = ObjectArray(result)
        result = AddSYNOP(result, synopdict=synopdict, debug=debug)
        result = RainSource(result, diff=diff, source=config.get('rainsource','bucket'), debug=debug)
        statusmsg[name1h] = 'synop and rain source successful'
    except:
        statusmsg[name1h] = 'synop and rain source failed'

    if debug and config.get('testplot',False):
        mp.plot(result)

    if not debug:
        connectdict = config.get('conncetedDB')
        for dbel in connectdict:
            dbw = connectdict.get(dbel)
            print ("    -- writing flags for BM35 and RCS to DB {}".format(dbel))
            if len(flaglistbm35) > 0:
                print ("    -- new bm35 flags:", len(flaglistbm35))
                flaglist2db(dbw,flaglistbm35)
            if len(flaglistrcs) > 0:
                print ("    -- new RCS flags:", len(flaglistrcs))
                flaglist2db(dbw,flaglistrcs)
        succ = ExportData(result, onlyarchive=onlyarchive, config=config)
    else:
        print (" Debug selected - not exporting")

    return result, succ, statusmsg


def snowornosnow(T,S,Exist,SYNOP,p_L=0,threshold=190, debug=False):
    """
    DESCRIPTION:
        Estimate whether snow is existing or not
        This method uses several parameters related to snow and 
        adds them up to a probability sum, indicating whether snow
        is accumulating or not. Currently five parameters are tested:
           1. Temperature (high probablity at low temperatures)
           2. Snow height (high probability at high values -> obviuos)
              problematic are low values (<5cm)
           3. SYNOP code: high probability is its snowing or hailing (SYNOP>70)
           4. Snow already existing
           5. some location related manual probability (0 in our case)
           Useful extensions:
           6. Soil or ground temperature (or eventually temperature history)
           7. Reflectivity
        Each parameter adds to a probability sum. If this sum 
        exceeds the given threshold value we assume a snow cover
    """
    def p_T(T): # linear probability decrease
               try:
                   if T >= 15:
                       return 0.
                   elif T <= 1:
                       return 100.
                   else:
                       return -7.14286*T  + 107.69231
               except:
                   return 0.

    def p_S(S): # exponential probability increase
               try:
                   if S <= 0:
                       return 0.
                   elif S >= 5:
                       return 100.
                   else:
                       return 4.*S*S
               except:
                   return 0.

    def p_SYNOP(SYNOP):
               try:
                   if int(SYNOP) > 70:
                      return 100.
                   else:
                      return 0.
               except:
                   return 0.

    def p_E(Exist):
               try:
                   if Exist in ['Snow','Schnee']:
                       return 100.
                   else:
                       return 0.
               except:
                   return 0.

    sump = p_T(T) + p_S(S) + p_SYNOP(SYNOP) + p_E(Exist) + p_L
    if debug:
        print ("    -- Testparameter:", p_T(T),p_S(S),p_SYNOP(SYNOP),p_E(Exist),p_L)
    print ("    -- current snow cover probability value: {}".format(sump))
    if sump > threshold:
        return 'Schnee'
    else:
        return '-'


def CreateWeatherTable(datastream, config={}, statusmsg={}, debug=False):
    """
    DESCRIPTION:
        Main method to create a short term mean weather values
    """

    currentvaluepath = config.get('currentvaluepath','')
    name2 = "{}-table".format(config.get('logname'))
    trans = config.get('lastSynop','')

    print (" -----------------------")
    print (" Creating data table for the last 30 min")
    try:
        # Get the last 30 min
        print ("    -- Coverage:", datastream._find_t_limits())
        lastdate = datastream.ndarray[0][-1]
        shortextract = datastream._select_timerange(starttime=num2date(lastdate)-timedelta(minutes=30), endtime=lastdate)
        poscom = KEYLIST.index('comment')
        posflag = KEYLIST.index('flag')
        shortextract[poscom] = np.asarray([])
        shortextract[posflag] = np.asarray([])

        shortstream = DataStream([],datastream.header,shortextract)
        vallst = [0 for key in KEYLIST]
        for idx,key in enumerate(datastream._get_key_headers()):
            if key in NUMKEYLIST:
                print ("    -- Dealing with key {}".format(key))
                # alternative
                col = shortstream._get_column(key)
                if len(col)> 0 and not np.isnan(col).all():
                    if debug:
                         print (len(col), col[0])
                    mean = np.nanmedian(col)
                else:
                    mean = np.nan
                vallst[idx] = mean
                print ("    -- Assigning values", idx, key, mean)

        # Update current value dictionary:
        if os.path.isfile(currentvaluepath):
            with open(currentvaluepath, 'r') as file:
                fulldict = json.load(file)
                valdict = fulldict.get('meteo')
        else:
            valdict = {}
            fulldict = {}
            #fulldict['meteo'] = valdict
        if not valdict:
            valdict = {}
        if debug:
             print ("    -> Got old currentvalues:", valdict) 

        if len(shortextract[KEYLIST.index('var4')]) > 0:
            try:
                SYNOP = max(shortextract[KEYLIST.index('var4')])
            except:
                SYNOP = 0
        else:
            SYNOP = 0

        cover = snowornosnow(vallst[2],vallst[1],valdict.get('Schnee',['-'])[0],SYNOP,p_L=0)

        valdict[u'Schnee'] = [cover,'']
        valdict[u'date'] = [str(num2date(lastdate).replace(tzinfo=None)), '']
        valdict[u'T'] = [vallst[2], 'degC']
        valdict[u'rh'] = [vallst[3], '%']
        valdict[u'P'] = [vallst[8], 'hPA']
        valdict[u'S'] = [vallst[1], 'cm']
        valdict[u'N'] = [vallst[0],'mm/h']
        valdict[u'Wind'] =  [vallst[5]*3.6,'km/h']
        valdict[u'Niederschlag'] = [trans,'SYNOP']
        fulldict[u'meteo'] = valdict

        if not debug:
            print ("     -- writing new data to currentvalues")
            with open(currentvaluepath, 'w',encoding="utf-8") as file:
                file.write(unicode(json.dumps(fulldict))) # use `json.loads` to do the reverse
                print ("Current meteo data written successfully to {}".format(currentvaluepath))
        else:
            print ("     -- debug selected - skipping writing new data to currentvalues")
            print ("     -- NEW dict looks like:", valdict)

        print ("     -- upload of current.data moved to rsync")
        statusmsg[name2] = 'Step2: recent data extracted'
    except:
        statusmsg[name2] = 'Step2: current data failed'
    print ("     -> Done")

    return statusmsg


def ShortTermPlot(datastream, config={}, statusmsg={}, endtime=datetime.utcnow(), debug=False):

    name3 = "{}-shortplot".format(config.get('logname'))
    imagepath = config.get('meteoimages') 

    print (" -----------------------")
    print (" Creating short range plot")

    try:
        print (" !! Please note - plotting will only work from cron or root")
        print (" !! ------------------------------")
        #import pylab
        # ############################
        # # Create plots ???? -> move to plot
        # ############################
        datastream.ndarray[2] = datastream.missingvalue(datastream.ndarray[2],3600,threshold=0.05,fill='interpolate')
        datastream.ndarray[3] = datastream.missingvalue(datastream.ndarray[3],600,threshold=0.05,fill='interpolate')

        longextract = datastream._select_timerange(starttime=endtime-timedelta(days=2), endtime=endtime)

        imagepath = config.get('meteoimages')
        #print ("Test", longextract)
        t = longextract[0]   # time
        y2 = longextract[2]  # rain
        y3 = longextract[3]  # schneehoehe
        y4 = longextract[4]  # temp
        y7 = longextract[7]  # 
        max1a = 0
        max1b = 0
        max2a = 0

        print (len(t), len(y2), len(y3), len(y4), len(y7))

        if len(y2) > 0:
            max1a = np.nanmax(y2)
        if max1a < 10 or np.isnan(max1a):
            max1a = 10
        if len(y3) > 0:
            max1b = np.nanmax(y3)
        if max1b < 100 or np.isnan(max1b):
            max1b = 100
        if len(y7) > 0:
            max2a = np.nanmax(y7)
        if max2a < 12 or np.isnan(max2a):
            max2a = 12

        fig, axarr = plt.subplots(3, sharex=True, figsize=(15,9))
        # first plot (temperature)
        axarr[0].set_ylabel('T [$ \circ$C]')
        if len(y4) > 0:
            axarr[0].plot_date(t,y4,'-',color='lightgray')
            axarr[0].fill_between(t,0,y4,where=y4<0,facecolor='blue',alpha=0.5)
            axarr[0].fill_between(t,0,y4,where=y4>=0,facecolor='red',alpha=0.5)
        axarr[1].set_ylabel('S [cm]')
        axarr[1].set_ylim([0,max1b])
        if len(y3) > 0:
            axarr[1].plot_date(longextract[0],longextract[3],'-',color='gray')
            axarr[1].fill_between(t,0,y3,where=longextract[3]>=0,facecolor='gray',alpha=0.5)
        ax1 = axarr[1].twinx()
        ax1.set_ylabel('N [mm/h]',color='blue')
        ax1.set_ylim([0,max1a])
        if len(y2) > 0:
            ax1.plot_date(t,y2,'-',color='blue')
            ax1.fill_between(t,0,y2,where=y2>=0,facecolor='blue',alpha=0.5)
        axarr[2].set_ylabel('Wind [m/s]')
        axarr[2].set_ylim([0,max2a])
        if len(y7) > 0:
            axarr[2].plot_date(t,y7,'-',color='gray')
            axarr[2].fill_between(t,0,y7,where=longextract[7]>=0,facecolor='gray',alpha=0.5)
        filedate = datetime.strftime(endtime,"%Y-%m-%d")
        if not debug:
            savepath = os.path.join(imagepath,'Meteo_0_'+filedate+'.png')
            print ("    -- Saving graph locally to {}".format(savepath))
            plt.savefig(savepath)
            plt.close(fig)
        #else:
        #    plt.show()
        statusmsg[name3] = 'two day plot finished'
        success=True
    except:
        print ("  Short term plot failed")
        statusmsg[name3] = 'two day plot failed'
        success=False

    print ("     -> Done")
    return statusmsg


def LongTermPlot(datastream, config={}, statusmsg={}, endtime=datetime.utcnow(), debug=False):

    name4 = "{}-longplot".format(config.get('logname'))
    imagepath = config.get('meteoimages')
    meteoproductpath = config.get('meteoproducts')
    meteofilename = 'meteo-1min_'
    starttime = endtime - timedelta(days=365)

    print (" -----------------------")
    print (" Creating long range plot")
    try:
        longterm=read(os.path.join(meteoproductpath,'{}*'.format(meteofilename)), starttime=starttime,endtime=endtime)
        print ("  Please note: long term plot only generated from cron or root")
        print ("  Long term plot", longterm.length(), starttime, endtime)
        longterm = FloatArray(longterm)
        t = longterm.ndarray[0].astype(float64)
        y2 = longterm.ndarray[2].astype(float64)  # Rain
        y3 = longterm.ndarray[3].astype(float64)  # Snow
        y4 = longterm.ndarray[4].astype(float64)  # Temp
        y7 = longterm.ndarray[7].astype(float64)

        if debug:
            print ("  LongTerm Parameter", len(t), len(y2), len(y3), len(y4), len(y7))

        max1a = np.nanmax(y2)
        if max1a < 10 or np.isnan(max1a):
            max1a = 10
        max1b = np.nanmax(y3)
        if max1b < 100 or np.isnan(max1b):
            max1b = 100
        max2a = np.nanmax(y7)
        if max2a < 12 or np.isnan(max2a):
            max2a = 12

        print ("  Max values redefined")
        fig, axarr = plt.subplots(3, sharex=True, figsize=(15,9))
        # first plot (temperature)
        axarr[0].set_ylabel('T [$ \circ$C]')
        axarr[0].plot_date(t,y4,'-',color='lightgray')
        try:
            axarr[0].fill_between(t,[0]*len(t),y4,where=y4<0,facecolor='blue',alpha=0.5)
            axarr[0].fill_between(t,0,y4,where=y4>=0,facecolor='red',alpha=0.5)
        except:
            pass
        axarr[1].set_ylabel('S [cm]')
        axarr[1].set_ylim([0,max1b])
        axarr[1].plot_date(t,y3,'-',color='gray')
        try:
            axarr[1].fill_between(t,0,y3,where=y3>=0,facecolor='gray',alpha=0.5)
        except:
            pass
        ax1 = axarr[1].twinx()
        ax1.set_ylabel('N [mm/h]',color='blue')
        ax1.set_ylim([0,max1a])
        ax1.plot_date(t,y2,'-',color='blue')
        try:
            ax1.fill_between(t,0,y2,where=y2>=0,facecolor='blue',alpha=0.5)
        except:
            pass
        axarr[2].set_ylabel('Wind [m/s]')
        axarr[2].set_ylim([0,max2a])
        axarr[2].plot_date(t,y7,'-',color='gray')
        try:
            axarr[2].fill_between(t,0,y7,where=y7>=0,facecolor='gray',alpha=0.5)
        except:
            pass
        if not debug:
            savepath = os.path.join(imagepath,'Meteo_1.png')
            plt.savefig(savepath)
            plt.close(fig)
        #else:
        #    plt.show()
        statusmsg[name4] = 'long term plot finished'
        success=True
    except:
        print ("Long term failed")
        statusmsg[name4] = 'long term plot failed'
        success=False
    print ("     -> Done")
    return statusmsg


def main(argv):
    try:
        version = __version__
    except:
        version = "1.0.0"
    configpath = ''
    statusmsg = {}
    debug=False
    endtime = None
    weatherstream = DataStream()
    testplot = False
    dayrange = 0
    source = 'database'
    onlyarchive=False

    try:
        opts, args = getopt.getopt(argv,"hc:e:r:aDP",["config=","endtime=","dayrange=","createarchive=","debug=","plot=",])
    except getopt.GetoptError:
        print ('weather_products.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- weather_products.py will determine the primary instruments --')
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
            print ('-e            : endtime - default is now')
            print ('-r            : range of days')
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
        elif opt in ("-r", "--range"):
            # get a range of days : default from cfg
            try:
                dayrange = int(arg)
            except:
                print ("  range needs to be an integer")
                dayrange = 0
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True
        elif opt in ("-a", "--createarchive"):
            # delete any / at the end of the string
            source = 'archive'
            onlyarchive = True
        elif opt in ("-P", "--plot"):
            # delete any / at the end of the string
            testplot = True

    print ("Running current_weather version {}".format(version))
    print ("--------------------------------")

    if endtime:
         try:
             endtime = DataStream()._testtime(endtime)
         except:
             print (" Could not interprete provided endtime. Please Check !")
             sys.exit(1)
    else:
         endtime = datetime.utcnow()

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname='mm-dp-weather.log', debug=debug)
    config['testplot'] = testplot
    if dayrange and dayrange > 0:
        config['meteorange'] = int(dayrange)

    name1 = "{}-flag".format(config.get('logname'))
    statusmsg[name1] = 'weather analysis successful'

    print ("3. Connect databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
    except:
        statusmsg[name1] = 'database failed'
    # it is possible to save data also directly to the brokers database - better do it elsewhere

    print ("4. Weather analysis")
    weatherstream, success, statusmsg = WeatherAnalysis(db, config=config,statusmsg=statusmsg, endtime=endtime, source=source, onlyarchive=onlyarchive, debug=debug)

    if not source=='archive':
        weatherstream = FloatArray(weatherstream) # convert object to float for fill_between plots
        print ("5. Create current data table")
        statusmsg = CreateWeatherTable(weatherstream, config=config, statusmsg=statusmsg, debug=debug)

        print ("6. Create short term plots")
        statusmsg = ShortTermPlot(weatherstream, config=config, statusmsg=statusmsg, endtime=endtime, debug=debug)

        print ("7. Create long term plots")
        statusmsg = LongTermPlot(weatherstream, config=config, statusmsg=statusmsg, endtime=endtime, debug=debug)

        if not debug:
            martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
            martaslog.telegram['config'] = config.get('notificationconfig')
            martaslog.msg(statusmsg)
        else:
            print ("Debug selected - statusmsg looks like:")
            print (statusmsg)

    else:
        print (" -> create archive selected: skipping plots and statusmessages")


if __name__ == "__main__":
   main(sys.argv[1:])

