
#!/usr/bin/env python
"""
DESCRIPTION
   Analyses weather data

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
import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
sys.path.insert(1,'/home/leon/Software/MARTAS/') # should be magpy2

import unittest

from magpy.stream import *
from magpy.core import plot as mp
from magpy.core import flagging
from magpy.core.methods import dictdiff, testtime

import numpy as np
import math
import copy
import json
import os
import getopt
import glob


"""
Sensors which are used to create the weather data products
- Disdrometer (Rain, T, Synop, Rainanalysis)
- RCS collection (T, rh, rain nbucket)
- METEO (RCS analysis by Andreas Winkelbauer)
- BM35 (pressure, T)
- Ultra-Anemometer (windspeed, winddirection, T)

# Approach - separate the jobs (ANALYSIS 1.0):
- SEPARATE: each sensor is flagged by martas.analysis job - TODO test the best flagging options (schedule ... every 5min?)
- Read each data source (DB table or archive file if timerange older than 1 week), filter to 1min and transform the data set to the
  meteo product stream structure, apply offsets and calculate specific contents (i.e. average rain)
- the order of inputs defines the significance (previous content might be replaced by new content
- eventually perform a similarity analysis of multiply sampled parameters and decide which one to choose, report in log and message
- save the new data structure 
- SEPARATE: create plots
- SEPARATE: create messages(logs pointing to strong variations, create messages for sognoficant events (strong rain, large pressure drop)

  # weather.py requires a configuration defining data sources and destinations, offsets, primary definitions deviating from order
     

"""

"""
# ################################################
#             SYNOP Definitions
# ################################################
"""

synopdict = {'de' : {"-1":"Sensorfehler",
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
                 "89":"Hagel",
                 "99":""},
             'en' : {"-1":"Sensor error",
                 "41":"Light to moderate precipitation (unidentified)",
                 "42":"Heavy precipitation (unidentified, unknown)",
                 "00":"No precipitation",
                 "51":"Light drizzle",
                 "52":"Moderate drizzle",
                 "53":"Heavy drizzle",
                 "57":"Light drizzle with rain",
                 "58":"Moderate to heavy drizzle with rain",
                 "61":"Light rain",
                 "62":"Moderate rain",
                 "63":"Heavy rain",
                 "67":"Light rain",
                 "68":"Moderate to heavy rain",
                 "77":"Snow sprinkles",
                 "71":"Light snowfall",
                 "72":"Moderate snowfall",
                 "73":"Heavy snowfall",
                 "74":"Light sleet",
                 "75":"Moderate sleet",
                 "76":"Heavy sleet",
                 "89":"Hail",
                 "99":""}
            }

# add the following methods to basic analysis techniques
def _data_from_db(name, starttime=None, endtime=None, samplingperiod=1, debug=False):
    """
    DESCRIPTION
        Extract data sets from database based on name fraction.
        - will get the lowest sampling period data equal or above the provided limit (default 1sec)
        - will also check coverage
    TODO:
        this method has the same name and almost identical applictaion are analysis._get_data_from_db
    RETURN
        datastream
    """
    datadict = {}
    determinesr = []
    datastream = DataStream()
    success = False
    if not starttime and not endtime:
        success = True

    # First get all existing sensors comaptible with name fraction
    sensorlist = self.db.select('DataID', 'DATAINFO', 'SensorID LIKE "%{}%"'.format(name))
    if debug:
        print("   -> Found {}".format(sensorlist))
        print("   a) select of highest resolution data equal/above samplingperiods of {} sec".format(
            samplingperiod))  # should be tested later again
    # Now get corresponding sampling rate
    projected_sr = samplingperiod
    for sensor in sensorlist:
        sr = 0
        res = self.db.select('DataSamplingrate', 'DATAINFO', 'DataID="{}"'.format(sensor))
        try:
            sr = float(res[0])
            if debug:
                print("    - Sensor: {} -> Samplingrate: {}".format(sensor, sr))
        except:
            if debug:
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print("Check sampling rate {} of {}".format(res, sensor))
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            determinesr.append(sensor)
        if sr >= projected_sr - 0.02:
            # if sr is larger to projected sr within 0.02 sec
            cont = {}
            cont['samplingrate'] = sr
            datadict[sensor] = cont
    if len(determinesr) > 0:
        if debug:
            print("   b) checking sampling rate of {} sensors without sampling rate".format(len(determinesr)))
        for sensor in determinesr:
            lastdata = self.db.get_lines(sensor, namedict.get('coverage', 7200))
            if len(lastdata) > 0:
                sr = lastdata.samplingrate()
                if debug:
                    print("    - Sensor: {} -> Samplingrate: {}".format(sensor, sr))
                # update samplingrate in db
                print("    - updating header with determined sampling rate:", lastdata.header)
                self.db.write(lastdata)
                if sr >= projected_sr - 0.02:
                    # if sr is larger to projected sr within 0.02 sec
                    cont = {}
                    cont['samplingrate'] = sr
                    datadict[sensor] = cont
    if debug:
        print("   -> {} data sets fulfilling search criteria after a and b".format(len(datadict)))

    data = DataStream()
    selectedsensor = ''
    sel_sr = 9999
    for dataid in datadict:
        cont = datadict.get(dataid)
        sr = cont.get('samplingrate')
        if sr < sel_sr:
            selectedsensor = dataid
            sel_sr = sr
            if debug:
                print("   -> {}: this sensor with sampling rate {} sec is selected".format(selectedsensor, sel_sr))
            ddata = self.db.read(selectedsensor, starttime=starttime, endtime=endtime)
            if len(ddata) > 0:
                data = join_streams(ddata, data)
            if debug:
                print("   c) now check whether the timerange is fitting")
            st, et = data.timerange()
            if starttime:
                # assume everything within oe hour to be OK
                if np.abs((starttime - st).total_seconds()) < 3600:
                    success = True
            if endtime:
                if np.abs((endtime - et).total_seconds()) < 3600:
                    success = True

    return data, success


def get_data(sname, starttime=None, endtime=None, stationid='SGO', apply_flags=True, config={}, debug=True):
    """
    same = like "ULTRA*"
    """
    dbname = config.get('', 'cobsdb')
    archivepath = config.get('', '/srv/archive')

    et = ''
    data = DataStream()
    if not endtime:
        pass
    elif endtime == 'now':
        et = 'now'
        endtime = datetime.now(timezone.utc).replace(tzdata=None)
    else:
        endtime = testtime(endtime)
    if not starttime:
        if et == 'now':
            starttime = endtime - timedelta(days=1)
    else:
        starttime = testtime(starttime)

    # connect_db
    db = False
    # if sname is a path with widcards then skip db test and read directly
    l = glob.glob(sname)
    if len(l) > 0:
        # file path with wildcards was provided
        data = read(sname, starttime=starttime, endtime=endtime)
    else:
        # check database first
        if db:
            dname = sname.replace("*", "%")
            ddata, success = _data_from_db(dname, starttime=starttime, endtime=endtime, samplingperiod=1, debug=debug)
        else:
            ddata = DataStream()
            success = False
        # if no fdata or incomplete check archive
        if not success:
            l1 = glob.glob(os.path.join(archive, stationid, sname))
            print(l1)
            if len(l1) > 1:
                print("name fragment is unspecific")
            if len(l1) > 0:
                for path in l1:
                    cdata = read(os.path.join(path, sname), starttime=starttime, endtime=endtime)
                    data = join_streams(cdata, data)
                if len(ddata) > 0:
                    data = join_streams(data, ddata)
        else:
            data = ddata.copy()
    # flags
    if db:
        fl = db.flags_from_db(sensorid=data.header.get("SensorID"), starttime=starttime, endtime=endtime)
        data = fl.apply_flags(data)

    return data


def pressure_to_sea_level(pressure, height, temp=[], rh=[], g0=9.80665):
    """
    DESCRIPTION
        caluclates pressure to sea level using the DWD formula
    PARAMETERS
        pressure  :  array with pressure values at station altitiude
        height    :  height of the station in m
        temp  :  temperature array with same length as pressure in degree C
        relhum :  relative humidity with len of pressure values
    RETURNS
        pr_sea_level : an array of length pressure with values at sea level in hPa
    """
    Rd = 287.05 # m2/(s2/K)
    a = 0.0065 # K/m

    if not len(pressure) > 0:
        print ("You need to provide pressure data at station level")
        return None
    if not height:
        print ("You need to provide a height of the measurement")
        return None
    if len(temp)>0 and len(rh)>0 and len(temp) == len(pressure) and len(rh) == len(pressure):
        # Solution 1 - source Wikipedia, after Beobachterhandbuch (BHB) für Wettermeldestellen des synoptisch-klimatologischen
        # Mess- und Beobachtungsnetzes (= Vorschriften und Betriebsunterlagen. Nr. 3). Dezember 2015, Kap. 6.6 Reduktion des Luftdrucks
        Th = temp + 273.25  # K
        es = 6.112 * np.exp((17.67*temp)/(temp+243.5))  # t in degree C, saturation vapor pressure
        E = rh/100. * es # hPa
        Ch = 0.12 # K/hPa
        print ("Using full DWD equation - citation")
        x = (g0*height)/(Rd*(Th+Ch*E+a*height/2))
        pr_sea = pressure * np.exp(x)
    else:
        # Solution 2 - internationale barometrische Hoehenformel (kein spezifisches Zitat auffindbar, zahllose Nennungen im WWW)
        # no additional parameter - used in java script WebMARTAS)
        # difference about 1 hPa
        print ("Using simplification - citation")
        pr_sea = (pressure / (1-(a*height)/288.15)**5.255)
    return pr_sea

def snow_or_nosnow(meteo,config=None, debug=False):
    """
    DESCRIPTION:
        Estimate whether a snow cover is probable or not. Background: very small snow heights
        might not be found because of uncertainties of snow height sensor. They are, however,
        critical for road conditions.
        Ideally we would use a camera... but
        This method uses several parameters related to snow and
        adds them up to a probability sum, indicating whether snow
        is accumulating or not. Currently five parameters are tested:
           1. Temperature (high probablity at low temperatures)
           2. Snow height (high probability at high values)
              problematic are low values (<5cm)
           3. SYNOP code: high probability if it is snowing or hailing (SYNOP>70)
           4. Snow cover already detected earlier
           5. some location related manual probability (0 in our case)
           Useful extensions:
           6. Soil or ground temperature (or eventually temperature history)
           7. Reflectivity
           and a CAM !!!
        Each parameter adds to a probability sum. If this sum
        exceeds the given threshold value we assume a snow cover
    """
    if not config:
        config = {}

    threshold = config.get('threshold',1.90)
    p0 = config.get('p0',0)
    ta = config.get('ta',-7.14286)
    tb = config.get('tb',107.69231)
    sa = 0.04

    tcol = meteo._get_column('f')
    if debug:
        print (tcol)
    tcol = np.where(tcol<=1, 1.0, tcol)
    tcol = np.where(tcol>=15, 0, tcol)
    probability_T = np.where(tcol>1, (ta*tcol+tb)/100., tcol)
    if debug:
        print ("probability T", probability_T)
    scol = meteo._get_column('z')
    if debug:
        print ("scol", scol)
    scol = np.where(scol<=0, 0, scol)
    scol = np.where(scol>=5, -1.0, scol)
    scol = np.where(scol>0, sa*scol*scol, scol)
    probability_SnowHeight = np.where(scol==-1, 1.0, scol)
    if debug:
        print ("probability SnowHeight", probability_SnowHeight)
    syncol = meteo._get_column('var4')
    syncol = np.where(syncol>70, 1.0, syncol)
    probability_Synop = np.where(syncol<=70, 0, syncol)
    if debug:
        print ("probability Synop", probability_Synop)
    ecol = meteo._get_column('dz')  # 1 (snow) or 0 (no-snow)
    if not len(ecol) == len(tcol):
        ecol = np.asarray([0]*len(tcol))
    probability_SnowExists = np.asarray([np.mean(ecol[idx-30:idx]) if idx > 30 and not np.isnan(el) else 0 for idx,el in enumerate(ecol)])
    if debug:
        print ("probability Existed recently", probability_SnowExists)

    sumprobcol = probability_T + probability_SnowHeight + probability_Synop + probability_SnowExists + p0
    if debug:
        print ("probability Sum", sumprobcol)
    resultcol = np.where(sumprobcol>threshold, 1, 0)
    meteo = meteo._put_column(resultcol, 'dz')
    meteo.header['col-dz'] = 'snow cover'

    return meteo




def transfrom_ultra(source, starttime=None, endtime=None, offsets={'t2':-0.87}, debug=False):
    """
    DESCRIPTION
        creates a 1-min data set, moves columns and apply offsets
    """
    t1 = datetime.now()
    ultra = get_data(source, starttime=starttime, endtime=endtime, debug=debug)
    #ultra = get_data(os.path.join(basepath, "ULTRA*"))
    if debug:
        print ("Headers: ", ultra._get_key_names())
        print ("Samplingrate {} sec and {} data points".format(ultra.samplingrate(), len(ultra)))
        print ("Range:", ultra.timerange())
    ultra = ultra.get_gaps()
    ultram = ultra.resample(keys=ultra._get_key_headers(),period=60)
    ultram = ultram.offset(offsets)
    ultram = ultram._move_column('t2','f')
    ultram = ultram._drop_column('var5') # resample might create a var5 column with gaps
    t2 = datetime.now()
    if debug:
        print (" transform needed {} sec".format((t2-t1).total_seconds()))
    return ultram


def transfrom_pressure(source, starttime=None, endtime=None, debug=False):
    """
    DESCRIPTION
        creates a 1-min data set, moves columns
    """
    t1 = datetime.now()
    bm35 = get_data(source, starttime=starttime, endtime=endtime, debug=debug)
    #bm35 = read(os.path.join(basepath, "Antares", "BM35*"))
    if debug:
        print ("Headers: ", bm35._get_key_names())
        print ("Samplingrate {} sec and {} data points".format(bm35.samplingrate(), len(bm35)))
        print ("Range:", bm35.timerange())
    #bm35s = bm35.filter()
    bm35 = bm35._move_column('var3','var5')
    bm35m = bm35.filter(filter_width=timedelta(minutes=2), resample_period=60.0)
    t2 = datetime.now()
    if debug:
        print ("filtered sampling rate", bm35m.samplingrate())
        print (" transform needed {} sec".format((t2-t1).total_seconds()))
    return bm35m

def transfrom_lnm(source, starttime=None, endtime=None, debug=False):
    """
    DESCRIPTION
        creates a 1-min data set, moves columns.
        Tested no-data issues: nan-values are returned
    """
    t1 = datetime.now()
    lnm = get_data(source, starttime=starttime, endtime=endtime, debug=debug)
    #lnm = read(os.path.join(basepath, "LNM_0351_0001_0001_*"))
    lnm = lnm.get_gaps()
    if debug:
        print ("Headers: ", lnm._get_key_names())
        print ("Samplingrate {} sec and {} data points".format(lnm.samplingrate(), len(lnm)))
        print ("Range:", lnm.timerange())

    # TEST - no-data (NAN-values returned
    #col = lnm._get_column('x')
    #print (len(col))
    #col = np.asarray([np.nan if 3000 < i < 4000 else el for i,el in enumerate(col)])
    #lnm = lnm._put_column(col,'x')

    lnmm = lnm.copy()
    lnmm = lnmm._drop_column('dz')
    lnmm = lnmm._drop_column('dy')
    lnmm = lnmm._drop_column('dx')
    lnmm = lnmm._drop_column('var5')
    lnmm = lnmm._drop_column('var4')
    lnmm = lnmm._drop_column('var3')
    lnmm = lnmm._drop_column('var2')
    lnmm = lnmm._drop_column('var1')
    lnmm = lnmm._drop_column('t1')
    lnmm = lnmm._drop_column('t2')
    lnmm = lnmm._drop_column('f')
    lnmm = lnmm._drop_column('z')
    synop = lnmm._get_column('str1')
    # Move Synop code to var4
    syn = []
    for el in synop:
        if el in ['','-']:
            syn.append(np.nan)
        else:
            try:
                syn.append(int(el))
            except:
                syn.append(np.nan)
    lnmm = lnmm._drop_column('str1')
    lnmm = lnmm._move_column('y','t2')
    col = lnmm.steadyrise('x', timedelta(minutes=60),sensitivitylevel=0.002)
    orgcol = np.asarray([1.0 if not np.isnan(el) else np.nan for el in lnmm._get_column('x')])
    col = col*orgcol
    lnmm = lnmm._put_column(col,'y')
    lnmm = lnmm.resample(keys=lnmm._get_key_headers(),period=60)
    lnmm = lnmm._put_column(np.asarray(syn)[:len(lnmm)],'var4')
    lnmm = lnmm._drop_column('var5') # resample might create a var5 column with gaps
    t2 = datetime.now()
    if debug:
        print (lnmm.timerange())
        print (len(lnm),len(syn[:len(lnmm)]), len(lnmm))
        print (" transform needed {} sec".format((t2-t1).total_seconds()))
    return lnmm

def transfrom_rcs(source, starttime=None, endtime=None, debug=False):
    """
    DESCRIPTION
        creates a 1-min data set, move columns.
        Tested maintainance flagging
    """
    # Problem with RCS data: data is not equidistant, contains numerous gaps
    # if get gaps is done, then cumulative rain is wrongly determined because of gaps
    t1 = datetime.now()
    #rcst7 = read(os.path.join(basepath, "RCST7*"))
    rcst7 = get_data(source, starttime=starttime, endtime=endtime, debug=debug)
    if debug:
        print ("Headers: ", rcst7._get_key_names())
        print ("Samplingrate {} sec and {} data points".format(rcst7.samplingrate(), len(rcst7)))
        print ("Range:", rcst7.timerange())

    pa = None
    fl = None
    #print ("GET THIS INTO FLAGGING")
    # TEST binary flagging
    #rcst7 = rcst7.offset({'z' : 1}, starttime="2025-10-12T12:00:00", endtime="2025-10-12T12:30:00")
    #fl = flagging.flag_binary(rcst7, 'z', flagtype=3, labelid='070', keystoflag=['x','t1'], sensorid=rcst7.header.get("SensorID"),
    #                      text="Maintainance switch activated", markallon=True)
    #fl = fl.union(level=1)
    #print (fl)
    # TEST JC spike flagging (water data set)
    if (rcst7.end()-rcst7.start()).total_seconds() < 86400*12: # limit to 12 days
        medianjc = rcst7.mean('x', meanfunction='median')
        fl = flagging.flag_range(rcst7, keys=['x'], above=medianjc+30.) # typical range is 2 hours, flag data eceeding the median by 30 cm
        print ("Got {} outliers".format(len(fl)))
        pa = fl.create_patch()

    # now determine the gaps and interpolate the rain accumulation data (for testing of accumulation)
    rcst7 = rcst7.get_gaps()
    #rcst7 = rcst7.interpolate_nans(keys=['t1'])
    # then apply the flags
    if fl and len(fl) > 0:
        rcst7 = fl.apply_flags(rcst7)
        # save flags?
    # calculate cumulative rain before filtering
    col = rcst7.steadyrise('t1', timedelta(minutes=60),sensitivitylevel=0.002)
    if debug:
        p,a = mp.tsplot(rcst7, keys=['x','y','t1','t2'], patch=pa, height=2)

    orgcol = np.asarray([1.0 if not np.isnan(el) else np.nan for el in rcst7._get_column('t1')])
    col = col*orgcol
    rcst7 = rcst7._put_column(col,'var5')
    # filter the data
    rcst7m = rcst7.filter(filter_width=timedelta(minutes=1),resample_period=60.0)
    rcst7m = rcst7m._drop_column('f')
    rcst7m = rcst7m._move_column('y','f')
    rcst7m = rcst7m._drop_column('z')
    rcst7m = rcst7m._move_column('x','z')
    rcst7m = rcst7m._move_column('t1','x')
    rcst7m = rcst7m._move_column('t2','t1')
    rcst7m = rcst7m._drop_column('x') # remove the rain accumulation
    rcst7m = rcst7m._drop_column('var1')
    rcst7m = rcst7m._drop_column('var2')
    rcst7m = rcst7m._drop_column('var3')
    rcst7m = rcst7m._drop_column('var4')
    rcst7m = rcst7m._move_column('var5','y')
    t2 = datetime.now()
    if debug:
        print (" transform needed {} sec".format((t2-t1).total_seconds()))
    return rcst7m


def transfrom_meteo(source, starttime=None, endtime=None, debug=False):
    t1 = datetime.now()
    #meteo = read(os.path.join(basepath, "METEO*"))
    meteo = get_data(source, starttime=starttime, endtime=endtime, debug=debug)
    meteom = meteo.copy()
    meteom = meteom.get_gaps()
    meteom = meteom._drop_column('var1')
    meteom = meteom._drop_column('var2')
    meteom = meteom._drop_column('var3')
    meteom = meteom._drop_column('var4')
    meteom = meteom._drop_column('var5')
    meteom = meteom._drop_column('x')
    meteom = meteom._drop_column('y')
    meteom = meteom._drop_column('t2')
    col = meteom.steadyrise('dx', timedelta(minutes=60),sensitivitylevel=0.002)
    orgcol = np.asarray([1.0 if not np.isnan(el) else np.nan for el in meteom._get_column('dx')])
    col = col*orgcol
    meteom = meteom._put_column(col,'y')
    meteom = meteom._drop_column('dx')
    if (meteom.end()-meteom.start()).total_seconds() < 86400*12: # limit to 12 days
        medianjc = meteom.mean('z', meanfunction='median')
        fl = flagging.flag_range(meteom, keys=['z'], above=medianjc+30.) # typical range is 2 hours, flag data eceeding the median by 30 cm
        print ("Got {} outliers".format(len(fl)))
        pa = fl.create_patch()
        if len(fl) > 0:
            meteom = fl.apply_flags(meteom)


    t2 = datetime.now()
    if debug:
        p,a = mp.tsplot(meteom, keys=['z'], patch=pa, height=2)
        print ("Headers: ", meteo._get_key_names())
        print ("Samplingrate {} sec and {} data points".format(meteo.samplingrate(), len(meteo)))
        print ("Range:", meteo.timerange())
        print (" transform needed {} sec".format((t2-t1).total_seconds()))
    return meteom

# old
def ExportData(datastream, onlyarchive=False, config={}):

    meteofilename = 'meteo-1min_'
    connectdict = config.get('conncetedDB')
    meteoproductpath = config.get('meteoproducts','')
    print (" -----------------------")
    print (" Exporting data")
    try:
        if meteoproductpath:
            # save result to products
            print (" Writing meteo data to file ...")
            datastream.write(meteoproductpath,filenamebegins=meteofilename,dateformat='%Y%m',coverage='month', mode='replace',format_type='PYCDF')
            print ("  -> METEO_adjusted data successfully written to yearly file")

        if len(connectdict) > 0 and not onlyarchive:
            for dbel in connectdict:
                dbw = connectdict[dbel]
                ## Set some important header infos
                datastream.header['StationID'] = 'SGO'
                datastream.header['SensorID'] = 'METEOSGO_adjusted_0001'
                datastream.header['DataID'] = 'METEOSGO_adjusted_0001_0001'
                datastream.header['SensorGroup'] = 'services'
                # check if table exists... if not use:
                tablename='METEOSGO_adjusted_0001_0001'
                if dbtableexists(dbw,tablename):
                    writeDB(dbw,datastream,tablename=tablename)
                else:
                    # Here the basic header info in DATAINFO and SENSORS will be created
                    writeDB(dbw,datastream)
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
    print ("Checking results:", result.samplingrate())

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
            if not onlyarchive:
                print ("    -- writing flags for BM35 and RCS to DB {}".format(dbel))
            if len(flaglistbm35) > 0 and not onlyarchive:
                print ("    -- new bm35 flags:", len(flaglistbm35))
                flaglist2db(dbw,flaglistbm35)
            if len(flaglistrcs) > 0 and not onlyarchive:
                print ("    -- new RCS flags:", len(flaglistrcs))
                flaglist2db(dbw,flaglistrcs)
        succ = ExportData(result, onlyarchive=onlyarchive, config=config)
    else:
        print (" Debug selected - not exporting")

    print ("Checking results again:", result.samplingrate())

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

