import json
#!/usr/bin/env python

import sys
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
sys.path.insert(1,'/home/leon/Software/MARTAS/') # should be magpy2

# MagPy - requires >= 2.0.0
from magpy.stream import *
from magpy.core import flagging
from magpy.core import methods as mama
# MARTAS - requires >= 2.0b9
from martas.core.methods import martaslog as ml
from martas.core import methods as mm
from martas.core import analysis as marana

import numpy as np
#import math
#import copy
#import json
import os
import getopt
import glob


"""
DESCRIPTION
   Combining weather data from various different sensors into a single adjusted product.
   This method uses the following SensorIDs from the Conrad Observatory. Adiitional sensors
   require a modification of the code and addition of transform functions.
   - ULTRASONICDSP* : Thiess Ultrasonic-DSP-Anemometer (windspeed, winddirection, T)
   - BM35* : Meteolab BM035 (pressure, T)
   - LNM* : Thiess Disdrometer (Rain, T, Synop, Rainanalysis)
   - RCST7* : remote-control-system, COBS specific, (T, rh, rain nbucket, snowheight)
   - METEOT7* : (RCS analysis by Andreas Winkelbauer)
   Depending on availability the job identifies existing data in a MARCOS database, and, if not found,
   afterwards in a MARCOS archive structure. Paths and offsets need to be defined in a configuration
   file "weather.cfg".
   requires: Python >= 3.7

METHODS

| class  |  method                 |  version |  tested  |              comment             | manual | *used by   |
| ------ |  ---------------------- |  ------- |  ------- |  ------------------------------- | ------ | ---------- |
|        |  combine_weather        |  2.0.0   | app_test |                                  | -      |            |
|        |  pressure_to_sea_level  |  2.0.0   | app_test |                                  | -      |            |
|        |  snow_or_nosnow         |  2.0.0   | app_test |                                  | -      |            |
|        |  transform_ultra        |  2.0.0   | app_test |                                  | -      |            |
|        |  transform_pressure     |  2.0.0   | app_test |                                  | -      |            |
|        |  transform_lnm          |  2.0.0   | app_test |                                  | -      |            |
|        |  transform_meteo        |  2.0.0   | app_test |                                  | -      |            |
|        |  transform_rcs          |  2.0.0   | app_test |                                  | -      |            |

PARAMETERS
    -c configurationfile   :   path    :  
    -s starttime           :   date    :  starttime, default is endtime - 2 days
    -e endtime             :   date    :  date, default is now (utc)

RETURNS
    Saves a data product with SensorID "METEO_adjusted_0001" in 1-min resolution.

APPLICATION
    Realtime analysis with cron:
        python weather.py -c ~/CONF/weather.cfg
    Analysis of a given time range:
        python weather.py -c ~/CONF/weather.cfg -s 2020-11-11 -e 2020-11-22

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


def combine_weather(ultram=DataStream(), bm35m=DataStream(), lnmm=DataStream(), rcst7m=DataStream(),
                    meteom=DataStream()):
    """
    DESCRIPTION
        combined the transformed weather records to a single adjusted one-minute record
    """
    main = DataStream()
    comments = []
    # Tests:
    # ultram = ultram.trim(starttime='2025-10-11',endtime='2025-10-16')
    # meteom = meteom.trim(starttime='2025-10-10',endtime='2025-10-14')
    # rcst7m = rcst7m.trim(starttime='2025-10-10',endtime='2025-10-14')

    # In order to combine stream in a certain priority order and keep the full length of all inputs:
    # 1. merge stream with primary info on position 1
    # 2. join result stream with secondary

    if len(ultram) > 0 and ultram.samplingrate() == 60:
        main = ultram.copy()
        if len(bm35m) > 0:
            print("Combining wind and pressure")
            tmain = DataStream()
            tmain = merge_streams(bm35m,
                                  main)  # merge will keep values of first stream, will keep the length of first stream
            tmain = join_streams(tmain,
                                 bm35m)  # join will keep values of first stream and add eventually missing data from second
            tmain = join_streams(tmain,
                                 main)  # join will keep values of first stream and add eventually missing data from second
            main = tmain.copy()
    elif len(bm35m) > 0 and bm35m.samplingrate() == 60:
        main = bm35m.copy()
    if not len(main) > 0:
        main = lnmm.copy()
    else:
        if len(lnmm) > 0 and int(lnmm.samplingrate()) == 60:
            print("Combining wind/pressure with precipitation")
            tmain = DataStream()
            tmain = merge_streams(lnmm, main)
            tmain = join_streams(tmain, lnmm)
            tmain = join_streams(tmain, main)
            main = tmain.copy()
    if not len(main) > 0:
        main = meteom.copy()
    else:
        if len(meteom) > 0 and int(meteom.samplingrate()) == 60:
            print("Combining wind/pressure/pre with meteo rcs")
            tmain = DataStream()
            tmain = merge_streams(meteom, main)
            tmain = join_streams(tmain, meteom)
            tmain = join_streams(tmain, main)
            main = tmain.copy()
    if not len(main) > 0:
        main = rcst7m.copy()
    else:
        if len(rcst7m) > 0 and int(rcst7m.samplingrate()) == 60:
            print("Combining wind/pressure/pre/rcs with rcs")
            tmain = DataStream()
            tmain = merge_streams(rcst7m, main)
            tmain = join_streams(tmain, rcst7m)
            tmain = join_streams(tmain, main)
            main = tmain.copy()

    result = main.copy()

    # define header
    ok = True
    if ok:
        result.header = {}
        result.header['col-x'] = 'rain accumulation (LNM)'
        result.header['unit-col-x'] = 'mm'
        result.header['col-y'] = 'rain'
        result.header['unit-col-y'] = 'mm/h'
        result.header['col-z'] = 'snow'
        result.header['unit-col-z'] = 'cm'
        result.header['col-f'] = 'T'
        result.header['unit-col-f'] = 'degC'
        result.header['col-t1'] = 'rh'
        result.header['unit-col-t1'] = 'percent'
        result.header['col-t2'] = 'visibility'
        result.header['unit-col-t2'] = 'm'
        result.header['col-var1'] = 'windspeed'
        result.header['unit-col-var1'] = 'm/s'
        result.header['col-var2'] = 'winddirection'
        result.header['unit-col-var2'] = 'deg'
        result.header['col-var3'] = 'P(0)'
        result.header['unit-col-var3'] = 'hPa'
        result.header['col-var4'] = 'synop'
        result.header['unit-col-var4'] = '4680'
        result.header['col-var5'] = 'P(h)'
        result.header['unit-col-var5'] = 'hPa'
        result.header['col-str1'] = 'Synop DE'
        result.header['col-str2'] = 'Synop EN'
        result.header['StationID'] = 'SGO'
        result.header['SensorID'] = 'METEOSGO_adjusted_0001'
        result.header['DataID'] = 'METEOSGO_adjusted_0001_0001'
        result.header['DataComment'] = ", ".join(comments)
        result.header['SensorDescription'] = "joind record from multiple meteorological sensors"
        result.header['SensorGroup'] = 'services'

    # add some calulated parameters
    # -----------------------------
    # 1.) sea level pressure
    if len(main._get_column('var5')) > 0:
        psea = pressure_to_sea_level(main._get_column('var5'), 1049, temp=main._get_column('f'),
                                     rh=main._get_column('t1'))
        result = result._put_column(psea, 'var3')
        comments.append("P_sealevel using DWD reduction formula")
    # 2.) get synop descriptions
    synop = result._get_column('var4')
    synop = [int(el) if not np.isnan(el) else 99 for el in synop]
    lang = 'de'
    synd = synopdict.get(lang)
    synop_de = [synd.get(str(el).zfill(2)) if not el == -1 else synd.get("-1") for el in synop]
    result = result._put_column(synop_de, 'str1')
    lang = 'en'
    synd = synopdict.get(lang)
    synop_en = [synd.get(str(el).zfill(2)) if not el == -1 else synd.get("-1") for el in synop]
    result = result._put_column(synop_en, 'str2')
    # 3.) get flags and relate them to METEO_adjusted

    # 4.) Determine possible snow cover on road
    if len(main._get_column('f')) > 0 and len(main._get_column('z')) > 0 and len(main._get_column('var4')) > 0:
        result = snow_or_nosnow(result)

    return result


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


def transfrom_ultra(source, starttime=None, endtime=None, offsets=None, config=None, debug=False):
    """
    DESCRIPTION
        creates a 1-min data set, moves columns and apply offsets
    VARIABLES
        offsets : (dict) like {"SensorID" : {"x":2, "y":"-1"}}
    """
    if not config:
        config = {}
    if not offsets:
        offsets = {"ULTRASONICDSP_0001106088_0001" : {'t2':-0.87}}
    t1 = datetime.now()
    maan = marana.MartasAnalysis(config=config.get('anaconf',{}))
    ultra = maan.get_marcos_data(source, starttime=starttime, endtime=endtime, debug=debug)
    #ultra = get_data(os.path.join(basepath, "ULTRA*"))
    if debug:
        print ("Headers: ", ultra._get_key_names())
        print ("Samplingrate {} sec and {} data points".format(ultra.samplingrate(), len(ultra)))
        print ("Range:", ultra.timerange())
    ultra = ultra.get_gaps()
    ultram = ultra.resample(keys=ultra._get_key_headers(),period=60)
    if offsets and len(ultram) > 0:
        offset = {}
        for offsetid in offsets:
            if offsetid in ultra.header.get("SensorID"):
                offset = offsets.get(offsetid)
                for off in offset:
                    offset[off] = float(offset.get(off))
        if offset:
            ultram = ultram.offset(offset)
    ultram = ultram._move_column('t2','f')
    ultram = ultram._drop_column('var5') # resample might create a var5 column with gaps
    t2 = datetime.now()
    if debug:
        print (" transform needed {} sec".format((t2-t1).total_seconds()))
    return ultram


def transfrom_pressure(source, starttime=None, endtime=None, config=None, debug=False):
    """
    DESCRIPTION
        creates a 1-min data set, moves columns
    """
    if not config:
        config = {}
    t1 = datetime.now()
    maan = marana.MartasAnalysis(config=config.get('anaconf',{}))
    bm35 = maan.get_marcos_data(source, starttime=starttime, endtime=endtime, debug=debug)
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


def transfrom_lnm(source, starttime=None, endtime=None, config=None, debug=False):
    """
    DESCRIPTION
        creates a 1-min data set, moves columns.
        Tested no-data issues: nan-values are returned
    """
    if not config:
        config = {}
    t1 = datetime.now()
    maan = marana.MartasAnalysis(config=config.get('anaconf',{}))
    lnm = maan.get_marcos_data(source, starttime=starttime, endtime=endtime, debug=debug)
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


def transfrom_rcs(source, starttime=None, endtime=None, config=None, debug=False):
    """
    DESCRIPTION
        creates a 1-min data set, move columns.
        Tested maintainance flagging
    """
    # Problem with RCS data: data is not equidistant, contains numerous gaps
    # if get gaps is done, then cumulative rain is wrongly determined because of gaps
    if not config:
        config = {}
    t1 = datetime.now()
    #rcst7 = read(os.path.join(basepath, "RCST7*"))
    maan = marana.MartasAnalysis(config=config.get('anaconf',{}))
    rcst7 = maan.get_marcos_data(source, starttime=starttime, endtime=endtime, debug=debug)
    if debug:
        print ("Headers: ", rcst7._get_key_names())
        print ("Samplingrate {} sec and {} data points".format(rcst7.samplingrate(), len(rcst7)))
        print ("Range:", rcst7.timerange())

    pa = None
    fl = flagging.Flags()
    #print ("GET THIS INTO FLAGGING")
    # TEST binary flagging
    #rcst7 = rcst7.offset({'z' : 1}, starttime="2025-10-12T12:00:00", endtime="2025-10-12T12:30:00")
    #fl = flagging.flag_binary(rcst7, 'z', flagtype=3, labelid='070', keystoflag=['x','t1'], sensorid=rcst7.header.get("SensorID"),
    #                      text="Maintainance switch activated", markallon=True)
    #fl = fl.union(level=1)
    #print (fl)
    # TEST JC spike flagging (water data set)
    if len(rcst7) > 0 and (rcst7.end()-rcst7.start()).total_seconds() < 86400*12: # limit to 12 days
        medianjc, stdvar = rcst7.mean('x', meanfunction='median', std=True)
        fl = flagging.flag_range(rcst7, keys=['x'], above=medianjc+2*stdvar+30.) # typical range is 2 hours, flag data eceeding the median by 30 cm
        print ("Got {} outliers".format(len(fl)))
        pa = fl.create_patch()

    # now determine the gaps and interpolate the rain accumulation data (for testing of accumulation)
    rcst7 = rcst7.get_gaps()
    #rcst7 = rcst7.interpolate_nans(keys=['t1'])
    # then apply the flags
    if fl:
        rcst7 = fl.apply_flags(rcst7)
        # save flags?

    # calculate cumulative rain before filtering
    col = rcst7.steadyrise('t1', timedelta(minutes=60),sensitivitylevel=0.002)

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
    return rcst7m, fl


def transfrom_meteo(source, starttime=None, endtime=None, config=None, debug=False):
    if not config:
        config = {}
    fl = flagging.Flags()
    t1 = datetime.now()
    #meteo = read(os.path.join(basepath, "METEO*"))
    maan = marana.MartasAnalysis(config=config.get('anaconf',{}))
    meteo = maan.get_marcos_data(source, starttime=starttime, endtime=endtime, debug=debug)
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
    if len(meteom) > 0 and (meteom.end()-meteom.start()).total_seconds() < 86400*12: # limit to 12 days
        medianjc = meteom.mean('z', meanfunction='median')
        fl = flagging.flag_range(meteom, keys=['z'], above=medianjc+30.) # typical range is 2 hours, flag data eceeding the median by 30 cm
        print ("Got {} outliers".format(len(fl)))
        pa = fl.create_patch()
        if len(fl) > 0:
            meteom = fl.apply_flags(meteom)

    t2 = datetime.now()
    if debug:
        print ("Headers: ", meteo._get_key_names())
        print ("Samplingrate {} sec and {} data points".format(meteo.samplingrate(), len(meteo)))
        print ("Range:", meteo.timerange())
        print (" transform needed {} sec".format((t2-t1).total_seconds()))
    return meteom, fl


def main(argv):
    configpath = ''
    logpath = ''
    debug=False
    endtime = None
    starttime = None
    receiver = None
    receiverconf = None
    notify = True # will be set to False if starttime and endtime is selected.
    proxies = {}
    statusmsg = {}

    try:
        opts, args = getopt.getopt(argv,"hc:e:s:D",["config=","endtime=","starttime=","debug=",])
    except getopt.GetoptError:
        print ('weather.py -c <config> -s <starttime> -e <endtime>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- weather.py will determine the primary instruments --')
            print ('-----------------------------------------------------------------')
            print ('Combining weather data from various different sensors into a single adjusted product.')
            print ('This method uses the following SensorIDs from the Conrad Observatory. Adiitional sensors')
            print ('require a modification of the code and addition of transform functions.')
            print ('- ULTRASONICDSP* : Thiess Ultrasonic-DSP-Anemometer (windspeed, winddirection, T)')
            print ('- BM35* : Meteolab BM035 (pressure, T)')
            print ('- LNM* : Thiess Disdrometer (Rain, T, Synop, Rainanalysis)')
            print ('- RCST7* : remote-control-system, COBS specific, (T, rh, rain nbucket, snowheight)')
            print ('- METEOT7* : (RCS analysis by Andreas Winkelbauer)')
            print ('Depending on availability the job identifies existing data in a MARCOS database, and, if not found,')
            print ('afterwards in a MARCOS archive structure. Paths and offsets need to be defined in a configuration')
            print ('file "weather.cfg".')
            print ('weather.cfg will hold a link to the general analysis configuration with MARCOS structure and ')
            print ('notification.')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python weather.py -c <config> -s <starttime> -e <endtime>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-s            : starttime - default is endtime - 2 days')
            print ('-e            : endtime - default is now')
            print ('-------------------------------------')
            print ('Application:')
            print ('python weather.py -c ~/.analysis/conf/analysis.cfg')
            print ('python weather.py -c ~/CONF/weather.cfg -s 2020-11-11 -e 2020-11-22')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-s", "--starttime"):
            # get an endtime
            starttime = arg
        elif opt in ("-e", "--endtime"):
            # get an endtime
            endtime = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    t1 = datetime.now()
    # get configuration data
    destinations = {}
    if not configpath:
        configpath = "../conf/weather.cfg"
    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check weather.py -h for more options and requirements')
        sys.exit()
    wconf = mm.get_conf(configpath)
    daystodeal = int(wconf.get("daystodeal",2))
    if wconf:
        # get notification data frm the general analysis-configuration
        logpath = wconf.get('logpath','') # logpath is used in most configfiles
        receiver = wconf.get('notification')
        receiverconf = wconf.get('notificationconf')
        if wconf.get('https'):
            proxies['https'] = wconf.get('https')
        if wconf.get('http'):
            proxies['http'] = wconf.get('http')
    if debug:
        print ("Weather config:", wconf)

    # if analysis is activated then the underlying analysis config is initialized
    maan = marana.MartasAnalysis(config=wconf)
    if debug:
        print("General analysis config:", maan.config)

    connectdict = maan.config.get('conncetedDB')
    for dbel in connectdict:
        destinations[dbel] = {"name": "METEOSGO_adjusted_0001_0001"}
    # destinations['cobsdb'] = {"name" : "METEOSGO_adjusted_0001_0001"}

    # get special weather configuration data
    if wconf.get('meteoproducts'):
        destinations[wconf.get('meteoproducts')] = {"name": wconf.get('meteofilename'), "dateformat": "%Y%m",
                                                    "coverage": "month", "mode": "replace", "format_type": "PYCDF"}
    if endtime:
        notify = False
        try:
            endtime = mama.testtime(endtime)
        except:
            print (" Could not interpret provided endtime. Please Check !")
            sys.exit(1)
    elif not starttime:
         endtime = datetime.now(timezone.utc).replace(tzinfo=None)
    if starttime:
        notify = False
        try:
            starttime = mama.testtime(starttime)
        except:
            print (" Could not interpret provided starttime. Please Check !")
            sys.exit(1)
        if not endtime:
            endtime = starttime+timedelta(days=daystodeal)
    else:
         starttime = endtime-timedelta(days=daystodeal)

    if debug:
        print ("Start, End", starttime, endtime)
        print ("Destinations:", destinations)
    statusdict = {"WIND" : "OK", "PRESSURE" : "OK", "SYNOP" : "OK", "RCST7" : "OK", "METEOT7" : "OK", "RESULT" : "OK"}
    # try and except not necessary: if the job fails completely then no METEO_adjusted is created, which is observed
    # by DB monitoring

    ultram = transfrom_ultra("ULTRASONICDSP*",starttime=starttime, endtime=endtime,offsets=wconf,config=wconf,debug=debug)
    if not len(ultram) > 0:
        print ("weather: no wind data available")
        statusdict["WIND"] = "FAILED"
    bm35m = transfrom_pressure("BM35*",starttime=starttime, endtime=endtime,config=wconf,debug=debug)
    if not len(bm35m) > 0:
        print ("weather: no pressure data available")
        statusdict["PRESSURE"] = "FAILED"
    lnmm = transfrom_lnm("LNM_0351_0001_0001*",starttime=starttime, endtime=endtime,config=wconf,debug=debug)
    if not len(lnmm) > 0:
        print ("weather: no disdrometer data available")
        statusdict["SYNOP"] = "FAILED"
    rcst7m, fl1 = transfrom_rcs("RCST7*",starttime=starttime, endtime=endtime,config=wconf,debug=debug)
    if not len(rcst7m) > 0:
        print ("weather: no rcs weather data available")
        statusdict["RCST7"] = "FAILED"
    meteom, fl2 = transfrom_meteo("METEOT7*",starttime=starttime, endtime=endtime,config=wconf,debug=debug)
    if not len(meteom) > 0:
        print ("weather: no rcs meteo data available")
        statusdict["METEOT7"] = "FAILED"

    # return flags and store them as well !!
    if len(fl1) > 0 and len(fl2) > 0:
        fl = fl1.join(fl2)
    elif len(fl1) > 0:
        fl = fl1
    else:
        fl = fl2
    #maan.update_flags_db(fl, debug=False)
    result = combine_weather(ultram=ultram, bm35m=bm35m, lnmm=lnmm, rcst7m=rcst7m, meteom=meteom)
    if not len(result) > 0:
        print ("weather: no data at all")
        statusdict["RESULT"] = "FAILED"
    elif debug:
        print ("weather:  got {} data points in a combined meteorological data set of 1 min resolution".format(len(result)))
    else:
        pass
        # export the adjusted data set
        #success = put_data(result, destinations=destinations, debug=True)

    statusmsg["weather"] = json.dumps(statusdict)
    # create a statusmessage with len(result) > 0 and contents (ultra=True etc)
    if debug:
        print(statusmsg)
    elif receiver and receiverconf and notify:
        # will only run if automatic scheduled analysis without give start- or endtime is performed
        martaslog = ml(logfile=logpath, receiver=receiver, proxies=proxies)
        if receiver == 'email':
            martaslog.email['config'] = receiverconf
        elif receiver == 'telegram':
            martaslog.telegram['config'] = receiverconf
    t2 = datetime.now()
    if debug:
        print ("weather.py : the job needed {} seconds".format((t2-t1).total_seconds()))

if __name__ == "__main__":
   main(sys.argv[1:])

