#!/usr/bin/env python3
# coding=utf-8

"""
DESCRIPTION
    Application to extract status parameters from the observatory database

REQUIREMENTS
     pip/conda install requests
     pip/conda install chardet

TESTING

   1. delete one of the json inputs from the memory file in /srv/archive/external/esa-nasa/cme
   2. run app without file option

   python3 status_extractor.py -c /home/cobs/CONF/wic.cfg -s testcfg -D

   # config directory looks like
   sourcedict = {"CO2 GMO tunnel": {"source":"MQ135_20220214_0001_0001", "key":"var1", "type":"gas", "group":"tunnel condition", "field":"gmo", "pierid":"", "range":30,"mode":"max","value_unit":"ppm","warning_high":2000,"critical_high":5000}, "Temperature GMO tunnel": {"source":"BE280_0X76I2C00003_0001_0001", "key":"t1", "type":"environment", "group":"tunnel condition", "field":"gmo", "pierid":"", "range":30,"mode":"mean","value_unit":"C","warning_high":10,"critical_high":20}, "Humidity GMO tunnel": {"source":"BE280_0X76I2C00003_0001_0001", "key":"var1", "type":"environment", "group":"tunnel condition", "field":"gmo", "pierid":"", "range":30,"mode":"mean","value_unit":"%"}, "Pressure GMO tunnel": {"source":"BE280_0X76I2C00003_0001_0001", "key":"var2", "type":"bme280", "group":"tunnel condition", "field":"", "pierid":"", "range":30,"mode":"mean","value_unit":"hP"}, "Vehicle alarm": {"source":"RCS2F2_20160114_0001_0001", "key":"x", "type":"environment", "group":"traffic", "field":"gmo", "pierid":"", "range":120,"mode":"max","warning_high":1}, "Meteo temperature": {"source":"METEOSGO_adjusted_0001_0001", "key":"f", "type":"meteorology", "group":"meteorology", "field":"sgo", "pierid":"", "range":30,"mode":"median","warning_high":30,"warning_high":40}, "Meteo humidity": {"source":"METEOSGO_adjusted_0001_0001", "key":"t1", "type":"meteorology", "group":"meteorology", "field":"sgo", "pierid":"", "range":30,"mode":"median","warning_high":30,"warning_high":40}}

   write_config(sourcepath, sourcedict)


{
    "CO2 GMO tunnel": {
        "source": "MQ135_20220214_0001_0001",
        "key": "var1",
        "type": "gas",
        "group": "tunnel condition",
        "field": "environment",
        "location": "gmo",
        "pierid": "",
        "range": 30,
        "mode": "max",
        "value_unit": "ppm",
        "warning_high": 2000,
        "critical_high": 5000
    },
    "Temperature GMO tunnel": {
        "source": "BE280_0X76I2C00003_0001_0001",
        "key": "t1",
        "type": "temperature",
        "group": "tunnel condition",
        "field": "environment",
        "location": "gmo",
        "pierid": "",
        "range": 30,
        "mode": "mean",
        "value_unit": "°C",
        "warning_high": 10,
        "critical_high": 20
    },
    "Humidity GMO tunnel": {
        "source": "BE280_0X76I2C00003_0001_0001",
        "key": "var1",
    ...
"""


from magpy.stream import *
from magpy.core import flagging
from magpy.core import methods as mama
# MARTAS - requires >= 2.0b9
from martas.core.methods import martaslog as ml
from martas.core import methods as mm
from martas.core import analysis

"""
How to deal with statusmessages - groups, typs, fields,

type: temperature, magnetic F, magnetic X, magnetic Y, magnetic Z, magnetic D, voltage, ...
field: environment, electronics, meteorology, magnetism, gravity, ...
group: building control, tunnel condition, system monitoring, earth observation
location

Combinations:
Bz         : magnetic Z, satellite L1, spaceweather
Kp         : magnetic activity, magnetism, spaceweather
CO2        : CO2 concentration, environment, tunnel condition
Fire-Water : water level, building, building control
PingCobs   : ping response, building, system monitoring
LEMI036 t1 : temperature, environment, tunnel condition
LEMI036_1 t2 : temperature, electronics, system monitoring
LEMI036_3 t2 : temperature, electronics, system monitoring
RCS 430_rh : rel.humidity, meteorology, earth observation
WIC_adjusted D : declination, magnetism, earth observation
HeliumLevel : helium level, gravity, system monitoring
barrier     : vehicle passed recently, environment, building control
...

* add notification groups for warning and critical (notification groups are defined by a list of telegram or email configs)
* location is not used  (ACE-L1, GMO, Kyoto, GFZ, SGI, Gams, GOES17, ...) 
* lat/long -> define the projected EPSG code - WGS84 plus altitude
* Notation convention: NameofComponent-SensorName-PierID-hash(5digits - from DataID and StationID))
* add a display name 

{
    "TEMP": {
        "source": "TEST001_1234_0001_0001",
        "key": "x",
        "type": "temperature",
        "group": "tunnel condition",
        "field": "environment",
        "location": "gmo",
        "pierid": "",
        "range": 30,
        "mode": "mean",
        "value_unit": "°C",
        "warning_high": 10,
        "critical_high": 20
    },
    "Average field X": {
        "source": "TEST001_1234_0001_0001",
        "key": "x",
        "type": "temperature",
        "group": "tunnel condition",
        "field": "environment",
        "location": "gmo",
        "pierid": "",
        "range": 30,
        "mode": "mean",
        "value_unit": "°C",
        "warning_high": 10,
        "critical_high": 20
    }
}

# Write a small method to create status inputs
"""

def import_old_status(oldstatusdict=None):
    pass

def create_status_stdinput(statusdict=None):
    """
    DESCRIPTION
        method to create status requests from std inputs and add them to statusdict
    """
    if not statusdict:
        statusdict = {}


def create_status_form(statusdict=None):
    """
    DESCRIPTION
        method to create status requests using an input form
    """
    if not statusdict:
        statusdict = {}


statuspath = "../conf/status.json"
statusdict = {}

if os.path.isfile(statuspath):
    statusdict = mm.get_json("../conf/status.json")

ms = analysis.MartasStatus(statusdict=statusdict, tablename='COBSSTATUS')
initsql = ms.statustableinit(debug=True)
sqllist = []
for elem in ms.statusdict:
    statuselem = ms.statusdict.get(elem)
    res = ms.read_data(statuselem=statuselem, debug=True)
    warnmsg = ms.check_highs(res.get('value'), statuselem=statuselem)
    newsql = ms.create_sql(elem, res, statuselem)
    print(warnmsg)
    sqllist.append(newsql)
# initsql = ms.statustableinit(debug=True)
sql = "; ".join(sqllist)

md = ms.db
cursor = ms.db.db.cursor()
message = md._executesql(cursor, initsql)
for el in sqllist:
    message = md._executesql(cursor, el)
md.db.commit()
