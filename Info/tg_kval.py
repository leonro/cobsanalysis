#!/usr/bin/env python
# coding=utf-8

"""
MagPy - Basic Runtime tests including durations  
"""
from __future__ import print_function
from __future__ import unicode_literals

# Define packges to be used (local refers to test environment) 
# ------------------------------------------------------------

from magpy.stream import *   
from magpy.database import *   
import magpy.opt.cred as mpcred
from scipy.interpolate import interp1d
import telegram_send
# Get new data from API
import urllib, json
from dateutil import tz

# Connect to test database 
# ------------------------------------------------------------
dbpasswd = mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print("Connecting to primary MARCOS...")
    db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")
    print("success")
except:
    print("... failed")
    try:
        print("Connecting to secondary MARCOS...")
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
        print("success")
    except:
        sys.exit()


newmsg = False
update = False
currentvaluepath = '/srv/products/data/current.data'

print ("Reading k values from database")

warnstart = "Possible consequences are"
warningmsg = {"5":"an aurora at high latitudes.", 
              "6":"an aurora at high latitudes, weak degradation of radio communication.",
              "7":"an aurora at high latitudes, disturbances in radio communication, and weak fluctuations in power grids.",
              "8":"an aurora even visible in mid latitudes, disturbances in radio communication, disruptions in navigation signals, and fluctuations in power grid.",
              "9":"an aurora visible at mid latitudes, significant disturbances in radio communication, outages in navigation signals, and strong power grid fluctuations."}

warnstartd = "Mögliche Auswirkungen:"
warningmsgd = {"5":"eine Aurora in hohen Breiten.", 
              "6":"eine Aurora in hohen Breiten, mögliche Störungen bei Radio-Kommunikation.",
              "7":"eine Aurora in hohen Breiten, Beeiträchtigungen der Radio-Kommunikation und schwache Fluktuationen im Stromnetz.",
              "8":"eine Aurora teilweise auch sichtbar in mittleren Breiten, Beeiträchtigungen der Radio-Kommunikation, Störungen bei Navigationssignalen und Fluktuationen im Stromnetz.",
              "9":"eine Aurora sichtbar in mittleren Breiten, starke Beeiträchtigungen der Radio-Kommunikation, Ausfälle bei Navigationssystemen, starke Fluktuationen im Stromnetz."}

data = readDB(db,'WIC_k_0001_0001', starttime=datetime.utcnow()-timedelta(hours=11))

print (data.ndarray)

if data.length()[0]> 0:
    # Check last input
    lastvalue = data.ndarray[7][-1]
    valid = num2date(data.ndarray[0][-1]).replace(tzinfo=None)+timedelta(minutes=90)
    print ("Current k-value: {}".format(lastvalue))
    print ("Valid until: {}".format(valid)) 
    from_zone = tz.tzutc()
    to_zone = tz.gettz('CET')
    validnew = valid.replace(tzinfo=from_zone)
    validcentral = validnew.astimezone(to_zone)

    # check whether a valid warning is existing for this k value
    if os.path.isfile(currentvaluepath):
            with open(currentvaluepath, 'r') as file:
                fulldict = json.load(file)
                valdict = fulldict.get('magnetism')
    else:
            valdict = {}
            fulldict = {}

    # TESTING: lastvalue = lastvalue+5
    if lastvalue >=5:
        print ("Found large k")
        # check whether a valid warning is existing for this k value
        if os.path.isfile(currentvaluepath):
            with open(currentvaluepath, 'r') as file:
                fulldict = json.load(file)
                valdict = fulldict.get('magnetism')
        else:
            valdict = {}
            fulldict = {}

        try:
            existingdate = datetime.strptime(valdict.get('k-valid',['1900-01-01 12:00',''])[0],"%Y-%m-%d %H:%M")
        except:
            existingdate = datetime(1900,1,1,12)
        existinglevel = float(valdict.get('k-warning',[0.,''])[0])
        print ("Existing:", existingdate, existinglevel)
        if valid > existingdate and int(lastvalue) != int(existinglevel):
            newmsg = True
            valdict[u'k-warning'] = [lastvalue,'']
            valdict[u'k-valid'] = [datetime.strftime(valid,"%Y-%m-%d %H:%M"),'']
            print ("Here")
        elif int(lastvalue) != int(existinglevel):
            newmsg = True
            valdict[u'k-warning'] = [lastvalue,'']
        elif valid > existingdate:
            update = True
            valdict[u'k-valid'] = [datetime.strftime(valid,"%Y-%m-%d %H:%M"),'']
            print ("DATE", valid,existingdate)
        else:
            newmsg = False
            update = False
        
        if newmsg or update:
                print ("Updating current value")
                # Add update flag if 'des' is already existing
                if newmsg:
                    msg = "*High geomagnetic activity*\n\n"
                    msgd = "*Hohe geomagnetische Aktivität*\n\n"
                else:
                    msg = "*Update on activity*\n"
                    msgd = "*Aktivitätsupdate*\n"
                msg += "Geomagnetic activity index _k_ of *{}* expected.\n".format(int(lastvalue))
                msg += "Warning is valid until {} UTC.\n".format(datetime.strftime(valid,"%Y-%m-%d %H:%M"))
                msgd += "Geomagnetischer Aktivitätsindex _k_=*{}* erwartet.\n".format(int(lastvalue))
                msgd += "Warnung ist gültig bis {} CET.\n".format(datetime.strftime(validcentral,"%Y-%m-%d %H:%M"))
                if newmsg:
                    msg += "\n{} {}\n".format(warnstart,warningmsg.get(str(int(lastvalue))))
                    msg += 'Based on [Conrad Observatory](http://www.conrad-observatory.at/) data'
                    msgd += "\n{} {}\n".format(warnstartd,warningmsgd.get(str(int(lastvalue))))
                    msgd += 'Basierend auf Daten des [Conrad Observatoriums](http://www.conrad-observatory.at/)'
                msg += ""
                print (msg)

                # TEST -> send to tg_base.cfg , move to tg_space.cfg if working
                telegram_send.send(messages=[msg],conf="/home/cobs/ANALYSIS/Info/conf/tg_space.cfg",parse_mode="markdown")
                telegram_send.send(messages=[msgd],conf="/home/cobs/ANALYSIS/Info/conf/tg_weltraum.cfg",parse_mode="markdown")
                #telegram_send.send(messages=[msg],conf="/home/cobs/ANALYSIS/Info/conf/tg_test.cfg",parse_mode="markdown")

    else:
        valdict[u'k-warning'] = [0,'']
        valdict[u'k-valid'] = ['','']


    print ("Updating json")
    fulldict[u'magnetism'] = valdict
    with open(currentvaluepath, 'w',encoding="utf-8") as file:
        file.write(unicode(json.dumps(fulldict))) 
    print ("K warning data has been updated")


print ("Successfully finished")
#data = read("https://www.minorplanetcenter.net/iau/MPCORB/PHA.txt")
#data = read("/home/leon/CronScripts/MagPyAnalysis/Asteroids/pha.txt")
#data = read("https://minorplanetcenter.net/Extended_Files/pha_extended.json.gz", dataformat='PHA')
#data = read("/home/leon/CronScripts/MagPyAnalysis/Asteroids/pha_extended.json.gz", format_type='PHA')

