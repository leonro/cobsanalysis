#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MagPy - Basic Runtime tests including durations  
"""
from __future__ import print_function

import telegram_send
from magpy.stream import *   
from magpy.database import *   
import magpy.opt.cred as mpcred

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

try: 
    from magpy.opt.analysismonitor import *
    analysisdict = Analysismonitor(logfile='/home/cobs/ANALYSIS/Logs/AnalysisMonitor_cobs.log')
    analysisdict = analysisdict.load()
except:
    print ("Analysis monitor failed")
    pass

uploadname = "upload_telegram_quakes"
ind = 0
#try:

print ("Get a list with all recent earthquakes")
lastquakes = dbselect(db,'time,x,y,z,f,var5,str2,str3,str4,str1','QUAKES',expert="ORDER BY time DESC LIMIT 200")
print ("Now select all relevant quakes")
print ("------------------------------")
print ("Please note:")
print ("Earthquakes in Austria (are taken from the Austrian Seismological Service")
print ("Earthquakes outside this region and magnetitude above 5.0 are taken from NEIC") 

#print (lastquakes)
#sys.exit()
# var5 is the distance from the Conrad Observatory
relevantquakes = []
for quake in lastquakes:
    # eventually only select manual determinations (quake[6] != ?'auto'?)
    if float(quake[5]) < 50:
        relevantquakes.append(quake)
    elif float(quake[1]) < 49.03 and float(quake[1]) > 46.35 and float(quake[2]) < 17.18 and float(quake[2]) > 9.52 and float(quake[4])>2.99:
        # "rectangular" Austrian region
        relevantquakes.append(quake)
    elif float(quake[5]) < 500 and float(quake[4])>4.9 and quake[-1].startswith('us'):
        relevantquakes.append(quake)
    elif float(quake[5]) < 1500 and float(quake[4])>5.9 and quake[-1].startswith('us'):
        relevantquakes.append(quake)
    elif float(quake[4])>6.99 and quake[-1].startswith('us'):
        relevantquakes.append(quake)

# continue only if list length > 0 
if len(relevantquakes) > 0:
    print ("Now get last record from temporary folder")
    try:
        lq = np.load('/tmp/lastquake.npy')
        ind, tmp = np.where(np.asarray(relevantquakes)==lq[0])
        ind = ind[0]
    except:
        ind = 1

if ind > 0:
    print ("Found new earthquakes")
    relevantquakes = relevantquakes[:ind]
    for quake in relevantquakes:
        print ("Creating message:")
        quakemsg = "{} at *{}* UTC\n".format(quake[0].split()[0],quake[0].split()[1])
        quakemsg += "{} with magnitude *{}*\n{}\n".format(quake[8],quake[4],quake[7].replace("REGION",""))
        quakemsg += "Latitude: {}°\n".format(quake[1])
        quakemsg += "Longitude: {}°\n".format(quake[2])
        quakemsg += "Depth: {}km".format(quake[3])
        print (quakemsg)
        zoom=9
        maplink = "[Show map](https://maps.google.com/maps/?q={},{}&ll={},{}&z={})\n".format(quake[1],quake[2],quake[1],quake[2],zoom)
        mapling = "Further info: [ZAMG/ConradObs](https://www.zamg.ac.at/cms/de/geophysik/erdbeben/aktuelle-erdbeben/karten-und-listen/)"
        print (maplink)
        print ("----------------------------\n")
        # Sending the data to the bot
        print ("Sending message to Telegram")
        telegram_send.send(messages=[quakemsg+"\n"+maplink],conf="/home/cobs/ANALYSIS/Info/conf/tg_quake.cfg",parse_mode="markdown")
        np.save('/tmp/lastquake', relevantquakes[0])

    # updating current data
    try:
      if len(relevantquakes)>0:
        # get strongest, manual detected quake
        mag = np.asarray([quake[4] for quake in relevantquakes if not quake[6].startswith('auto')])
        ind = np.argmax(mag)
        quake = relevantquakes[ind]
        # Creating Current data input
        if quake and len(quake)>0:
            if os.path.isfile(currentvaluepath):
                # read log if exists and exentually update changed information
                # return changes
                with open(currentvaluepath, 'r') as file:
                    fulldict = json.load(file)
                    valdict = fulldict.get('seismology')
            else:
                valdict = {}
                fulldict = {}
            valdict[u'date'] = [quake[0],'']
            valdict[u'magnitude'] = [quake[4],'']
            valdict[u'location'] =  [quake[7].replace("REGION",""),'']
            valdict[u'latitdue'] = [quake[1],'deg']
            valdict[u'longitude'] = [quake[2],'deg']
            valdict[u'depth'] = [quake[3],'m']
            fulldict[u'seismology'] = valdict
            with open(currentvaluepath, 'w',encoding="utf-8") as file:
                file.write(unicode(json.dumps(fulldict))) # use `json.loads` to do the reverse
                print ("Current quake data written successfully to {}".format(currentvaluepath))
    except:
      print ("Error occured when writing current data")
else:
    print ("No new quakes found - thats good")

#print (analysisdict)
#analysisdict.check({": ['success','=','success']})

#except:
#print ("Programm abortet") # add monitor
#analysisdict.check({uploadname: ['failure','=','success']})
