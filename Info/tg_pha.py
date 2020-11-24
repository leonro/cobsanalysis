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


from martas import martaslog as ml
logpath = '/var/log/magpy/mm-info.log'
statusmsg = {}
phamem = '/home/cobs/ANALYSIS/Logs/tg_pha.json'
#phamem = '/tmp/tg_pha.json'  # tmp will be deletet if Pc is restarted

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


# Size estimation:
def getsize(h):
    # https://www.minorplanetcenter.net/iau/lists/Sizes.html
    sizearray = [[1050,2400,17.0],[840,1900,17.5],[670,1500,18.0],[530,1200,18.5],[420,940,19.0],[330,740,19.5],[260,590,20.0],[210,470,20.5],[170,370,21.0],    [130,300,21.5],[110,240,22.0],[85,190,22.5],[65,150,23.0],[50,120,23.5],[40,95,24.0],[35,75,24.5],[25,60,25.0],[20,50,25.5],[17,37,26.0],[13,30,26.5],[11,24,27.0],[8,19,27.5],[7,15,28.0],[5,12,28.5],[4,9,29.0],[3,7,29.5],[3,6,30.0],[2,5,30.5],[2,4,31.0],[1,3,31.5],[1,2,32.0],[1,2,32.5]]

    ma = np.asarray([el[1] for el in sizearray])
    mi = np.asarray([el[0] for el in sizearray])
    href = np.asarray([el[2] for el in sizearray])
    mif = interp1d(href, mi)
    maf = interp1d(href, ma)

    minval = np.round(float(mif(h)))
    maxval = np.round(float(maf(h)))

    sizerange = "{:.0f}-{:.0f} m".format(minval,maxval)
    return sizerange

try:
    datemax = datetime.strftime((datetime.now()+timedelta(days=730)),"%Y-%m-%d")
    #datemax = "2018-08-31"
    print ("Checking data until:", datemax)
    url = "https://ssd-api.jpl.nasa.gov/cad.api?dist-max=1LD&date-min=1900-01-01&date-max={}&sort=date".format(datemax)
    proxies = {'https': 'http://138.22.173.44:3128'}
    response = urllib.urlopen(url,proxies=proxies)
    data = json.loads(response.read())
    field = data.get(u'fields')
    alldata = data.get(u'data')

    print ("Database containing {} datasets".format(len(alldata)))
    statusmsg['Telegram-PHA'] = 'downloading new PHAs successful'
except:
    statusmsg['Telegram-PHA'] = 'downloading new PHAs failed' 


# Check existig data on disk
try:
    with open(phamem) as json_file:  
         existdata = json.load(json_file)
    existalldata = existdata['data']
    #print (existdata['fields'].index('des'))
    #print (field.index('des'))
    existname = [elem[field.index('des')] for elem in existalldata]
    print ("Existing data loaded ...")
    #print ("Names: {}".format(existname))
except:
    existdata = data
    existalldata = []
    existname = []

exceptlist = ["2020 SO"]
# 2020 SO is not unique, two times the same name...

print ("Checking for new objects ...")
if len(alldata) > 0:
    for elem in alldata:
        aname = elem[field.index('des')]
        # add a mail repetion two weeks before
        if not elem in existalldata and not elem[0] in exceptlist:
            print ("Found new data set")
            # Add new data to list
            existalldata.append(elem)
            # Creating new message if approach date is in the future:
            approachdate = datetime.strptime(elem[field.index('cd')],"%Y-%b-%d %H:%M") #TDB
            print ("Got approachdate", approachdate)
            if approachdate >= datetime.utcnow()-timedelta(days=2):  # ~30 secs diff between UTC and TDB
                msgd = "*Erdnahes Objekt (z.B. Asteroid)*\n"
                # Create message
                obs = 'NEW'
                obsd = 'Neues'
                if aname in existname:
                    obs = 'UPDATE to'
                    obsd = 'Update zu'
                # Add update flag if 'des' is already existing 
                msg = "{} [NEO](https://cneos.jpl.nasa.gov/ca/): *{}*\n".format(obs, elem[field.index('des')])
                msgd += "\n{} [NEO](https://cneos.jpl.nasa.gov/ca/): *{}*\n".format(obsd, elem[field.index('des')])
                msg += "(NEO = Near Earth object)\n"
                msg += "Approaching earth on\n*{} UTC*\n".format(elem[field.index('cd')])
                if approachdate >= datetime.utcnow():
                    msgd += "Nähert "
                else:
                    msgd += "Näherte "
                msgd += "sich der Erde am\n*{} UTC*\n".format(elem[field.index('cd')])
                distance = elem[field.index('dist')]
                distancekm = float(distance)*149598073.
                msg += "within {:.6f} AU ({:d} km)\n".format(float(distance), int(distancekm))
                msgd += "in einem Abstand von von {:.6f} AU ({:d} km)\n".format(float(distance), int(distancekm))
                amplitude = elem[field.index('h')]
                if float(amplitude) < 32.5 and float(amplitude) > 17.0:
                    msg += "Size: {}\n".format(getsize(float(amplitude)))
                    msgd += "Durchmesser: {}\n".format(getsize(float(amplitude)))
                msg += 'Based on [Jet Propulsion Labs SBDB](https://ssd-api.jpl.nasa.gov/) (Small-Body DataBase)'
                msgd += '_Daten des [Jet Propulsion Labs SBDB](https://ssd-api.jpl.nasa.gov/) (Small-Body DataBase)_'
                msg += ""
                print (msg)
                telegram_send.send(messages=[msg],conf="/home/cobs/ANALYSIS/Info/conf/tg_space.cfg",parse_mode="markdown")
                telegram_send.send(messages=[msgd],conf="/home/cobs/ANALYSIS/Info/conf/tg_weltraum.cfg",parse_mode="markdown")


with open(phamem, 'wb') as outfile:
    json.dump(data, outfile,encoding="utf-8")


print ("Successfully finished")
#data = read("https://www.minorplanetcenter.net/iau/MPCORB/PHA.txt")
#data = read("/home/leon/CronScripts/MagPyAnalysis/Asteroids/pha.txt")
#data = read("https://minorplanetcenter.net/Extended_Files/pha_extended.json.gz", dataformat='PHA')
#data = read("/home/leon/CronScripts/MagPyAnalysis/Asteroids/pha_extended.json.gz", format_type='PHA')

print (statusmsg)
martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)
