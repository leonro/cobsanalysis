#!/usr/bin/env python
# coding=utf-8

"""
MagPy - Weekly baseline/value information
"""
from __future__ import print_function
from __future__ import unicode_literals

# Define packges to be used (local refers to test environment)
# ------------------------------------------------------------
from magpy.stream import *
from magpy.database import *
import magpy.mpplot as mp
import magpy.opt.cred as mpcred

from pickle import load as pload
import telegram_send
from os import listdir
from os.path import isfile, join

from shutil import copyfile
import itertools
import getopt
import pwd
import socket
import sys  # for sys.version_info()


scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, GetPrimaryInstruments, getstringdate, combinelists
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf


def load_current_data_sub(currentvaluepath, group):
    """
    Loads a speciffic group of  current data json
    """
    valdict = {}
    fulldict = {}
    if os.path.isfile(currentvaluepath):
        # read log if exists and exentually update changed information
        # return changes
        with open(currentvaluepath, 'r') as file:
            fulldict = json.load(file)
            valdict = fulldict.get(group)
    if not valdict:
        valdict = {}
    if not fulldict:
        fulldict = {}

    return fulldict, valdict


def get_quakes(db, limit=200, debug=False):
    print ("Get a list with all recent earthquakes")
    lastquakes = dbselect(db,'time,x,y,z,f,var5,str2,str3,str4,str1','QUAKES',expert="ORDER BY time DESC LIMIT {}".format(limit))
    if debug:
        print (" GetQuakes: Obtained {} records ({} is limit)".format(len(lastquakes),limit))
        if lastquakes and len(lastquakes) > 0:
            print (" -> last one from {}".format(lastquakes[0][0]))
    return lastquakes

def select_relevant_quakes(lastquakes, criteria={}, debug=False):
    print ("Now select all relevant quakes")
    print ("------------------------------")
    print ("Please note:")
    print ("Earthquakes in Austria (are taken from the Austrian Seismological Service")
    print ("Earthquakes outside this region and magnetitude above 5.0 are taken from NEIC")

    relevantquakes = []
    """
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

    return relevantquakes
    """


    criteria = {'cond1' :  {'radius':50},
                'cond2' : {'magn':2.99, 'lat':[46.35,49.03], 'long':[9.52,17.18]},
                'cond3' : {'magn':4.9, 'radius':500},
                'cond4' : {'magn':5.9, 'radius':1500},
                'cond5' : {'magn':6.99}}

    def _test_criteria(quake, criteria, debug=False):
        rad = float(quake[5])
        lati = float(quake[1])
        longi = float(quake[2])
        magn = float(quake[4])
        typus = quake[6]
        criteriamet = False
        conditionssum = []
        for cond in criteria:
            condmetlist = []
            if debug:
                print ("Testing condition", cond)
                print (criteria.get(cond))
            condition = criteria.get(cond)
            #print (" -> amount of individual criteria: {}".format(len(condition)))
            crad = condition.get('radius',1)
            #print ("Test",rad,crad)
            if rad < crad:
                condmetlist.append(cond)
            clati = condition.get('lat',[1000,1200])
            if lati >= clati[0] and lati <= clati[1]:
                condmetlist.append(cond)
            clongi = condition.get('lon',[998,999])
            if longi >= clongi[0] and longi <= clongi[1]:
                condmetlist.append(cond)
            #print (" condmetlist looks like", condmetlist)
            cmagn = condition.get('magn',999)
            #print ("Test magn",magn,cmagn)
            if magn > cmagn:
                condmetlist.append(cond)
            #print (" condmetlist looks like", condmetlist)
            ctypus = condition.get('type','none')
            if typus.find(ctypus) >= 0 or ctypus == 'all':
                condmetlist.append(cond)
            if debug:
                print (" condmetlist looks like", condmetlist)
            if len(condmetlist) == len(condition):
                if debug:
                    print (" -> all requested conditions for {} met".format(cond))
                conditionssum.append(cond)
        conditionssum = list(set(conditionssum))
        if debug:
            print ( " Testing quake: {}".format(quake))
            print ( "  -> Conditions met: {}".format(conditionssum))
        if len(conditionssum) > 0:
            criteriamet = True
        return criteriamet

    for quake in lastquakes:
        if _test_criteria(quake,criteria, debug=debug):
            relevantquakes.append(quake)

    if debug:
        print (" Found {} relevant (for COBS instrumentation) earthquakes".format(len(relevantquakes)))

    # Sort with youngest at the end - for correct memory usage
    if len(relevantquakes) > 1:
        relevantquakes.sort(key=lambda x: x[0])

    return relevantquakes


def new_quakes(relquakes, memorypath='',debug=False):
    relevantquakes = []
    firstrun = True
    ind = -1
    if debug:
        print (" relevant quakes:", len(relquakes))

    # continue only if list length > 0
    if len(relquakes) > 0:
        print ("Now get last record from temporary folder")
        try:
            lq = np.load(memorypath)
            if debug:
                print (memorypath, lq)
            ind, tmp = np.where(np.asarray(relquakes)==lq[0])
            #ind = [3] # force reselection
            if len(ind) > 0:
                ind = ind[0]
                print (" -> Found last quake at index", ind)
            else:
                print (" -> last notification quake not found within the most recent 200 quakes - activating firstrun")
                firstrun = True
                ind=-1
        except:
            # first run? memory deleted?
            print (" -> Could not access memory - firstrun or memory deleted?")
            firstrun = True

    if debug:
        print (" selection", ind, len(relquakes))
    if (ind >= 0 and len(relevantquakes) > ind+1) or firstrun:
        if debug:
            print ("Found new earthquakes")
        relevantquakes = relquakes[ind+1:]
    if debug:
        print (" after checking memory: {} earthquakes".format(len(relevantquakes)))

    return relevantquakes


def send_quake_message(relevantquakes, tgconfig='path/to/tg.cdf', memorypath='', debug=False):

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
        if not debug:
            print ("Sending message to Telegram")
            telegram_send.send(messages=[quakemsg+"\n"+maplink],conf=tgconfig,parse_mode="markdown")
            print (" -> sending done ... adding to memory")
            np.save(memorypath, quake)
            print (" -> Done")
        else:
            print (" DEBUG selected - not sending and saving anything")
            print ("  -> projected telegram configuration: {}".format(tgconfig))

def write_current_data(relevantquakes,currentvaluepath,debug=False):
      # updating current data
      if len(relevantquakes)>0:
        # get strongest, manual detected quake
        mag = np.asarray([quake[4] for quake in relevantquakes if not quake[6].startswith('auto')])
        ind = np.argmax(mag)
        quake = relevantquakes[ind]
        # Creating Current data input
        if quake and len(quake)>0:
            fulldict, valdict = load_current_data_sub(currentvaluepath, 'seismology')
            print ("valdict", valdict)
            valdict[u'date'] = [quake[0],'']
            valdict[u'magnitude'] = [quake[4],'']
            valdict[u'location'] =  [quake[7].replace("REGION",""),'']
            valdict[u'latitdue'] = [quake[1],'deg']
            valdict[u'longitude'] = [quake[2],'deg']
            valdict[u'depth'] = [quake[3],'km']
            fulldict[u'seismology'] = valdict
            with open(currentvaluepath, 'w',encoding="utf-8") as file:
                file.write(unicode(json.dumps(fulldict))) # use `json.loads` to do the reverse
                print ("Current quake data written successfully to {}".format(currentvaluepath))



def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    debug=False
    channelconfig=''


    try:
        opts, args = getopt.getopt(argv,"hc:t:D",["config=","telegram=","debug="])
    except getopt.GetoptError:
        print ('tg_base.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- tg_base.py will obtain baseline plots --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python tg_base.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-t (required) : telegram channel configuration')
            print ('-------------------------------------')
            print ('Application:')
            print ('python tg_base.py -c /etc/marcos/wic.cfg -t /etc/marcos/telegram.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-t", "--channel"):
            # delete any / at the end of the string
            channelconfig = os.path.abspath(arg)
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running tg_quake version {}".format(version))
    print ("--------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    # 1. conf and logger:
    # ###########################

    print ("Read and check validity of configuration data")
    config = GetConf(configpath)
    print (" -> Done")

    print ("Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "Info", job=os.path.basename(__file__), newname='mm-info-quake.log', debug=debug)
    print (" -> Done")

    # 2. database:
    # ###########################

    name1 = "{}-tgquake".format(config.get('logname'))
    statusmsg[name1] = 'successful'
    continueeval = True

    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        connectdict = config.get('conncetedDB')
    except:
        statusmsg[name1] = 'database failed'

    # SOME DEFINITIONS:
    temporarypath = config.get('temporarydata')
    memorypath = os.path.join(temporarypath,'lastquake.npy')
    currentvaluepath = config.get('currentvaluepath')
    if not channelconfig:
        channelconfig = config.get('notificationconfig')
    if not channelconfig:
        print ("No message channel defined - aborting")
        sys.exit(1)

    # 3. get quakes:
    # ###########################
    msg = 'problem with basic list generation'
    try:
        lastquakes = get_quakes(db, debug=debug)
        msg = 'problem with selecting relavant quakes'
        relevantquakes = select_relevant_quakes(lastquakes, criteria={}, debug=debug)
        msg = 'problem with new quakes'
        if debug:
            print (" the following quakes appear relevant:", relevantquakes)
        relevantquakes = new_quakes(relevantquakes, memorypath=memorypath, debug=debug)
    except:
        statusmsg[name1] = msg
        relevantquakes = []

    # 4. sending notification:
    # ###########################
    try:
        if len(relevantquakes) > 0:
            send_quake_message(relevantquakes, tgconfig=channelconfig, memorypath=memorypath, debug=debug)
    except:
        statusmsg[name1] = 'problem sending notification'
        continueeval = False

    # 5. writing current data
    # ###########################
    try:
        if len(relevantquakes) > 0 and currentvaluepath and continueeval and not debug:
            write_current_data(relevantquakes,currentvaluepath,debug=debug)
    except:
        statusmsg[name1] = 'problem writing current data'

    print ("tg_quake successfully finished")

    # 6. Logging section
    # ###########################
    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])
