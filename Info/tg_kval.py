#!/usr/bin/env python
# coding=utf-8

"""
MagPy - Send ou k value messages
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
from dateutil import tz


scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, load_current_data_sub
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf


languagedict = {'english' : {'warnstart': "Possible consequences are", 'msgnew':'*High geomagnetic activity*',
                             'msgupdate':'*Update on activity*','msgbody1':'Geomagnetic activity index _k_ of ',
                             'msgbody2':'expected','msgvalid':'Warning is valid until','timezone':'UTC',
                             'msgref':'Based on data from the [Conrad Observatory]','channeltype':'telegram',
                             'channelconfig':'/etc/martas/tg_space.cfg',
                             'level':{"5":"an aurora at high latitudes.",
                                      "6":"an aurora at high latitudes, weak degradation of radio communication.",
                                      "7":"an aurora at high latitudes, disturbances in radio communication, and weak fluctuations in power grids.",
                                      "8":"an aurora even visible in mid latitudes, disturbances in radio communication, disruptions in navigation signals, and fluctuations in power grid.",
                                      "9":"an aurora visible at mid latitudes, significant disturbances in radio communication, outages in navigation signals, and strong power grid fluctuations."}},
                'deutsch' : {'warnstart': "Mögliche Auswirkungen:", 'msgnew':'*Hohe geomagnetische Aktivität*',
                             'msgupdate':'*Aktivitätsupdate*','msgbody1':'Geomagnetischer Aktivitätsindex _k_=',
                             'msgbody2':'erwwartet','msgvalid':'Warnung ist gültig bis','timezone':'CET',
                             'msgref':'Basierend auf Daten des [Conrad Observatoriums]','channeltype':'telegram',
                             'channelconfig':'/etc/martas/tg_weltraum.cfg',
                             'level': {"5":"eine Aurora in hohen Breiten.", 
                                       "6":"eine Aurora in hohen Breiten, mögliche Störungen bei Radio-Kommunikation.",
                                       "7":"eine Aurora in hohen Breiten, Beeiträchtigungen der Radio-Kommunikation und schwache Fluktuationen im Stromnetz.",
                                       "8":"eine Aurora teilweise auch sichtbar in mittleren Breiten, Beeiträchtigungen der Radio-Kommunikation, Störungen bei Navigationssignalen und Fluktuationen im Stromnetz.",
                                       "9":"eine Aurora sichtbar in mittleren Breiten, starke Beeiträchtigungen der Radio-Kommunikation, Ausfälle bei Navigationssystemen, starke Fluktuationen im Stromnetz."}}
                 }

def get_kvals(db, debug=False):
    print ("Reading k values from database")
    data = readDB(db,'WIC_k_0001_0001', starttime=datetime.utcnow()-timedelta(hours=11))
    print (" -> got", data.ndarray)
    return data


def send_kval_message(data, currentvaluepath='', debug=False):
    # Check last input
    lastvalue = data.ndarray[7][-1]
    valid = num2date(data.ndarray[0][-1]).replace(tzinfo=None)+timedelta(minutes=90)
    print ("Current k-value: {}".format(lastvalue))
    print ("Valid until: {}".format(valid))

    fulldict,valdict = load_current_data_sub(currentvaluepath, 'magnetism')
    # TESTING: lastvalue = lastvalue+5
    if lastvalue >=5:
        print ("Found large k")
        # check whether a valid warning is existing for this k value
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
            print ("Creating message - Updating current value")
            for language in languagedict:
                langdic = languagedict.get(language)
                timezone = langdic.get('timezone','UTC')
                channelconf = langdic.get('channelconfig')
                if debug:
                    channelconf = '/etc/martas/telegram.cfg'
                if not timezone in ['utc','UTC']:
                    try:
                        from_zone = tz.tzutc()
                        to_zone = tz.gettz(timezone)
                        validnew = valid.replace(tzinfo=from_zone)
                        valid = validnew.astimezone(to_zone)
                    except:
                        timezone = 'UTC'
                        pass

                if newmsg:
                    msg = "{}\n\n".format(langdic.get('msgnew'))
                else:
                    msg = "{}\n\n".format(langdic.get('msgupdate'))
                msg += "{}*{}* {}.\n".format(langdic.get('msgbody1'),int(lastvalue),langdic.get('msgbody2'))
                msg += "{} {} {}.\n".format(langdic.get('msgvalid'),datetime.strftime(valid,"%Y-%m-%d %H:%M"),timezone)

                warningmsg = langdic.get('level')
                msg += "\n{} {}\n".format(langdic.get('warnstart'),warningmsg.get(str(int(lastvalue))))
                msg += '{}(http://www.conrad-observatory.at/)'.format(langdic.get('msgref'))
                msg += ""
                if debug:
                    print (msg)
                else:
                    telegram_send.send(messages=[msg],conf=channelconf,parse_mode="markdown")

    else:
        valdict[u'k-warning'] = [0,'']
        valdict[u'k-valid'] = ['','']

    if not debug:
        print ("Updating json")
        fulldict[u'magnetism'] = valdict
        with open(currentvaluepath, 'w',encoding="utf-8") as file:
            file.write(unicode(json.dumps(fulldict)))
        print ("K warning data has been updated")


def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    debug=False
    channelconfig = ''

    try:
        opts, args = getopt.getopt(argv,"hc:t:D",["config=","telegram=","debug="])
    except getopt.GetoptError:
        print ('tg_kval.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- tg_kval.py will obtain baseline plots --')
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
            print ('-l (required) : language dictionary')
            print ('-t (optional) : telegram channel configuration')
            print ('-------------------------------------')
            print ('Application:')
            print ('python tg_kval.py -c /etc/marcos/analysis.cfg -t /etc/marcos/telegram.cfg')
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

    print ("Running tg_kval version {}".format(version))
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
    config = DefineLogger(config=config, category = "Info", job=os.path.basename(__file__), newname='mm-info-kavl.log', debug=debug)
    print (" -> Done")

    # 2. database:
    # ###########################

    name1 = "{}-kval".format(config.get('logname'))
    statusmsg[name1] = 'successful'

    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        connectdict = config.get('conncetedDB')
    except:
        statusmsg[name1] = 'database failed'

    # SOME DEFINITIONS:
    currentvaluepath = config.get('currentvaluepath')
    if not channelconfig:
        channelconfig = config.get('notificationconfig')
    if not channelconfig:
        print ("No message channel defined - aborting")
        sys.exit(1)

    # 3. get quakes:
    # ###########################
    try:
        data = get_kvals(db, debug=debug)
    except:
        statusmsg[name1] = 'problem with list generation'
        data = DataStream()

    # 4. sending notification:
    # ###########################
    if data.length()[0]> 0:
        try:
            send_kval_message(data, currentvaluepath=currentvaluepath, debug=debug)
        except:
            statusmsg[name1] = 'problem sending notification'

    print ("tg_kval successfully finished")

    # 5. Logging section
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
