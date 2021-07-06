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


def CreateBLVPlot(db, blvname, blvdata, starttime,endtime, plotdir, plttitle, debug=False):
    print (" Loading absolute data: {}".format(blvdata))
    absresult = read(blvdata,starttime=starttime,endtime=endtime)
    print (" -> {} data points".format(absresult.length()[0]))
    try:
        blvflagname = blvname.replace("comp","").replace(".txt","")
        flags = db2flaglist(db,blvflagname)
        print ("  Obtained {} flags".format(len(flags)))
        if len(flags) > 0:
            absresult = absresult.flag(flags)
            absresult = absresult.remove_flagged()
    except:
        print ("  flagging failed")
    try:
        absresult = absresult._drop_nans('dx')
        absresult = absresult._drop_nans('dy')
        absresult = absresult._drop_nans('dz')
        print (" -> {} data points".format(absresult.length()[0]))
        func = absresult.fit(['dx','dy','dz'],fitfunc='spline', knotstep=0.3)
        print (" Saving to {}".format(plotdir))
        if not debug:
            mp.plot(absresult,['dx','dy','dz'],symbollist=['o','o','o'],padding=[2.5,0.005,2.5],function=func,plottitle=plttitle,outfile=os.path.join(plotdir,'basegraph.png'))
        caption = "{}: Basevalues and adopted baseline".format(datetime.strftime(endtime,"%Y-%m-%d"))
    except:
        caption = "Not enough data points for creating new baseline graph"
    if debug:
        print ("Caption:" , caption)


    return caption


def GetFailed(analyzepath,endtime,debug=False):
    onlyfiles = [f for f in listdir(analyzepath) if isfile(join(analyzepath, f))]
    print ("FAILED ANALYSES: {}".format(onlyfiles))
    failedmsg = ''
    if len(onlyfiles) > 0:
        failedmsg = '*Failed analyses:*\n'.format(datetime.strftime(endtime,"%Y-%m-%d %H:%M"))
        for f in onlyfiles:
            failedmsg += '{}\n'.format(f)
        failedmsg += 'at *{}*'.format(datetime.strftime(endtime,"%Y-%m-%d %H:%M"))
        failedmsg = failedmsg.replace("_","")

    print (failedmsg)
    return failedmsg


def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    debug=False

    dipath = '/srv/archive/WIC/DI'
    analyzepath = os.path.join(dipath,'analyze')
    datapath = os.path.join(dipath,'data')
    priminst = '/home/cobs/ANALYSIS/Logs/primaryinst.pkl'
    plotdir = '/home/cobs/ANALYSIS/Info/plots'
    endtime = datetime.utcnow()
    starttime = endtime-timedelta(days=380)
    pier = "A2"
    caption = ''
    channelconfig='/home/cobs/ANALYSIS/Info/conf/tg_base.cfg'
    failedmsg = ''


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
            print ('python tg_base.py -c /etc/marcos/analysis.cfg -t /etc/marcos/telegram.cfg')
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

    print ("Running tg_base version {}".format(version))
    print ("--------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "Info", job=os.path.basename(__file__), newname='mm-info-tgbase.log', debug=debug)

    name1 = "{}-tgbase".format(config.get('logname'))
    statusmsg[name1] = 'Baseline notification successful'


    print ("3. Connect databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        connectdict = config.get('conncetedDB')
    except:
        statusmsg[name1] = 'database failed'


    # SOME DEFINITIONS:
    datapath = config.get('dipath')
    dipath = os.path.join(config.get('dipath'),'..')
    analyzepath = os.path.join(dipath,'analyze')
    pier = config.get('primarypier')
    plotdir = config.get('magfigurepath')

    #priminst = '/home/cobs/ANALYSIS/Logs/primaryinst.pkl'
    plotdir = '/home/cobs/ANALYSIS/Info/plots'
    caption = ''


    # 1. get primary instruments:
    # ###########################
    #lst = pload(open(priminst,'rb'))
    config, statusmsg = GetPrimaryInstruments(config=config, statusmsg=statusmsg, debug=debug)
    variosens = config.get('primaryVario')
    scalarsens = config.get('primaryScalar')
    print ("PRIMARY INSTRUMENTS: vario={}, scalar={}".format(variosens,scalarsens))

    # 2. define BLV filename
    # ###########################
    blvname = "BLVcomp_{}_{}_{}.txt".format(variosens,scalarsens,pier)
    blvdata = os.path.join(datapath,blvname)
    print ("BASEVALUE SOURCE: {}".format(blvdata))

    # 3. Read BLV fiel and create BLV plot for the last year
    # ###########################
    plttitle = "{}: {} and {}".format(pier,variosens,scalarsens)
    caption = CreateBLVPlot(db, blvname, blvdata, starttime,endtime, plotdir, plttitle, debug=debug)

    # 4. read file list of *.txt files remaining in DI/analyse
    # ###########################
    failedmsg = GetFailed(analyzepath, endtime, debug=debug)

    # 5. send all info to telegramchannel
    # ###########################
    if not debug:
        with open(os.path.join(plotdir,'basegraph.png'), "rb") as f:
            telegram_send.send(images=[f],captions=[caption],conf=channelconfig,parse_mode="markdown")
        if not failedmsg == '':
            telegram_send.send(messages=[failedmsg],conf=channelconfig,parse_mode="markdown")
    else:
        print ("Debug selected")


    print ("tg_base successfully finished")

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
