#!/usr/bin/env python

from magpy.database import *
import getopt


scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, DoVarioCorrections, DoBaselineCorrection, DoScalarCorrections,ConnectDatabases, GetPrimaryInstruments, getcurrentdata, writecurrentdata
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__


"""
DESCRIPTION
   Downloads DST index data from Kyoto:
PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods
PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -y year                :   int     :  default year is obtained from utcnow
    -m month               :   int     :  default month is obtained from utcnow

APPLICATION
    PERMANENTLY with cron:
        python3 dst_import.py -c /home/user/CONF/wic.cfg
"""

def get_dst(year=2023, month=11, baseurl='https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/',debug=False):
    dst = DataStream()
    sm = int((year/100. - int(year/100.))*100)
    url = '{c}{a}{b}/dst{d}{b}.for.request'.format(a=year, b=str(month).zfill(2), c=baseurl, d=sm)
    if debug:
        print ("URL = ", url)
    else:
        dst = read(url)
    return dst

def get_range(year,month, debug=False):
    now = datetime.utcnow()
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    if not year:
        years = [now.year]
    elif year in ['all','All','ALL']:
        years = range(now.year-5, now.year+1)
    else:
        try:
            years = [year]
        except:
            pass
    if not month and not year:
        months = [now.month]
    elif month and month in months:
        months = [month]
    if debug:
        print ("Getting data for ranges {} and {}".format(years, months))
    return years, months

def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    year, month = None, None
    baseurl = 'https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/'
    savepath = '/home/cobs/SPACE/incoming/Kyoto/Dst'
    debug=False

    try:
        opts, args = getopt.getopt(argv,"hc:y:m:s:D",["config=","year=","month=","savepath=","debug="])
    except getopt.GetoptError:
        print ('dst_import.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- dst_import.py will obtain dst values directly from kyoto obs --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python dst_import.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-y            : year, default is obtained from utcnow')
            print ('-m            : month, default is obtained from utcnow')
            print ('-s            : saving data to a specific path')
            print ('-------------------------------------')
            print ('Application:')
            print ('python dst_import.py -c /etc/marcos/analysis.cfg')
            print ('python dst_import.py -c /etc/marcos/analysis.cfg -y 2022 -m 11')
            print ('python dst_import.py -c /etc/marcos/analysis.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-y", "--year"):
            # get an endtime
            if arg in ['all','All','ALL']:
                year = arg
            else:
                year = int(arg)
        elif opt in ("-m", "--month"):
            # get an endtime
            month = int(arg)
        elif opt in ("-s", "--savepath"):
            # get an endtime
            savepath = os.path.abspath(arg)
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running flagging version {}".format(version))
    print ("--------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname='mm-di-quakes.log', debug=debug)

    namedst = "{}-dst".format(config.get('logname'))
    currentvaluepath = config.get('currentvaluepath')

    print ("3. Connect databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        connectdict = config.get('conncetedDB')
        print(" -> connected databases: {}".format(connectdict))
    except:
        statusmsg[namedst] = 'database failed'


    prox = config.get('proxy','')
    proxport = config.get('proxyport')
    if prox:
        proxy = "--proxy http://{}:{} ".format(prox,proxport)

    years, months = get_range(year,month,debug=debug)

    if debug:
        print("Getting DST data")
        print("---------------------")
    for ye in years:
        for mo in months:
            dst = get_dst(year=ye, month=mo,debug=debug)
            if dst.length()[0] > 0:
                print ("Success for year {} and month {}".format(ye,mo))
                dst.header['DataSource'] = 'Kyoto Observatory'
                dst.header['DataReferences'] = baseurl
                dst.write(savepath, filenamebegins='Dst_', format_type='PYCDF',dateformat='%Y%m',coverage='month')


    print ("------------------------------------------")
    print ("  dst_import finished")
    print ("------------------------------------------")
    print ("SUCCESS")

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
        pass
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])
