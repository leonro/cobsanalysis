#!/usr/bin/env python

"""
DESCRIPTION
   Converts data from a database to any specified data format. Unlike mpconvert
   it automatically chooses the best match containing the given sensor name fragment

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)

APPLICATION
    PERMANENTLY with cron:
        python3 convert_data.py -c ~/CONF/wic.cfg -s BM35 -o /srv/products/data/meteo/pressure/
"""

from magpy.stream import *
from magpy.database import *
import magpy.opt.cred as mpcred
import json
import getopt
import pwd
import socket
import sys  # for sys.version_info()

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, getstringdate
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf

def convert(db, sensor,path, starttime='', endtime=datetime.utcnow(), samplingrate=1, format_type='PYASCII',debug=False):
    dbdateformat1 = "%Y-%m-%d %H:%M:%S.%f"
    dbdateformat2 = "%Y-%m-%d %H:%M:%S"

    where = 'SensorID LIKE "{}%" AND DataSamplingRate Like "{}.%"'.format(sensor,int(samplingrate))

    senslist = dbselect(db, 'DataID', 'DATAINFO',where)
    sens=[]
    if debug:
        print (" found sensorlist:", senslist)
    for s in senslist:
        last = dbselect(db,'time',s,expert="ORDER BY time DESC LIMIT 1")
        if debug:
            print ("Check", s, last)
        if len(last) > 0:
            try:
                if datetime.strptime(last[0], dbdateformat1) > starttime:
                    sens.append(s)
            except:
                if datetime.strptime(last[0], dbdateformat2) > starttime:
                    sens.append(s)

    st = DataStream([],{},np.asarray([[] for key in KEYLIST]))
    for se in sens:
        # select the one with meta information indicating outside sensor
        dat = readDB(db,se,starttime=starttime,endtime=endtime)
        if not debug:
            dat.write(path,filenamebegins=se+'_', format_type=format_type)
        else:
            print (" debug selected - skipping write")
            
def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    debug=False
    sensor = ''
    outpath = '/tmp'
    duration=2
    format_type='PYASCII'
    endtime = None

    try:
        opts, args = getopt.getopt(argv,"hc:s:o:e:d:f:D",["config=","sensor=","outputpath=","endtime=","duration=","format=","debug=",])
    except getopt.GetoptError:
        print ('getprimary.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- getprimary.py will determine the primary instruments --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python getprimary.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-------------------------------------')
            print ('Application:')
            print ('python getprimary.py -c /etc/marcos/analysis.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-s", "--sensor"):
            sensor = arg
        elif opt in ("-o", "--outputpath"):
            # delete any / at the end of the string
            outpath = os.path.abspath(arg)
        elif opt in ("-e", "--endtime"):
            # get an endtime
            endtime = arg
        elif opt in ("-d", "--duration"):
            duration = int(arg)
        elif opt in ("-f", "--format"):
            format_type = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running convert data version {}".format(version))
    print ("--------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    if not sensor:
        print ('Specify a sensor name')
        sys.exit(0)
    if endtime:
         endtime = DataStream()._testtime(endtime)
    else:
         endtime = datetime.utcnow()
         
    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category="DataProducts", job=os.path.basename(__file__), newname='mm-dp-convert{}.log'.format(sensor))

    name1 = "{}-{}".format(config.get('logname'),sensor)
    statusmsg[name1] = 'successful'

    print ("3. Connect databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        if debug:
            print ("   -- success")
    except:
        if debug:
            print ("   -- database failed")
        statusmsg[name1] = 'database failed'

    print ("4. Running conversion")
    try:
        starttime = datetime.strptime(datetime.strftime(endtime-timedelta(days=duration),"%Y-%m-%d"),"%Y-%m-%d")
        convert(db, sensor,outpath,starttime=starttime,endtime=endtime,format_type=format_type,debug=debug)
    except:
        if debug:
            print ("   -- conversion failed")
        statusmsg[name1] = 'conversion failed'

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])


