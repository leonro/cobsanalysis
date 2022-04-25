#!/usr/bin/env python
#********************************************************************
# Regularly download XRS data from GOES16/17.
#
# Cronjob activated .
#	$ crontab -e
#	24 2,5,8,11,14,17,20,23 * * * python /home/cobs/CronScripts/kpdownload/gfzkp_download.py
#	(Job runs every three hours.)
#
#********************************************************************

import sys
import getopt
import os
from magpy.stream import *
import magpy.mpplot as mp
import urllib.request, json 
import dateutil.parser as dparser


#xray = 'https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json'
#path = '/home/cobs/SPACE/incoming/GOES/16'

def readXRAY(source,debug=False):
    stream = DataStream()
    array1 = [[] for key in KEYLIST]
    array2 = [[] for key in KEYLIST]
    header = {}
    sat = 0

    header['SensorName'] = 'XRS'
    header['SensorGroup'] =  'GOES SolarSatellite'
    header['SensorType'] =  'XRS'

    with urllib.request.urlopen(source) as url:
        data = json.loads(url.read().decode())
        print(data)
        pos1 = KEYLIST.index('x')
        pos2 = KEYLIST.index('var1')
        for element in data:
            if element.get('energy') == '0.1-0.8nm':
                date = dparser.parse(element.get('time_tag'))
                array1[0].append(date2num(date))
                array1[pos1].append(float(element.get('flux')))
                array1[pos1+1].append(float(element.get('observed_flux')))
                array1[pos1+2].append(float(element.get('electron_correction')))
                array1[pos1+3].append(int(element.get('electron_contaminaton')))
                sat = element.get('satellite')
                #print (date)
            else:
                date = dparser.parse(element.get('time_tag'))
                var1 = float(element.get('flux'))
                var2 = float(element.get('observed_flux'))
                var3 = float(element.get('electron_correction'))
                var4 = int(element.get('electron_contaminaton'))
                array2[0].append(date2num(date))
                array2[pos2].append(float(element.get('flux')))
                array2[pos2+1].append(float(element.get('observed_flux')))
                array2[pos2+2].append(float(element.get('electron_correction')))
                array2[pos2+3].append(int(element.get('electron_contaminaton')))
                sat = element.get('satellite')
        header['SensorDataLogger'] =  'GOES{}'.format(sat)
        header['SensorID'] = "{}_{}_0001".format(header.get('SensorName'),header.get('SensorDataLogger'))
        header['col-x'] = '0.1-0.8nm'
        header['unit-col-x'] = 'Watts/mw'
        header['col-y'] = 'observed_flux'
        header['col-z'] = 'electron_correction'
        header['col-f'] = 'electron_contaminaton'
        header['col-var1'] = '0.05-0.4nm'
        header['unit-col-var1'] = 'Watts/mw'
        header['col-var2'] = 'observed_flux'
        header['col-var3'] = 'electron_correction'
        header['col-var4'] = 'electron_contamination'
        stream1 = DataStream([LineStruct()], header, np.asarray([np.asarray(el) for el in array1],dtype=object))
        stream2 = DataStream([LineStruct()], header, np.asarray([np.asarray(el) for el in array2],dtype=object))
        return mergeStreams(stream1,stream2)


def read_xrs_data(source, debug=False):

    key = 'x' # contains flux in 1-8Angström range
    xrsdata = read(source,starttime=datetime.utcnow()-timedelta(hours=6))
    if xrsdata.length()[0] < 1:
        print ("XRS CRITICAL: no data found")
        active = 0
    else:
        t0,tend = xrsdata._find_t_limits()
        if tend < datetime.utcnow()-timedelta(minutes=30):
            active = 0
        else:
            active = 1

    m,t = xrsdata._get_max(key, returntime=True)
    comment = "data from {}".format(xrsdata.header.get('SensorID'))

    if debug:
         print (" maximum value during the last 6 hours: ", m)
         print (" at ", num2date(t))
         if m > thresholdtable[1]:
             print (" X5 warning reached - > X5")
         elif m > thresholdtable[0]:
             print (" M5 warning reached - > M5")

    sql = _create_xrsnow_sql(m,num2date(t).replace(tzinfo=None),active,comment)

    return [sql]


def _create_xrsnow_sql(gicval,start,active,comment):
    now = datetime.utcnow()
    xrsnewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format('XRS-6h','nowcast','x-ray 1-8A','goes',gicval,start,start,'NASA GOES',comment,now,active,'nowcast','x-ray 1-8A','goes',gicval,start,start,'NASA GOES',comment,now,active)
    return xrsnewsql

def main(argv):
    """
    METHODS:
        read_kpnow_data(source)   
        read_swpam_data(source, limit=5) 
        _execute_sql
        swtableinit()
    """
    version = '1.0.0'
    xraypath = 'https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json'
    outpath = '/home/cobs/SPACE/incoming/GOES/16'
    configpath = ''
    xray = ''
    statusmsg = {}
    debug = False

    usage = 'goes_download.py -c <config> -x <xraysource> '
    try:
        opts, args = getopt.getopt(argv,"hc:x:o:D",["config=","xraysource=","outpath=","debug=",])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('-------------------------------------')
            print('Description:')
            print('goes_download.py donwload xray data from GOES16/17 ')
            print('-------------------------------------')
            print('Usage:')
            print(usage)
            print('-------------------------------------')
            print('Options:')
            print('-c            : configuration data ')
            print('-x            : Xray source - default is ')
            print('-o            : output (directory) ')
            print('-------------------------------------')
            print('Examples:')
            print('---------')
            print('python3 goes_download_broker.py -c /home/cobs/CONF/wic.cfg -x https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json -o /home/cobs/SPACE/incoming/GOES/16 -D')
            print('---------')
            sys.exit()
        elif opt in ("-c", "--config"):
            configpath = os.path.abspath(arg)
        elif opt in ("-o", "--outpath"):
            outpath = os.path.abspath(arg)
        elif opt in ("-x", "--xraysource"):
            xray = arg
        elif opt in ("-D", "--debug"):
            debug = True

    if debug:
        print ("Running goes_download version:", version)

    if xray:
        xraypath = xray

    # 1. conf and logger:
    # ###########################
    if debug:
        print ("Read and check validity of configuration data")
        print (" and activate logging scheme as selected in config")
    if configpath:
        print ("Configpath found - defining logger")
        config = GetConf(configpath)
        config = DefineLogger(config=config, category = "DataImport", job=os.path.basename(__file__), newname='mm-dataimport-goesxrs.log', debug=debug)
        if debug:
            print (" -> Done")
    else:
        print ("no config file found - skipping logger")
        config = False
    

    # 6. Read GIC data:
    # ###########################
    try:
        data = readXRAY(xraypath)
        data.write(outpath, filenamebegins="{}_".format(data.header.get('SensorID')), format_type='PYCDF', mode='replace')
        statusmsg['XRS Download'] = 'success'
    except:
        statusmsg['XRS Download'] = 'failed'


    print (read_xrs_data(os.path.join(outpath,'*.cdf')))
    
    mp.plot(data)

    # Logging section
    # ###########################
    if not debug and config:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])
