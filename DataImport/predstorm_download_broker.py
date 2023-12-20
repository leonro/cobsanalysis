#!/usr/bin/env python
# -*- coding: utf-8 -*-


from magpy.stream import *
import magpy.mpplot as mp
import getopt
import sys  # for sys.version_info()

#import format_predstorm as fp


def read_predstorm_data(source,starttime=datetime.utcnow(),debug=False):

    sqllist = []
    ok = True
    if ok:
        data = read(source,starttime=starttime)
        stime, etime = data._find_t_limits()
        tlist = [0,3,6,9,12,15,18,21]
        newt = stime
        for t in tlist:
             nt = stime.replace(hour=t,minute=0,second=0)
             if stime >= nt:
                 newt = nt
             else:
                 start = newt
                 break
        step = start
        daylist = []
        l1,l2 = [],[]
        d = {}
        N = 0
        while step <= etime:
            dataframe = data._select_timerange(step, step+timedelta(hours=3))
            meant = step+timedelta(minutes=90)
            step = step+timedelta(hours=3)
            kplist = np.asarray(dataframe[KEYLIST.index('var2')]).astype(float)  # Kp
            sollist = np.asarray(dataframe[KEYLIST.index('t2')]).astype(float)   # SolarWindSpeed
            if debug:
                print (meant, np.nanmean(kplist))
            if meant.date() in daylist:
                l1.append(np.nanmean(kplist))
                l2.append(np.nanmean(sollist))
                name = "Kp_{}".format(N)
                d[name] = {'date':meant.date(),'kp':l1,'sw':l2}
            else:
                l1,l2 = [],[]
                N += 1
                l1.append(np.nanmean(kplist))
                l2.append(np.nanmean(sollist))
            daylist.append(meant.date())

        for day in d:
            dic = d[day]
            l1 = dic.get('kp')
            l2 = dic.get('sw')
            start = dic.get('date')
            maxkpval = np.max(np.asarray(l1))
            maxwindval = np.max(np.asarray(l2))
            if debug:
                print (day, start, maxkpval, maxwindval)
            # create sql inputs
            kpsql = _create_kpforecast_sql(day, maxkpval, start )
            swsql = _create_swforecast_sql(day.replace('Kp','SolarWind'), maxwindval, start )
            print (kpsql)
            print (swsql)
            sqllist.append(kpsql)
            sqllist.append(swsql)
    return sqllist


def _create_kpforecast_sql(kpname, kpval,start ):
    active=1
    knewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format(kpname,'forecast','geomagactivity','geomag',kpval,start,start,'PREDSTORM','',datetime.utcnow(),active,'forecast','geomagactivity','geomag',kpval,start,start,'PREDSTORM','',datetime.utcnow(),active)
    return knewsql

def _create_swforecast_sql(swname, swval,start ):
    active=1
    swnewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format(swname,'forecast','solarwind','solar',swval,start,start,'PREDSTORM','',datetime.utcnow(),active,'forecast','solarwind','solar',swval,start,start,'PREDSTORM','',datetime.utcnow(),active)
    return swnewsql



def main(argv):
    version = "1.0.0"
    configpath = ''
    inputsource = ''
    outputpath = ''
    statusmsg = {}
    debug=False


    try:
        opts, args = getopt.getopt(argv,"hc:i:o:D",["config=","input=","output=","debug=",])
    except getopt.GetoptError:
        print ('try predstorm_extract.py -h for instructions')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- predstorm_extract.py will plot sensor data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python predstorm_extract.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-i            : input path')
            print ('-o            : output directory (or file) to save the graph')
            print ('-------------------------------------')
            print ('Application:')
            print ('python3 predstorm_extract.py -c ../conf/wic.cfg -i ../conf/sensordef_plot.json -e 2020-12-17')
            print ('python3 predstorm_download_broker.py -i https://helioforecast.space/static/sync/predstorm_real_1m.txt -o /home/cobs/SPACE/incoming/PREDSTORM/')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-i", "--input"):
            # delete any / at the end of the string
            inputsource = arg
        elif opt in ("-o", "--output"):
            # delete any / at the end of the string
            outputpath = os.path.abspath(arg)
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    if debug:
        print ("Running predstorm loader version {}".format(version))

    # move the rest to sw_extractor
    #data = read_predstorm_data(os.path.join(outputpath,"PREDSTORM*"),debug=debug)

    if configpath:
        # requires martas import
        print ("1. Read and check validity of configuration data")
        config = GetConf(configpath)

        print ("2. Activate logging scheme as selected in config")
        config = DefineLogger(config=config, category = "GetPredstorm", job=os.path.basename(__file__), newname=newloggername, debug=debug)

    try:
        if debug:
            print ("Loading PREDSTORM data ...")
        # read and save data
        data = read(inputsource, starttime=datetime.utcnow())
        data.header['SensorID'] = 'PREDSTORM_HELIO1M_0001'
        print (data.length()[0], data.header.get('DataID'))
        data.write(outputpath, filenamebegins="{}_".format(data.header.get('SensorID')), format_type='PYCDF', mode='replace')
        statusmsg['PREDSTORM download'] = 'success'
    except:
        statusmsg['PREDSTORM download'] = 'failure'

    if configpath and not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])

