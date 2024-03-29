#!/usr/bin/env python3
# coding=utf-8

from magpy.database import *
from magpy.opt import cred as mpcred
import getopt

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, load_current_data_sub
from martas import martaslog as ml
from martas import sendmail as sm
from acquisitionsupport import GetConf2 as GetConf
from datetime import time as dttime

"""
Programm to extract GIC values for all stations covering a certain time range.
Analyzes availability, variability, extrema and current values.
Creates
"""
def read_gic_data(db,source='GICAUT',sensornums=[1,2,3,4,5,6,7,8], minutes=5, maxvals=5, debug=False):
    knewsql = ''
    gicdata = []
    status = {}
    amount = 0
    active = 0
    sql = ''
    addcommlist = []
    t1 = datetime.utcnow()

    start = datetime.utcnow()-timedelta(minutes=minutes)
    trange = datetime.strftime(start, "%Y-%m-%d %H:%M:%S")
    valueresults = []
    streamlist = []
    for i in sensornums:
        name = "{}_GIC{:02d}_0001_0001".format(source,i)
        sn = "GIC{:02d}".format(i)
        try:
            if debug:
                print ("Checking table ", name)
            #gicdat = dbselect(db,'x', name,'time > "{}"'.format(trange))
            gicdata = readDB(db, name, starttime=start)
            #gicdat = gicdata._get_column('x')
            if debug:
                print (" - obtained {} datapoints ".format(gicdata.length()[0]))
            status[name] = gicdata.length()[0]
            if gicdata.length()[0] > 0:
                # filter the data and extract maximam and minima and last value
                gicfilt = gicdata.filter()
                gicrange = [np.abs(gicfilt._get_max('x')),np.abs(gicfilt._get_min('x'))]
                gicmax = np.max(gicrange)
                gicmin = np.min(gicrange)
                lastdate = gicfilt.ndarray[0][-1]
                giclast = np.abs(gicfilt.ndarray[1][-1])
                #print (gicdata.header.get('DataSource')) # this should contain the APG Station code
                valueres = [giclast,gicmax,gicmin,num2date(lastdate).replace(tzinfo=None),gicdata.header.get('DataSource',sn)]
                if debug:
                    print (num2date(lastdate), giclast, gicmax, gicmin)
                amount += 1
                valueresults.append(valueres)
                streamlist.append(gicdata)
            # combine GIC data to a csv file
        except:
            pass
    for stream in streamlist:
        # join them all
        pass

    # get the largest last value which is less then 15min old
    # if no existing, add the last existing value and set zero to 0
    # if no values are available set active to zero
    print (valueresults)
    fastmax = -1
    fasti = -1
    slowmax = -1
    slowi = -1
    for idx,vals in enumerate(valueresults):
        print (idx, vals[3])
        if vals[3] > datetime.utcnow()-timedelta(minutes=15):
            if vals[0] > fastmax:
                fasti = idx
        else:
            if vals[0] > slowmax:
                slowi = idx

    if fasti > 0:
        result = valueresults[fasti]
        active = 1
    elif slowi > 0:
        result = valueresults[slowi]
        active = 0
    else:
        result = [0,0,0,None,'None']
        active = 0

    sql = _create_sql('GICmeas', 'activity', 'gic', 'spaceweather', result[0]/1000., result[2]/1000., result[1]/1000., 0, 'A', 2,
                        8, 0, 0, start, result[3], active, 'TU Graz', result[4], 'geomagnetically induced currents as measured at the neutral point of the transformer')

    t2 = datetime.utcnow()
    print ("Duration", (t2-t1).total_seconds())
    return sql

def _create_sql(notation,stype,group,field,value,value_min,value_max,uncert,value_unit,warning_high,critical_high,warning_low,critical_low,start,end,active,source,location,comment):
    now = datetime.utcnow()
    sql = "INSERT INTO COBSSTATUS (status_notation,status_type,status_group,status_field,status_value,value_min,value_max,value_std,value_unit,warning_high,critical_high,warning_low,critical_low,validity_start,validity_end,source,location,comment,date_added,active) VALUES ('{}','{}','{}','{}',{},{},{},{},'{}',{},{},{},{},'{}','{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE status_type = '{}',status_group = '{}',status_field = '{}',status_value = {},value_min = {},value_max = {},value_std = {},value_unit = '{}',warning_high = {},critical_high = {},warning_low = {},critical_low = {},validity_start = '{}',validity_end = '{}',source = '{}',location = '{}',comment='{}',date_added = '{}',active = {} ".format(notation,stype,group,field,value,value_min,value_max,uncert,value_unit,warning_high,critical_high,warning_low,critical_low,start,end,source,location,comment,now,active,stype,group,field,value,value_min,value_max,uncert,value_unit,warning_high,critical_high,warning_low,critical_low,start,end,source,location,comment,now,active)
    return [sql]

def _execute_sql(db,sqllist, debug=False):
    """
    DESCRIPTION
        sub method to execute sql requests
    """
    if len(sqllist) > 0:
        cursor = db.cursor()
        for sql in sqllist:
            if debug:
                print ("executing: {}".format(sql))
            try:
                cursor.execute(sql)
            except mysql.Error as e:
                emsg = str(e)
                print ("mysql error - {}".format(emsg))
            except:
                print ("unknown mysql error when executing {}".format(sql))
        db.commit()
        cursor.close()


def main(argv):
    """
    METHODS:
        read_gic_data
    """
    version = "1.0.0"
    GICNUMS = [1,2,3,4,5,6,7,8,9]
    configpath = '' # is only necessary for monitoring
    sqllist = []
    gicrange = 1440
    savepath = "/tmp/gicdata.csv"
    endtime = datetime.utcnow()
    debug = False
    init = False # create table if TRUE
    statusmsg = {}

    usage = 'gic_extractor.py -c <config>'
    try:
        opts, args = getopt.getopt(argv,"hc:r:e:s:ID",["config=","init=","range=","endtime=","savepath=","debug=",])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('-------------------------------------')
            print('Description:')
            print('gic_extractor.py extracts CME predictions from Scoreboard ')
            print('-------------------------------------')
            print('Usage:')
            print(usage)
            print('-------------------------------------')
            print('Options:')
            print('-c            : configuration data ')
            print('-r            : range in minutes ')
            print('-e            : endtime, default is utcnow')
            print('-s            : savepath for exporting csv file ')
            print('-------------------------------------')
            print('Examples:')
            print('---------')
            print('python3 gic_extractor.py -c /home/cobs/CONF/wic.cfg -D')
            print('---------')
            sys.exit()
        elif opt in ("-c", "--config"):
            configpath = os.path.abspath(arg)
        elif opt in ("-r", "--range"):
            gicrange = arg
        elif opt in ("-e", "--endtime"):
            endtime = arg
        elif opt in ("-s", "--savepath"):
            savepath = os.path.abspath(arg)
        elif opt in ("-I", "--init"):
            init = True
        elif opt in ("-D", "--debug"):
            debug = True

    if debug:
        print ("Running gic-extractor version:", version)

    # 1. conf and logger:
    # ###########################
    if debug:
        print ("Read and check validity of configuration data")
        print (" and activate logging scheme as selected in config")
    config = GetConf(configpath)
    config = DefineLogger(config=config, category = "Info", job=os.path.basename(__file__), newname='mm-info-sw.log', debug=debug)
    if debug:
        print (" -> Done")

    # 2. database:
    # ###########################
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        connectdict = config.get('conncetedDB')
    except:
        statusmsg[name1] = 'database failed'

    # 6. Read GIC data:
    # ###########################
    #try:
    ok = True
    if ok:
        if debug:
            print(" -  Getting GIC:")
        newsql = read_gic_data(db,source='GICAUT',sensornums=GICNUMS, minutes=gicrange, debug=debug)
        sqllist.extend(newsql)
        statusmsg['GIC data access'] = 'success'
    #except:
    #    statusmsg['GIC data access'] = 'failed'

    sqllist = [el for el in sqllist if el]

    if debug:
        print ("Debug selected - sql call looks like:")
        print (sqllist)
    else:
        for dbel in connectdict:
            db = connectdict[dbel]
            print ("     -- Writing data to DB {}".format(dbel))
            if len(sqllist) > 0:
                _execute_sql(db, sqllist, debug=debug)

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
