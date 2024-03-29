#!/usr/bin/env python3
# coding=utf-8

"""
DESCRIPTION
Appliaction to extract SolarWind Parameters from ACE (or DSCOVR) and Kp from the GFZ

REQUIREMENTS

TESTING

   1. delete one of the json inputs from the memory file in /srv/archive/external/esa-nasa/cme
   2. run app without file option
       python3 sw_extractor.py -c /home/cobs/CONF/wic.cfg -k /srv/archive/external/gfz/kp/ - s /srv/archive/external/esa-nasa/ace/raw/


"""


import requests
import dateutil.parser as dparser
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

def read_predstorm_data(source,starttime=datetime.utcnow(),endtime=datetime.utcnow()+timedelta(days=3),debug=False):

    sqllist = []
    ok = True
    if ok:
        print ("Reading PREDSTORM data ({}) between {} and {}".format(source, starttime,endtime))
        data = read(source,starttime=starttime,endtime=endtime)
        stime, etime = data._find_t_limits()
        print ("Reading done: N={}".format(data.length()[0]))
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
        l1,l2,l3 = [],[],[]
        d = {}
        N = 0
        print ("Obtaining three hour means of predicted Kp and solarwindspeed - using typical Kp time ranges")
        while step <= etime:
            dataframe = data._select_timerange(step, step+timedelta(hours=3))
            meant = step+timedelta(minutes=90)
            step = step+timedelta(hours=3)
            kplist = np.asarray(dataframe[KEYLIST.index('var2')]).astype(float)  # Kp
            sollist = np.asarray(dataframe[KEYLIST.index('t2')]).astype(float)   # SolarWindSpeed
            dstlist = np.asarray(dataframe[KEYLIST.index('var1')]).astype(float)   # Dst
            if debug:
                print (meant, np.nanmean(kplist))
            if meant.date() in daylist:
                l1.append(np.nanmean(kplist))
                l2.append(np.nanmean(sollist))
                l3.append(np.nanmean(dstlist))
                name = "Kp_{}".format(N)
                d[name] = {'date':meant.date(),'kp':l1,'sw':l2,'dst':l3}
            else:
                l1,l2,l3 = [],[],[]
                N += 1
                l1.append(np.nanmean(kplist))
                l2.append(np.nanmean(sollist))
                l3.append(np.nanmean(dstlist))
            daylist.append(meant.date())

        for day in d:
            dic = d[day]
            l1 = dic.get('kp')
            l2 = dic.get('sw')
            l3 = dic.get('dst')
            start = dic.get('date')
            maxkpval = np.max(np.asarray(l1))
            maxwindval = np.max(np.asarray(l2))
            mindstval = np.min(np.asarray(l3))
            if debug:
                print ("Maximum values:", day, start, maxkpval, maxwindval, mindstval)
            # create sql inputs
            kpsql = _create_kpforecast_sql(day, maxkpval, start )
            swsql = _create_swforecast_sql(day.replace('Kp','SolarWind'), maxwindval, start )
            print ("here")
            dstsql = _create_dstforecast_sql(day.replace('Kp','Dst'), mindstval, start )
            print ("Done")
            if debug:
                print (kpsql)
                print (swsql)
                print (dstsql)
            sqllist.append(kpsql)
            sqllist.append(swsql)
            sqllist.append(dstsql)
    return sqllist


def _create_kpforecast_sql(kpname, kpval,start ):
    active=1
    knewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format(kpname,'forecast','geomagactivity','geomag',kpval,start,start,'PREDSTORM','',datetime.utcnow(),active,'forecast','geomagactivity','geomag',kpval,start,start,'PREDSTORM','',datetime.utcnow(),active)
    return knewsql


def _create_swforecast_sql(swname, swval,start ):
    active=1
    swnewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format(swname,'forecast','solarwind','solar',swval,start,start,'PREDSTORM','',datetime.utcnow(),active,'forecast','solarwind','solar',swval,start,start,'PREDSTORM','',datetime.utcnow(),active)
    return swnewsql

def _create_dstforecast_sql(dstname, dstval,start ):
    active=1
    dstnewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format(dstname,'forecast','geomagactivity','geomag',dstval,start,start,'PREDSTORM','',datetime.utcnow(),active,'forecast','geomagactivity','geomag',dstval,start,start,'PREDSTORM','',datetime.utcnow(),active)
    return dstnewsql


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
         thresholdtable = [0.00005,0.0005]
         print (" maximum value during the last 6 hours: ", m)
         print (" at ", num2date(t))
         if m > thresholdtable[1]:
             print (" X5 warning reached - > X5")
         elif m > thresholdtable[0]:
             print (" M5 warning reached - > M5")

    sql = _create_xrsnow_sql(m,num2date(t).replace(tzinfo=None),active,comment)

    return [sql]


def _create_xrsnow_sql(xrsval,start,active,comment):
    now = datetime.utcnow()
    xrsnewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format('XRS-6h','nowcast','x-ray 1-8A','goes',xrsval,start,start,'NASA GOES',comment,now,active,'nowcast','x-ray 1-8A','goes',xrsval,start,start,'NASA GOES',comment,now,active)
    return xrsnewsql


def read_gicnow_data(db,source='GICAUT',maxsensor=10, minutes=5, maxvals=5, debug=False):
    knewsql = ''
    gicdata = []
    status = {}
    amount = 0
    addcommlist = []
    start = datetime.utcnow()-timedelta(minutes=minutes)
    trange = datetime.strftime(start, "%Y-%m-%d %H:%M:%S")
    for i in range(1,maxsensor):
        name = "{}_GIC{:02d}_0001_0001".format(source,i)
        sn = "GIC{:02d}".format(i)
        try:
            if debug:
                print ("Checking table ", name)
            gicdat = dbselect(db,'x', name,'time > "{}"'.format(trange))
            status[name] = len(gicdat)
            if len(gicdat) > 0:
                amount += 1
                addcommlist.append(sn)
            gicdata.extend(gicdat)
        except:
            pass

    def is_number(var):
        try:
            var = float(var)
            if np.isnan(var):
                return False
            return True
        except:
            return False

    # remove nans and using absolutes
    cleangicdata = [np.abs(x) for x in gicdata if is_number(x)]
    if debug:
        print ("GIC data", cleangicdata)
    if len(cleangicdata) > 5:
        # get the 5 largest values and calculate median
        sortedgic = sorted(cleangicdata)
        gicvals = sortedgic[-maxvals:]
    else:
        gicvals = cleangicdata
    if len(cleangicdata) > 2:
        active = 1
        gicval = np.median(gicvals)
    else:
        active = 0
        gicval = 0.0
    if debug:
        print (gicval, active, amount)
    comment = "median of {} largest absolut values from {} stations ({})".format(maxvals, amount, ",".join(addcommlist))

    gicnewsql = _create_gicnow_sql(gicval,start,active, comment)

    return [gicnewsql]

def read_kpnow_data(source, debug=False):
    knewsql = ''
    if debug:
        print (" Kp: accessing {}".format(source))
    kpdata = read(source,starttime=datetime.utcnow()-timedelta(days=1))
    if kpdata.length()[0] < 1:
        print ("Kp CRITICAL: no data found")
        return ['']
    start = kpdata._testtime(kpdata.ndarray[0][-1])
    kpvalue = kpdata._get_column('var1')[-1]
    if debug:
        print (" Kp: Obtained {} at {}".format(kpvalue,start))
    knewsql = _create_kpnow_sql(kpvalue,start,start+timedelta(hours=3))
    return [knewsql]


def read_swepam_data(source, limit=5, debug=False):
    swsql = ''
    pdsql = ''
    if debug:
        print (" SWE: accessing {}".format(source))
    swpam = read(source,starttime=datetime.utcnow()-timedelta(days=1))
    if swpam.length()[0] < 1:
        print ("SWEPAM CRITICAL: no data found")
        return ['','']
    # get at least the last 5 individual records
    start = swpam._testtime(swpam.ndarray[0][-limit])
    pdlst = swpam._get_column('var1')[-limit:]
    swlst = swpam._get_column('var2')[-limit:]
    pd = np.nanmean(pdlst)
    sw = np.nanmean(swlst)
    if debug:
        print (" SWE: Obtained {} and {} at {}".format(sw,pd,start))
    swsql = _create_swnow_sql(sw,pd,start)
    pdsql = _create_pdnow_sql(sw,pd,start)
    return [swsql,pdsql]

def read_mag_data(source, limit=5, debug=False):
    magsql = ''
    if debug:
        print (" SWE: accessing {}".format(source))
    swmag = read(source,starttime=datetime.utcnow()-timedelta(days=1))
    if swmag.length()[0] < 1:
        print ("SW MAG CRITICAL: no data found")
        return ['']
    # get at least the last 5 individual records
    start = swmag._testtime(swmag.ndarray[0][-limit])
    maglst = swmag._get_column('z')[-limit:]
    mag = np.nanmean(maglst)
    if debug:
        print (" SWE: Obtained {} at {}".format(mag,start))
    magsql = _create_magnow_sql(mag,start)
    return [magsql]

def _create_gicnow_sql(gicval,start,active,comment):
    end = datetime.utcnow()
    gicnewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format('GICAUT','nowcast','inducedcurrents','gicreal',gicval,start,end,'TU Graz',comment,end,active,'nowcast','inducedcurrents','gicreal',gicval,start,end,'TU Graz',comment,end,active)
    return gicnewsql

def _create_kpnow_sql(kpval,start,end):
    active=0
    if end > datetime.utcnow()-timedelta(hours=3):
        active=1
    knewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format('Kp','nowcast','geomagactivity','geomag',kpval,start,end,'GFZ Potsdam','',datetime.utcnow(),active,'nowcast','geomagactivity','geomag',kpval,start,end,'GFZ Potsdam','',datetime.utcnow(),active)
    return knewsql


def _create_swnow_sql(sw,pd,start):
    end = datetime.utcnow()
    active=1
    if start < datetime.utcnow()-timedelta(hours=1):
        active=0
    knewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format('SolarWind','nowcast','solarwind','solar',sw,start,end,'ACE','',datetime.utcnow(),active,'nowcast','solarwind','solar',sw,start,end,'ACE','',datetime.utcnow(),active)
    return knewsql

def _create_magnow_sql(mag,start):
    end = datetime.utcnow()
    active=1
    if start < datetime.utcnow()-timedelta(hours=1):
        active=0
    knewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format('Bz','nowcast','solarwind','solar',mag,start,end,'ACE','',datetime.utcnow(),active,'nowcast','solarwind','solar',mag,start,end,'ACE','',datetime.utcnow(),active)
    return knewsql


def _create_pdnow_sql(sw,pd,start):
    end = datetime.utcnow()
    active=1
    if start < datetime.utcnow()-timedelta(hours=1):
        active=0
    knewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format('ProtonDensity','nowcast','solarwind','solar',pd,start,end,'ACE','',datetime.utcnow(),active,'nowcast','solarwind','solar',pd,start,end,'ACE','',datetime.utcnow(),active)
    return knewsql


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


def swtableinit(db, debug=True):
    """
    DESCRIPTION
        creating a SPACEWEATHER Database table
    """
    columns = ['sw_notation','sw_type','sw_group','sw_field','sw_value','value_min','value_max','sw_uncertainty', 'validity_start','validity_end','location','latitude','longitude','source','comment','date_added','active']
    coldef = ['CHAR(100)','TEXT','TEXT','TEXT','FLOAT','FLOAT','FLOAT','FLOAT', 'DATETIME','DATETIME','TEXT','FLOAT','FLOAT','TEXT','TEXT','DATETIME','INT']
    fulllist = []
    for i, elem in enumerate(columns):
        newelem = '{} {}'.format(elem, coldef[i])
        fulllist.append(newelem)
    sqlstr = ', '.join(fulllist)
    sqlstr = sqlstr.replace('sw_notation CHAR(100)', 'sw_notation CHAR(100) NOT NULL UNIQUE PRIMARY KEY')
    createtablesql = "CREATE TABLE IF NOT EXISTS SPACEWEATHER ({})".format(sqlstr)
    _execute_sql(db,[createtablesql], debug=debug)

def time_between(now,start,end):
    """
    now needs to be like datetime.now().time()
    start is a datetime.time for the starthour
    end is a datetime.time for the endhour
    """
    from datetime import time as dttime
    if start < end:
        if now >= start and now <= end:
            return True
    else:
        if now >= start or now <= end:
            return True
    return False

def main(argv):
    """
    METHODS:
        read_kpnow_data(source)
        read_swpam_data(source, limit=5)
        _execute_sql
        swtableinit()
    """
    version = "1.0.0"
    kpsource = '/srv/archive/external/gfz/kp/'
    swsource = '/srv/archive/external/esa-nasa/ace/raw/'
    kpname = 'gfzkp*'
    swename = '*_swepam_1m.txt'
    magname = '*_mag_1m.txt'
    configpath = '' # is only necessary for monitoring
    sqllist = []
    debug = False
    init = False # create table if TRUE
    statusmsg = {}


    usage = 'sw_extractor.py -c <config> -k <kpsource> -s <swesource> '
    try:
        opts, args = getopt.getopt(argv,"hc:k:s:o:p:b:e:ID",["config=","kpsource=","swesource=","init=","debug=",])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('-------------------------------------')
            print('Description:')
            print('sw-extractor.py extracts CME predictions from Scoreboard ')
            print('-------------------------------------')
            print('Usage:')
            print(usage)
            print('-------------------------------------')
            print('Options:')
            print('-c            : configuration data ')
            print('-k            : Kp source - default is ')
            print('-s            : SWE source - default is ')
            print('-------------------------------------')
            print('Examples:')
            print('---------')
            print('python3 sw_extractor.py -c /home/cobs/CONF/wic.cfg -k /srv/archive/external/gfz/kp/gfzkp* -s /srv/archive/external/esa-nasa/ace/raw/ -D')
            print('---------')
            sys.exit()
        elif opt in ("-c", "--config"):
            configpath = os.path.abspath(arg)
        elif opt in ("-k", "--kpsource"):
            kpsource = os.path.abspath(arg)
        elif opt in ("-s", "--swesource"):
            swsource = os.path.abspath(arg)
        elif opt in ("-n", "--kpname"):
            kpname = arg
        elif opt in ("-w", "--swename"):
            swname = arg
        elif opt in ("-m", "--magname"):
            magname = arg
        elif opt in ("-I", "--init"):
            init = True
        elif opt in ("-D", "--debug"):
            debug = True

    if debug:
        print ("Running sw-extractor version:", version)

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


    # 3. Read Kp data:
    # ###########################
    try:
        if debug:
            print(" -  Getting Kp:")
        sqllist = read_kpnow_data(os.path.join(kpsource,kpname),debug=debug)
        statusmsg['Kp access'] = 'success'
    except:
        statusmsg['Kp access'] = 'failed'

    # 4. Read ACE swepam data:
    # ###########################
    try:
        if debug:
            print(" -  Getting ACE sw:")
        newsql = read_swepam_data(os.path.join(swsource,swename),debug=debug)
        sqllist.extend(newsql)
        statusmsg['ACE swepam access'] = 'success'
    except:
        statusmsg['ACE swepam access'] = 'failed'

    # 5. Read ACE mag data:
    # ###########################
    try:
        if debug:
            print(" -  Getting ACE mag:")
        newsql = read_mag_data(os.path.join(swsource,magname),debug=debug)
        sqllist.extend(newsql)
        statusmsg['ACE mag access'] = 'success'
    except:
        statusmsg['ACE mag access'] = 'failed'

    # 6. Read GIC data:
    # ###########################
    try:
        if debug:
            print(" -  Getting GIC:")
        newsql = read_gicnow_data(db,source='GICAUT',maxsensor=9, minutes=5, debug=debug)
        sqllist.extend(newsql)
        statusmsg['GIC data access'] = 'success'
    except:
        statusmsg['GIC data access'] = 'failed'

    # 7. Read GOES data:
    # ###########################
    try:
        if debug:
            print (" - Running GOES")
        goespath = '/srv/archive/external/esa-nasa/goes'
        newsql = read_xrs_data(os.path.join(goespath,'XRS_GOES16*'), debug=debug)
        sqllist.extend(newsql)
        statusmsg['XRS data access'] = 'success'
    except:
        statusmsg['XRS data access'] = 'failed'

    # 8. Read PREDSTORM data:
    # ###########################
    try:
        if debug:
            print (" - Running PREDSTORM")
        predpath = '/srv/archive/external/helio4cast/predstorm'
        psql = read_predstorm_data(os.path.join(predpath,'PREDSTORM*'), debug=debug)
        sqllist.extend(psql)
        statusmsg['PREDSTORM data access'] = 'success'
    except:
        # no predstorm data between 23:00 and 2:00 MET
        # just put success message if hitting except in this time range
        statusmsg['PREDSTORM data access'] = 'failed'
        if time_between(datetime.utcnow().time(),dttime(21,0),dttime(0,0)):
            statusmsg['PREDSTORM data access'] = 'success'

    sqllist = [el for el in sqllist if el]

    if debug:
        print ("Debug selected - sql call looks like:")
        print (sqllist)
    else:
      for dbel in connectdict:
        db = connectdict[dbel]
        print ("     -- Writing data to DB {}".format(dbel))
        if init:
            swtableinit(db)
        if len(sqllist) > 0:
            _execute_sql(db,sqllist, debug=debug)


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
