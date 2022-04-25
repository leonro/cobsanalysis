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
    
    # remove nans and using absolutes
    cleangicdata = [np.abs(x) for x in gicdata if not np.isnan(x)]
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
        gicval = float(nan)
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
        sqllist = read_kpnow_data(os.path.join(kpsource,kpname),debug=debug)
        statusmsg['Kp access'] = 'success'
    except:
        statusmsg['Kp access'] = 'failed'

    # 4. Read ACE swepam data:
    # ###########################
    try:
        newsql = read_swepam_data(os.path.join(swsource,swename),debug=debug)
        sqllist.extend(newsql)
        statusmsg['ACE swepam access'] = 'success'
    except:
        statusmsg['ACE swepam access'] = 'failed'

    # 5. Read ACE mag data:
    # ###########################
    try:
        newsql = read_mag_data(os.path.join(swsource,magname),debug=debug)
        sqllist.extend(newsql)
        statusmsg['ACE mag access'] = 'success'
    except:
        statusmsg['ACE mag access'] = 'failed'

    # 6. Read GIC data:
    # ###########################
    try:
        newsql = read_gicnow_data(db,source='GICAUT',maxsensor=9, minutes=5, debug=debug)
        sqllist.extend(newsql)
        statusmsg['GIC data access'] = 'success'
    except:
        statusmsg['GIC data access'] = 'failed'

    
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
