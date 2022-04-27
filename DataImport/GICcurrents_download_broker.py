#!/usr/bin/env python3
# coding=utf-8

"""
DESCRIPTION
Appliaction to extract SolarWind Parameters from ACE (or DSCOVR) and Kp from the GFZ

REQUIREMENTS 

TESTING

   1. delete one of the json inputs from the memory file in /srv/archive/external/esa-nasa/cme
   2. run app without file option
       python3 cme_extractor.py -c /home/cobs/CONF/wic.cfg -o db,telegram -p /srv/archive/external/esa-nasa/cme/

"""

from magpy.database import *
from magpy.opt import cred as mpcred
import getopt
import dateutil.parser as dparser

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, load_current_data_sub
from martas import martaslog as ml
from martas import sendmail as sm
from acquisitionsupport import GetConf2 as GetConf


import urllib.request
import json 
with urllib.request.urlopen("https://feanpc41.tugraz.at:8000/") as url:
    data = json.loads(url.read().decode())
    print(data)

#https://feanpc41.tugraz.at:8000/

sys.exit()


def read_gic_data(source):
    gicsql = ''
    amount=0
    # source should be a list of tablenames
    for dataid in source:
        #gicdata = readDB(source)
        # get last 30 gic elements from each table if time > datetime.utcnow()-timedelta(minutes=5)
        if len(gicdata) > 0:
            amount++
        # exent
    # remove nan 
    # sort
    # if len > 0
    #    if len > 10
    #       from all tables get the 10 largest values [:-10]
    #       giclist = giclist[:-10]
    #    calculate the median of these values
    #    meangic=np.median(giclist)
    # else
    #    active = 0
    #    meangic = 555000
    start = datetime.utcnow()-timedelta(minutes=5)
    end = datetime.utcnow()
    gicsql = _create_gicnow_sql(meangic,start,end, amount)
    return gicsql

def _create_gicnow_sql(gicval,start,end,amount):
    active=0
    if gicval == 555000:
        active=1
        gicval = float(nan)
    gicsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format('GICmax','nowcast','maximumgic','gic',gicval,start,end,'TU Graz','',datetime.utcnow(),active,'nowcast','maximumgic','gic',kpval,start,end,'TU Graz','',datetime.utcnow(),active)    
    return gicsql


def read_kpnow_data(source):
    knewsql = ''
    kpdata = read(source)
    start = kpdata.ndarray[0][-1]
    kpvalue = kpdata._get_column('var1')[-1]
    knewsql = _create_kpnow_sql(kpval,start,start+timedelta(hours=3))
    return knewsql


def read_swpam_data(source, limit=5):
    swsql = ''
    pdsql = ''
    swpam = read(source)
    # get at least the last 5 individual records 
    start = kpdata.ndarray[0][-limit]
    pd = swpam._get_column('var1')[-limit:]
    sw = swpam._get_column('var2')[-limit:]
    swsql = _create_swnow_sql(sw,pd,start)
    pdsql = _create_pdnow_sql(sw,pd,start)
    return [swsql,pdsql]


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
        extract_config()    -> read analysis config
        get_readlist()      -> get calls to read data chunks
        get_data()          ->   
        get_chunk_config()  -> obtain details of chunk
        get_chunk_feature() -> get statistical features for each chunk
           - get_emd_features()
               - obtain_basic_emd_characteristics()
                   - get_features()
           - get_wavelet_features()

    """
    version = "1.0.0"
    kpsource = 'https://kauai.ccmc.gsfc.nasa.gov/CMEscoreboard/'
    swsource = 'https://kauai.ccmc.gsfc.nasa.gov/CMEscoreboard/'
    configpath = '' # is only necessary for monitoring
    debug = False
    init = False # create table if TRUE
    statusmsg = {}


    usage = 'sw-extractor.py -c <config> -s <source> -o <output> -p <path> -b <begin> -e <end>'
    try:
        opts, args = getopt.getopt(argv,"hc:s:o:p:b:e:ID",["config=","source=","output=","path=","begin=","end=","init=","debug=",])
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
            print('-s            : source - default is ')
            print('-o            : output (list like db,file)')
            print('-------------------------------------')
            print('Examples:')
            print('---------')
            print('python3 sw_extractor.py -c /home/cobs/CONF/wic.cfg -o file,db,telegram,email -p /srv/archive/external/esa-nasa/cme/ -D')
            print('---------')
            sys.exit()
        elif opt in ("-c", "--config"):
            configpath = os.path.abspath(arg)
        elif opt in ("-s", "--source"):
            swsource = arg
        elif opt in ("-o", "--output"):
            output = arg.split(',')
        elif opt in ("-I", "--init"):
            init = True
        elif opt in ("-D", "--debug"):
            debug = True

    if debug:
        print ("Running cme-extractor version:", version)
        print ("Selected output:", output)

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
        sqllist = read_kpnow_data(kpsource)
        statusmsg['Kp access'] = 'success'
    except:
        statusmsg['Kp access'] = 'failed'

    # 4. Read ACE data:
    # ###########################
    try:
        newsql = read_kpnow_data(kpsource)
        sqllist.extend(newsql)
        statusmsg['ACE access'] = 'success'
    except:
        statusmsg['ACE access'] = 'failed'
    
    sqllist = [el for el in sqllist if el]

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
