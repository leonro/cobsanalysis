#!/usr/bin/env python3
# coding=utf-8

"""
DESCRIPTION
Appliaction to extract Status Parameters from the Observatory database

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

def read_db_data(db,source,key,range=30,endtime=datetime.utcnow(),mode="mean",debug=False):
    """
    DESCRIPTION
        Read date for a certain table, a given key and timerange from the database.
        Calculate the parameter as defined by mode.
    RETURN
        Returns the calculated parameter and an active bool (0: no data available or problem with calc; 1: everything fine)
    """
    sqllist = []
    active=0
    value=0
    starttime = endtime-timedelta(minutes=30)
    ok = True
    if ok:
        data = dbselect(db,key, source,'time > "{}" AND time < "{}"'.format(starttime,endtime))
        status[name] = len(gicdat)
        data = readDB(db,source,starttime=starttime,endtime=endtime)
        cleandata = [x for x in data if not np.isnan(x)]
        if len(cleandata) > 0:
            if mode=="median":
                value = np.median(cleandata)
            elif mode=="min":
                value = np.min(cleandata)
            elif mode=="max":
                value = np.max(cleandata)
            elif mode=="max":
                value = np.std(cleandata)
            else: # mode == mean
                value = np.mean(cleandata)
            active = 1

    return value, starttime, endtime, active


def _create_sql(notation,type,group,field,value,start,end,active,source,comment):
    now = datetime.utcnow()
    sql = "INSERT INTO COBSSTATUS (status_notation,status_type,status_group,status_field,status_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE status_type = '{}',status_group = '{}',status_field = '{}',status_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format(notation,type,group,field,value,start,end,source,comment,now,active,type,group,field,value,start,end,source,comment,now,active)
    return sql

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


def statustableinit(db, tablename="COBSSTATUS", debug=True):
    """
    DESCRIPTION
        creating a STATUS Database table
    """
    columns = ['status_notation','status_type','status_group','status_field','status_value','value_min','value_max','status_uncertainty', 'validity_start','validity_end','location','latitude','longitude','source','comment','date_added','active']
    coldef = ['CHAR(100)','TEXT','TEXT','TEXT','FLOAT','FLOAT','FLOAT','FLOAT', 'DATETIME','DATETIME','TEXT','FLOAT','FLOAT','TEXT','TEXT','DATETIME','INT']
    fulllist = []
    for i, elem in enumerate(columns):
        newelem = '{} {}'.format(elem, coldef[i])
        fulllist.append(newelem)
    sqlstr = ', '.join(fulllist)
    sqlstr = sqlstr.replace('status_notation CHAR(100)', 'status_notation CHAR(100) NOT NULL UNIQUE PRIMARY KEY')
    createtablesql = "CREATE TABLE IF NOT EXISTS {} ({})".format(tablename,sqlstr)
    _execute_sql(db,[createtablesql], debug=debug)


def main(argv):
    """
    METHODS:
        read_db_data(source)
        _execute_sql
        swtableinit()
    """
    version = "1.0.0"
    configpath = '' # is only necessary for monitoring
    sqllist = []
    debug = False
    init = False # create table if TRUE
    endtime=datetime.utcnow()
    statusmsg = {}


    usage = 'status_extractor.py -c <config> -k <kpsource> -s <swesource> '
    try:
        opts, args = getopt.getopt(argv,"hc:k:s:o:p:b:e:ID",["config=","kpsource=","swesource=","init=","debug=",])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('-------------------------------------')
            print('Description:')
            print('status-extractor.py extracts observatory status data ')
            print('-------------------------------------')
            print('Usage:')
            print(usage)
            print('-------------------------------------')
            print('Options:')
            print('-c            : configuration data ')
            print('-e            : endtime - default is now ')
            print('-s            : sourcepath - a json dictionary ')
            print('-------------------------------------')
            print('Examples:')
            print('---------')
            print('python3 status_extractor.py -c /home/cobs/CONF/wic.cfg -k /srv/archive/external/gfz/kp/gfzkp* -s /srv/archive/external/esa-nasa/ace/raw/ -D')
            print('---------')
            sys.exit()
        elif opt in ("-c", "--config"):
            configpath = os.path.abspath(arg)
        elif opt in ("-s", "--sourcepath"):
            sourcepath = os.path.abspath(arg)
        elif opt in ("-e", "--endtime"):
            endtime = DataStream()._testtime(arg)
        elif opt in ("-I", "--init"):
            init = True
        elif opt in ("-D", "--debug"):
            debug = True

    if debug:
        print ("Running status-extractor version:", version)

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


    # 3. Read job json list:
    # ###########################
    try:
        sourcedict = json.read(sourcepath)
        statusmsg['Source config access'] = 'success'
    except:
        statusmsg['Source config access'] = 'failed'

    sourcedict = {"CO2 GMO": {"source":"MQ135", "key":"x", "type":"gas", "group":"tunnel condition", "field":"", "pierid":"", "range":30,"mode":"max"}}
    #Necessary jobs: CO2, Temp, Meteo, Earthquakes, Magnetic Dir,
    # groups: tunnel condictions, observatory area, power and building, meteorology, magnetism, seismology, etc
    # 4. Extract parameter for all jobs:
    # ###########################
    try:
        for job in sourcedict:
            try:
                #jobname is the unique status_notation
                print (job,job.get(type))
                value, starttime, endtime, active = read_db_data(db, job.get(source),job.get(key),job.get(range),endtime=endtime,job.get(mode), debug=debug)
                newsql = _create_sql(job,job.get(type),job.get(group),job.get(field),value,start,end,active,job.get(source),job.get(comment))
                sqllist.extend(newsql)
                statusmsg[job] = 'success'
            except:
                statusmsg[job] = 'success'
        statusmsg['Joblist treatment'] = 'success'
    except:
        statusmsg['Joblist treatment'] = 'failed'

    sqllist = [el for el in sqllist if el]

    if debug:
        print ("Debug selected - sql call looks like:")
        print (sqllist)
    else:
        for dbel in connectdict:
            db = connectdict[dbel]
            print ("     -- Writing data to DB {}".format(dbel))
            if init:
                statustableinit(db)
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
