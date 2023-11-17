#!/usr/bin/env python3
# coding=utf-8

"""
DESCRIPTION
Appliaction to extract Status Parameters from the Observatory database

REQUIREMENTS

TESTING

   1. delete one of the json inputs from the memory file in /srv/archive/external/esa-nasa/cme
   2. run app without file option
       

   python3 status_extractor.py -c /home/cobs/CONF/wic.cfg -s testcfg -D
   
   
   # config directory looks like
   sourcedict = {"CO2 GMO tunnel": {"source":"MQ135_20220214_0001_0001", "key":"var1", "type":"gas", "group":"tunnel condition", "field":"gmo", "pierid":"", "range":30,"mode":"max","value_unit":"ppm","warning_high":2000,"critical_high":5000}, "Temperature GMO tunnel": {"source":"BE280_0X76I2C00003_0001_0001", "key":"t1", "type":"environment", "group":"tunnel condition", "field":"gmo", "pierid":"", "range":30,"mode":"mean","value_unit":"C","warning_high":10,"critical_high":20}, "Humidity GMO tunnel": {"source":"BE280_0X76I2C00003_0001_0001", "key":"var1", "type":"environment", "group":"tunnel condition", "field":"gmo", "pierid":"", "range":30,"mode":"mean","value_unit":"%"}, "Pressure GMO tunnel": {"source":"BE280_0X76I2C00003_0001_0001", "key":"var2", "type":"bme280", "group":"tunnel condition", "field":"", "pierid":"", "range":30,"mode":"mean","value_unit":"hP"}, "Vehicle alarm": {"source":"RCS2F2_20160114_0001_0001", "key":"x", "type":"environment", "group":"traffic", "field":"gmo", "pierid":"", "range":120,"mode":"max","warning_high":1}, "Meteo temperature": {"source":"METEOSGO_adjusted_0001_0001", "key":"f", "type":"meteorology", "group":"meteorology", "field":"sgo", "pierid":"", "range":30,"mode":"median","warning_high":30,"warning_high":40}, "Meteo humidity": {"source":"METEOSGO_adjusted_0001_0001", "key":"t1", "type":"meteorology", "group":"meteorology", "field":"sgo", "pierid":"", "range":30,"mode":"median","warning_high":30,"warning_high":40}}
   
   write_config(sourcepath, sourcedict)
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


def write_config(memorypath, memdict):
        """
        DESCRIPTION
             write memory

         -> taken from IMBOT
        """
        try:
            with open(memorypath, 'w', encoding='utf-8') as f:
                json.dump(memdict, f, ensure_ascii=False, indent=4)
        except:
            return False
        return True


def read_config(memorypath,debug=False):
        """
        DESCRIPTION
             read memory

         -> taken from IMBOT
        """
        memdict = {}
        try:
            if os.path.isfile(memorypath):
                if debug:
                    print ("Reading memory: {}".format(memorypath))
                with open(memorypath, 'r') as file:
                    memdict = json.load(file)
            else:
                print ("Memory path not found - please check (first run?)")
        except:
            print ("error when reading file {} - returning empty dict".format(memorypath))
        if debug:
            print ("Found in Memory: {}".format([el for el in memdict]))
        return memdict


def read_db_data(db,source,key,trange=30,endtime=datetime.utcnow(),mode="mean",debug=False):
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
    value_min=0
    value_max=0
    uncert=0
    starttime = endtime-timedelta(minutes=trange)
    ok = True
    if ok:
        data = dbselect(db,key, source,'time > "{}" AND time < "{}"'.format(starttime,endtime))
        if debug:
            print ("DEBUG: got dataset: ", data)
        cleandata = [x for x in data if x and not np.isnan(x)]
        if len(cleandata) > 0:
            value_min = np.min(cleandata)
            value_max = np.max(cleandata)
            uncert = np.std(cleandata)
            if mode=="median":
                value = np.median(cleandata)
            elif mode=="min":
                value = value_min
            elif mode=="max":
                value = value_max
            elif mode=="std":
                value = np.std(cleandata)
            else: # mode == mean
                value = np.mean(cleandata)
            active = 1

    if debug:
        print ("DEBUG: returning value={}, starttime={}, endtime={} and active={}".format(value,starttime,endtime,active))

    return value, value_min, value_max, uncert, starttime, endtime, active

def check_highs(value, value_unit, warning_high, critical_high, warning_low, critical_low):
    msg = 'values OK'
    if value:
        if critical_high and value >= critical_high:
            msg = "CRITCAL STATUS: value exceeding {} {}".format(critical_high, value_unit)
        elif warning_high and value >= warning_high:
            msg = "WARNING: value exceeding {} {}".format(warning_high, value_unit)
        elif critical_low and value <= critical_low:
            msg = "CRITICAL STATUS: value below {} {}".format(critical_low, value_unit)
        elif warning_low and value <= warning_low:
            msg = "WARNING: value below {} {}".format(warning_low, value_unit)
    return msg

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


def statustableinit(db, tablename="COBSSTATUS", debug=True):
    """
    DESCRIPTION
        creating a STATUS Database table
    """
    columns = ['status_notation','status_type','status_group','status_field','status_value','value_min','value_max','value_std','value_unit','warning_high','critical_high','warning_low','critical_low', 'validity_start','validity_end','location','latitude','longitude','source','comment','date_added','active']
    coldef = ['CHAR(100)','TEXT','TEXT','TEXT','FLOAT','FLOAT','FLOAT','FLOAT','TEXT','FLOAT','FLOAT','FLOAT','FLOAT', 'DATETIME','DATETIME','TEXT','FLOAT','FLOAT','TEXT','TEXT','DATETIME','INT']
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
    warningmsg = {}
    warnlog = "/var/log/magpy/cron-info-warn-status.log"
    warnconfig ="/etc/martas/telegram_cobsnoti.cfg"
    warnreceiver = "telegram"

    sourcedict = {"CO2 GMO": {"source":"MQ135_20220214_0001_0001", "key":"var1", "type":"gas", "group":"tunnel condition", "field":"", "pierid":"", "range":30,"mode":"max"}}


    usage = 'status_extractor.py -c <config> -s <source> -e <endtime> -r <receiver> -w <warnconfig> -l <warnlog>'
    try:
        opts, args = getopt.getopt(argv,"hc:s:e:r:w:l:ID",["config=","source=","endtime=","receiver=","warnconfig=","warnlog=","init=","debug=",])
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
            print('-r            : receiver for warnings - default telegram ')
            print('-w            : config for warnings - default.. ')
            print('-l            : logpath for warning - /var/log/magpy/cron-info-warn-status.log ')
            print('-------------------------------------')
            print('Examples:')
            print('---------')
            print('python3 status_extractor.py -c /home/cobs/CONF/wic.cfg -s /home/cobs/CONF/statusdata.cfg -D')
            print('---------')
            sys.exit()
        elif opt in ("-c", "--config"):
            configpath = os.path.abspath(arg)
        elif opt in ("-s", "--sourcepath"):
            sourcepath = os.path.abspath(arg)
        elif opt in ("-e", "--endtime"):
            endtime = DataStream()._testtime(arg)
        elif opt in ("-r", "--receiver"):
            warnreceiver = arg
        elif opt in ("-w", "--warnconfig"):
            warnconfig = os.path.abspath(arg)
        elif opt in ("-l", "--warnlog"):
            warnlog = os.path.abspath(arg)
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
    config = DefineLogger(config=config, category = "Info", job=os.path.basename(__file__), newname='mm-info-status.log', debug=debug)
    if debug:
        print (" -> Done")

    # 2. database:
    # ###########################
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        connectdict = config.get('conncetedDB')
    except:
        statusmsg['database'] = 'database failed'


    # 3. Read job json list:
    # ###########################
    try:
        if debug:
            print (" Reading dictionary with status elements to extract....")
            print (sourcepath)
        if not sourcepath.endswith('testconfig'):
            sourcedict = read_config(sourcepath,debug=False)
            statusmsg['Source config access'] = 'success'
        else:
            print (" !Test dictionary selected !")
    except:
        statusmsg['Source config access'] = 'failed'

    #Necessary jobs: CO2, Temp, Meteo, Earthquakes, Magnetic Dir,
    # groups: tunnel condictions, observatory area, power and building, meteorology, magnetism, seismology, etc
    # 4. Extract parameter for all jobs:
    # ###########################
    try:
        for element in sourcedict:
            try:
                #element is the unique status_notation
                warnmsg = 'Fine'
                if debug:
                     print ("Checking ", element)
                job = sourcedict.get(element)
                value, value_min, value_max, uncert, starttime, endtime, active = read_db_data(db, job.get("source"),job.get("key"),trange=job.get("range"),endtime=endtime,mode=job.get("mode"), debug=debug)
                warnmsg = check_highs(value,job.get("value_unit"),job.get("warning_high",0),job.get("critical_high",0),job.get("warning_low",0),job.get("critical_low",0))
                newsql = _create_sql(element,job.get("type"),job.get("group",""),job.get("field",""),value, value_min, value_max, uncert,job.get("value_unit",""),job.get("warning_high",0),job.get("critical_high",0),job.get("warning_low",0),job.get("critical_low",0),starttime,endtime,active,job.get("source",""),job.get("location",""),job.get("comment",""))
                sqllist.extend(newsql)
                warningmsg[element] = warnmsg
                statusmsg[element] = 'success'
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

    # Warning section
    # ###########################
    if warningmsg and not debug:
        warnlog = ml(logfile=warnlog,receiver=warnreceiver)
        warnlog.telegram['config'] = warnconfig
        warnlog.msg(warningmsg)
    else:
        print ("Debug selected - warningmsg looks like:")
        print (warningmsg)


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
