#!/usr/bin/env python3
# coding=utf-8

"""
DESCRIPTION
Appliaction to extract CME information from the CME scoreboard and report such data by mail or telegram
Data is also provided in a database

REQUIREMENTS 

Python packages: pandas, lxml

IMPORTANT
Please note: Altough lots of information is read by configuration or option data, this file contains a dictionary with language dependent contents for messages. Please modify if needed.

Please note: e-mail receivers are not yet defined - TODO

If debug is selected (and telegram or email are selected), then a test message is send. 

TODO
email function not yet working - test martas sm elsewhere and then correct

TESTING

   1. delete one of the json inputs from the memory file in /srv/archive/external/esa-nasa/cme
   2. run app without file option
       python3 cme_extractor.py -c /home/cobs/CONF/wic.cfg -o db,telegram -p /srv/archive/external/esa-nasa/cme/

"""


import requests
import pandas as pd
import dateutil.parser as dparser
from magpy.database import *
from magpy.opt import cred as mpcred
import getopt
import json
import telegram_send

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, load_current_data_sub
from martas import martaslog as ml
from martas import sendmail as sm
from acquisitionsupport import GetConf2 as GetConf

def write_memory(memorypath, memdict):
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


def read_memory(memorypath,debug=False):
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

def read_meta_data(sourcepath, filename = "meta*.txt"):
        """
        DESCRIPTION
             read additional metainformation for the specific observatory
         -> taken from IMBOT
        """
        newhead = {}
        if os.path.isfile(sourcepath):
            print (" ReadMetaData: from sourcepath=file")
            metafilelist = [sourcepath]
        else:
            print (" ReadMetaData: from sourcepath=directory")
            metafilelist = glob.glob(os.path.join(sourcepath,filename))
        print (" Loading meta file:", metafilelist)

        if len(metafilelist) > 0:
            if os.path.isfile(metafilelist[0]):
                with open(metafilelist[0], 'r') as infile:
                    for line in infile:
                        if not line.startswith('#'):
                            if line.find(" : ") > 0 or line.find("\t:\t") > 0 or line.find(" :\t") > 0 or line.find("\t: ") > 0:
                                paralist = line.replace(" ","").split(":")
                                # convert paralist [0] to MagPy keys
                                key = paralist[0].strip()
                                try:
                                    newhead[key] = paralist[1].strip()
                                except:
                                    pass
        return newhead


def get_new_inputs(memory,newdict,report='new'):
        """
        DESCRIPTION
            will return a dictionary with key/value pairs from dir analysis
            which are not in memory
        """
        # newly uploaded
        newlist = []
        tmp = {k:v for k,v in newdict.items() if k not in memory}
        for key in tmp:
            newlist.append(key)
        # newly uploaded and updated
        updatelist = []
        C = {k:v for k,v in newdict.items() if k not in memory or v != memory[k]}
        for key in C:
            if not key in newlist:
                updatelist.append(key)
        #print (updatelist)
        #print (newlist)
        full = {**memory,**newdict}

        return full, newlist, updatelist


def _get_cme_data(res):
    CMEstart = ''
    CMEarrival = ''
    KPrange = ''
    SCOREamount = 0
    for line in res:
        #print (line)
        for el in line:
            if str(el).find("CME:") > -1 and not CMEstart:
                start = line[0].replace('CME: ','').replace('-CME-001','')
                CMEstart = dparser.parse(start,fuzzy=True)
                CMEname = 'CME-{}'.format(datetime.strftime(CMEstart,"%Y%m%dT%H%M%S"))
                CMEstartst = datetime.strftime(CMEstart,"%Y-%m-%dT%H:%M:%S")
            if str(el).find("Average of all Methods") > -1  and not CMEarrival:
                CMEarrival = dparser.parse(line[0],fuzzy=True)
                CMEarrivalst = datetime.strftime(CMEarrival,"%Y-%m-%dT%H:%M:%S")
                KPrange = line[5].replace('Max Kp Range: ','')
    SCOREamount = len(res)-4 # remove header, comment, CME:, and Average lines 
    if CMEstart:
        #print (CMEname,CMEstartst, CMEarrivalst, KPrange, SCOREamount)
        return CMEname,CMEstartst, CMEarrivalst, KPrange, SCOREamount
    else:
        return '','','','',0


def get_cme_dictionary_scoreboard(url='https://kauai.ccmc.gsfc.nasa.gov/CMEscoreboard/'):
    """
    DESCRIPTION
       Extracts CME data from scoreboad webpage and put that into a dictionary
    """

    dic = {}
    html = requests.get(url).content
    reqhtml = html[str(html).find('Active CME'):]
    # get dataframe
    df_list = pd.read_html(reqhtml)

    for df in df_list:
        res = df.values.tolist()
        CMEname, CMEstart, CMEarrival, KPrange, SCOREamount = _get_cme_data(res)
        if CMEstart:
            # Put data into a dictionary
            dic[CMEname] = {'start': CMEstart, 'arrival': CMEarrival, 'KPrange': KPrange, 'N': SCOREamount}
    return dic


def _extract_sql_criteria(valuedict, hours_threshold=12):
    end = dparser.parse(valuedict.get('arrival'),fuzzy=True) + timedelta(hours=hours_threshold)
    kprange = valuedict.get('KPrange').split('-')
    if len(kprange) == 2:
        minval = float(kprange[0])
        maxval = float(kprange[1])
    if not end < datetime.utcnow():
        return True,minval,maxval
    else:
        return False,minval,maxval

def _create_k_sql(arrival,maxval):
    CMEarrival = dparser.parse(arrival,fuzzy=True)
    valid_from = CMEarrival.replace(hour=0,minute=0,second=0,microsecond=0)
    valid_until = CMEarrival.replace(hour=23,minute=59,second=59,microsecond=0)
    active=0
    if valid_until > datetime.utcnow():
        active=1
    knewsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,sw_value,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},'{}','{}','{}','{}','{}',{}) ON DUPLICATE KEY UPDATE sw_type = '{}',sw_group = '{}',sw_field = '{}',sw_value = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='{}',date_added = '{}',active = {} ".format('Kp','forecast','geomagactivity','geomag',maxval,valid_from,valid_until,'CMEscoreboard','',datetime.utcnow(),active,'forecast','geomagactivity','geomag',maxval,valid_from,valid_until,'CMEscoreboard','',datetime.utcnow(),active)    
    return knewsql
    

def update_database(db, full,new,up,swsource,hours_threshold=12,debug=False):
    """
    DESCRIPTION
        this method will update the SPACEWEATHER Database
    REQUIRES
        a table called SPACEWEATHER
    """
    
    # cleanup
    sqllist = []
    #delsql = "DELETE FROM SPACEWEATHER WHERE sw_group LIKE 'cmescore' and validity_end < '{}'".format(datetime.utcnow() - timedelta(hours=hours_threshold))
    delsql = "DELETE FROM SPACEWEATHER WHERE sw_type LIKE 'forecast' and validity_end < '{}'".format(datetime.utcnow() - timedelta(hours=hours_threshold))
    sqllist.append(delsql)
    # add new inputs
    for el in new:
        valdict = full.get(el)
        cont, minval,maxval = _extract_sql_criteria(valdict, hours_threshold=hours_threshold)
        if cont:
            addsql = "INSERT INTO SPACEWEATHER (sw_notation,sw_type,sw_group,sw_field,value_min,value_max,validity_start,validity_end,source,comment,date_added,active) VALUES ('{}', '{}', '{}','{}',{},{},'{}','{}','{}','based on {} estimates','{}',{})".format(el,'forecast','cmescore','helio',minval,maxval,valdict.get('start'),valdict.get('arrival'),swsource,valdict.get('N'),datetime.utcnow(),1)
            sqllist.append(addsql)
            knewsql = _create_k_sql(valdict.get('arrival'),maxval)
            sqllist.append(knewsql)

    # add updates
    for el in up:
        valdict = full.get(el)
        cont, minval,maxval = _extract_sql_criteria(valdict, hours_threshold=hours_threshold)
        if cont:
            upsql = "UPDATE SPACEWEATHER SET sw_type = '{}',sw_group = '{}',sw_field = '{}',value_min = {},value_max = {},validity_start = '{}',validity_end = '{}',source = '{}',comment='based on {} estimates',date_added = '{}',active = {} WHERE sw_notation LIKE '{}'".format('forecast','cmescore','helio',minval,maxval,valdict.get('start'),valdict.get('arrival'),swsource,valdict.get('N'),datetime.utcnow(),1,el)
            sqllist.append(upsql)
            knewsql = _create_k_sql(valdict.get('arrival'),maxval)
            sqllist.append(knewsql)

    _execute_sql(db,sqllist, debug=debug)

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
    swsource = 'https://kauai.ccmc.gsfc.nasa.gov/CMEscoreboard/'
    path = ''
    creddb = 'cobsdb'
    output = []
    stime = ''
    etime = ''
    configpath = '' # is only necessary for monitoring
    cmedict = {}
    hours_threshold = 12
    debug = False
    init = False # create table if TRUE
    statusmsg = {}

    receivers = {'deutsch' : {'userid1': {'name':'roman leon', 'email':'roman_leonhardt@web.de', 'language':'deutsch'}}}

    languagedict = {'english' : {'msgheader': "Coronal mass ejection - CME",
                                 'msgnew':'New CME started at ',
                                 'msgupdate':'Update on CME from ',
                                 'msgarrival':'Estimated arrival: ',
                                 'msgpred':'Expected geomagnetic activity (Kp): ',
                                 'timezone':'UTC',
                                 'msgref':'Based on experimental data from [CMEscoreboard](https://kauai.ccmc.gsfc.nasa.gov/CMEscoreboard/)',
                                 'msguncert':'arrival time estimates usually have an uncertainty of +/- 7 hrs',
                                 'channeltype':'telegram',
                                 'channelconfig':'/etc/martas/telegram.cfg',
                                },
                    'deutsch' : {'msgheader': "Sonneneruption - CME",
                                 'msgnew':'Neuer CME (koronaler Massenauswurf) am ',
                                 'msgupdate':'Update zu CME vom ',
                                 'msgarrival':'Geschätzte Ankunftszeit: ',
                                 'msgpred':'Erwartete geomagnetische Aktivität (Kp): ',
                                 'timezone':'UTC',
                                 'msgref':'Basierend auf experimentellen Daten des [CMEscoreboard](https://kauai.ccmc.gsfc.nasa.gov/CMEscoreboard/)',
                                 'msguncert':'geschätzte Ankunftszeiten sind meistens mit Fehlern von +/- 7 hrs behaftet',
                                 'channeltype':'telegram',
                                 'channelconfig':'/etc/martas/telegram.cfg',
                                }
                    }

    #'channelconfig':'/etc/martas/tg_weltraum.cfg',
    #'channelconfig':'/etc/martas/tg_space.cfg',

    usage = 'cme-extractor.py -c <config> -s <source> -o <output> -p <path> -b <begin> -e <end>'
    try:
        opts, args = getopt.getopt(argv,"hc:s:o:p:b:e:ID",["config=","source=","output=","path=","begin=","end=","init=","debug=",])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('-------------------------------------')
            print('Description:')
            print('cme-extractor.py extracts CME predictions from Scoreboard ')
            print('-------------------------------------')
            print('Usage:')
            print(usage)
            print('-------------------------------------')
            print('Options:')
            print('-c            : configuration data ')
            print('-s            : source - default is ')
            print('-o            : output (list like db,file)')
            print('-p            : path')
            print('-b            : starttime')
            print('-e            : endtime')
            print('-------------------------------------')
            print('Examples:')
            print('---------')
            print('python3 cme_extractor.py -c /home/cobs/CONF/wic.cfg -o file,db,telegram,email -p /srv/archive/external/esa-nasa/cme/ -D')
            print('---------')
            sys.exit()
        elif opt in ("-c", "--config"):
            configpath = os.path.abspath(arg)
        elif opt in ("-s", "--source"):
            swsource = arg
        elif opt in ("-o", "--output"):
            output = arg.split(',')
        elif opt in ("-p", "--path"):
            path = os.path.abspath(arg)
        elif opt in ("-b", "--starttime"):
            stime = arg
        elif opt in ("-e", "--endtime"):
            etime = arg
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
    config = DefineLogger(config=config, category = "Info", job=os.path.basename(__file__), newname='mm-info-kavl.log', debug=debug)
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

    try:
        if swsource.startswith('http'):
            cmedict = get_cme_dictionary_scoreboard(url=swsource)
        elif os.path.isfile(swsource):
            cmedict = read_memory(swsource)
        else:
            sys.exit()
        statusmsg['CMEaccess'] = 'success'
    except:
        statusmsg['CMEaccess'] = 'failed'

    if debug:
        print (cmedict)

    if not cmedict:
        statusmsg['CMEaccess'] = 'failed'

    # read memory and extract new and updated data
    if path:
        if os.path.isdir(path):
            path = os.path.join(path,"cme_{}.json".format(datetime.strftime(datetime.utcnow(),"%Y"))) 
        # open existing json, extend dictionary, eventually update contents
        data = read_memory(path,debug=False)
        full, new, up = get_new_inputs(data,cmedict)

    if debug:
        print ('Saveing to path:', path)
    print ('new:', new)
    print ('update:', up)

    if 'file' in output and (new or up):
        print (" Dealing with job: file")
        success = write_memory(path, cmedict)
        if success:
            statusmsg['CME2file'] = 'success'
            print (" -> everything fine")
        else:
            statusmsg['CME2file'] = 'failed'
            print (" -> failed")
    if 'db' in output and creddb:
        print (" Dealing with job: db")
        # Only add data with arrival time +12h > now to database
        # delete data with arrivaltime+12h < now from db
        success = False
        statusmsg['CME2db'] = 'failed'
        try:
            if debug:
                print ("Accessing database ...")
            config = ConnectDatabases(config=config, debug=debug)
            db = config.get('primaryDB')
            connectdict = config.get('conncetedDB')
            if debug:
                print (" ... done")
            success = True
        except:
            pass
        try:
            for dbel in connectdict:
                db = connectdict[dbel]
                print ("     -- Writing data to DB {}".format(dbel))
                if init:
                    swtableinit(db)
                if success:
                    update_database(db,full, new, up, swsource,debug=debug)
            statusmsg['CME2db'] = 'success'
            print (" -> everything fine")
        except:
            print (" -> failed")

    if 'telegram' in output or 'email' in output:
        print (" Dealing with jobs: telegram and email")
        # Access memory of already send data, send update/new
        statusmsg['CME2telegram'] = 'success'
        statusmsg['CME2mail'] = 'success'
        total = new + up
        for el in total:
            # Construct markdown message for each language provided
            valdict = full.get(el)
            for lang in languagedict:
                langdic = languagedict[lang]
                msghead = "*{}*".format(langdic.get('msgheader'))
                if el in new:
                    msgbody = "\n\n{} {}\n".format(langdic.get('msgnew'), valdict.get('start'))
                else:
                    msgbody = "\n\n{} {}\n".format(langdic.get('msgupdate'), valdict.get('start'))
                msgbody += "\n{} *{}* {}\n".format(langdic.get('msgarrival'), valdict.get('arrival') ,langdic.get('timezone'))
                msgbody += "{} {}\n".format(langdic.get('msgpred'), valdict.get('KPrange'))
                msgbody += '{}\n'.format(langdic.get('msgref'))
                msgbody += ""
                msg = msghead+msgbody
                if debug:
                    print (msg)

                if not debug and 'email' in output and lang == 'deutsch':
                    #if 'email' in output and lang == 'deutsch':
                    #TODO e-mail is not yet working
                    print ("Now starting e-mail") 
                    # read e-mail receiver list from dictionary
                    #try:
                    mailcfg = config.get('emailconfig','/etc/martas/mail.cfg')
                    maildict = read_meta_data(mailcfg)
                    maildict['Subject'] = msghead
                    # email is a comma separated string
                    reclist = receivers.get(lang,{})
                    for dic in reclist:
                        userdic = reclist[dic]
                        email = userdic.get('email')
                        name = userdic.get('name')
                        preferedlang = userdic.get('language')
                        maildict['Text'] = msgbody
                        maildict['To'] = email
                        #print ("       receivers are: {}".format(maildict['To']))
                        #### Stop here with debug mode for basic tests without memory and mails
                        if preferedlang == lang:
                            print ("  ... sending mail in language {} now: {}".format(lang,maildict))
                            sm(maildict)
                            print ("  ... done")
                    statusmsg['CME2mail'] = 'success'
                    print (" -> email fine")
                    #except:
                    #statusmsg['CME2mail'] = 'failed'

                if not debug and 'telegram' in output:
                    print ("Now starting telegram")
                    try:
                        telegram_send.send(messages=[msg],conf=langdic.get('channelconfig'),parse_mode="markdown")
                        print (" -> telegram fine")
                    except:
                        statusmsg['CME2telegram'] = 'failed'
                        print (" -> telegram failed")
                if debug and 'telegram' in output and lang == 'deutsch':
                    telegram_send.send(messages=[msg],conf=config.get('notificationconfig'),parse_mode="markdown")
                    print (" -> debug telegram fine")

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


