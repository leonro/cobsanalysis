#!/usr/bin/env python

"""
Flagging data

DESCRIPTION
   This method can be used to flag data regularly, to clean up the
   existing flagging database, to upload new flags from files and
   to archive "old" flags into json file structures. It is also possible
   to delete flags from the database. The delete method will always save
   a backup before removing flag data.

PREREQUISITES
   The following packages are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

   The upload method also works for pkl (pickle) files. However,
   a successful upload requires that the upload is performed with
   the same major python version as used for pkl creation. 

PARAMETERS
    flagdict          :  dict       :  currently hardcoded into the method
            { SensorNamePart : 
              [timerange, keys, threshold, window, markall, lowlimit, highlimit]
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -j joblist             :   list    :  jobs to be performed - default "flag"
                                          (flag, clean, upload, archive, delete)
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"
    -p path                :   string  :  upload - path to upload directory
    -s sensor              :   string  :  delete - sensor of which data is deleted 
    -o comment             :   string  :  delete - flag comment for data sets to be deleted

APPLICATION
    PERMANENT with cron:
        python flagging.py -c /etc/marcos/analysis.cfg
    YEARLY with cron:
        python flagging.py -c /etc/marcos/analysis.cfg -j archive
    DAILY with cron:
        python flagging.py -c /etc/marcos/analysis.cfg -j upload,clean -p /srv/archive/flags/uploads/
    REDO:
        python flagging.py -c /etc/marcos/analysis.cfg -e 2020-11-22
    DELETE data with comment:
        python flagging.py -c /etc/marcos/analysis.cfg -j delete -s MYSENSORID -o "my strange comment"
    DELETE data for FlagID Number (e.g. all automatic flags):
        python flagging.py -c /etc/marcos/analysis.cfg -j delete -s MYSENSORID -o "1"
    DELETE data all flags for key "f":
        python flagging.py -c /etc/marcos/analysis.cfg -j delete -s MYSENSORID -o "f"

FLAGGING STRUCTURE:
{
    # Sensors"
    "SENSORNAME_OR_PART": {
        "timerange": 7200, # data window in seconds from now into the past
        "keys": [          # if empty, than all available keys are selected
            "x",
            "y",
            "z"
        ],
        "threshold": 6,
        "window" : 300,
        "markall" : "True",
        "lowerlimit" : "None",
        "upperlimit" : "None"
    }
}
"""

from magpy.stream import *
from magpy.database import *
from magpy.transfer import *
import magpy.core.flagging as fl   # consecutive method

from shutil import copyfile
import getopt
import sys  # for sys.version_info()


scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, getstringdate, combinelists, ReadMemory
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf

def get_flagging_options(elem, begin, endtime, debug=False):
    timerange = elem.get("timerange", 7200)
    keys = elem.get("keys", ["all"])
    if keys in [['all'], ['All']]:
        keys = None
    threshold = elem.get("threshold", None)
    if threshold in ['Default', 'default', 'None', 'none', '', None]:
        threshold = None
    window = None
    windowlen = elem.get("window", None)
    if windowlen in ['Default', 'default', 'None', 'none', '', None]:
        windowlen = None
    if windowlen:
        window = timedelta(seconds=windowlen)
    markall = elem.get("markall", False)
    if markall in ["True", "true", "TRUE"]:
        markall = True
    else:
        markall = False
    lowlimit = elem.get("lowerlimit", None)
    if lowlimit in ['Default', 'default', 'None', 'none', '', None]:
        lowlimit = None
    highlimit = elem.get("upperlimit", None)
    if highlimit in ['Default', 'default', 'None', 'none', '', None]:
        highlimit = None
    if not begin:
        starttime = endtime - timedelta(seconds=timerange)
    else:
        timerange = (endtime - begin).total_seconds()
        starttime = begin
    if debug:
        print(
            "   Using the following parameters: keys={},threshold={},window={},limits={}".format(keys, threshold, window,
                                                                                                [lowlimit, highlimit]))
        print("   Timerange: Begin = {}, End = {}, coverage in sec = {}".format(starttime, endtime, timerange))
    return starttime, endtime, timerange, keys, threshold, window, lowlimit, highlimit

def get_validsensor_db(db,sensorid, starttime, endtime=datetime.utcnow(), debug=False):
    validsensors = []
    validsr = []

    # Checking available sensors
    sensorlist = dbselect(db, 'DataID', 'DATAINFO', 'SensorID LIKE "{}%"'.format(sensorid))
    if debug:
        print("   -> Found {} in DATAINFO".format(sensorlist))
        print(
            "   -> selecting only 1 second or highest resolution data with periods above 1sec")  # should be tested later again
    validsensors1 = []
    determinesr = []
    srlist = []
    if debug:
        print("   checking sampling rates for sensor group: {}".format(sensorlist))
    for sensor in sensorlist:
        res = dbselect(db, 'DataSamplingrate', 'DATAINFO', 'DataID="{}"'.format(sensor))
        try:
            sr = float(res[0])
            if debug:
                print("    - Sensor: {} -> sampling rate: {}".format(sensor, sr))
            if sr >= 1:
                validsensors1.append(sensor)
                srlist.append(sr)
        except:
            if debug:
                print("    - Sensor: {} sampling rate {} could not be interpreted".format(sensor, res))
                print("      determining sampling rate directly from data later")
            determinesr.append(sensor)
    if debug and len(determinesr) > 0:
        print("    Trying to get correct sampling rates for still missing sensors ...")
    for sensor in determinesr:
        lastdata = dbgetlines(db, sensor, timerange)
        if lastdata.length()[0] > 0:
            sr = lastdata.samplingrate()
            if debug:
                print("    - Sensor: {} -> sampling rate: {}".format(sensor, sr))
            if sr >= 1:
                if debug:
                    print("     -> adding to list")
                validsensors1.append(sensor)
                srlist.append(sr)
    print("   checking for recent data")
    for idx, sensor in enumerate(validsensors1):
        last = dbselect(db, 'time', sensor, expert="ORDER BY time DESC LIMIT 1")
        if last and len(last)>0:
            try:
                dbdate = getstringdate(last[0])
            except:
                if debug:
                    print ("    problem interpreting last date in data table of {}".format(sensor))
                dbdate = starttime
            if dbdate > starttime and dbdate <= endtime+timedelta(seconds=60):
                print("  -> DataID {} will be flagged".format(sensor))
                validsensors.append(sensor)
                validsr.append(srlist[idx])
            else:
                if debug:
                    print("  NO data within selected flagging timerange")
        else:
            if debug:
                print ("  did NOT find valid data dates in database")
    return validsensors, validsr

def flagging_data(validsensors, validsr, starttime, endtime, threshold, window, markall, lowlimit, highlimit):
    for idx, sensor in enumerate(validsensors):
        if debug:
            print ("   Running flagging for {}".format(sensor))
        try:
            lines = int(timerange / validsr[idx])
            lastdata = dbgetlines(db, sensor, lines)
        except:
            lastdata = DataStream()
        if debug:
            print("    - got {} datapoints".format(lastdata.length()[0]))
        if lastdata.length()[0] > 0:
            sensorid = "_".join(sensor.split('_')[:-1])
            if debug:
                print("   - getting existing flags for {}".format(sensorid))
            vflag = db2flaglist(db, sensorid, begin=datetime.strftime(starttime, "%Y-%m-%d %H:%M:%S"), end=datetime.strftime(endtime, "%Y-%m-%d %H:%M:%S"))
            if debug:
                print("   - found {} existing flags within the given timerange".format(len(vflag)))
            if len(vflag) > 0:
                try:
                    if debug:
                        print("    - removing existing flags")
                    lastdata = lastdata.flag(vflag)
                    lastdata = lastdata.remove_flagged()
                    if debug:
                        print("       ...success")
                except:
                    print("  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    print("    Failed to apply flags TODO need to check that")
            flaglist = []

            if threshold:
                if debug:
                    print("  - determining new outliers")
                flagls = lastdata.flag_outlier(keys=keys, threshold=threshold, timerange=window, returnflaglist=True,
                                               markall=markall)
                # now check flaglist---- if more than 20 consecutive flags... then drop them
                flaglist = fl.union(flagls, remove=True)
                if len(flaglist) > 0:
                    print("  - new outlier flags: {}; after combination: {}".format(len(flagls), len(flaglist)))
            if lowlimit:
                print("  - flagging data below lower limit")
                flaglow = lastdata.flag_range(keys=keys, below=lowlimit, text='below lower limit {}'.format(lowlimit),
                                              flagnum=3)
                if len(flaglow) > 0:
                    print("  - new lower limit flags: {}".format(len(flaglow)))
                if len(flaglist) == 0:
                    flaglist = flaglow
                else:
                    flaglist.extend(flaglow)
            if highlimit:
                print("  - flagging data above higher limit")
                flaghigh = lastdata.flag_range(keys=keys, above=highlimit,
                                               text='exceeding higher limit {}'.format(highlimit), flagnum=3)
                if len(flaghigh) > 0:
                    print("  - new upper limit flags: {}".format(len(flaghigh)))
                if len(flaglist) == 0:
                    flaglist = flaghigh
                else:
                    flaglist.extend(flaghigh)

            print(" -> SENSOR {}: found {} new flags".format(sensorid, len(flaglist)))

            if not debug and len(flaglist) > 0:
                for dbel in connectdict:
                    dbt = connectdict[dbel]
                    print("  -- Writing flags for sensors {} to DB {}".format(sensor, dbel))
                    print("  -- New flags: {}".format(len(flaglist)))
                    prevflaglist = db2flaglist(dbt, sensorid)
                    if len(prevflaglist) > 0:
                        lastdata.flagliststats(prevflaglist, intensive=True)
                    else:
                        print("  - no flags so far for this sensor")
                    name3 = "{}-toDB-{}".format(config.get('logname'), dbel)
                    statusmsg[name3] = 'flags successfully written to DB'
                    try:
                        flaglist2db(dbt, flaglist)
                    except:
                        statusmsg[name3] = 'flags could not be written to DB - disk full?'
                    aftflaglist = db2flaglist(dbt, sensorid)
                    lastdata.flagliststats(aftflaglist, intensive=True)
            elif debug:
                print (" DEBUG selected - not writing anything")


def main(argv):
    version = '1.1.0'
    configpath = ''
    flagoptions = ""
    statusmsg = {}
    debug=False
    endtime = datetime.utcnow()
    begin = endtime - timedelta(seconds=7200)
    joblist = []
    varioinst = ''
    scalainst = ''
    #joblist = ['flag','clean','archive','update','delete']
    joblist = ['flag']
    flagfilearchivepath = '' # default:    flagarchive : /srv/archive/flags
    flagfilepath = ''
    consecutivethreshold = 100000
    delsensor = 'RCST7_20160114_0001'
    delcomment = 'aof - threshold 5.0 window 43200.0 sec'

    # each input looks like:
    # { SensorNamePart : [timerange, keys, threshold, window, markall, lowlimit, highlimit]
    flagdict_fallback = {'LEMI036':{"timerange":7200,"keys":["x","y","z"],"threshold":6,"markall":True}}
    """
    flagdict_fallback = {'LEMI036':{"timerange":7200,"keys":["x","y","z"],"threshold":6,"markall":True},
            'LEMI025':[7200,'x,y,z',6,'Default',True,'None','None'],
            'FGE':[7200,'x,y,z',5,'Default',True,'None','None'],
            'GSM90_14245':[7200,'f',5,'default',False,'None','None'],
            'GSM90_6':[7200,'f',5,300,False,'None','None'],
            'GSM90_3':[7200,'f',5,300,False,'None','None'],
            'GP20S3NSS2':[7200,'f',5,'Default',False,'None','None'],
            'POS1':[7200,'f',4,100,False,'None','None'],
            'BM35':[7200,'var3','None','None',False,750,1000]}
    """

    try:
        opts, args = getopt.getopt(argv,"hc:f:b:e:j:p:s:o:D",["config=","flaggingoptions=","begin=","endtime=","joblist=","path=","sensor=","comment=","debug="])
    except getopt.GetoptError:
        print ('flagdata.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- flagging.py will determine the primary instruments --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python flagdata.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-f            : flagging options (json) - can be provided with config')
            print ('-e            : endtime, default is now')
            print ('-j            : joblist: flag,clean,archive,update,delete; default is flag,clean')
            print ('-p            : update - path to json files which end with flags.json')
            print ('-s            : delete - sensor')
            print ('-o            : delete - comment')
            print ('-------------------------------------')
            print ('Application:')
            print ('python flagdata.py -c /etc/marcos/analysis.cfg')
            print ('Once per year:')
            print (' python flagdata.py -c /etc/marcos/analysis.cfg -j archive')
            print ('Eventually always:')
            print (' python flagdata.py -c /etc/marcos/analysis.cfg -j upload -p /srv/archive/flags/uploads/')
            print ('Once a day/week:')
            print (' python flagdata.py -c /etc/marcos/analysis.cfg -j clean')
            print ('Testing the method:')
            print (' python3 flagdata.py -c ../conf/wic.cfg -f ../conf/flaggingoptions.json -D')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-f", "--flaggingoptions"):
            # get an endtime
            flagoptions = os.path.abspath(arg)
        elif opt in ("-b", "--begin"):
            # get an endtime
            begin = DataStream()._testtime(arg)
        elif opt in ("-e", "--endtime"):
            # get an endtime
            endtime = DataStream()._testtime(arg)
        elif opt in ("-j", "--joblist"):
            # get an endtime
            joblist = arg.split(',')
        elif opt in ("-p", "--path"):
            # delete any / at the end of the string
            flagfilepath = os.path.abspath(arg)
        elif opt in ("-s", "--sensor"):
            # hidden: delete sensor data
            delsensor = arg
        elif opt in ("-o", "--comment"):
            delcomment = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running flagging version {}".format(version))
    print ("--------------------------------")

    if debug:
        print ("1. checking validity of input parameter and preparations")
    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    if debug:
        print ("1a) Read and check validity of configuration data")
        config = GetConf(configpath)

    if debug:
        print ("1b) Activate logging scheme as selected in config and asign status names")
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname='mm-dp-flagdata.log', debug=debug)
    name1 = "{}-flag".format(config.get('logname'))
    name2 = "{}-flag-lemitest".format(config.get('logname'))
    name3 = "{}-cleaning".format(config.get('logname'))
    name4 = "{}-archive".format(config.get('logname'))
    name5 = "{}-upload".format(config.get('logname'))

    if debug:
        print ("1c) getting flagging options as defined in config or by option")
    flagoptionspath = config.get('flaggingoptions',"")
    if os.path.exists(flagoptions):
        flagoptionspath = flagoptions

    if flagoptionspath:
        if debug:
            print ("    -> using flagging options from {}".format(flagoptionspath))
        flagdict = ReadMemory(flagoptionspath, debug=debug)
    else:
        if debug:
            print ("    -> could not detect valid flagging options")
        flagdict = flagdict_fallback

    if debug:
        print ("1d) getting archive and save file paths")
    flagfilearchivepath = config.get('flagarchive','')
    if debug:
        print ("    -> using archive path for flags: {}".format(flagfilearchivepath))
    if not os.path.isdir(flagfilearchivepath):
        flagfilearchivepath = ''
    if not os.path.isdir(flagfilepath):
        flagfilepath = ''

    if debug:
        print ("1e) initializing status messages")
    statusmsg[name1] = 'flagging data sets successful'
    statusmsg[name2] = 'Lemitest not performed'
    statusmsg[name3] = 'Cleanup: cleaning database successful'
    statusmsg[name4] = 'Archive: not time for archiving'
    statusmsg[name5] = 'Upload: nothing to do'

    if debug:
        print ("1f) Connecting databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        connectdict = config.get('conncetedDB')
    except:
        statusmsg[name1] = 'database failed'

    if debug:
        print ("2. Running joblist ...")

    if 'flag' in joblist:
        print (" 2 - flag: Dealing with flagging dictionary")
        for sensor in flagdict:
            print (" -------------------------------------------")
            print (" Dealing with sensor group containing name fragment {}".format(sensor))
            print (" -------------------------------------------")
            #try:
            ok = True
            if ok:
                if debug:
                    print ("  Extracting flagging options for this sensor ...")
                    print("  ------------------------------------------------")
                subdict = flagdict.get(sensor)
                starttime, endtime, timerange, keys, threshold, window, lowlimit, highlimit = get_flagging_options(subdict, begin, endtime, debug=debug)

                if debug:
                    print("  Checking database whether data is available ...")
                    print("  ------------------------------------------------")
                validsensors, validsr = get_validsensor_db(db, sensor, starttime, endtime, debug=debug)

                if validsensors and len(validsensors) > 0:
                    if debug:
                        print("  flagging data ...")
                        print("  ------------------------------------------------")
                    flagging_data(validsensors, validsr, starttime, endtime, threshold, window, markall, lowlimit,
                                  highlimit)
            #except:
            #    print(" -> flagging failed")
            #    statusmsg[name1] = 'Step1: flagging failed'

    if 'upload' in joblist and flagfilepath:
        print ("5. Upload flagging lists from files")
        filelist = []
        print (" Searching for new flagging files")
        for fi in os.listdir(flagfilepath):
            if fi.endswith("flags.json") or fi.endswith("flags.pkl"):
                print ("   -> found: {}".format(os.path.join(flagfilepath, fi)))
                filelist.append(os.path.join(flagfilepath, fi))
        if len(filelist) > 0:
            for fi in filelist:
                fileflaglist = loadflags(fi)
                try:
                    instnamel = os.path.basename(fi).split('_')
                    instname = ["_".join(instnamel[:3])]
                    # Get instnames from fileflaglist
                except:
                    instname = []
                try:
                    flagdict = [{"starttime" : el[0], "endtime" : el[1], "components" : el[2].split(','), "flagid" : el[3], "comment" : el[4], "sensorid" : el[5], "modificationdate" : el[6]} for el in fileflaglist]
                    instname2 = [el.get('sensorid') for el in flagdict]
                    uniqueinstnames = list(set(instname2))
                    instname = uniqueinstnames
                    if debug:
                        print (" - Sensorname(s) extracted from flag file")
                except:
                    if debug:
                        print (" - Sensorname(s) extracted from file name")
                    pass
                if len(fileflaglist) > 0:
                    print(" - Loaded {} flags from file {} for {}".format(len(fileflaglist),fi,instname))
                    # get all flags from DB
                    dbflaglist = []
                    for inst in instname:
                        tmpdbflaglist = db2flaglist(db,inst)
                        dbflaglist = combinelists(dbflaglist,tmpdbflaglist)
                    print(" - {} flags in DB for {}".format(len(dbflaglist),instname))
                    # combine flaglist
                    fileflaglist = combinelists(fileflaglist,dbflaglist)
                    # clean flaglist - necessary here as dublicates might come from combinations
                    flaglist = DataStream().flaglistclean(fileflaglist)
                    print (" - {} flags remaining after cleaning. Delete {} replicates".format( len(flaglist),len(fileflaglist)-len(flaglist) ))
                    print (" - Combining consecutives")
                    flaglist = fl.union(flaglist)
                    print (" - {} flags remaining after joining consecutives.".format(len(flaglist)))
                    # copy file to ...json.uploaded
                    date = datetime.strftime(datetime.utcnow(), "%Y%m%d%H%M")
                    if not debug:
                        copyfile(fi, "{}.uploaded{}".format(fi,date))
                        # delete existing flags in DB and fill with new 'clean' flaglist
                        for dbel in connectdict:
                            dbt = connectdict[dbel]
                            print ("  -- Writing flags to DB {}".format(dbel))
                            for inst in instname:
                                flaglist2db(db,flaglist,mode='delete',sensorid=inst)
                        # delete flagfile
                        os.remove(fi)
                        print (" -> Done")
                    else:
                        print (" -> debug: will not modify or upload any datasets: {}".format(date))
                    statusmsg[name5] = 'Upload: new flagging data sets uploaded'
                else:
                    print (" -> Flaglist {} is empty. If pkl file check python version...".format(fi))


    if 'delete' in joblist and flagfilearchivepath:
        print ("Not existing. Deleting content")
        # not yet available
        #delsensor = 'RCST7_20160114_0001'
        #delsensor = 'LEMI036_3_0001'
        #delcomment = 'aof - threshold 5.0 window 43200.0 sec'
        #delcomment = 'aof - threshold: 6, window: 600.0 sec'
        # Backup any data too bee deleted?
        parameter = 'comment'
        if delcomment in ['0','1','2','3','4']:
            parameter = 'flagnumber'
            print (" - found a valid flagnumber as value: removing flags with FlagID {}".format(delcomment))
        elif delcomment in KEYLIST:
            parameter = 'key'
            print (" - found a valid key as value: removing all flags for key {}".format(delcomment))
        print (" - selected sensor {}".format(delsensor))
        flaglist = db2flaglist(db,delsensor)
        print (" - got {} flags".format(len(flaglist)))
        toberemovedflaglist = DataStream().flaglistmod('select', flaglist, parameter=parameter, value=delcomment)
        print (" - will backup and then remove {} flags matching your criteria".format(len(toberemovedflaglist)))
        flagfile = os.path.join(flagfilearchivepath,'flags_{}_backup_{}.json'.format(delsensor,datetime.strftime(datetime.utcnow(),"%Y%m%d%H%M")))
        succ = saveflags(toberemovedflaglist, flagfile)
        if succ:
            print (" - backup saved to {}".format(flagfile))
            remainingflaglist = DataStream().flaglistmod('delete', flaglist, parameter=parameter, value=delcomment)
            print (" - remaining {} flags".format(len(remainingflaglist)))
            if not debug:
                for dbel in connectdict:
                    dbt = connectdict[dbel]
                    print ("  -- Writing flags to DB {}".format(dbel))
                    flaglist2db(dbt,remainingflaglist,mode='delete',sensorid=delsensor)
                print (" -> Done")
            else:
                print (" -> Debug selected - no changes made to DB")
        else:
            print (" Backup could not be saved - aborting - check directory permissions")


    if 'clean' in joblist:
        print ("6. Cleaning flagging list")
        try:
            print (" Cleaning up all records")
            cumflag = []
            stream = DataStream()
            flaglist = db2flaglist(db,'all')
            if debug:
                print ("   -> Found {} flags in database".format(len(flaglist)))
            print (" --------------------------------------")
            stream.flagliststats(flaglist, intensive=True)
            print (" --------------------------------------")
            currentyear = endtime.year
            yearlist = [i for i in range(2000,currentyear+2)]
            for year in yearlist:
                startyear = year -1
                print (" Checking data from {} until {}".format(startyear, year))
                beg = '{}-01-01'.format(startyear)
                end = '{}-01-01'.format(year)
                flaglist_tmp = db2flaglist(db,'all',begin=beg, end=end)
                print ("   -> Found {} flags in database between {} and {}".format(len(flaglist_tmp),startyear,year))
                if len(flaglist_tmp) > 0:
                    print ("  - Cleaning up flaglist")
                    clflaglist_tmp = stream.flaglistclean(flaglist_tmp,progress=True)
                    print ("   -> {} flags remaining".format(len(clflaglist_tmp)))
                    if len(clflaglist_tmp) < consecutivethreshold:
                        print ("  - Combining consecutives")
                        coflaglist_tmp = fl.union(clflaglist_tmp) #debug=debug)
                    else:
                        coflaglist_tmp = clflaglist_tmp
                    print ("   -> {} flags remaining".format(len(coflaglist_tmp)))
                    if len(cumflag) == 0:
                        cumflag = coflaglist_tmp
                    else:
                        cumflag.extend(coflaglist_tmp)
            if debug:
                print ("   -> cleaned record contains {} flags".format(len(cumflag)))
            print (" --------------------------------------")
            stream.flagliststats(cumflag, intensive=True)
            print (" --------------------------------------")
            if not debug:
                for dbel in connectdict:
                    dbt = connectdict[dbel]
                    print ("  -- Writing flags to DB {}".format(dbel))
                    flaglist2db(dbt,cumflag,mode='delete',sensorid='all')
                print ("   -> cleaned flaglist uploaded to DB")
                statusmsg[name3] = 'Cleanup: cleaning database successful'
        except:
            print ("   -> failure while cleaning up")
            statusmsg[name3] = 'Cleanup: failure'


    # schedule with crontab at February 1st 6:00 (analyze yearly) flagging -c /wic.cfg -j archive
    if 'archive' in joblist and flagfilearchivepath:
        print ("7. Saving archive and deleting old db contents")
        print (" Archiving flags")
        print (" ---------------")
        print (" Every year in February - archive full year two years ago")
        print (" Delete all inputs older than two years from DB")
        ## e.g. Feb 2019 -> Keep only 2017 and 2018 in DB
        ## archive everything before 2017
        ## delete everything before 2017
        ## -> archive containsnow : flags_2016_final.pkl, # and 2015,etc
        ## ->                       flags_archive.pkl current backup (only monthly)
        ## -> DB contains 2017 to present ... 3 years max
        stream = DataStream()
        flaglist = db2flaglist(db,'all')
        if debug:
            print ("   -> Found {} flags in database".format(len(flaglist)))
        print (" --------------------------------------")
        stream.flagliststats(flaglist)
        print (" --------------------------------------")
        # Backup and export all old flags
        minyear = 2015
        succ = False
        currentyear = endtime.year
        if currentyear-3 > minyear:
             yearlist = [i for i in range(minyear,currentyear-2)]
        else:
             yearlist = [2015]
        flaglist_tmp = []
        for year in yearlist:
            startyear = 2000
            if year > 2015:
                startyear = year-1

            print (" Archiving flaglist until {}".format(year))
            flagfile = os.path.join(flagfilearchivepath,'flags_{}-{}.json'.format(startyear,year))
            beg = '{}-01-01'.format(startyear)
            end = '{}-01-01'.format(year)
            print ("  -> Checking database contents (flags) between {} and {}".format(beg,end))
            flaglist_tmp = db2flaglist(db,'all',begin=beg, end=end)
            if len(flaglist_tmp) > 0:
                print ("   -> Found {} flags in database between {} and {}".format(len(flaglist_tmp),startyear,year))
                if os.path.isfile(flagfile):
                    fileflaglist = loadflags(flagfile)
                    print ("   -> Found {} flags in file".format(len(fileflaglist)))
                    flaglist_tmp.extend(fileflaglist)
                # Cleaning has been done already
                print ("  - Saving flag archive to {}".format(flagfilepath))
                succ = saveflags(flaglist_tmp, flagfile, overwrite=True)
                print ("   -> Done")
            else:
                print ("    -> DB empty")

        if succ:
            # drop all flags from flaglist
            print (" Droping all flags until year {}".format(year))
            newflaglist = stream.flaglistmod('delete', flaglist, starttime='2000-01-01', endtime=end)
            print ("   -> remaining amount of flags: {}".format(len(newflaglist)))
            # Cleaning has been done already
            print (" Uploading new list to database and deleting all other inputs")
            print (" --------------------------------------")
            stream.flagliststats(newflaglist)
            print (" --------------------------------------")
            if not debug:
                for dbel in connectdict:
                    dbt = connectdict[dbel]
                    print ("  -- Writing flags to DB {}".format(dbel))
                    flaglist2db(dbt,newflaglist, mode='delete')
                print ("   -> Done")
            else:
                print ("   -> Debug selected - no changes made to DB")
            statusmsg[name4] = 'Archiving flags: done until {}'.format(year)
        else:
            print ("   -> Problem with saving files - aborting")
            statusmsg[name4] = 'Archiving flags: file saving problem'

    print ("------------------------------------------")
    print ("  flagging finished")
    print ("------------------------------------------")
    print ("SUCCESS")

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])


