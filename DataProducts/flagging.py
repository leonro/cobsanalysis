#!/usr/bin/env python

"""
Flagging data
"""

from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred
#import magpy.core.flagging as fl   # consecutive method

from shutil import copyfile
import itertools
import getopt
import pwd
import socket
import sys  # for sys.version_info()


coredir = os.path.abspath(os.path.join('/home/cobs/MARTAS', 'core'))
coredir = os.path.abspath(os.path.join('/home/leon/Software/MARTAS', 'core'))
sys.path.insert(0, coredir)
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf



# ################################################
#            Flagging dictionary
# ################################################

## comments: switched from 5 to 6 for LEMI025 and LEMI036 at 01.08.2019

# each input looks like:
# { SensorNamePart : [timerange, keys, threshold, window, markall, lowlimit, highlimit]
flagdict = {'LEMI036':[7200,'x,y,z',6,'Default',True,'None','None'],
            'LEMI025':[7200,'x,y,z',6,'Default',True,'None','None'],
            'FGE':[7200,'x,y,z',5,'Default',True,'None','None'],
            'GSM90_14245':[7200,'f',5,'default',False,'None','None'],
            'GSM90_6':[7200,'f',5,300,False,'None','None'],
            'GSM90_3':[7200,'f',5,300,False,'None','None'],
            'GP20S3NSS2':[7200,'f',5,'Default',False,'None','None'],
            'POS1':[7200,'f',4,100,False,'None','None'],
            'BM35':[7200,'var3','None','None',False,750,1000]}


def DefineLogger(config={}, category="DataProducts", newname='', debug=False):
    host = socket.gethostname()
    jobname = os.path.splitext(os.path.basename(__file__))[0]
    name = "{}-{}-{}".format(host.upper(),category,jobname)
    # extract loggingpath from config
    if not newname == '':
        logpath = config.get('logfile')
        logdir = os.path.split(logpath)[0]
        logpath = os.path.join(logdir,newname)
        config['logfile'] = logpath
        if debug:
            print ("    - Saving logs to {}".format(logpath))
    # add name to config dict
    config['logname'] = name

    return config

def getstringdate(strdate):
    dbdateformat1 = "%Y-%m-%d %H:%M:%S.%f"
    dbdateformat2 = "%Y-%m-%d %H:%M:%S"
    try:
        value = datetime.strptime(strdate,dbdateformat1)
    except:
        try:
            value = datetime.strptime(strdate,dbdateformat2)
        except:
            print ("Get primary: error when converting database date to datetime")
            return datetime.utcnow()
    return value


def ConnectDatabases(config={}, debug=False):
    """
    DESCRIPTION:
        Database connection
    """

    connectdict = {}
    config['primaryDB'] = None

    dbcreds = config.get('dbcredentials')
    if not isinstance(dbcreds,list):
        dbcreds = [dbcreds]

    # First in list is primary
    for credel in dbcreds:
        # Get credentials
        dbpwd = mpcred.lc(credel,'passwd')
        dbhost = mpcred.lc(credel,'host')
        dbuser = mpcred.lc(credel,'user')
        dbname = mpcred.lc(credel,'db')

        # Connect DB
        if dbhost:
            try:
                if debug:
                    print ("    -- Connecting to database {} ...".format(credel))
                    print ("    -- {} {} {}".format(dbhost, dbuser, dbname))
                connectdict[credel] = mysql.connect(host=dbhost,user=dbuser,passwd=dbpwd,db=dbname)
                if debug:
                    print ("...success")
            except:
                pass

    if len(connectdict) == 0:
        print ("  No database found - aborting")
        sys.exit()
    else:
        if debug:
            print ("    -- at least on db could be connected")

    if connectdict.get(dbcreds[0],None):
        if debug:
            print ("    -- primary db is available: {}".format(dbcreds[0]))
        config['primaryDB'] = connectdict[dbcreds[0]]
    else:
        print (" Primary database not available - selecting alternative as primary")
        for el in dbcreds:
            if connectdict.get(el,None):
                config['primaryDB'] = connectdict[el]
                print ("   -> selected database {} as primary".format(el))
                break

    config['conncetedDB'] = connectdict

    return config


def consecutive_check(flaglist, sr=1, overlap=True, singular=False, remove=False, critamount=20, flagids=None, debug=False):
    """
    DESCRIPTION:
        Method to inspect a flaglist and check for consecutive elements
    PARAMETER:
        sr           (float) :  [sec] Sampling rate of underlying flagged data sequence
        critamount   (int)   :  Amount of maximum allowed consecutive (to be used when removing consecutive data)
        result       (BOOL)  :  True will replace consecutive data with a new flag, False will remove consecutive data from flaglist
        overlap      (BOOL)  :  if True than overlapping flags will also be combined, comments from last modification will be used
        singular     (BOOL)  :  if True than only single time stamp flags will be investigated (should be spikes)
    INPUT: 
        flaglist with line like
        [datetime.datetime(2016, 4, 13, 16, 54, 40, 32004), datetime.datetime(2016, 4, 13, 16, 54, 40, 32004), 't2', 3,
         'spike and woodwork', 'LEMI036_1_0002', datetime.datetime(2016, 4, 28, 15, 25, 41, 894402)]
    OUTPUT:
        flaglist

    """
    if flagids:
        if isinstance(flagids, list):
            uniqueids = flagids
        elif isinstance(flagids, int):
            uniqueids = [flagids]
        else:
            uniqueids = [0,1,2,3,4]
    else:
        uniqueids = [0,1,2,3,4]

    if not len(flaglist) > 0:
        return flaglist

    # Ideally flaglist is a list of dictionaries:
    # each dictionary consists of starttime, endtime, components, flagid, comment, sensorid, modificationdate
    flagdict = [{"starttime" : el[0], "endtime" : el[1], "components" : el[2].split(','), "flagid" : el[3], "comment" : el[4], "sensorid" : el[5], "modificationdate" : el[6]} for el in flaglist]

    ## Firstly extract all flagging IDs from flaglst
    if len(flaglist[0]) > 6:
        ids = [el[5] for el in flaglist]
        uniquenames = list(set(ids))
    else:
        print ("Found an old flaglist type - aborting")
        return flaglist

    newflaglist = []
    for name in uniquenames:
        cflaglist = [el for el in flaglist if el[5] == name]
        # if singular, extract flags with identical start and endtime
        if singular:
            nonsingularflaglist = [el for el in flaglist if el[0] != el[1]]
            testlist = [el for el in flaglist if el[0] == el[1]]
            newflaglist.extend(nonsingularflaglist)
        else:
            testlist = cflaglist
        
        #if debug:
        #    print (name, len(testlist))
        # extract possible components
        #uniquecomponents = list(set([item for sublist in [el[2].split(',') for el in testlist] for item in sublist]))
        # better use componentgroups
        uniquecomponents = list(set([el[2] for el in testlist]))
        if debug:
            print ("Components", uniquecomponents)

        for unid in uniqueids:
            idlist = [el for el in testlist if el[3] == unid]
            for comp in uniquecomponents:
                complist = [el for el in idlist if comp == el[2]]
                if debug:
                    print ("Inputs for component {}: {}".format(comp,len(complist)))
                extendedcomplist = []
                for line in complist:
                    tdiff = (line[1]-line[0]).total_seconds()
                    if tdiff > sr-(0.05*sr):
                        # add steps
                        firstt = line[0]
                        lastt = line[1]
                        steps = int(np.ceil(tdiff/float(sr)))
                        #line[1] = firstt
                        #extendedcomplist.append(line)
                        for step in range(0,steps):
                            val0 = firstt+timedelta(seconds=step*sr)
                            extendedcomplist.append([val0,val0,line[2],line[3],line[4],line[5],line[6]])
                        extendedcomplist.append([lastt,lastt,line[2],line[3],line[4],line[5],line[6]])
                    else:
                        extendedcomplist.append(line)
                if debug:
                    print (" - Individual time stamps: {}".format(len(extendedcomplist)))
                if overlap:
                    if debug:
                        print ("removing overlaps")
                    # Now sort the extendedlist according to modification date
                    extendedcomplist.sort(key=lambda x: x[-1], reverse=True)
                    #print (extendedcomplist)
                    # Now remove all overlapping data
                    seen = set()
                    new1list = []
                    for item in extendedcomplist:
                        ti = item[0]
                        if item[0] not in seen:
                            new1list.append(item)
                            seen.add(ti)
                    extendedcomplist = new1list
                    if debug:
                        print (" - After overlap removal - time stamps: {}".format(len(extendedcomplist)))
                # now combine all subsequent time steps below sr to single inputs again
                extendedcomplist.sort(key=lambda x: x[0])
                new2list = []
                startt = None
                endt = None
                tmem = None
                for idx,line in enumerate(extendedcomplist):
                    if idx < len(extendedcomplist)-1:
                        t0 = line[0]
                        t1 = extendedcomplist[idx+1][0]
                        tdiff = (t1-t0).total_seconds()
                        if tdiff <= sr+(0.05*sr):
                            if not tmem:
                                tmem = t0
                            endt = None
                        else:
                            startt = t0
                            if tmem:
                                startt = tmem
                            endt = t0
                    else:
                        t0 = line[0]
                        startt = t0
                        if tmem:
                            startt = tmem
                        endt = t0                            
                    if startt and endt:
                        # add new line
                        if not remove:
                            new2list.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                            newflaglist.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                        else:
                            if unid == 1 and (endt-startt).total_seconds()/float(sr) >= critamount:
                                # do not add subsequent automatic flags 
                                pass
                            else:
                                new2list.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                                newflaglist.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                        tmem = None
                if debug:
                    print (" - After recombination: {}".format(len(new2list)))
            #print (unid, len(newflaglist))

    return newflaglist


def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    debug=False
    endtime = datetime.utcnow()
    joblist = []
    varioinst = ''
    scalainst = ''
    #joblist = ['flag','clean','archive','update','delete']
    joblist = ['flag','update']
    flagfilearchivepath = '' # default:    flagarchive : /srv/archive/flags
    flagfilepath = ''

    try:
        opts, args = getopt.getopt(argv,"hc:e:j:p:D",["config=","endtime=","joblist=","path=","debug=",])
    except getopt.GetoptError:
        print ('flagging.py -c <config>')
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
            print ('python flagging.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-e            : endtime, default is now')
            print ('-j            : joblist: flag,clean,archive,update; default is flag,clean')
            print ('-p            : update - path to json files which end with flags.json')
            print ('-------------------------------------')
            print ('Application:')
            print ('python flagging.py -c /etc/marcos/analysis.cfg')
            print ('Once per year:')
            print (' python flagging.py -c /etc/marcos/analysis.cfg -j archive')
            print ('Eventually always:')
            print (' python flagging.py -c /etc/marcos/analysis.cfg -j upload -p /srv/archive/flags/uploads/')
            print ('Once a day/week:')
            print (' python flagging.py -c /etc/marcos/analysis.cfg -j clean')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-e", "--endtime"):
            # get an endtime
            endtime = arg.split(',')
        elif opt in ("-j", "--joblist"):
            # get an endtime
            joblist = arg.split(',')
        elif opt in ("-p", "--path"):
            # delete any / at the end of the string
            flagfilepath = os.path.abspath(arg)
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running flagging version {}".format(version))
    print ("--------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "DataProducts", newname='mm-dp-flagging.log')

    name1 = "{}-flag".format(config.get('logname'))
    name2 = "{}-flag-lemitest".format(config.get('logname'))
    name3 = "{}-cleaning".format(config.get('logname'))
    name4 = "{}-archive".format(config.get('logname'))
    name5 = "{}-upload".format(config.get('logname'))
    statusmsg[name1] = 'flagging data sets successful'
    statusmsg[name2] = 'Lemitest not performed'
    statusmsg[name3] = 'Cleanup: cleaning database successful'
    statusmsg[name4] = 'Archive: not time for archiving'
    statusmsg[name5] = 'Upload: nothing to do'

    flagfilearchivepath = config.get('flagarchive','')
    print (flagfilearchivepath)
    if not os.path.isdir(flagfilearchivepath):
        flagfilearchivepath = ''
    if not os.path.isdir(flagfilepath):
        flagfilepath = ''

    print ("3. Connect databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        connectdict = config.get('conncetedDB')
    except:
        statusmsg[name1] = 'database failed'

    if 'flag' in joblist:
      print ("4. Dealing with flagging dictionary")
      try:
        #ok = True
        #if ok:
        for elem in flagdict:
            print (" -------------------------------------------")
            print (" Dealing with sensorgroup which starts with {}".format(elem))
            print (" -------------------------------------------")
            # Get parameter
            timerange = flagdict[elem][0]
            keyspar = flagdict[elem][1]
            if keyspar in ['Default','default','All','all','',None]:
               keys = None
            else:
               keys = keyspar.split(',')
            threshold = flagdict[elem][2]
            if threshold in ['Default','default','None','none','',None]:
                threshold = None
            windowpar = flagdict[elem][3]
            if windowpar in ['Default','default','None','none','',None]:
               window = None
            else:
               window = timedelta(seconds=windowpar)
            markall = flagdict[elem][4]
            lowlimit = flagdict[elem][5]
            if lowlimit in ['Default','default','None','none','',None]:
               lowlimit = None
            highlimit = flagdict[elem][6]
            if highlimit in ['Default','default','None','none','',None]:
               highlimit = None
            starttime = datetime.utcnow()-timedelta(seconds=timerange)
            print (" - Using the following parameter: keys={},threshold={},window={},limits={}".format(keys, threshold, window,[lowlimit,highlimit]))
            # Checking available sensors
            sensorlist = dbselect(db, 'DataID', 'DATAINFO','SensorID LIKE "{}%"'.format(elem))
            print ("   -> Found {}".format(sensorlist))
            print ("   a) select 1 second or highest resolution data") # should be tested later again
            validsensors1 = []
            determinesr = []
            srlist = []
            for sensor in sensorlist:
                res = dbselect(db,'DataSamplingrate','DATAINFO','DataID="{}"'.format(sensor))
                try:
                    sr = float(res[0])
                    print ("    - Sensor: {} -> Samplingrate: {}".format(sensor,sr))
                    if sr >= 1:
                        validsensors1.append(sensor)
                        srlist.append(sr)
                except:
                    print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    print ("Check sampling rate {} of {}".format(res,sensor))
                    print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                    determinesr.append(sensor)
            print (" b) checking sampling rate of failed sensors")
            for sensor in determinesr:
                lastdata = dbgetlines(db,sensor,timerange)
                if lastdata.length()[0] > 0:
                    sr = lastdata.samplingrate()
                    print ("    - Sensor: {} -> Samplingrate: {}".format(sensor,sr))
                    if sr >= 1:
                        validsensors1.append(sensor)
                        srlist.append(sr)
            print (" c) Check for recent data")
            validsensors = []
            validsr = []
            for idx,sensor in enumerate(validsensors1):
                last = dbselect(db,'time',sensor,expert="ORDER BY time DESC LIMIT 1")
                print (last)
                try:
                    dbdate = last[0]
                except:
                    print ("    - No data found for {}".format(sensor))
                print (" TESTTTTTTTTT", getstringdate(dbdate))
                try:
                    if getstringdate(dbdate) > starttime:
                        print ("    - Valid data for {}".format(sensor))
                        validsensors.append(sensor)
                        validsr.append(srlist[idx])
                except:
                    print ("  Dateformat problem for {}".format(sensor))
            print (" d) Flagging data")
            for idx,sensor in enumerate(validsensors):
                lines = int(timerange/validsr[idx])
                lastdata = dbgetlines(db,sensor,lines)
                print ("    - got {} datapoints".format(lastdata.length()[0]))
                if lastdata.length()[0] > 0:
                    sensorid = "_".join(sensor.split('_')[:-1])
                    print ("   - getting existing flags for {}".format(sensorid))
                    vflag = db2flaglist(db,sensorid,begin=datetime.strftime(starttime,"%Y-%m-%d %H:%M:%S"))
                    print ("   - found {} existing flags".format(len(vflag)))
                    if len(vflag) > 0:
                        try:
                            print ("    - removing existing flags")
                            lastdata = lastdata.flag(vflag)
                            lastdata = lastdata.remove_flagged()
                            print ("       ...success")
                        except:
                            print (" ------------------------------------------------")
                            print (" -- Failed to apply flags TODO need to check that")
                    flaglist = []
                    if threshold:
                       print ("  - determining new outliers")
                       print (markall)
                       flagls = lastdata.flag_outlier(keys=keys,threshold=threshold,timerange=window,returnflaglist=True,markall=markall)
                       # now check flaglist---- if more than 10 consecutive flags... then drop it
                       flaglist = consecutive_check(flagls, remove=True)
                       #if len(flagls) > len(flaglist)+1 and sensor.startswith("LEMI036_1"):   #+1 to add some room
                       #    statusmsg[name2] = 'Step1: removed consecutive flags for {}: Found {}, Clean: {}'.format(sensor, len(flagls), len(flaglist))
                       print ("  - new flags: {}".format(len(flagls)))
                    if lowlimit:
                       print ("  - flagging data below lower limit")
                       flaglow = lastdata.flag_range(keys=keys,below=lowlimit, text='below lower limit {}'.format(lowlimit),flagnum=3)
                       if len(flaglist) == 0:
                           flaglist = flaglow
                       else:
                           flaglist.extend(flaglow)
                    if highlimit:
                       print ("  - flagging data above higher limit")
                       flaghigh = lastdata.flag_range(keys=keys,above=highlimit, text='exceeding higher limit {}'.format(highlimit),flagnum=3)
                       if len(flaglist) == 0:
                           flaglist = flaghigh
                       else:
                           flaglist.extend(flaghigh)

                    print ("RESULT: found {} new flags".format(flaglist))

                    if not debug and len(flaglist) > 0:
                        for dbel in connectdict:
                            dbt = connectdict[dbel]
                            print ("  -- Writing flags for sensors {} to DB {}".format(sensor,dbel))
                            print ("  -- New flags: {}".format(len(flaglist)))
                            prevflaglist = db2flaglist(dbt,sensorid)
                            if len(prevflaglist) > 0:
                                lastdata.flagliststats(prevflaglist, intensive=True)
                            else:
                                print ("  - no flags so far for this sensor")
                            flaglist2db(dbt,flaglist)
                            aftflaglist = db2flaglist(dbt,sensorid)
                            lastdata.flagliststats(aftflaglist, intensive=True)
      except:
        print (" -> flagging failed")
        statusmsg[name1] = 'Step1: flagging failed'


    if 'upload' in joblist and flagfilepath:
        print ("5. Upload flagging lists from files")
        filelist = []
        print (" Searching for new flagging files")
        for fi in os.listdir(flagfilepath):
            if fi.endswith("_flags.json"):
                print ("   -> found: {}".format(os.path.join(flagfilepath, fi)))
                filelist.append(os.path.join(flagfilepath, fi))
        if len(filelist) > 0:
            for fi in filelist:
                fileflaglist = loadflags(fi)
                instname = os.path.basename(fi).replace('_flags.json','')
                if len(fileflaglist) > 0:
                    print(" - Loaded {} flags from file for {}".format(len(fileflaglist),instname))
                    # get all flags from DB
                    dbflaglist = db2flaglist(db,instname)
                    print(" - {} flags in DB for {}".format(len(dbflaglist),instname))      
                    # combine flaglist
                    if len(dbflaglist) > 0:
                        fileflaglist.extend(dbflaglist)
                    # clean flaglist - necessary here as dublicates might come from combinations
                    flaglist = DataStream().flaglistclean(fileflaglist)
                    print (" - {} flags remaining after cleaning. Delete {} replicates".format( len(flaglist),len(fileflaglist)-len(flaglist) ))
                    print (" - Combining consecutives")
                    flaglist = consecutive_check(flaglist)
                    print (" - {} flags remaining after joining consecutives.".format(len(flaglist)))
                    # copy file to ...json.uploaded
                    date = datetime.strftime(datetime.utcnow(), "%Y%m%d%H%M")
                    if not debug:
                        copyfile(fi, "{}.uploaded{}".format(fi,date))
                        # delete existing flags in DB and fill with new 'clean' flaglist
                        for dbel in connectdict:
                            dbt = connectdict[dbel]
                            print ("  -- Writing flags to DB {}".format(dbel))
                            flaglist2db(db,flaglist,mode='delete',sensorid=instname)
                        # delete flagfile
                        os.remove(fi)
                        print (" -> Done")
                    else:
                        print (" -> debug: will upload new datasets: {}".format(date))
                    statusmsg[name5] = 'Upload: new flagging data sets uploaded'


    if 'delete' in joblist and flagfilearchivepath:
        print ("Not existing. Deleting content")
        # not yet available
        delsensor = 'RCST7_20160114_0001'
        delsensor = 'LEMI036_3_0001'
        delcomment = 'aof - threshold 5.0 window 43200.0 sec'
        delcomment = 'aof - threshold: 6, window: 600.0 sec'
        # Backup any data too bee deleted?
        print (" - selected sensor {}".format(delsensor))
        flaglist = db2flaglist(db,delsensor)
        print (" - got {} flags".format(len(flaglist)))
        flagfile = os.path.join(flagfilearchivepath,'flags_{}_backup_{}.json'.format(delsensor,datetime.strftime(datetime.utcnow(),"%Y%m%d%H%M")))
        succ = saveflags(flaglist, flagfile)
        if succ:
            print (" - backup saved to {}".format(flagfile))
            remainingflaglist = DataStream().flaglistmod('delete', flaglist, parameter='comment', value=delcomment)
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
        #try:
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
        yearlist = [i for i in range(2000,currentyear+1)]
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
                if len(clflaglist_tmp) < 25000:
                    # TODO this method leads to a killed process sometimes...
                    print ("  - Combining consecutives")
                    coflaglist_tmp = consecutive_check(clflaglist_tmp) #debug=debug)
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
            statusmsg[name3] = 'Cleanup successfully finished'


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
        #martaslog = ml(logfile=logpath,receiver='telegram')
        #martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
        #martaslog.msg(statusmsg)
        pass
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])


