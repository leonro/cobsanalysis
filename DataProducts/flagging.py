#!/usr/bin/env python

"""
Flagging data

DESCRIPTION
   This method can be used to flag data regularly, to clean up the
   existing falgging database, to upload new flags from files and
   to archive "old" flags into json file structures. It is also possibe
   to delete flags from the database. The delete method will always save
   a backup before removing flag data.

PREREQUISITES
   The following packegas are required:
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
                                          (flag, clean, uploud, archive, delete)
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


scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, getstringdate, combinelists
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf



# ################################################
#            Flagging dictionary
# ################################################

## comments: switched from 5 to 6 for LEMI025 and LEMI036 at 01.08.2019

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

    ok = True
    if ok:
        newflaglist = []
        for name in uniquenames:
            if debug:
                print (" Dealing with {}".format(name))
            cflaglist = [el for el in flaglist if el[5] == name]
            # if singular, extract flags with identical start and endtime
            if singular:
                nonsingularflaglist = [el for el in flaglist if el[0] != el[1]]
                testlist = [el for el in flaglist if el[0] == el[1]]
                newflaglist.extend(nonsingularflaglist)
            else:
                testlist = cflaglist

            uniquecomponents = list(set([el[2] for el in testlist]))
            if debug:
                print (" - found flags for components", uniquecomponents)

            for comp in uniquecomponents:
                extendedcomplist = []
                extlistlenend = 0
                for unid in uniqueids:
                    idlist = [el for el in testlist if el[3] == unid]
                    complist = [el for el in idlist if comp == el[2]]
                    if debug:
                        print ("Inputs for component {} and ID {}: {}".format(comp,unid,len(complist)))
                        print (complist)
                    extlistlenstart = extlistlenend
                    for line in complist:
                        tdiff = (line[1]-line[0]).total_seconds()
                        if tdiff > sr:
                            # add steps
                            firstt = line[0]
                            lastt = line[1]
                            steps = int(np.ceil(tdiff/float(sr)))
                            for step in range(0,steps):
                                val0 = firstt+timedelta(seconds=step*sr)
                                extendedcomplist.append([val0,val0,line[2],line[3],line[4],line[5],line[6]])
                            extendedcomplist.append([lastt,lastt,line[2],line[3],line[4],line[5],line[6]])
                        else:
                            extendedcomplist.append(line[:7])
                    extlistlenend = len(extendedcomplist)
                    if debug:
                        print (" - Individual time stamps in second resolution: {} (appended {} elements in this run)".format(len(extendedcomplist),extlistlenend-extlistlenstart))
                if overlap:
                    if debug:
                        print ("removing overlaps")
                    # Now sort the extendedlist according to modification date
                    extendedcomplist.sort(key=lambda x: x[-1], reverse=True)
                    #print (extendedcomplist[0])
                    # Now remove all overlapping data
                    seen = set()
                    new1list = []
                    for item in extendedcomplist:
                        try:
                            ti = np.round((item[0]-datetime(1900, 1, 1)).total_seconds(),0)
                        except:
                            ti = np.round((item[0]-datetime.datetime(1900, 1, 1)).total_seconds(),0)
                        # use a second resolution for identifying identical inputs
                        if ti not in seen:
                            new1list.append(item)
                            seen.add(ti)
                    extendedcomplist = new1list
                    if debug:
                        print (" - After overlap removal - time stamps: {}".format(len(extendedcomplist)))

                # now combine all subsequent time steps below sr with identical id to single inputs again
                extendedcomplist.sort(key=lambda x: x[0])
                new2list = []
                startt = None
                endt = None
                tmem = None
                for idx,line in enumerate(extendedcomplist):
                    idnum0 = line[3]
                    if idx < len(extendedcomplist)-1:
                        t0 = line[0]
                        t1 = extendedcomplist[idx+1][0]
                        idnum1 = extendedcomplist[idx+1][3]
                        tdiff = (t1-t0).total_seconds()
                        if tdiff <= sr and idnum0 == idnum1:
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
                            if idnum0 == 1 and (endt-startt).total_seconds()/float(sr) >= critamount:
                                # do not add subsequent automatic flags
                                pass
                            else:
                                new2list.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                                newflaglist.append([startt,endt,line[2],line[3],line[4],line[5],line[6]])
                        tmem = None
                if debug:
                    print (" - After recombination: {}".format(len(new2list)))

    return newflaglist

def consecutive_check_old(flaglist, sr=1, overlap=True, singular=False, remove=False, critamount=20, flagids=None, debug=False):
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
            print (" - Components", uniquecomponents)

        for unid in uniqueids:
            idlist = [el for el in testlist if el[3] == unid]
            for comp in uniquecomponents:
                complist = [el for el in idlist if comp == el[2]]
                if debug:
                    print ("  - Inputs for component {} with flagID {}: {}".format(comp,unid,len(complist)))
                idxtmp = 0
                testcnt = 0
                while idxtmp < len(complist):
                    complist = complist[idxtmp:]
                    extendedcomplist = []
                    for idx,line in enumerate(complist):
                        tdiff = (line[1]-line[0]).total_seconds()
                        if tdiff > sr-(0.05*sr):
                            # add steps
                            firstt = line[0]
                            lastt = line[1]
                            steps = int(np.ceil(tdiff/float(sr)))
                            for step in np.arange(steps):
                                val0 = firstt+timedelta(seconds=int(step)*sr)
                                extendedcomplist.append([val0,val0,line[2],line[3],line[4],line[5],line[6]])
                            extendedcomplist.append([lastt,lastt,line[2],line[3],line[4],line[5],line[6]])
                        else:
                            extendedcomplist.append(line)
                        if len(extendedcomplist) > 500000:
                            idxtmp = idx+1
                            break
                        idxtmp = idx+1
                    if debug:
                        print ("    -> Individual time stamps: {}".format(len(extendedcomplist)))
                    if overlap:
                        if debug:
                            print ("    -> removing overlaps")
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
                            print ("    -> After overlap removal - time stamps: {}".format(len(extendedcomplist)))
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
                        print ("    -> After recombination: {}".format(len(new2list)))
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
    joblist = ['flag']
    flagfilearchivepath = '' # default:    flagarchive : /srv/archive/flags
    flagfilepath = ''
    consecutivethreshold = 100000
    delsensor = 'RCST7_20160114_0001'
    delcomment = 'aof - threshold 5.0 window 43200.0 sec'

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


    try:
        opts, args = getopt.getopt(argv,"hc:e:j:p:s:o:D",["config=","endtime=","joblist=","path=","sensor=","comment=","debug="])
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
            print ('-j            : joblist: flag,clean,archive,update,delete; default is flag,clean')
            print ('-p            : update - path to json files which end with flags.json')
            print ('-s            : delete - sensor')
            print ('-o            : delete - comment')
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

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname='mm-dp-flagging.log', debug=debug)

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
                if debug:
                    print ("   Last time", last)
                try:
                    dbdate = last[0]
                except:
                    print ("    - No data found for {}".format(sensor))
                try:
                    if getstringdate(dbdate) > starttime:
                        print ("    - Valid data for {}".format(sensor))
                        validsensors.append(sensor)
                        validsr.append(srlist[idx])
                except:
                    print ("  Dateformat problem for {}".format(sensor))
            print (" d) Flagging data")
            for idx,sensor in enumerate(validsensors):
                try:
                     lines = int(timerange/validsr[idx])
                     lastdata = dbgetlines(db,sensor,lines)
                except:
                     lastdata = DataStream()
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
                       if debug:
                           print ("MARK all: ",  markall)
                       flagls = lastdata.flag_outlier(keys=keys,threshold=threshold,timerange=window,returnflaglist=True,markall=markall)
                       # now check flaglist---- if more than 20 consecutive flags... then drop them
                       flaglist = consecutive_check(flagls, remove=True)
                       #if len(flagls) > len(flaglist)+1 and sensor.startswith("LEMI036_1"):   #+1 to add some room
                       #    statusmsg[name2] = 'Step1: removed consecutive flags for {}: Found {}, Clean: {}'.format(sensor, len(flagls), len(flaglist))
                       print ("  - new outlier flags: {}; after combination: {}".format(len(flagls),len(flaglist)))
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

                    print (" -> RESULT: found {} new flags".format(len(flaglist)))

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
                            name3 = "{}-toDB-{}".format(config.get('logname'),dbel)
                            statusmsg[name3] = 'flags successfully written to DB'
                            try:
                                flaglist2db(dbt,flaglist)
                            except:
                                statusmsg[name3] = 'flags could not be written to DB - disk full?'
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


