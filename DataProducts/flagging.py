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

import itertools

# ################################################
#             Logging
# ################################################


## New Logging features 
from martas import martaslog as ml
logpath = '/var/log/magpy/mm-dp-flagging.log'
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
name = "{}-DataProducts-flagging".format(sn)


# ################################################
#             Connecting DB
# ################################################

dbpasswd = mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print ("Connecting to primary MARCOS...")
    db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")
except:
    print ("... failed")
    try:
        # Test MARCOS 2
        print ("Connecting to secondary MARCOS...")
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
    except:
        print ("... failed -- aborting")
        sys.exit()
print ("...success")

# ################################################
#            Flagging dictionary
# ################################################


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
            'BM35':[7200,'var3','None','None',False,750,1000]
           }
## comments: switched ftom 5 to 6 for LEMI025 and LEMI036 at 01.08.2019

# ################################################
#             Config
# ################################################

step1 = True
step2 = True  # cleanup flaglist
step3 = False  # archive all old flags

dbdateformat = "%Y-%m-%d %H:%M:%S.%f"
submit = True # submit changes to database - False -> Test mode


# The following method will be part of the new flagging class
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
        
        if debug:
            print (name, len(testlist))
        # extract possible components
        #uniquecomponents = list(set([item for sublist in [el[2].split(',') for el in testlist] for item in sublist]))
        # better use componentgroups       
        uniquecomponents = list(set([el[2] for el in testlist]))
        if debug:
            print ("Components", uniquecomponents)

        for unid in uniqueids:
            idlist = [el for el in testlist if el[3] == unid]
            print (unid, len(idlist))
            for comp in uniquecomponents:
                complist = [el for el in idlist if comp == el[2]]
                if debug:
                    print ("Inputs for component {}: {}".format(comp,len(complist)))
                extendedcomplist = []
                for line in complist:
                    tdiff = (line[1]-line[0]).total_seconds()
                    if tdiff > sr:
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
                        if tdiff <= sr:
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
            print (unid, len(newflaglist))

    return newflaglist



if step1:
    ### SELECT all DataID's too be flagged
    name1 = name+'-step1'
    name2 = name+'-step1-lemitest'
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
            print ("Using the following parameter: keys={},threshold={},window={},limits={}".format(keys, threshold, window,[lowlimit,highlimit]))
            # Checking available sensors
            sensorlist = dbselect(db, 'DataID', 'DATAINFO','SensorID LIKE "{}%"'.format(elem))
            print ("   -> Found {}".format(sensorlist))
            print (" a) select 1 second or highest resolution data") # should be tested later again
            validsensors1 = []
            determinesr = []
            srlist = []
            for sensor in sensorlist:
                res = dbselect(db,'DataSamplingrate','DATAINFO','DataID="{}"'.format(sensor))
                try:
                    sr = float(res[0])
                    print ("Sensor: {} -> Samplingrate: {}".format(sensor,sr))
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
                    print ("Sensor: {} -> Samplingrate: {}".format(sensor,sr))
                    if sr >= 1:
                        validsensors1.append(sensor)
                        srlist.append(sr)
            print (" c) Check for recent data")
            validsensors = []
            validsr = []
            for idx,sensor in enumerate(validsensors1):
                last = dbselect(db,'time',sensor,expert="ORDER BY time DESC LIMIT 1")
                dbdate = last[0]
                if not dbdate.find('.') > 0:
                    dbdate = dbdate+'.0'
                #print ("   - checking whether data not older than {} sec is present".format(timerange))
                try:
                    if datetime.strptime(dbdate, dbdateformat) > starttime:
                        print ("   Valid data for {}".format(sensor))
                        validsensors.append(sensor)
                        validsr.append(srlist[idx])
                except:
                    print ("Dateformat problem for {}".format(sensor))
            print (" d) Flagging data")
            for idx,sensor in enumerate(validsensors):
                lines = int(timerange/validsr[idx])
                lastdata = dbgetlines(db,sensor,lines)
                print ("   - got {} datapoints".format(lastdata.length()[0]))
                if lastdata.length()[0] > 0:
                    sensorid = "_".join(sensor.split('_')[:-1])
                    print ("   - getting existing flags for {}".format(sensorid))
                    vflag = db2flaglist(db,sensorid,begin=datetime.strftime(starttime,"%Y-%m-%d %H:%M:%S"))
                    print ("   - found {} existing flags".format(len(vflag)))
                    if len(vflag) > 0:
                        try:
                            print ("  - removing existing flags")
                            lastdata = lastdata.flag(vflag)
                            lastdata = lastdata.remove_flagged()
                            print ("    ...success")
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
                       #flaglist = flagls
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
                    try:
                        if sensor.startswith('LEMI036_1') and len(flaglist) > 20:
                            statusmsg[name2] = 'Step1: flagging LEMI036 test: data range: {}, length 7200, first flag: {}, last flag: {}'.format(lastdata._find_t_limits(),flaglist[0],flaglist[-1])
                    except:
                        pass
                    if submit and len(flaglist) > 0:
                        print ("  -- Writing flags for sensors {} to DB".format(sensor))
                        print ("  -- New flags: {}".format(len(flaglist)))
                        prevflaglist = db2flaglist(db,sensorid)
                        if len(prevflaglist) > 0:
                            lastdata.flagliststats(prevflaglist, intensive=True)
                        else:
                            print ("  - no flags so far for this sensor")
                        flaglist2db(db,flaglist)
                        aftflaglist = db2flaglist(db,sensorid)
                        lastdata.flagliststats(aftflaglist, intensive=True)

        statusmsg[name1] = 'Step1: flagging data sets successful'
        print ("------------------------------------------")
        print (" Step1 flagging finished")
        print ("------------------------------------------")        
    except:
        print ("Step 1 failed")
        statusmsg[name1] = 'Step1: flagging failed'
                       
if step2:
    #try:
    print ("Cleaning up all records")
    stream = DataStream()
    flaglist2 = db2flaglist(db,'all')
    stream.flagliststats(flaglist2)
    #flaglist11 = stream.flaglistclean(flaglist2)
    #flaglist2db(db,flaglist11,mode='delete',sensorid='all')
    #except:

if step3:
    print ("Archiving flags")

    print ("Every year in February - archive full year two years ago")
    print ("Delete all inputs older than two years from DB") 
    ## e.g. Feb 2019 -> Keep only 2017 and 2018 in DB
    ## archive everything before 2017
    ## delete everything before 2017
    ## -> archive containsnow : flags_2016_final.pkl, # and 2015,etc
    ## ->                       flags_archive.pkl current backup (only monthly)
    ## -> DB contains 2017 to present ... 3 years max

martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)

