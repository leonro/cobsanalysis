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
# { SensorName : [timerange, keys, threshold, window, markall, lowlimit, highlimit]
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

if step1:
    ### SELECT all DataID's too be flagged
    name1 = name+'-step1'
    name2 = name+'-step1-lemitest'
    try:
        #ok = True
        #if ok:
        for elem in flagdict:
            print (" -------------------------------------------")
            print (" Dealing with sensorgroup {}".format(elem))
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
            print (window)
            markall = flagdict[elem][4]
            lowlimit = flagdict[elem][5]
            if lowlimit in ['Default','default','None','none','',None]:
               lowlimit = None
            highlimit = flagdict[elem][6]
            if highlimit in ['Default','default','None','none','',None]:
               highlimit = None
            starttime = datetime.utcnow()-timedelta(seconds=timerange)
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
                       flaglist = flagls
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

