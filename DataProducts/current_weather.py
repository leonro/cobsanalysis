
#!/usr/bin/env python
"""
MagPy - RCS Analysis 
Analyze Weather measurements from different sources
Sources are RCST7, LNM, ULTRASONIC, PRESSURE 
Provides:
- General minute METEO date for plots and distribution 
- Provide some specific combinantions for certain projects
- Analysis checks of data validity in source data
"""
from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred
import json, os

"""
# ################################################
#             Logging
# ################################################
"""

## New Logging features 
from martas import martaslog as ml
logpath = '/var/log/magpy/mm-dp-weather.log'
#import socket
#sn = socket.gethostname().upper()
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
name = "{}-DataProducts-weather".format(sn)

try: 
    from magpy.opt.analysismonitor import *
    analysisdict = Analysismonitor(logfile='/home/cobs/ANALYSIS/Logs/AnalysisMonitor_cobs.log')
    # next two line only necessary for first run
    #analysisdict['data_threshold_rain_RCST7_20160114_0001'] = [0.0,'<',0.3]
    #analysisdict.save('/home/cobs/ANALYSIS/Logs/AnalysisMonitor.pkl')
    analysisdict = analysisdict.load()
except:
    print ("Analysis monitor failed")
    pass



"""
# ################################################
#             General configurations
# ################################################
"""

## STEPS
## -------
product1 = True  # creates one minute cumulative meteo file, one second bm35 file, and data table
product2 = True  # table inputs
product3 = True  # creates short term plots
product4 = True  # creates long term plots
product5 = True  # upload table to broker - change that 

## CONFIG
## -------
submit = True ## True: data should be be uploaded
showplots = False  ## True: all plots should be shown (plt.show())
cleanupRCSflags = False  ## True: delete duplicate flags from RCS T7 record
writefile = True ## True: data will be written in files
writetodb = True ## True: data will be written to primary and secondary database
meteofilename = 'meteo-1min_'
currentvaluepath = '/srv/products/data/current.data'

## Definitions for part 1
## -------
# Define time frame  - for past data analysis use: starttime='2012-10-12'
dayrange = 3
endtime = datetime.utcnow()
starttime = endtime-timedelta(days=dayrange)
#starttime='2019-01-01'
#starttime = datetime(2018,1,1)
#endtime = datetime(2018,3,1)
source = 'database'  # access database
#source = 'archive'  # access dataarchive using sgopath
add2db = True #False

## PATHS
## -------
remotepath = 'zamg/images/graphs/meteorology'
remotedatapath = 'zamg/images/data'
imagepath = '/srv/products/graphs/meteo'
meteoproductpath = '/srv/products/data/meteo'
datapath = '/srv/products/data/meteo'
path2log = '/home/cobs/ANALYSIS/Logs/transfer_weather.log'
cred = 'cobshomepage'
brokercred = 'broker'
sgopath = '/srv/archive/SGO'

dbdateformat = "%Y-%m-%d %H:%M:%S.%f"

"""
# ################################################
#             Connect database
# ################################################
"""
primarydb = 'vegadb'
secondarydb = 'soldb'

primdbpwd = mpcred.lc(primarydb,'passwd')
primdbhost = mpcred.lc(primarydb,'host')
secdbpwd = mpcred.lc(secondarydb,'passwd')
secdbhost = mpcred.lc(secondarydb,'host')
secdb = False

try:
    # Test MARCOS 1
    print ("Connecting to primary DB at {} ...".format(primdbhost))
    db = mysql.connect(host=primdbhost,user="cobs",passwd=primdbpwd,db="cobsdb")
    print ("...success")
    try:
        print ("Connecting also secondary DB at {} ...".format(secdbhost))
        secdb = mysql.connect(host=secdbhost,user="cobs",passwd=secdbpwd,db="cobsdb")
        print ("...success")
    except:
        print ("...failed")
        pass
except:
    print ("... failed")
    try:
        # Test MARCOS 2
        print ("Connecting only secondary DB at {} ... - primary failed".format(secdbhost))
        db = mysql.connect(host=secdbhost,user="cobs",passwd=secdbpwd,db="cobsdb")
        print ("...success")
    except:
        print ("... failed -- aborting")
        sys.exit()

"""
# ################################################
#             SYNOP Definitions
# ################################################
"""

synopdict = {"-1":"Sensorfehler",
                 "41":"Leichter bis maessiger Niederschlag (nicht identifiziert)",
                 "42":"Starker Niederschlag (nicht identifiziert, unbekannt)",
                 "00":"Kein Niederschlag",
                 "51":"Leichter Niesel",
                 "52":"Maessiger Niesel",
                 "53":"Starker Niesel",
                 "57":"Leichter Niesel mit Regen",
                 "58":"Maessiger bis starker Niesel mit Regen",
                 "61":"Leichter Regen",
                 "62":"Maessiger Regen",
                 "63":"Starker Regen",
                 "67":"Leichter Regen",
                 "68":"Maessiger bis starker Regen",
                 "77":"Schneegriesel",
                 "71":"Leichter Schneefall",
                 "72":"Maessiger Schneefall",
                 "73":"Starker Schneefall",
                 "74":"Leichte Graupel",
                 "75":"Maessige Graupel",
                 "76":"Starke Graupel",
                 "89":"Hagel"}
trans=''

"""
# ################################################
#             Time, Range and Credentials
# ################################################
"""

fd = datetime.utcnow()
e = fd+timedelta(days=1)
highb = e - timedelta(days=1)
mb = e - timedelta(days=2)
hb = e - timedelta(days=365)
filedate = datetime.strftime(fd,"%Y-%m-%d")
fileyear = datetime.strftime(fd,"%Y")


# Getting credentials for homepage upload
address=mpcred.lc(cred,'address')
user=mpcred.lc(cred,'user')
passwd=mpcred.lc(cred,'passwd')
port=mpcred.lc(cred,'port')

# Getting credentials for cobs1 upload
brokeraddress=mpcred.lc(brokercred,'address')
brokeruser=mpcred.lc(brokercred,'user')
brokerpasswd=mpcred.lc(brokercred,'passwd')
brokerport=mpcred.lc(brokercred,'port')


def upload2homepage(source,destination,passwd,name):
           """
           Upload data to homepage by using 644 permissions
           """
           try:
               # to send with 664 permission use a temporary directory
               tmppath = "/tmp"
               tmpfile= os.path.join(tmppath,os.path.basename(source))
               from shutil import copyfile
               copyfile(source,tmpfile)
           except:
               pass
           try:
               scptransfer(tmpfile,'94.136.40.103:'+destination,passwd,timeout=100)
               os.remove(tmpfile)
           except:
               os.remove(tmpfile)


"""
# ################################################
#             part 1
# ################################################
 combines combinations of columns from different files into one data set
 example: an filtered one-minute output of all meteorological data
"""
windsens = ''
lnmsens = ''

if product1:
    print ("##############################################")
    print ("Part 1 - weather analysis - started at {}".format(fd))
    print ("##############################################")

    name1a = name+'-1a'
    name1b = name+'-1b'
    name1c = name+'-1c'
    name1d = name+'-1d'
    name1e = name+'-1e'
    name1f = name+'-1f'
    name1g = name+'-1g'
    name1h = name+'-1h'


    # ############################
    # # READ data
    # ############################
    # datasources are
    # LNM - Visibity, synop code, Rain, temperature
    # ULTRASONIC - windspeed, winddirection, temperature
    # BM35 - pressure -- flags are determined and written - data is filtered 
    # T7 - Rain, temperature, humidity, pressure, snowheight -- flags are determined and written
    # METEO - as T7, not well filtered, but realtime
    # others to be added

    # --------------------------------------------------------    
    # LNM - read suitable data from all available instruments and construct a combined dataset
    # Rain: x,df , Temperature: t1, Particle: f, visibility: y, synopcode?, Particles slow: dx Particles fast: dy
    print ("-----------------------")
    print ("1. SOURCE LNM")
    print ("-----------------------")
    print (" -- LNM - reading available data sets and apply existing flags ...")
    synop = ''
    trans = ''
    #ok = True
    try:
        #if ok:
        lnmlist = dbselect(db, 'DataID', 'DATAINFO','SensorID LIKE "LNM%"')
        lnmsens=[]
        for lnm in lnmlist:
            last = dbselect(db,'time',lnm,expert="ORDER BY time DESC LIMIT 1")
            if datetime.strptime(last[0], dbdateformat) > starttime:
                lnmsens.append(lnm)
        lnmst = DataStream([],{},np.asarray([[] for key in KEYLIST]))
        for lnm in lnmsens:
            print (" -- Dealing with {}".format(lnm))
            try:
                if source == 'database':
                    data = readDB(db,lnm,starttime=starttime,endtime=endtime)
                else:
                    data = read(os.path.join(sgopath,lnm[:-5],'raw/*'),starttime=starttime,endtime=endtime)
            except:
                data = DataStream()
            print (" -- Got data with range:", data._find_t_limits())

            # Get latest sysop data
            for index,elem in enumerate(data.ndarray):
                if len(elem) > 0:
                    #print ("KEY: {} = {} ({})".format(KEYLIST[index],elem[-1],data.header.get('col-{}'.format(KEYLIST[index]))))
                    if KEYLIST[index] == 'str1':
                        synop = elem[-1]

            transtmp = synopdict.get(str(synop))
            try:
                if (datetime.utcnow()-num2date(data.ndarray[0][-1]).replace(tzinfo=None)).total_seconds() < 3600:
                    print (" -- Current Weather: {}".format(transtmp))
                    trans = transtmp
                else:
                    print (" -- !!! No recent SYNOP data available - check LNM data") 
            except:
                pass

            if data.length()[0] > 0:
                print (" -- Getting existing flags ...")
                flaglist = db2flaglist(db,data.header.get("SensorID"))
                print (" -- Found existing flags: {}".format(len(flaglist)))
                data = data.flag(flaglist)
                data = data.remove_flagged()
                lnmst.extend(data.container,data.header,data.ndarray)
        print (" -- Determine average rain from LNM")
        if lnmst.length()[0] > 0:
            res2 = lnmst.steadyrise('x', timedelta(minutes=60),sensitivitylevel=0.002)
            lnmst= lnmst._put_column(res2, 'df', columnname='Percipitation',columnunit='mm/1h')
            lnmst= lnmst.resample(lnmst._get_key_headers(),period=60,startperiod=60)
            # Test merge to get synop data again
            print (" -- Merging synop code into resampled stream") 
            syn = data._get_column('str1')
            lnmst = lnmst._drop_column('var4')
            if len(syn) > 0:
                print ("TODO TEST", len(syn))
                syn = np.asarray([float(el) for el in syn])
                #print (len(syn))
                data = data._put_column(syn,'var4')
                #print ("Here")
                #print (data._get_column('var4'))
                #print (lnmst.header.get('col-var4'))
                lnmst = mergeStreams(lnmst,data, keys=['var4'])
            else:
                emp = [0]*lnmst.length()[0]
                lnmst = lnmst._put_column(emp,'var4')
                print ("No percipitaion found during the covered time range")
            if showplots:
                mp.plot(lnmst)
            #print (lnmst.ndarray)
        if lnmst.length()[0] > 0:
            statusmsg[name1a] = 'LNM data finished - data available'
        else:
            statusmsg[name1a] = 'LNM data finished - no data'
    except:
        statusmsg[name1a] = 'LNM data failed'

    # --------------------------------------------------------    
    # ULTRA - read suitable data from all available instruments and construct a combined dataset
    # Temperature: t2, Windspeed: var1, WindDir: var1
    print ("-----------------------")
    print (" 2. SOURCE ULTRASONIC")
    print ("-----------------------")
    print ("ULTRA - reading available data sets and apply existing flags ...")
    try:
        ultralist = dbselect(db, 'DataID', 'DATAINFO','SensorID LIKE "ULTRASONIC%"')
        ultrasens=[]
        #print (ultralist)
        for ultra in ultralist:
            #print ("ULTRA", ultra)
            last = dbselect(db,'time',ultra,expert="ORDER BY time DESC LIMIT 1")
            ### last2 should be the better alternative
            last2 = dbselect(db,'DataMaxTime','DATAINFO','DataID="{}"'.format(ultra))
            #print (last, starttime, last2)
            if datetime.strptime(last[0], dbdateformat) > starttime:
                ultrasens.append(ultra)
        ultrast = DataStream([],{},np.asarray([[] for key in KEYLIST]))
        for ultra in ultrasens:
            print (" -- Dealing with {}".format(ultra))
            try:
                if source == 'database':
                    data = readDB(db,ultra,starttime=starttime,endtime=endtime)
                else:
                    data = read(os.path.join(sgopath,ultra[:-5],'raw/*'),starttime=starttime,endtime=endtime)
            except:
                data = DataStream()
            print (" -- Got data with range:", data._find_t_limits())

            if data.length()[0] > 0:
                #data = readDB(db,ultra,starttime=starttime,endtime=endtime)
                print (" -- Getting existing flags ...")
                flaglist = db2flaglist(db,data.header.get("SensorID"))
                print (" -- Found existing flags: {}".format(len(flaglist)))
                data = data.flag(flaglist)
                data = data.remove_flagged()
                ultrast.extend(data.container,data.header,data.ndarray)
        if ultrast.length()[0] > 0:
            ultrast= ultrast.resample(ultrast._get_key_headers(),period=60,startperiod=60)
            statusmsg[name1b] = 'ULTRA data finished - data available'
        else:
            statusmsg[name1b] = 'ULTRA data finished - no data'
    except:
        statusmsg[name1b] = 'ULTRA data failed'

    print ("-----------------------")
    print (" 3. SOURCE BM35")
    print ("-----------------------")
    # --------------------------------------------------------    
    # BM35 - read suitable data from all available instruments and construct a combined dataset
    # Pressure: var3 
    print ("BM35 - reading available data sets and apply existing flags ...")
    try:
        bm3list = dbselect(db, 'DataID', 'DATAINFO','SensorID LIKE "BM35%" and DataID LIKE "%_0001"')
        bm3sens=[]
        for bm3 in bm3list:
            last = dbselect(db,'time',bm3,expert="ORDER BY time DESC LIMIT 1")
            if datetime.strptime(last[0], dbdateformat) > starttime:
                bm3sens.append(bm3)
        bm3st = DataStream([],{},np.asarray([[] for key in KEYLIST]))
        for bm3 in bm3sens:
            ## Please note: this approach needs to be changed if several BM35 are running in parallel
            try:
                if source == 'database':
                    data = readDB(db,bm3,starttime=starttime,endtime=endtime)
                else:
                    data = read(os.path.join(sgopath,bm3[:-5],'raw/*'),starttime=starttime,endtime=endtime)
            except:
                data = DataStream()
            if data.length()[0] > 0:
                bm3st.extend(data.container,data.header,data.ndarray)
        print (" -- Got data with range:", bm3st._find_t_limits())

        if bm3st.length()[0] > 0:
            bm3st= bm3st.filter(filter_width=timedelta(seconds=3.33333333),resample_period=1)
            print (" -- Getting existing flags ...")
            flaglist = db2flaglist(db,bm3st.header.get("SensorID"))
            print (" -- Found existing flags: {}".format(len(flaglist)))
            bm3st = bm3st.flag(flaglist)
            bm3st = bm3st.remove_flagged()
            flaglist2 = bm3st.flag_range(keys=['var3'],above=1000, text='pressure exceeding value range',flagnum=3)
            flaglist3 = bm3st.flag_range(keys=['var3'],below=750, text='pressure below value range',flagnum=3)
            #print flaglist
            if add2db:
                print ("  -- Writing flags  for sensors {} to DB".format(bm3st.header.get('SensorID')))
                print ("  -- New flags:", len(flaglist2)+len(flaglist3))
                if len(flaglist2) > 0:
                    flaglist2db(db,flaglist2)
                if len(flaglist3) > 0:
                    flaglist2db(db,flaglist3)
                # write to DB
                #writeDB(db, bm3st)  # -> move to filter job
            #print ("2", bm3st.ndarray)
            # drop flags
            bm3st = bm3st.flag(flaglist2)
            bm3st = bm3st.flag(flaglist3)
            bm3st=bm3st.remove_flagged()
            bm3st=bm3st.filter() # minute data

        if bm3st.length()[0] > 0:
            statusmsg[name1c] = 'BM35 data finished - data available'
        else:
            statusmsg[name1c] = 'BM35 data finished - no data'
    except:
        statusmsg[name1c] = 'BM35 data failed'


    # --------------------------------------------------------    
    # RCS - Get data from RCS (no version/revision control in rcs) 
    # Schnee: x, Temperature: y,  Maintainance: z, Pressure: f, Rain: t1,var1, Humidity: t2
    print ("-----------------------")
    print (" 4. SOURCE RCST7")
    print ("-----------------------")
    print ("RCST7 - reading available data sets and apply existing flags ...")
    filtrcst7st = DataStream()
    try:
        #ok = True
        #if ok:
        try:
            if source == 'database':
                rcst7st = readDB(db,'RCST7_20160114_0001_0001',starttime=starttime)
            else:
                rcst7st = read(os.path.join(sgopath,'RCST7_20160114_0001','raw/*'),starttime=starttime,endtime=endtime)
        except:
            rcst7st = DataStream()
        rcsstart,rcsend = rcst7st._find_t_limits()
        print (" -- Got data ranging from {} to {}".format(rcsstart,rcsend) )

        if rcst7st.length()[0]>0:
            if cleanupRCSflags:
                print (" -- Cleaning up all existing flags for {} ...".format(rcst7st.header.get("SensorID")))
                flaglist5 = db2flaglist(db,rcst7st.header.get("SensorID"))
                rcst7st.flagliststats(flaglist5, intensive=True)
                flaglist10 = rcst7st.flaglistclean(flaglist5)
                rcst7st.flagliststats(flaglist10, intensive=True)
                flaglist2db(db,flaglist10,mode='delete',sensorid=rcst7st.header.get("SensorID"))
                print (" -- Flag statistics  after cleanup:")
                rcst7st.flagliststats(flaglist, intensive=True)
            print (" -- Getting existing flags for {} ...".format(rcst7st.header.get("SensorID")))
            flaglist = db2flaglist(db,rcst7st.header.get("SensorID"),begin=rcsstart, end=rcsend)
            print (" -- Found {} flags for given time range".format(len(flaglist)))
            if len(flaglist) > 0:
                rcst7st = rcst7st.flag(flaglist)
            if showplots:
                mp.plot(rcst7st,annotate=True)
            rcst7st = rcst7st.remove_flagged()
            #mp.plot(rcst7st,annotate=True)
            rcst7st.header['col-y'] = 'T'
            rcst7st.header['unit-col-y'] = 'deg C'
            rcst7st.header['col-t2'] = 'rh'
            rcst7st.header['unit-col-t2'] = 'percent'
            rcst7st.header['col-f'] = 'P'
            rcst7st.header['unit-col-f'] = 'hPa'
            rcst7st.header['col-x'] = 'snowheight'
            rcst7st.header['unit-col-x'] = 'cm'
            flaglist = []
            print (" -- Cleanup snow height measurement - outlier")  # WHY NOT SAVED??
            removeimmidiatly = True
            if removeimmidiatly:
                rcst7st = rcst7st.flag_outlier(keys=['x'],timerange=timedelta(days=5),threshold=3)
                rcst7st = rcst7st.remove_flagged()
            else:
                flaglist = rcst7st.flag_outlier(keys=['x'],timerange=timedelta(days=5),threshold=3,returnflaglist=True)
            print (" --> Size of flaglist now {}".format(len(flaglist)))
            print (" -- Cleanup rain measurement")
            try:
                z = rcst7st._get_column('z')
                if np.mean('z') >= 0 and np.mean('z') <= 1:
                    flaglist0 = rcst7st.bindetector('z',1,['t1','z'],rcst7st.header.get("SensorID"),'Maintanence switch for rain bucket',markallon=True)
                else:
                    print (" --> flagging of service switch rain bucket failed")
            except:
                print (" --> flagging of service switch rain bucket failed")
                flaglist0 = []
            if len(flaglist) > 0 and len(flaglist0) > 0:
                flaglist.extend(flaglist0)
            elif not len(flaglist) > 0 and len(flaglist0) > 0:
                flaglist = flaglist0
            print (" --> Size of flaglist now {}".format(len(flaglist)))
            print (" -- Cleanup temperature measurement")
            flaglist1 = rcst7st.flag_outlier(keys=['y'],timerange=timedelta(hours=12),returnflaglist=True)
            if len(flaglist) > 0 and len(flaglist1) > 0:
                flaglist.extend(flaglist1)
            elif not len(flaglist) > 0 and len(flaglist1) > 0:
                flaglist = flaglist1
            print (" --> Size of flaglist now {}".format(len(flaglist)))
            print (" -- Cleanup pressure measurement")
            flaglist2 = rcst7st.flag_range(keys=['f'], flagnum=3, keystoflag=['f'], below=800,text='pressure below value range')
            if len(flaglist) > 0 and len(flaglist2) > 0:
                flaglist.extend(flaglist2)
            elif not len(flaglist) > 0 and len(flaglist2) > 0:
                flaglist = flaglist2
            flaglist3 = rcst7st.flag_range(keys=['f'], flagnum=3, keystoflag=['f'], above=1000,text='pressure exceeding value range')
            if len(flaglist) > 0 and len(flaglist3) > 0:
                flaglist.extend(flaglist3)
            elif not len(flaglist) > 0 and len(flaglist3) > 0:
                flaglist = flaglist3
            print (" --> Size of flaglist now {}".format(len(flaglist)))
            print (" -- Cleanup humidity measurement")
            flaglist4 = rcst7st.flag_range(keys=['t2'], flagnum=3, keystoflag=['t2'], above=100, below=0,text='humidity not valid')
            if len(flaglist) > 0 and len(flaglist4) > 0:
                flaglist.extend(flaglist4)
            elif not len(flaglist) > 0 and len(flaglist4) > 0:
                flaglist = flaglist4
            print (" --> Size of flaglist now {}".format(len(flaglist)))
            rcst7st = rcst7st.flag(flaglist)
            if showplots:
                mp.plot(rcst7st,annotate=True)
            print (" -- Found new flags: {}".format(len(flaglist)))
            if add2db:
                if len(flaglist) > 0:
                    print (" -- Adding {} flags to database: Sensor {}".format(len(flaglist),rcst7st.header.get('SensorID')))
                    #print (flaglist)
                    flaglist2db(db,flaglist)
                flagcheck = True
                if flagcheck:
                    print (" -- Getting again existing flags for {} ...".format(rcst7st.header.get("SensorID")))
                    flaglist = db2flaglist(db,rcst7st.header.get("SensorID"),begin=rcsstart, end=rcsend)
                    rcst7st.flagliststats(flaglist, intensive=True)
                    print (" -- Found now {} flags for given time range".format(len(flaglist)))

            #mp.plot(rcst7st, annotate=True)
            rcst7st = rcst7st.remove_flagged()
            # Now Drop flag and comment line - necessary because of later filling of gaps
            flagpos = KEYLIST.index('flag')
            commpos = KEYLIST.index('comment')
            rcst7st.ndarray[flagpos] = np.asarray([])
            rcst7st.ndarray[commpos] = np.asarray([]) 
            ## Now use missingvalue treatment
            rcst7st.ndarray[1] = rcst7st.missingvalue(rcst7st.ndarray[1],120,threshold=0.05,fill='interpolate')
            print ("Test plot with all flags removed")
            #mp.plot(rcst7st, annotate=True)
            print (" -- Determine average rain")
            res = rcst7st.steadyrise('t1', timedelta(minutes=60),sensitivitylevel=0.002)
            rcst7st= rcst7st._put_column(res, 'var1', columnname='Percipitation',columnunit='mm/1h')
            print (" -- Filter all RCS data columns to 1 min")
            filtrcst7st = rcst7st.filter(missingdata='interpolate')
        else:
            filtrcst7st = DataStream()
        
        if filtrcst7st.length()[0] > 0:
            statusmsg[name1d] = 'RCST7 data finished - data available'
        else:
            statusmsg[name1d] = 'RCST7 data finished - no data'
    except:
        statusmsg[name1d] = 'RCST7 data failed'


    # --------------------------------------------------------    
    # METEO - Get data from RCS (no version/revision control in rcs) 
    # Schnee: z, Temperature: f, Humidity: t1, Pressure: var5
    print ("-----------------------")
    print (" 5. SOURCE METEO from FP77")
    print ("-----------------------")
    try:
        try:
            if source == 'database':
                meteost = readDB(db,'METEO_T7_0001_0001',starttime=starttime)
            else:
                meteost = read(os.path.join(sgopath,'METEO_T7_0001','raw/*'),starttime=starttime,endtime=endtime)
        except:
            meteost = DataStream()
        print (" -- Got data with range:", meteost._find_t_limits())

        if meteost.length()[0] > 0:
            flaglist = db2flaglist(db,meteost.header.get("SensorID"))
            print (" -- Found existing flags: {}".format(len(flaglist)))
            meteost = meteost.flag(flaglist)
            meteost = meteost.remove_flagged()
            #mp.plot(rcst7st)
            meteost = meteost.flag_outlier(keys=['f','z'],timerange=timedelta(days=5),threshold=3)
            # meteo data is not flagged
            meteost = meteost.remove_flagged()
            print (" -- Cleanup pressure measurement")
            flaglist1 = meteost.flag_range(keys=['var5'], flagnum=3, keystoflag=['var5'], below=800,text='pressure below value range')
            flaglist.extend(flaglist1)
            flaglist15 = meteost.flag_range(keys=['var5'], flagnum=3, keystoflag=['var5'], above=1000,text='pressure exceeding value range')
            flaglist.extend(flaglist15)
            print (" -- Cleanup humidity measurement")
            flaglist2 = meteost.flag_range(keys=['t1'], flagnum=3, keystoflag=['t1'], above=100, below=0)
            flaglist.extend(flaglist2)
            meteost = meteost.flag(flaglist)
            meteost = meteost.remove_flagged()
            print (" -- Determine average rain")
            res = meteost.steadyrise('dx', timedelta(minutes=60),sensitivitylevel=0.002)
            meteost= meteost._put_column(res, 'y', columnname='Percipitation',columnunit='mm/1h')

            flagpos = KEYLIST.index('flag')
            commpos = KEYLIST.index('comment')
            meteost.ndarray[flagpos] = np.asarray([])
            meteost.ndarray[commpos] = np.asarray([])

        if meteost.length()[0] > 0:
            statusmsg[name1e] = 'METEO (FP77)  data finished - data available'
        else:
            statusmsg[name1e] = 'METEO (FP77)  data finished - no data'
    except:
        statusmsg[name1e] = 'METEO (FP77) data failed'

    print ("Data contents:")
    print ("Length LNM:",lnmst.length()[0], lnmst._find_t_limits())
    print ("Length Ultra:",ultrast.length()[0], ultrast._find_t_limits())
    print ("Length RCS:",filtrcst7st.length()[0], filtrcst7st._find_t_limits())
    print ("Length METEO:", meteost.length()[0], meteost._find_t_limits())
    print ("Length BM35:", bm3st.length()[0], bm3st._find_t_limits())


    # ############################
    # # Extract values
    # ############################

    print ("-----------------------")
    print (" 6. checking rain values")
    print ("-----------------------")
    try:
        # now get: filter data to 1 minute
        print ("Compare rain data from bucket and LNM")
        # TODO extract similar time rangee from both datasets
        print (lnmst._find_t_limits())
        print (filtrcst7st._find_t_limits())
        res = np.asarray([el for el in filtrcst7st._get_column('var1') if not np.isnan(el)])
        res2 = np.asarray([el for el in lnmst._get_column('df') if not np.isnan(el)][:len(res)])  # limit to the usually shorter rcst7 timeseries
        print ("Rain t7 and lnm", res, res2, np.sum(res), np.sum(res2))
        if len(res) > 1440*int(dayrange*0.5) and not np.mean(res) == 0:
            istwert = np.abs((np.mean(res) - np.mean(res2))/np.mean(res))
            sollwert = 0.3
            print (istwert,sollwert)
            #analysisdict.check({'data_threshold_rain_RCST7_20160114_0001': [istwert,'<',sollwert]})
            if np.abs((np.mean(res) - np.mean(res2))/np.mean(res)) > 0.3:
                print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print ("Current Weather: large differences between rain measurements !!") # add this to consistency log
                print (np.mean(res), np.mean(res2))
                print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                # you can switch to lnm data by activating the following two lines
                #data2 = data2._put_column(res2, 't2', columnname='Niederschlag',columnunit='mm/1h')
                #data = mergeStreams(data,data2,keys=['t2'],mode='replace')
        statusmsg[name1f] = 'Step1: checking rain succesful'
    except:
        statusmsg[name1f] = 'Step1: checking rain failed'


    #  Get average T, Rain, etc-> extract current synop
    # take only last 30 min of all data sets
    #lnmst._drop_column('str2')

    # ############################
    # # Create a new one minute combined record
    # ############################
    print ("-----------------------")
    print (" 7. REORDER data file")
    print ("-----------------------")

    # Maintainance: x, Rain: y, Snow: z, Temperature: f, Humidity: t1, t2, var1, var2, var3, Synop: var4, Pressure: var5,
    # rain, windspeed, winddirection, synop, 

    # start from Meteo data and gradually merge new data from rcs, ultra and lnm and bm35 to fill remaining gaps

    try:
        if ultrast.length()[0] > 0:
            # Ultra
            # Temperature: t2->f, Windspeed: var1, WindDir: var2
            ultrast._move_column('t2','f')
            ultrast._drop_column('t2')
        if bm3st.length()[0] > 0:
            # BM35
            # pressure: var3->var5
            bm3st._move_column('var3','var5')
            bm3st._drop_column('var3')
        if lnmst.length()[0] > 0:
            # LNM
            # Temperature: t1->f, Synop: str1->int(var4), Rain: df->y, Pressure: d->var5$
            lnmst._drop_column('x')
            lnmst._move_column('y','t2')
            lnmst._drop_column('z')
            lnmst._move_column('t1','f')
            lnmst._drop_column('t1')
            lnmst._drop_column('var1')
            lnmst._drop_column('var2')
            lnmst._drop_column('var3')
            #lnmst._move_column('str1','var4') # needs to be done before resampling
            #lnmst._drop_column('str1')
            #lnmst._drop_column('str2')
            lnmst._drop_column('var5')
            lnmst._drop_column('dx')
            lnmst._drop_column('dy')
            lnmst._drop_column('dz')
            lnmst._move_column('df','y')
            lnmst._drop_column('df')
            result = lnmst
        if filtrcst7st.length()[0] > 0:
            # RCS
            # -- reformating to meteo structure
            # Schnee: x->z, Temperature: y->f,  Maintainance: z, Pressure: f->var5, Rain: var1->y, Humidity: t2->t1
            filtrcst7st._move_column('f','var5')
            filtrcst7st._move_column('y','f')
            filtrcst7st._move_column('x','z')
            filtrcst7st._drop_column('x')
            filtrcst7st._move_column('var1','y')
            filtrcst7st._move_column('t2','t1')
            filtrcst7st._drop_column('t2')
            filtrcst7st._drop_column('var1')
            filtrcst7st._drop_column('var2')
            filtrcst7st._drop_column('var3')
            filtrcst7st._drop_column('var4')
            result = filtrcst7st
        if meteost.length()[0] > 0:
            # Meteo
            meteost._drop_column('x')
            meteost._drop_column('t2')
            meteost._drop_column('var1')
            meteost._drop_column('var2')
            meteost._drop_column('var3')
            meteost._drop_column('var4')
            meteost._drop_column('dx')
            meteost._drop_column('dy')
            meteost._drop_column('dz')
            meteost._drop_column('df')
            result = meteost

        statusmsg[name1g] = 'Step1: stream reformatting finished'
    except:
        statusmsg[name1g] = 'Step1: stream reformatting failed'

    print ("-----------------------")
    print (" 8. CREATING combined one minute data file")
    print ("-----------------------")
    #ok = True
    try:
        #if ok:
        if filtrcst7st.length()[0] > 0:
            print ("Merging meteo and rcs data")
            #print (result._find_t_limits())
            #print (filtrcst7st._find_t_limits())
            #print ("1:", result.ndarray)
            try:
                print ("Replacing meteo data with raw data from rcs")
                result = mergeStreams(result,filtrcst7st, mode='replace')
            except:
                print ("Alternative: inserting meteo data into rcs record")
                result = appendStreams([result, filtrcst7st])
            #result = result.remove_flagged()
            #mp.plot(result, annotate=True
            print (" -> new range:  {}".format(result._find_t_limits()))
        if lnmst.length()[0] > 0:
            # LNM
            print ("Merging lnm data")
            #print ("2:", result.ndarray)
            try:
                #print ("Result before", result.length())
                result = joinStreams(result, lnmst)
                result = mergeStreams(result, lnmst,mode='insert')
                #result = mergeStreams(result, lnmst, mode='insert')
                #print ("Result after lnm", result.length())
            except:
                #print ("Alternative: appending lnm data")
                pass
            #sys.exit()
            print (" -> new range:  {}".format(result._find_t_limits()))

        if ultrast.length()[0] > 0:
            # Ultra
            print ("Merging ultrasonic data")
            #print ("3:", result.ndarray)
            try:
                result = mergeStreams(result, ultrast, mode='insert')
            except:
                #print ("Alternative: appending ultra data")
                #result = appendStreams([result, ultrast])
                pass
        if bm3st.length()[0] > 0:
            # BM35
            print ("Merging bm35 data")
            #print ("4:", result.ndarray)
            try:
                result = mergeStreams(result, bm3st, mode='insert')
            except:
                #print ("Alternative: appending BM35 data")
                #result = appendStreams([result, bm3st])
                pass

        result.header['col-y'] = 'rain'
        result.header['unit-col-y'] = 'mm/h'
        result.header['col-z'] = 'snow'
        result.header['unit-col-z'] = 'cm'
        result.header['col-f'] = 'T'
        result.header['unit-col-f'] = 'deg C'
        result.header['col-t1'] = 'rh'
        result.header['unit-col-t1'] = 'percent'
        result.header['col-var4'] = 'synop'
        result.header['unit-col-var4'] = '4680'
        result.header['col-var5'] = 'P'
        result.header['unit-col-var5'] = 'hPa'
        result.header['col-t2'] = 'visibility'
        result.header['unit-col-t2'] = 'm'

        statusmsg[name1h] = 'Step1: combined meteo stream established'
    except:
        statusmsg[name1h] = 'Step1: combining meteo data failed'

    print ("DONE --------------")
    #print (result.ndarray)
    # Add plain text synop descriptions
    syno = result._get_column('var4')
    txt= []
    for el in syno:
        try:
            itxt = str(int(el))
            txt.append(synopdict.get(itxt,''))
        except:
            txt.append('')
    txt = np.asarray(txt)
    #print (len(txt), len(syno), txt, syno)
    result._put_column(txt,'str2')

    if writefile:
        # save result to products
        result.write(meteoproductpath,filenamebegins=meteofilename,dateformat='%Y',coverage='year', mode='replace',format_type='PYCDF')
    if writetodb:
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print (" Writing results to database, group services")
        ## Set some important header infos
        result.header['SensorID'] = 'METEO_adjusted_0001'
        result.header['DataID'] = 'METEO_adjusted_0001_0001'
        result.header['SensorGroup'] = 'services'
        writeDB(db,result)
        if secdb:
            writeDB(secdb,result)
        print ("METEO_adjusted written to DB")

if product2 and product1:
    print ("Part 2 - table contents - started at {}".format(fd))
    print ("-------------------------------------------------")
    name2 = name+'-2'
    try:
        # Get the last 30 min
        print ("Coverage:", result._find_t_limits())
        lastdate = result.ndarray[0][-1]
        shortextract = result._select_timerange(starttime=num2date(lastdate)-timedelta(minutes=30), endtime=lastdate)
        poscom = KEYLIST.index('comment')
        posflag = KEYLIST.index('flag')
        shortextract[poscom] = np.asarray([])
        shortextract[posflag] = np.asarray([])
        #print shortextract
        # get means
        #mp.plot(result)

        shortstream = DataStream([],result.header,shortextract)
        vallst = [0 for key in KEYLIST]
        for idx,key in enumerate(result._get_key_headers()):
            if key in NUMKEYLIST:
                # alternative
                col = shortstream._get_column(key)
                if len(col)> 0 and not np.isnan(col).all():
                    print (len(col), col[0])
                    mean = np.nanmedian(col)
                else:
                    mean = np.nan
                vallst[idx] = mean
                #shortstream = shortstream._drop_nans(key)
                #mean = shortstream.mean(key,meanfunction='median',percentage=25)
                #vallst[idx] = mean
                print ("Assigning values", idx, key, mean)
        # print ("Res", vallst)
        # Update current value dictionary:
        if os.path.isfile(currentvaluepath):
            # read log if exists and exentually update changed information
            # return changes
            with open(currentvaluepath, 'r') as file:
                fulldict = json.load(file)
                valdict = fulldict.get('meteo')
        else:
            valdict = {}
            fulldict = {}
            #fulldict['meteo'] = valdict
        print ("Got old", valdict) 

        ### Snow or not snow, this is the question
        def snowornosnow(T,S,Exist,SYNOP,p_L=0,threshold=190):
           """
           Estimate whether snow is existing or not
           This method uses several parameters related to snow and 
           adds them up to a probability sum, indicating whether snow
           is accumulating or not. Currently five parameters are tested:
           1. Temperature (high probablity at low temperatures)
           2. Snow height (high probability at high values -> obviuos)
              problematic are low values (<5cm)
           3. SYNOP code: high probability is its snowing or hailing (SYNOP>70)
           4. Snow already existing
           5. some location related manual probability (0 in our case)
           Useful extensions:
           6. Soil or ground temperature (or eventually temperature history)
           7. Reflectivity
           Each parameter adds to a probability sum. If this sum 
           exceeds the given threshold value we assume a snow cover
           """
           def p_T(T): # linear probability decrease
               try:
                   if T >= 15:
                       return 0.
                   elif T <= 1:
                       return 100.
                   else:
                       return -7.14286*T  + 107.69231
               except:
                   return 0.

           def p_S(S): # exponential probability increase
               try:
                   if S <= 0:
                       return 0.
                   elif S >= 5:
                       return 100.
                   else:
                       return 4.*S*S
               except:
                   return 0.

           def p_SYNOP(SYNOP):
               try:
                   if int(SYNOP) > 70:
                      return 100.
                   else:
                      return 0.
               except:
                   return 0.

           def p_E(Exist):
               try:
                   if Exist in ['Snow','Schnee']:
                       return 100.
                   else:
                       return 0.
               except:
                   return 0.

           sump = p_T(T) + p_S(S) + p_SYNOP(SYNOP) + p_E(Exist) + p_L
           print (p_T(T),p_S(S),p_SYNOP(SYNOP),p_E(Exist),p_L)
           print (" -- current snow cover probability value: {}".format(sump))
           if sump > threshold:
               return 'Schnee'
           else:
               return '-'

        if len(shortextract[KEYLIST.index('var4')]) > 0:
            try:
                SYNOP = max(shortextract[KEYLIST.index('var4')])
            except:
                SYNOP = 0
        else:
            SYNOP = 0

        cover = snowornosnow(vallst[2],vallst[1],valdict.get('Schnee',['-'])[0],SYNOP,p_L=0)

        valdict[u'Schnee'] = [cover,'']
        valdict[u'date'] = [str(num2date(lastdate).replace(tzinfo=None)), '']
        valdict[u'T'] = [vallst[2], 'degC']
        valdict[u'rh'] = [vallst[3], '%']
        valdict[u'P'] = [vallst[8], 'hPA']
        valdict[u'S'] = [vallst[1], 'cm']
        valdict[u'N'] = [vallst[0],'mm/h']
        valdict[u'Wind'] =  [vallst[5]*3.6,'km/h']
        valdict[u'Niederschlag'] = [trans,'SYNOP']
        fulldict[u'meteo'] = valdict
        print ("Here2")
        with open(currentvaluepath, 'w',encoding="utf-8") as file:
            file.write(unicode(json.dumps(fulldict))) # use `json.loads` to do the reverse
            print ("Current meteo data written successfully to {}".format(currentvaluepath))

        print ("Here3")

        # save extracts to project directories
        writeline = r'Date:{z},T(degC):{a:.1f},rh(%):{b:.1f},P(hPA):{c:.1f},S(cm):{d:.0f},N(mm/h):{e:.1f},Wind(m/s):{f:.1f},Niederschlag:{g},Am Boden:{h}\n'.format(z=str(num2date(lastdate).replace(tzinfo=None)), a=vallst[2],b=vallst[3],c=vallst[7],d=vallst[1],e=vallst[0],f=vallst[5],g=trans,h=cover)
        print ("DataLine", writeline)
        with open(os.path.join(datapath,'currentmeteo.csv'), 'wb') as myfile:
            myfile.write(writeline)
  
        if submit:
            #ftpdatatransfer(localfile=os.path.join(datapath,'currentmeteo.csv'),ftppath=remotedatapath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
            savedatapathnew = os.path.join(datapath,'currentmeteo.csv')
            try:
                upload2homepage(savedatapathnew,remotedatapath,passwd,'currentmeteo_data')
                print (" -- Upload to conrad homepage successful")
            except:
                print (" -- Failed to upload current_meteo")
            try: # to upload current_data.json - for the moment save it to conrad homepage ... to cobs.zamg.ac.at
                upload2homepage(currentvaluepath,remotedatapath,passwd,'current_data')
                print (" -- Upload to conrad homepage successful")
            except:
                print (" -- Failed to upload current value json")
        statusmsg[name2] = 'Step2: recent data extracted'
    except:
        print ("step 2 failed")
        statusmsg[name2] = 'Step2: current data failed'

if product3 and product1:
    print ("Part 3 - short term weather plot - started at {}".format(fd))
    print ("-------------------------------------------------")
    name3 = name+'-3'

    try:
        import pylab
        # ############################
        # # Create plots ???? -> move to plot
        # ############################
        result.ndarray[2] = result.missingvalue(result.ndarray[2],3600,threshold=0.05,fill='interpolate')
        result.ndarray[3] = result.missingvalue(result.ndarray[3],600,threshold=0.05,fill='interpolate')

        longextract = result._select_timerange(starttime=datetime.utcnow()-timedelta(days=2), endtime=endtime)

        #print ("Test", longextract)
        t = longextract[0]
        y2 = longextract[2]
        y3 = longextract[3]
        y4 = longextract[4]
        y7 = longextract[7]
        max1a = 0
        max1b = 0
        max2a = 0

        print (len(t), len(y2), len(y3), len(y4), len(y7))

        if len(y2) > 0:
            max1a = np.nanmax(y2)
        if max1a < 10 or np.isnan(max1a):
            max1a = 10
        if len(y3) > 0:
            max1b = np.nanmax(y3)
        if max1b < 100 or np.isnan(max1b):
            max1b = 100
        if len(y7) > 0:
            max2a = np.nanmax(y7)
        if max2a < 12 or np.isnan(max2a):
            max2a = 12

        fig, axarr = plt.subplots(3, sharex=True, figsize=(15,9))
        # first plot (temperature)
        axarr[0].set_ylabel('T [$ \circ$C]')
        axarr[0].plot_date(t,y4,'-',color='lightgray')
        axarr[0].fill_between(t,0,y4,where=y4<0,facecolor='blue',alpha=0.5)
        axarr[0].fill_between(t,0,y4,where=y4>=0,facecolor='red',alpha=0.5)
        #ax0 = axarr[0].twinx()
        #ax0.set_ylim([0,100])
        #ax0.set_ylabel('RH [%]')
        #ax0.plot_date(longextract[0],longextract[5],'-',color='green')
        axarr[1].set_ylabel('S [cm]')
        axarr[1].set_ylim([0,max1b])
        axarr[1].plot_date(longextract[0],longextract[3],'-',color='gray')
        axarr[1].fill_between(t,0,y3,where=longextract[3]>=0,facecolor='gray',alpha=0.5)
        ax1 = axarr[1].twinx()
        ax1.set_ylabel('N [mm/h]',color='blue')
        ax1.set_ylim([0,max1a])
        ax1.plot_date(t,y2,'-',color='blue')
        ax1.fill_between(t,0,y2,where=y2>=0,facecolor='blue',alpha=0.5)
        axarr[2].set_ylabel('Wind [m/s]')
        axarr[2].set_ylim([0,max2a])
        axarr[2].plot_date(t,y7,'-',color='gray')
        axarr[2].fill_between(t,0,y7,where=longextract[7]>=0,facecolor='gray',alpha=0.5)
        savepath = os.path.join(imagepath,'Meteo_0_'+filedate+'.png')
        pylab.savefig(savepath)
        if showplots:
            plt.show()
        if submit:
            try:
                upload2homepage(savepath,remotepath,passwd,'meteo0_graph')
            except:
                print ("Submitting plot to homepage failed - no warning")
        statusmsg[name3] = 'Step3: two day plot finished'
    except:
        print ("Step 3 failed")
        statusmsg[name3] = 'Step3: two day plot failed'


if product4:
    print ("Part 4 - long term weather plot - started at {}".format(fd))
    print ("-------------------------------------------------")
    name4 = name+'-4'
    try:
        longterm=read(os.path.join(meteoproductpath,'{}*'.format(meteofilename)), starttime=hb,endtime=fd)
        print ("Please note: long term plot only generated from cron or root")
        print ("Long term plot", longterm.length(), fd, hb)
        print (t.dtype)
        t = longterm.ndarray[0].astype(float64)
        print (t.dtype)
        y2 = longterm.ndarray[2].astype(float64)  # Rain
        y3 = longterm.ndarray[3].astype(float64)  # Snow
        y4 = longterm.ndarray[4].astype(float64)  # Temp
        y7 = longterm.ndarray[7].astype(float64)

        print ("LongTerm Parameter", len(t), len(y2), len(y3), len(y4), len(y7))
        #mp.plot(longterm)

        max1a = np.nanmax(y2)
        if max1a < 10 or np.isnan(max1a):
            max1a = 10
        max1b = np.nanmax(y3)
        if max1b < 100 or np.isnan(max1b):
            max1b = 100
        max2a = np.nanmax(y7)
        if max2a < 12 or np.isnan(max2a):
            max2a = 12

        print ("Max values redefined")
        fig, axarr = plt.subplots(3, sharex=True, figsize=(15,9))
        # first plot (temperature)
        axarr[0].set_ylabel('T [$ \circ$C]')
        axarr[0].plot_date(t,y4,'-',color='lightgray')
        try:
            axarr[0].fill_between(t,[0]*len(t),y4,where=y4<0,facecolor='blue',alpha=0.5)
            axarr[0].fill_between(t,0,y4,where=y4>=0,facecolor='red',alpha=0.5)
        except:
            pass
        #ax0 = axarr[0].twinx()
        #ax0.set_ylim([0,100])
        #ax0.set_ylabel('RH [%]')
        #ax0.plot_date(longextract[0],longextract[5],'-',color='green')
        axarr[1].set_ylabel('S [cm]')
        axarr[1].set_ylim([0,max1b])
        axarr[1].plot_date(t,y3,'-',color='gray')
        try:
            axarr[1].fill_between(t,0,y3,where=y3>=0,facecolor='gray',alpha=0.5)
        except:
            pass
        ax1 = axarr[1].twinx()
        ax1.set_ylabel('N [mm/h]',color='blue')
        ax1.set_ylim([0,max1a])
        ax1.plot_date(t,y2,'-',color='blue')
        try:
            ax1.fill_between(t,0,y2,where=y2>=0,facecolor='blue',alpha=0.5)
        except:
            pass
        axarr[2].set_ylabel('Wind [m/s]')
        axarr[2].set_ylim([0,max2a])
        axarr[2].plot_date(t,y7,'-',color='gray')
        try:
            axarr[2].fill_between(t,0,y7,where=y7>=0,facecolor='gray',alpha=0.5)
        except:
            pass
        savepath = os.path.join(imagepath,'Meteo_1.png')
        pylab.savefig(savepath)
        print ("plot generated")
        if showplots:
            plt.show()
        if submit:
            #ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
            #scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
            upload2homepage(savepath,remotepath,passwd,'meteo1_graph')
        statusmsg[name4] = 'Step4: long term plot finished'
    except:
        print ("Step 4 failed")
        statusmsg[name4] = 'Step4: long term plot failed'

if product5:
    print ("Part 5 - Save meteo data on Broker - started at {}".format(fd))
    print ("-------------------------------------------------")
    name5a = name+'-5a'
    name5b = name+'-5b'

    ### Save result table directly to the brokers database
    ### 1. Get all info
    try:
        #print (result.length())
        #print (result.samplingrate())
        result.header['SensorID'] = 'METEO_comb_0001'
        result.header['DataID'] = 'METEO_comb_0001_0001'
        result.header['StationIAGAcode'] = ''
        result.header['SensorKeys'] = ''
        result.header['SensorDescription'] = ''
        result.header['SensorSerialNum'] = ''
        result.header['DataFormat'] = ''
        result.header['SensorElements'] = ''
        result.header['SecondarySensorID'] = ''
        result.header['SensorDataLogger'] = ''
        #result.header
        print (" -- Uploading table to database on internal ZAMG broker:")
        print ("    Tablename: {}".format('DataID'))
        #print (result.header)
        #print (result.ndarray)
        ###
        try:
            brokerdb = mysql.connect(host=brokeraddress,user=brokeruser,passwd=brokerpasswd,db="cobsdb")
            statusmsg[name5b] = 'Step5: database on broker {} connected'.format(brokeraddress)
            print ("Step 5: connected to broker")
            if submit:
                writeDB(brokerdb,result)
            #if submit:  ## MOVED THE FOLLOWING Directly to the BROKER
            #    print ("Uploading current weather data (json) to broker")
            #    timeout=60
            #    remotepath="{}:/home/cobs/TABLES/".format(brokeraddress)
            #    scptransfer(currentvaluepath,remotepath,brokerpasswd,timeout=timeout)
            #    #sshdatatransfer(currentvaluepath,remotepath)
        except:
            statusmsg[name5b] = 'Step5: connection to broker  {} failed'.format(brokeraddress)
            print ("Step 5: broker connection failed")
        statusmsg[name5a] = 'Step5: upload to public broker finished'
    except:
        print ("Step 5 failed")
        statusmsg[name5a] = 'Step5: broker upload failed'

print ("Script weather analysis finished at {}".format(datetime.utcnow()))  
print ("-------------------------------------")    

martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)
