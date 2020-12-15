#!/usr/bin/env python

"""
Magnetism products and graphs
"""

from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred
import io, pickle

import itertools
from threading import Thread
from subprocess import check_output   # used for checking whether send process already finished

# ################################################
#             Logging
# ################################################

## New Logging features 
from martas import martaslog as ml
logpath = '/var/log/magpy/mm-dp-magnetism.log'
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
name = "{}-DataProducts-magnetism".format(sn)

try: 
    from magpy.opt.analysismonitor import *
    analysisdict = Analysismonitor(logfile='/home/cobs/ANALYSIS/Logs/AnalysisMonitor_cobs.log')
    analysisdict = analysisdict.load()
except:
    print ("Analysis monitor failed")
    pass

# ################################################
#             Database connection
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


# ################################################
#             Configuration
# ################################################


part1 = True # analysis of primary sensors -> adjusted data
part2 = False # submit adjusted data
part3 = False # Quasidefinitive data
part4 = False # submit QD data
part5 = False # create graphs and plot
part6 = False

endtime = datetime.utcnow()
daystodeal = 1
primpier = 'A2'

submit2gin = False

currentvaluepath = '/srv/products/data/current.data'

#dbdateformat = "%Y-%m-%d %H:%M:%S.%f" ## not used since 0.3.99rc0 -> replace by getstringdate method

#Instrument Lists
variolist = ['LEMI036_1_0002_0002','LEMI025_22_0003_0002','FGE_S0252_0001_0001']
scalalist = ['GSM90_14245_0002_0002','GSM90_6107631_0001_0001','GP20S3NSS2_012201_0001_0001']

# ################################################
#             Paths and derived time ranges
# ################################################

starttimedt = endtime-timedelta(days=daystodeal)
starttime=datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d")
date = datetime.strftime(endtime,"%Y-%m-%d")
today1 = datetime.strftime(endtime,"%Y%m%d")
yesterd1 = datetime.strftime(endtime-timedelta(days=daystodeal),"%Y%m%d")
yesterd2 = datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d")
weekago = datetime.strftime(endtime-timedelta(days=6),"%Y-%m-%d")

uploadlist = [today1,yesterd1]

if daystodeal > 2:
    uploadlist = [datetime.strftime(endtime-timedelta(days=i),"%Y%m%d") for i in range(0,daystodeal,1)]

# Manual upload
#uploadlist = ['20180109', '20180108', '20180107', '20180106', '20180105', '20180104', '20180103', '20180102', '20180101', '20171231', '20171230', '20171229', '20171228']

vpath = '/srv/products/data/magnetism/variation/'
vpathsec = '/srv/products/data/magnetism/variation/sec/'
vpathmin = '/srv/products/data/magnetism/variation/min/'
vpathcdf = '/srv/products/data/magnetism/variation/cdf/'
qpath = '/srv/products/data/magnetism/quasidefinitive/'
qpathsec = '/srv/products/data/magnetism/quasidefinitive/sec/'
qpathcdf = '/srv/products/data/magnetism/quasidefinitive/cdf/'
figpath = '/srv/products/graphs/magnetism/'


# ################################################
#             Credentials
# ################################################

gincred = 'gin'
ginaddress=mpcred.lc(gincred,'address')
ginuser=mpcred.lc(gincred,'user')
ginpasswd=mpcred.lc(gincred,'passwd')

zamgcred = 'zamg'
zamgaddress=mpcred.lc(zamgcred,'address')
zamguser=mpcred.lc(zamgcred,'user')
zamgpasswd=mpcred.lc(zamgcred,'passwd')
zamgport=mpcred.lc(zamgcred,'port')

cred = 'cobshomepage'
address=mpcred.lc(cred,'address')
user=mpcred.lc(cred,'user')
passwd=mpcred.lc(cred,'passwd')
port=mpcred.lc(cred,'port')
remotepath = 'zamg/images/graphs/magnetism/'
path2log = '/home/cobs/ANALYSIS/Logs/graph.log'

#qdlist = False
uploadminlist,uploadseclist = [],[]

# ################################################
#            Local Methods
# ################################################


def getstringdate(input):
     # Part of Magpy starting with version ??
    dbdateformat1 = "%Y-%m-%d %H:%M:%S.%f"
    dbdateformat2 = "%Y-%m-%d %H:%M:%S"
    try:
        value = datetime.strptime(input,dbdateformat1)
    except:
        try:
            value = datetime.strptime(input,dbdateformat2)
        except:
            print ("Geetprimary: error when converting database date to datetime")
            return datetime.utcnow()
    return value


def active_pid(name):
     # Part of Magpy starting with version ??
    try:
        pids = map(int,check_output(["pidof",name]).split())
    except:
        return False
    return True


def convertGeoCoordinate(lon,lat,pro1,pro2):
     # Part of Magpy starting with version ??
    try:
        from pyproj import Proj, transform
        p1 = Proj(init=pro1)
        x1 = lon
        y1 = lat
        # projection 2: WGS 84
        p2 = Proj(init=pro2)
        # transform this point to projection 2 coordinates.
        x2, y2 = transform(p1,p2,x1,y1)
        return x2, y2
    except:
        return lon, lat

p1start = datetime.utcnow()
if part1:
    """
    Create and submit variation/adjusted data
    Use one specific (primary) variometer and scalar instrument with current data
    Done here to be as close as possible to data acquisition
    """
    print ("----------------------------------------------------------------")
    print ("Part 1: Create adjusted one second data from primary instruments")
    print ("----------------------------------------------------------------")

    print (" a) get primary instruments")
    name1a = "{}-step1a".format(name)
    varioinst = ''
    scalainst = ''
    variosens = ''
    scalasens = ''
    #try:
    ok = True
    if ok:
        if os.path.isfile(currentvaluepath):
            with open(currentvaluepath, 'r') as file:
                fulldict = json.load(file)
                valdict = fulldict.get('magnetism')
            varioinst = valdict.get('primary vario','')[0]
            scalainst = valdict.get('primary scalar','')[0]
            variosens = "_".join(varioinst.split('_')[:-1])
            scalasens = "_".join(scalainst.split('_')[:-1])
        if not varioinst == "":
            print ("Found {} as primary variometer and {} as scalar instrument".format(varioinst,scalainst))
            statusmsg[name1a] = 'primary instruments selected'
        else:
            varioinst = variolist[0]
            statusmsg[name1a] = 'primary instrument could not be assigned automatically'
    #except:
    #    statusmsg[name] = 'primary instrument assignment failed'

    print (" b) getting variometer data, applying flags, offsets, WGS coordinates {}".format(varioinst))
    name1b = "{}-step1b".format(name)
    if not varioinst == '':
        vario = readDB(db,varioinst,starttime=yesterd2)
        if (vario.length()[0]) > 0:
            # Apply existing flags from DB
            varioflag = db2flaglist(db,variosens,begin=datetime.strftime(starttimedt,"%Y-%m-%d %H:%M:%S"))
            print ("     -- getting flags from DB: {}".format(len(varioflag)))
            if len(varioflag) > 0:
                vario = vario.flag(varioflag)
                vario = vario.remove_flagged()
            vario = applyDeltas(db,vario)
            #convert latlong
            vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude'] = convertGeoCoordinate(vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude'],'epsg:31253','epsg:4326')
            #print vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude']
            vario.header['DataLocationReference'] = 'WGS84, EPSG:4326'
            statusmsg[name1b] = 'variometer data loaded'
        else:
            print ("Did not find variometer data - aborting")
            statusmsg[name1b] = 'variometer data load faild -aborting'
            sys.exit()
    else:
        print ("No variometer - aborting")
        statusmsg[name1b] = 'no variometer specified - aborting'
        sys.exit()

    print (" c) getting scaladata, flags and offsets: {}".format(scalainst))
    name1c = "{}-step1c".format(name)
    if not scalainst == '':
        scalar = readDB(db,scalainst,starttime=yesterd2)
        if (scalar.length()[0]) > 0:
            print ("     -- obtained data")
            scalarflag = db2flaglist(db,scalasens,begin=datetime.strftime(starttimedt,"%Y-%m-%d %H:%M:%S"))
            print ("     -- getting flags from DB: {}".format(len(scalarflag)))
            #mp.plot(scalar)
            if len(scalarflag) > 0:
                scalar = scalar.flag(scalarflag)
                scalar = scalar.remove_flagged()
            #mp.plot(scalar)
            scalar = applyDeltas(db,scalar)
            statusmsg[name1c] = 'scalar data loaded'
        else:
            print ("Did not find scalar data - aborting")
            statusmsg[name1c] = 'scalar data load faild - aborting'
            #sys.exit()
    else:
        print ("No scalar instrument - aborting")
        statusmsg[name1c] = 'no scalar instrument specified - aborting'
        sys.exit()

    print (" d) performing simple baseline correction based on pier {}".format(primpier))
    print ("    {} and {}".format(varioinst, scalainst))
    name1d = "{}-step1d".format(name)
    # Define BLV source:
    blvdata = 'BLV_{}_{}_{}'.format(variosens,scalasens,primpier) 
    print ("    using BLV data".format(blvdata))
    # Check if such a baseline is existing - if not abort and inform

    try:
        #basesimple = True
        #if basesimple:
        absr = readDB(db,blvdata, starttime = endtime-timedelta(days=100))
        blvflaglist = db2flaglist(db,blvdata)
        print ("  - Found {} flags for baseline values".format(len(blvflaglist)))
        if len(blvflaglist) > 0:
            absr = absr.flag(blvflaglist)
            absr = absr.remove_flagged()
        absr = absr._drop_nans('dx')
        absr = absr._drop_nans('dy')
        absr = absr._drop_nans('dz')
        bh, bhstd = absr.mean('dx',meanfunction='median',std=True)
        bd, bdstd = absr.mean('dy',meanfunction='median',std=True)
        bz, bzstd = absr.mean('dz',meanfunction='median',std=True)

        print ("  - Basevalues for last 100 days:")
        print ("  - Delta H = {a} +/- {b}".format(a=bh, b=bhstd))
        print ("  - Delta D = {a} +/- {b}".format(a=bd, b=bdstd))
        print ("  - Delta Z = {a} +/- {b}".format(a=bz, b=bzstd))
        
        print ("  - Running Analysis jobs ... ")
        analysisname1 = 'data_threshold_amount_BLV_'+varioinst[:-5]+'_'+scalainst[:-5]+'_A2'
        analysisname2 = 'data_actuality_time_BLV_'+varioinst[:-5]+'_'+scalainst[:-5]+'_A2'
        analysisname3 = 'data_threshold_base_BLV_'+varioinst[:-5]+'_'+scalainst[:-5]+'_A2'
        analysisdict.check({analysisname1: [absr.length()[0],'>',10]})
        solldatum = endtime - timedelta(days=14)
        istdatum = num2date(absr.ndarray[0][-1])
        analysisdict.check({analysisname2: [istdatum,'>',solldatum]})
        analysisdict.check({analysisname3: [bhstd+bzstd,'<',5]})

        print ("  - Performing constant basevalue correction")
        vario = vario.simplebasevalue2stream([bh,bd,bz])
        statusmsg[name1d] = 'baseline correction successful'
    except:
        print (" !!!!!!!!!! Baseline correction failed")
        statusmsg[name1d] = 'baseline correction failed'

    print (" e) combining data sets, adding header, writing sec and min data")
    name1e = "{}-step1e".format(name)
    try:
        prelim = mergeStreams(vario,scalar,keys=['f'])
        print ("  - preliminary data file after MERGE:", prelim.length())
        prelim.header['DataPublicationLevel'] = '2'
        prelim.header['DataPublicationDate'] = date
        # Eventually convert it to XYZ
        prelim = prelim.hdz2xyz()
        if len(prelim.header['DataComponents']) < 4:
            prelim.header['DataComponents'] += 'F'
        prelim = prelim._drop_column('flag')
        prelim = prelim._drop_column('comment')
        # Save it
        print ("  - Saving one second data - IAGA")
        #prelim.write(vpathsec,filenamebegins="wic",dateformat="%Y%m%d",filenameends="psec.sec",format_type='IAGA')
        # supported keys of IMAGCDF -> to IMF format
        #supkeys = ['time','x','y','z','f','df']
        print ("  - Saving one second data - CDF")
        #prelim.write(vpathcdf,filenamebegins="wic_",dateformat="%Y%m%d_%H%M%S",format_type='IMAGCDF',filenameends='_'+prelim.header['DataPublicationLevel']+'.cdf')
        print (prelim.length()[0])
        print ("  - Saving one minute data - IAGA")
        prelimmin = prelim.filter()
        #prelimmin.write(vpathmin,filenamebegins="wic",dateformat="%Y%m%d",filenameends="pmin.min",format_type='IAGA')
        #mp.plot(prelimmin)
        print ("  - Saving one minute adjusted data to database")
        prelimmin.header['DataID'] = "wic_adjusted_0001_0002"
        prelimmin.header['SensorID'] = "wic_adjusted_0001"
        variocol = np.asarray([varioinst for el in prelimmin.ndarray[0]])
        scalacol = np.asarray([scalainst for el in prelimmin.ndarray[0]])
        prelimmin = prelimmin._put_column(variocol, 'str1')
        prelimmin = prelimmin._put_column(scalacol, 'str2')
        #print (prelimmin.ndarray)
        #writeDB(db,prelimmin,tablename="wic_adjusted_0001_0002")
        statusmsg[name1e] = 'all data files saved'
    except:
        print (" !!!!!!!!!!! Saving data failed")
        statusmsg[name1e] = 'data files saving failed'
 
    p1end = datetime.utcnow()
    print "-----------------------------------"
    print "Part1 needs", p1end-p1start
    print "-----------------------------------"


if part2:
    """
    Publish adjusted data
    - requires an uploadlist for the specific time range
    """
    print ("----------------------------------------------------------------")
    print ("Part 2: Publish adjusted data")
    print ("----------------------------------------------------------------")
    name2 = "{}-step2".format(name)

    print ("  uploading one second data to ZAMG Server and eventually to GIN")
    for da in uploadlist:
        #try:
        ok = True
        if ok:
            print ("Uploading data for {}".format(da))
            print ("  -- THREAD for IAGA data to FTP: {}".format(da))
            # Send second data in background mode
            Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(vpathsec,'wic'+da+'psec.sec'),'ftppath':'/data/magnetism/wic/variation/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/vsec-transfer.log'}).start()
            # Send minute data in background mode
            Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(vpathmin,'wic'+da+'pmin.min'),'ftppath':'/data/magnetism/wic/variation/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/vmin-transfer.log'}).start()

            if submit2gin:
                print ("Submitting to gin if no other curl job detected: active_pid = ", active_pid('curl'))
                print ("#################################")
                if not active_pid('curl'):
                    print ("  -- Uploading second data to GIN - active now")
                    uploadsec = ginupload(os.path.join(vpathsec,'wic'+da+'psec.sec'), ginuser, ginpasswd, ginaddress,stdout=True)
                    print (uploadsec)
                    print ("  -- Uploading minute data to GIN: {}".format(da))
                    uploadmin = ginupload(os.path.join(vpathmin,'wic'+da+'pmin.min'), ginuser, ginpasswd, ginaddress,faillog=True,stdout=True)
                    print (uploadmin)
                else:
                    print (" !!!!!!!!!!!!! curl still active sending data in next round")
            statusmsg[name2] = 'upload successful'
        #except:
        #    print (" !!!!!!!!!!!!!!! data upload failed")
        #    statusmsg[name2] = 'upload failed'

sys.exit()

if part3:
    """
    Determine Observatory F (used for DI, data review, 
    selection of primary F)
    """

    ### scalalist = ['GSM90_14245_0002_0002','GSM90_6107631_0001_0001']
    ### 2018-10-24 -> Dropped GP20S3 because of problems with non - secondary time
    ### need to resolve that in MagPy
    ### affects readDB  
    ###
    ###  File "/home/cobs/anaconda2/lib/python2.7/site-packages/magpy/database.py", line 2767, in readDB
    ###  if str(col[0]) == '' or str(col[0]) == '-' or str(col[0]).find('0000000000000000') or str(col[0]).find('xyz'):
    ###  IndexError: index 0 is out of bounds for axis 0 with size 0
    ###  -------------------------> corrected for 0.4.5 -------------------
    p3start = datetime.utcnow()

    streamlist = []
    for idx, inst in enumerate(scalalist):
        print ("Step3: testing suitability of {}".format(inst))
        data = readDB(db,inst,starttime=yesterd2) #'2016-11-14') #yesterd2)
        if data.length()[0] > 1:
            dataflag = db2flaglist(db,inst[:-5])
            data = data.flag(dataflag)
            data=data.remove_flagged()

            # check for suitable data
            #if inst.startswith('GP20S3'):
            #    fcol = data._get_column('y')*0.001
            #else:
            fcol = data._get_column('f')
            fcol = fcol[~np.isnan(fcol.astype(float))]
            ftest = np.mean(fcol)
            if data.length()[0] > 1 and ftest and 45000 < ftest < 55000:
                print ("Suitable for step 3: {}".format(inst))
                #if inst.startswith('POS1'):
                #    #print ("Part3 - Found POS1 - flagging all data with dF larger then 0.5 - TODO")
                #    #flagls = data.flag_range(keys=['df'],keystoflag=['f','var1','df'],above=0.5,flagnum=1,text='automatically flagged - df larger than 0.5')
                #    #if len(flagls) > 0:
                #    #    flaglist2db(db,flagls,sensorid=inst[:-5])
                #    #    # TODO remove this data?
                data = applyDeltas(db,data)
                #if inst.startswith('GP20S3'):
                #    print ("Part3 - Found Supergrad F - using sensor 2")
                #    data = data._move_column('x','f')
                #    data = data.multiply({'f':0.001})
                data = data.filter()
                print ("Part3 - Adding {} at position {}".format(inst,len(streamlist)))
                streamlist.append(data)

    if len(streamlist) == 0:
        print ("Part3 - Critical error: No scalar stream identified")

    if len(streamlist) > 1:
        meanlst = []
        idxlst = [i for i in range(len(streamlist))]
        # Select always 2 streams from streamlist and get its difference
        for subset in itertools.combinations(idxlst,2):
            print subset
            sub = subtractStreams(streamlist[subset[0]],streamlist[subset[1]])
            ms = sub.mean('f',meanfunction='std')
            print "Delta F:", sub.mean('f',std=True)
            if np.isnan(ms):
                ms = 9999
            subset = [el for el in subset]
            subset.append(ms)
            meanlst.append(subset)
            # Create plot to be uploaded later
            title = streamlist[subset[0]].header.get('SensorID','None')+'-'+streamlist[subset[1]].header.get('SensorID','None')
            try:
                mp.plotStreams([streamlist[subset[0]],streamlist[subset[1]],sub],[['f'],['f'],['f']],plottitle = title,noshow=True)
                savepath = "/srv/products/graphs/magnetism/deltaf%d%d_%s.png" % (subset[0],subset[1],date)
                plt.savefig(savepath)
            except:
                pass

        print ("Stddev list:", meanlst)
        std = [el[2] for el in meanlst]
        mi = np.min(std)
        subs = meanlst[std.index(mi)]
        streamlist = [el for idx,el in enumerate(streamlist) if idx in subs]
        optimalf = np.min([int(subs[0]),int(subs[1])])
        print ("Optimal F instrument is {}".format(scalalist[optimalf])) # Eventually add this to consistancy log
    elif len(streamlist) == 1:
        print ("Only one f stream found")

    try:
        #print("Stacking Stream: {}".format(streamlist))
        stacked = stackStreams(streamlist,get='mean',uncert=True)
        #print("stacked length: {}".format(stacked.length()))
        for idx,col in enumerate(stacked.ndarray):
            indf = KEYLIST.index('f')
            inddf = KEYLIST.index('df')
            if not idx in [0,indf,inddf]:
                stacked = stacked._drop_column(KEYLIST[idx])

        stacked = stacked.extract('time',date2num(datetime.utcnow()),'<')
        print("stacked length: {}".format(stacked.length()))
        #print("stacked: {}".format(stacked.ndarray))
        #mp.plot(stacked)
        stacked.write('/srv/products/data/magnetism/stacked/',filenamebegins='Stacked_scalar_',dateformat='%Y-%m',coverage='month',mode='replace',format_type='PYCDF')
    except:
        print ("Failed to calculate and write Stacked F records")

    p3end = datetime.utcnow()
    print "-----------------------------------"
    print "Part3 needs", p3end-p3start
    print "-----------------------------------"
    print "Part 1 to 3 needs", p3end-p1start
    print "-----------------------------------"


# eventually set to false if not correct time range (2:00 am to 3:00 am)
if part4:
    """
    Check for time range of quasi-definitive construction and obtain basevalue data:
    basevalue data is required for parts 5 and 6
    """

    p4start = datetime.utcnow()
    weekday = p4start.weekday()
    # Run this job only between 2:00 am and 3:00 am (assuming that nobody did flagging then)
    # and only on fridays
    ldate = datetime(p4start.year, p4start.month, p4start.day,2)  # 2
    udate = datetime(p4start.year, p4start.month, p4start.day,3)  # 3

    print (ldate, udate, weekday)
    if ldate<p4start<udate and weekday in [5,'5']:  # 5
        print "Running Quasidefinitve data determinations - checking for new flags"
        part5 = True
    else:
        print "Not time for Quasidefinitve data determinations - waiting for 2:00 am"
        part5 = False

    bas = True
    if bas:
        # Get baseline
        # ##############################################
        va = varioinst[:-5]
        sc = scalainst[:-5]
        print va, sc
        #if not sc in ['GSM90_14245_0002']:
        #    sc = 'GSM90_14245_0002'
        #print "Baselinedata for", va, sc
        absst= readDB(db,'BLV_'+va+'_'+sc+'_A2')
        #print "Basedata", absst.length()
        absflag = db2flaglist(db,'BLV_'+va+'_'+sc+'_A2')
        absst = absst.flag(absflag)
        absst=absst.remove_flagged()

qdlist = None # will contain data if quasi-definitives are calculated
if part5:
    """
    Determine and submit quasidefinitiv record
    """
    p5start = datetime.utcnow()

    #try: # Select optimal f instrument with least noise
    #    scalainst = scalalist[optimalf]
    #except:
    #    pass
    print varioinst, scalainst

    # Get last modifictaion time from flag list # Done : Where FlagID in [2,3] and SensorID in magsenslist

    senslist = dbselect(db, 'SensorID', 'SENSORS','SensorGroup LIKE "%agnetism"')
    senslist = ["'"+elem+"'" for elem in senslist]
    where = "FlagNum IN (2,3) AND SensorID IN ({})".format(','.join(senslist))
    quasidefval = dbselect(db,'ModificationDate','FLAGS',where)
    tmp = DataStream()
    quasidefval = [tmp._testtime(el) for el in quasidefval]
    qdendtime = max(quasidefval)
    #qdendtime = datetime.strptime(qdendtime,dbdateformat)

    print qdendtime
    # Get previous modification time from file 
    qdpath = '/srv/products/data/magnetism/quasidefinitive/qdpara.pkl'
    if os.path.isfile(qdpath):
        pkl_file = open(qdpath, 'rb')
        mydict = pickle.load(pkl_file)
        lastqdtime = mydict['lastqdtime']
        pkl_file.close()
        print ("Got start date from file: {}".format(lastqdtime))
    else:
        lastqdtime = qdendtime-timedelta(days=7)

    if lastqdtime < qdendtime:
        d = (qdendtime-lastqdtime)
        daydiff = d.days
        qdlist = [datetime.strftime(qdendtime-timedelta(days=n),'%Y%m%d') for n in range(1,daydiff+2)]

        starttime = datetime.strftime(lastqdtime-timedelta(days=1),'%Y-%m-%d')
        endtime = datetime.strftime(qdendtime,'%Y-%m-%d')
        # Read Vario data from varioinst seleceted above
        # ##############################################
        #print starttime, endtime, varioinst, vario.header.get('SensorID','')
        vario = readDB(db,varioinst,starttime=starttime, endtime=endtime)
        varioflag = db2flaglist(db,vario.header.get('SensorID',varioinst))
        vario = vario.flag(varioflag)
        print len(varioflag)
        vario=vario.remove_flagged()
        vario = applyDeltas(db,vario)
        # -- convert latlong
        vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude'] = convertGeoCoordinate(vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude'],'epsg:31253','epsg:4326')
        #print vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude']

        # Get baseline
        # ##############################################
        # -- get most recent startdate from baseline tab
        #baselst = getBaseline(db,va)
        #print baselst
        baselst = getBaseline(db,va,date=date)
        startabs = baselst[1][0]
        func = baselst[4][0]
        fitdeg = int(baselst[5][0])
        fitknot = float(baselst[6][0])
        absst = absst._drop_nans('dx')
        absst = absst._drop_nans('dy')
        #print absst.ndarray
        #func = 'spline'
        #fitknot = 0.3
        #print "Got here", baselst, func, fitdeg, fitknot
        if vario._testtime(startabs) < datetime(2015,8,15):
            baselinefunc = vario.baseline(absst,startabs='2015-08-15',fitfunc=func,fitdegree=fitdeg,knotstep=fitknot)
        else:
            baselinefunc = vario.baseline(absst,startabs=startabs,fitfunc=func,fitdegree=fitdeg,knotstep=fitknot)
        #baselinefunc = vario.baseline(absst,startabs=baselst[1][0])
        #mp.plot(absst, ['dx','dy','dz'],symbollist=['o','o','o'],function=baselinefunc, plottitle='Baseline (until %s)' % (datetime.utcnow().date()))
        #print ("Baselinefile", absst.ndarray)
        #absst = absst.removeduplicates()
        #mp.plot(absst,['dx','dy','dz'],function=baselinefunc)

        vario = vario.bc()

        # Read Scalar data from the 'best' F
        print scalar.header.get('SensorID'),scalainst
        scalar = readDB(db,scalainst,starttime=starttime, endtime=endtime)
        scalarflag = db2flaglist(db,scalar.header.get('SensorID',scalainst))
        scalar = scalar.flag(scalarflag)
        scalar=scalar.remove_flagged()
        scalar = applyDeltas(db,scalar)


        # Produce QD dataset
        # ##############################################
        qd = mergeStreams(vario,scalar,keys=['f'])
        qd.header['DataPublicationLevel'] = '3'
        qd.header['DataPublicationDate'] = date
        # Convert to xyz
        qd = qd.hdz2xyz()
        if len(qd.header['DataComponents']) < 4:
            qd.header['DataComponents'] += 'F'
        # Save it
        qd.write(qpathsec,filenamebegins="wic",dateformat="%Y%m%d",filenameends="qsec.sec",format_type='IAGA')
        # supported keys of IMAGCDF -> to IMF format
        #supkeys = ['time','x','y','z','f','df']
        qd.write(qpathcdf,filenamebegins="wic_",dateformat="%Y%m%d_%H%M%S",format_type='IMAGCDF',filenameends='_'+prelim.header['DataPublicationLevel']+'.cdf'
)

        #write qd data to products directory, write info file on last update
        mydict = {}
        mydict['lastqdtime'] = qdendtime
        output = open(qdpath, 'wb')
        pickle.dump(mydict, output)
        output.close()
    else:
        print ("No new flag modification - skipping QD")
        pass

    p5end = datetime.utcnow()
    print "-----------------------------------"
    print "Part5 needs", p5end-p5start
    print "-----------------------------------"


if part6:
    """
    Upload diagrams
    """
    print (prelim.length()[0])

    # Create realtime diagram and upload to webpage  WDC
    if prelim.length()[0] > 0 and absst.length()[0] > 0:
        # Filter prelim and upload minute data
        prelim = prelim.filter()
        prelim.write(vpathmin,filenamebegins="wic",dateformat="%Y%m%d",filenameends="pmin.min",format_type='IAGA')

        print ("Part6 - Creating plot:", varioinst, scalainst)
        pnd = prelim._select_timerange(starttime=yesterd2)
        pst = DataStream([LineStruct()],prelim.header,pnd)
        #print "Got here", pst.length()
        #print pst.ndarray
        pst = pst.xyz2hdz()
        #print pst.ndarray
        #try:
        mp.plotStreams([pst],[['x','y','z','f']], gridcolor='#316931',fill=['x','z','f'],confinex=True, fullday=True, opacity=0.7, plottitle='Geomagnetic variation (until %s)' % (datetime.utcnow().date()),noshow=True)
        pltsavepath = "/srv/products/graphs/magnetism/magvar_%s.png" % date
        plt.savefig(pltsavepath)
        #except:
        #    pass
        print ("Uploading data for {}".format(uploadlist))
        for da in uploadlist:
            # Send in background mode
            print ("Uploading data for {}".format(da))
            print ("  -- THREAD for IAGA data to FTP: {}".format(da))
            Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(vpathmin,'wic'+da+'pmin.min'),'ftppath':'/data/magnetism/wic/variation/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/vmin-transfer.log'}).start()
            if submit2gin:
                if not active_pid('curl'):
                    print ("  -- Uploading minute data to GIN: {}".format(da))
                    #Thread(target=ginupload, kwargs={'filename':os.path.join(vpathmin,'wic'+da+'pmin.min'),'url':ginaddress,'user':ginuser,'password':ginpasswd,'logpath':'/home/cobs/ANALYSIS/Logs/ginsec-transfer.log','faillog':True,'stdout':True}).start()
                    upload = ginupload(os.path.join(vpathmin,'wic'+da+'pmin.min'), ginuser, ginpasswd, ginaddress,faillog=True,stdout=True)
                    uploadname = 'upload_GIN_minutedata'
                    path = os.path.join('/tmp',uploadname+'.pkl')
                    fi = io.open(path,'wb')
                    if upload:
                        analysisdict.check({uploadname: ['success','=','success']})
                        path = os.path.join('/tmp',uploadname+'.pkl')
                        fi = io.open(path,'wb')
                        pickle.dump(datetime.utcnow(), fi)
                        fi.close()
                    else:
                        try:  # will fail if no success file is existing
                            date = pickle.load(fi)
                            fi.close()
                            if datetime.utcnow()-timedelta(hours=1) < date:
                                analysisdict.check({uploadname: ['failure','=','success']})
                        except:
                            pass
                    #if upload:
                    #    analysisdict.check({uploadname: ['success','=','success']})
                    #else:
                    #    analysisdict.check({uploadname: ['failure','=','success']})
            # Create Webpage plot
            #Thread(target=ftpdatatransfer, kwargs={'localfile':pltsavepath,'ftppath':'zamg/images/graphs/magnetism/','myproxy':address,'port':port,'login':user,'passwd':passwd,'logfile':'/home/cobs/ANALYSIS/Logs/magvar.log'}).start()
            #Thread(target=ftpdatatransfer, kwargs={'localfile':pltsavepath,'ftppath':'cmsjoomla/images/stories/currentdata/wic/','myproxy':address,'port':port,'login':user,'passwd':passwd,'logfile':'/home/cobs/ANALYSIS/Logs/magvar.log'}).start()
            try:
               # to send with 664 permission use a temporary directory
               tmppath = "/tmp"
               tmpfile= os.path.join(tmppath,os.path.basename(pltsavepath))
               from shutil import copyfile
               copyfile(pltsavepath,tmpfile)
               remotepath = 'zamg/images/graphs/magnetism/'
               scptransfer(tmpfile,'94.136.40.103:'+remotepath,passwd)
               os.remove(tmpfile)
               analysisdict.check({'upload_homepage_magnetism_graph': ['success','=','success']})
            except:
               analysisdict.check({'upload_homepage_magnetism_graph': ['failure','=','success']})
               pass



        """
        baselinefunc = prelim.baseline(absst)
        prelim = prelim.bc()
        # create and upload plot 
        mp.plot(prelim,['x','y','z','f'], gridcolor='#316931',fill=['x','z','f'],confinex=True, fullday=True, opacity=0.7, plottitle='Geomagnetic variation (until %s)' % (datetime.utcnow().date()))#,noshow=True)
        #savepath = "/srv/products/graphs/magnetism/magvar_%s.png" % date
        #plt.savefig(savepath)
        """
    # Upload delta F plots

    # Upload QD data to WDC
    if qdlist:
        print qdlist
        for da in qdlist:
            #print da
            # Send in background mode
            Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(qpathsec,'wic'+da+'qsec.sec'),'ftppath':'/data/magnetism/wic/quasidefinitive/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/qsec-transfer.log'}).start()


    print "-----------------------------------"
    print "All Parts needed:", datetime.utcnow()-p1start
    print "-----------------------------------"


