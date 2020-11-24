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


# QDtype:
name3a = "{}-step3a".format(name)
name3b = "{}-step3b".format(name)
name3c = "{}-step3c".format(name)
name4 = "{}-step4".format(name)
statusmsg[name3a] = "last suitability test for quasidefinitive finished"
statusmsg[name3b] = "last quasidefinitive calculation successful" # will be newly set if conducted
statusmsg[name3c] = "qd coverage ok"
statusmsg[name4] = 'last upload of QD successful' # will be set to failed in case of error in  step 4

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
primarydb = 'vegadb'
secondarydb = 'soldb'

dbpasswd = mpcred.lc('cobsdb','passwd')

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

# ################################################
#             Configuration
# ################################################


part0 = True # check for availability of paths
part1 = True # analysis of primary sensors -> adjusted data
part2 = True # submit adjusted data
part3 = True # Quasidefinitive data
part4 = True # submit QD data
part5 = True # create graphs and plot


endtime = datetime.utcnow()
daystodeal = 2
primpier = 'A2'

submit2gin = True
submit2app = True

submitlist = ['submitGINasIAGA', 'submitGINasIMCDF', 'submitZAMGFTPasIAGA', 'submitZAMGFTPasIMCDF', 'submitAPPasIAGA']

currentvaluepath = '/srv/products/data/current.data'

# QD data
qdstarthour = 3
qdendhour = 4
qdweekday = 5  # 5=Saturday

# Logging 
uploadcheck = 6 # only report errors when uploading pics to homepage fails for 6 hours - will be replaced by webservice anyway

########
#TODO

# - add time of last plot upload -> only send error message if unsuccessful for 8 hours
# - add umount -> mount job to file system check


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
weekago = endtime-timedelta(days=7)

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
qpathmin = '/srv/products/data/magnetism/quasidefinitive/min/'
figpath = '/srv/products/graphs/magnetism/'
dipath = '/srv/archive/WIC/DI/data'

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

artcred = 'art'
artaddress=mpcred.lc(artcred,'address')
artuser=mpcred.lc(artcred,'user')
artpasswd=mpcred.lc(artcred,'passwd')
artport=mpcred.lc(artcred,'port')

#Host: ftp.warrenarmstrong.net
#Port: 21
#Username: imouser@warrenarmstrong.net


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


def getcurrentdata(path):
    """
    usage: getcurrentdata(currentvaluepath)
    example: update kvalue
    >>> fulldict = getcurrentdata(currentvaluepath)
    >>> valdict = fulldict.get('magnetism',{})
    >>> valdict['k'] = [kval,'']
    >>> valdict['k-time'] = [kvaltime,'']
    >>> fulldict[u'magnetism'] = valdict
    >>> writecurrentdata(path, fulldict) 
    """
    if os.path.isfile(currentvaluepath):
        with open(currentvaluepath, 'r') as file:
            fulldict = json.load(file)
        return fulldict
    else:
        print ("path not found")

def writecurrentdata(path,dic):
    """
    usage: writecurrentdata(currentvaluepath,fulldict)
    example: update kvalue
    >>> see getcurrentdata
    >>>
    """
    with open(currentvaluepath, 'w',encoding="utf-8") as file:
        file.write(unicode(json.dumps(dic)))


# ################################################
#             Part 0
# ################################################
if part0:
    """
    Check avilability of paths 
    """
    def umount(path,pwd):
        """usage: umount("/srv/archive")"""
        cmd = 'umount ' + path
        print ("Sending umount command: {}".format(cmd))
        echo = 'echo {}|sudo -S {}'.format(pwd,cmd)
        subprocess.Popen(str(echo), shell=True, stdout=subprocess.PIPE)
        print ("Done")

    def mount(path,pwd):
        """usage: mount("/srv/archive")"""
        cmd = 'mount ' + path
        print ("Sending command: {}".format(cmd))
        echo = 'echo {}|sudo -S {}'.format(pwd,cmd)
        subprocess.Popen(str(echo), shell=True, stdout=subprocess.PIPE)

    name0 = "{}-obligatory directories".format(name) 
    statusmsg[name0] = 'all accessible'
    if not os.path.isdir(vpath) and not os.path.isdir(qpath) and not os.path.isdir(figpath):
        print ("products not accessible?")
        statusmsg[name0] = 'products unavailable'
        part1=False
        part2=False
        part3=False
        part4=False
        part5=False
        try:
            print ("unmounting...")
            umount("/srv/products",dbpasswd)
            time.sleep(10)
            print ("mounting products again...")
            mount("-a",dbpasswd)
            print ("success...")
            statusmsg[name0] = 'products unavailable - remounting successful'
        except:
            statusmsg[name0] = 'products unavailable - remounting failed'
    if not os.path.isdir(dipath):
        print ("archive not accessible?")
        statusmsg[name0] = 'archive unavailable'
        part1=False
        part2=False
        part3=False
        part4=False
        part5=False
        try:
            print ("unmounting...")
            umount("/srv/archive",dbpasswd)
            time.sleep(10)
            print ("mounting archive again...")
            mount("-a",dbpasswd)
            print ("success...")
            statusmsg[name0] = 'archive unavailable - remounting successful'
            part1=True
            part2=True
            part3=True
            part4=True
            part5=True
        except:
            statusmsg[name0] = 'archive unavailable - remounting failed'

# ################################################
#             Part 1
# ################################################


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
    try:
        #ok = True
        #if ok:
        if os.path.isfile(currentvaluepath):
            with open(currentvaluepath, 'r') as file:
                fulldict = json.load(file)
                valdict = fulldict.get('magnetism')
            try:
                varioinst = valdict.get('primary vario','')[0]
                variosens = "_".join(varioinst.split('_')[:-1])
            except:
                varioinst = ''
            try:
                scalainst = valdict.get('primary scalar','')[0]
                scalasens = "_".join(scalainst.split('_')[:-1])
            except:
                scalainst = ''
            try:
                lastQDdate = valdict.get('QD analysis date','')[0]  # format 2019-11-22
            except:
                lastQDdate = ''
            try:
                QDenddate = valdict.get('QD enddate','')[0]  # format 2019-11-22
            except:
                QDenddate = ''
        if not varioinst == "":
            print ("Found {} as primary variometer and {} as scalar instrument".format(varioinst,scalainst))
            statusmsg[name1a] = 'primary instruments selected'
        else:
            varioinst = variolist[0]
            statusmsg[name1a] = 'primary instrument could not be assigned automatically'
    except:
        statusmsg[name] = 'primary instrument assignment failed'
        print (" !!!!!! primary data read failed")

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
            print ("   -- applying deltas")
            vario = applyDeltas(db,vario)
            # Apply compensation offsets
            print ("   -- offsets")
            offdict = {}
            xcomp = vario.header.get('DataCompensationX','0')
            ycomp = vario.header.get('DataCompensationY','0')
            zcomp = vario.header.get('DataCompensationZ','0')
            if not float(xcomp) == 0:
                offdict['x'] = -1*float(xcomp)*1000.
            if not float(ycomp) == 0:
                offdict['y'] = -1*float(ycomp)*1000.
            if not float(zcomp) == 0:
                offdict['z'] = -1*float(zcomp)*1000.
            print ('  - applying compensation fields: x={}, y={}, z={}'.format(xcomp,ycomp,zcomp))
            vario = vario.offset(offdict)
            print ("  -- rotation")
            rotstring = vario.header.get("DataRotationAlpha","0")
            # Apply rotation data
            try:
                rotdict = string2dict(rotstring,typ='listofdict')[0]
            except:
                rotdict = string2dict(rotstring,typ='listofdict')
            #print ("     -> rotation dict: {}".format(rotdict))
            try:
                lastrot = sorted(rotdict)[-1]
                rotangle = float(rotdict.get(lastrot,0))
            except:
                rotangle = 0.0
                lastrot = '0000'
            print ("Found rotation angle of {}".format(rotangle))
            vario = vario.rotation(alpha=rotangle) 
            print ('  - applying rotation: alpha={} determined in {}'.format(rotangle,lastrot))
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
        print ("Test", scalar.ndarray)
        if (scalar.length()[0]) > 0:
            print ("     -- obtained data - last F = {}".format(scalar.ndarray[4][-1]))
            scalarflag = db2flaglist(db,scalasens,begin=datetime.strftime(starttimedt,"%Y-%m-%d %H:%M:%S"))
            print ("     -- getting flags from DB: {}".format(len(scalarflag)))
            #mp.plot(scalar)
            if len(scalarflag) > 0:
                scalar = scalar.flag(scalarflag)
                scalar = scalar.remove_flagged()
            #mp.plot(scalar)
            print ("     -- applying deltasB:")
            scalar = applyDeltas(db,scalar)
            print ("     -- corrections performed -last F = {}".format(scalar.ndarray[4][-1]))
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
    blvdata = 'BLVcomp_{}_{}_{}'.format(variosens,scalasens,primpier) 
    blvpath = os.path.join(dipath,blvdata+'.txt')
    print ("    using BLV data".format(blvdata))
    # Check if such a baseline is existing - if not abort and inform

    try:
        #basesimple = True
        #if basesimple:
        #absr = readDB(db,blvdata, starttime = endtime-timedelta(days=100))
        absr = read(blvpath, starttime = endtime-timedelta(days=100))
        blvflagdata = blvdata.replace("comp","")
        blvflaglist = db2flaglist(db,blvflagdata)
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
        sys.exit()

    print (" e) combining data sets, adding header, writing sec and min data")
    name1e = "{}-step1e".format(name)
    try:
        scalar = scalar.resample(['f'],period=1)
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
        prelim.write(vpathsec,filenamebegins="wic",dateformat="%Y%m%d",filenameends="psec.sec",format_type='IAGA')
        # supported keys of IMAGCDF -> to IMF format
        #supkeys = ['time','x','y','z','f','df']
        print ("  - Saving one second data - CDF")
        prelim.write(vpathcdf,filenamebegins="wic_",dateformat="%Y%m%d_%H%M%S",format_type='IMAGCDF',filenameends='_'+prelim.header['DataPublicationLevel']+'.cdf')
        print (prelim.length()[0])
        print ("  - Saving one minute data - IAGA")
        prelimmin = prelim.filter()
        prelimmin.write(vpathmin,filenamebegins="wic",dateformat="%Y%m%d",filenameends="pmin.min",format_type='IAGA')
        #mp.plot(prelimmin)
        print ("  - Saving one minute adjusted data to database")
        prelimmin.header['DataID'] = "WIC_adjusted_0001_0002"
        prelimmin.header['SensorID'] = "WIC_adjusted_0001"
        variocol = np.asarray([varioinst for el in prelimmin.ndarray[0]])
        scalacol = np.asarray([scalainst for el in prelimmin.ndarray[0]])
        prelimmin = prelimmin._put_column(variocol, 'str1')
        prelimmin = prelimmin._put_column(scalacol, 'str2')
        #print (prelimmin.ndarray)
        print ("Writing adjusted data to primary DB")
        writeDB(db,prelimmin,tablename="WIC_adjusted_0001_0002")
        if secdb:
            print ("Writing adjusted data to secondary DB")
            writeDB(secdb,prelimmin,tablename="WIC_adjusted_0001_0002")
        statusmsg[name1e] = 'all data files saved'
    except:
        print (" !!!!!!!!!!! Saving data failed")
        statusmsg[name1e] = 'data files saving failed'
 
    p1end = datetime.utcnow()
    print "-----------------------------------"
    print "Part1 needs", p1end-p1start
    print "-----------------------------------"

# ################################################
#             Part 2
# ################################################

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
    try:
        for da in uploadlist:
            #ok = True
            #if ok:
            print ("Uploading data for {}".format(da))
            print ("  -- THREAD for IAGA data to FTP: {}".format(da))
            if 'submitZAMGFTPasIAGA' in submitlist:
                # Send second data in background mode
                Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(vpathsec,'wic'+da+'psec.sec'),'ftppath':'/data/magnetism/wic/variation/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/psec-transfer.log'}).start()
            if 'submitZAMGFTPasIAGA' in submitlist:
                # Send minute data in background mode
                Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(vpathmin,'wic'+da+'pmin.min'),'ftppath':'/data/magnetism/wic/variation/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/pmin-transfer.log'}).start()

            if 'submitAPPasIAGA' in submitlist and submit2app:
                # Send second data in background mode
                print ("Uploading data to art project") 
                Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(vpathsec,'wic'+da+'psec.sec'),'ftppath':'/all-obs/','myproxy':artaddress,'port':artport,'login':artuser,'passwd':artpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/psec2app-transfer.log'}).start()

            if 'submitGINasIAGA' in submitlist and submit2gin:
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
    except:
        print (" !!!!!!!!!!!!!!! data upload failed")
        statusmsg[name2] = 'upload failed'


# ################################################
#             Part 3
# ################################################

# eventually set to false if not correct time range (2:00 am to 3:00 am)
if part3 and part1:
    """
    Check for valid time of quasi-definitive determination and obtain basevalue data:
    basevalue data is required for QD
    # requires part1 for extracting primary instruments and basevalue def
    """
    print ("----------------------------------------------------------------")
    print ("Part 3: determine quasidefinite data")
    print ("----------------------------------------------------------------")
    
    runqd = False
    rotangle = 0.0
    p3start = datetime.utcnow()

    try:

        # Get the current weekday
        p4start = datetime.utcnow()
        weekday = p4start.weekday()
        print ("  - Current weekday: {}".format(weekday))

        print (" a) Checking whether current time is suitable for QD")
        print ("    - QD calculation will be performed on day {} between {}:00 and {}:00".format(qdweekday,qdstarthour,qdendhour))
        # run the analysis only once at the scheduled weekday between starthour and endhour

        # update current.data if quasidefinite data has been calculated
        ldate = datetime(p4start.year, p4start.month, p4start.day,qdstarthour)  # 2
        udate = datetime(p4start.year, p4start.month, p4start.day,qdendhour)  # 3

        if ldate<p4start<udate and weekday in [qdweekday,str(qdweekday)]:  # 5
            print ("  - Running Quasidefinitve data determinations - checking for new flags")
            runqd = True
        else:
            print ("  - Not time for Quasidefinitve data determinations - waiting for 2:00 am")
            runqd = False

        if runqd:
            flaglist = []
            print (" b) QD time - checking actuality of flags")
            varioflag = db2flaglist(db,variosens)
            scalaflag = db2flaglist(db,scalasens)
            if len(varioflag) > 0:
                flaglist = varioflag
                if len(scalaflag) > 0:
                    flaglist.extend(scalaflag)
            elif len(scalaflag) > 0:
                flaglist = scalaflag
            if len(flaglist) > 0:
                print ("     -- found flags: {}".format(len(flaglist)))
                # checking last input date (modifications dates)
                #print (flaglist[0])
                moddates = [el[-1] for el in flaglist if el[3] in [0,2,3,'2','3']]
                print ("     -- last flag modification with ID 2 or 3 at {}".format(max(moddates)))
                newQDenddate = datetime.strftime(max(moddates)-timedelta(days=7),"%Y-%m-%d")
                # now get the last flag date and define lastflagdate -7 days as the new QD enddate
                print (" - Found new flags -> assuming QD conditions for the week before") 
            else:
                runqd = False

        if runqd:
            print (" c) suitable QD time and flags found - now checking whether QD determination has already been performed within the current time period")
            print ("    - last analysis performed on {} (today: {})".format(lastQDdate, date))
            if lastQDdate == date:
                print ("  -> QD determination already performed (or tried) in this period")
                runqd = False

        if runqd:
            print (" d) checking whether an analysis is already existing for the period in question")
            print (QDenddate, newQDenddate)
            if QDenddate == newQDenddate or newQDenddate == '':
                print ("  -> The projected period has already been analyzed")
                runqd = False
    except:
        statusmsg[name3a] = "suitability test for quasidefinitive failed"

    if runqd:
        try:
            # first time condition
            endtime = datetime.strptime(newQDenddate,"%Y-%m-%d") + timedelta(days=1)
            if not QDenddate == '':
                # QDenddate is 8 days before newQDenddate
                starttime = datetime.strptime(QDenddate,"%Y-%m-%d") - timedelta(days=1)
            else:
                starttime = datetime.strptime(newQDenddate,"%Y-%m-%d") - timedelta(days=8)
            print (" e) all conditions met - running QD analysis")
            print ("    Analyzing data between:")
            print ("    Start: {}".format(starttime))
            print ("    End:   {}".format(endtime))

            # QDenddate should be updated already now with newQDenddate in current.data file to prevent restart of job in next schedule, if running the analysis is not yet finished
            if os.path.isfile(currentvaluepath):
                with open(currentvaluepath, 'r') as file:
                    fulldict = json.load(file)
                    valdict = fulldict.get('magnetism')
                    valdict['QD enddate'] = [newQDenddate,'']
                    fulldict[u'magnetism'] = valdict
                with open(currentvaluepath, 'w',encoding="utf-8") as file:
                    file.write(unicode(json.dumps(fulldict))) # use `json.loads` to $
                    print ("QDenddate has been updated from {} to {}".format(QDenddate,newQDenddate))

            # Get baseline
            # ##############################################
            print (" f) getting basevalues")
            absst= read(blvpath)
            absflag = db2flaglist(db,blvflagdata)
            print ("    - got {} flags for basevalues".format(len(absflag)))
            if len(absflag) > 0:
                absst = absst.flag(absflag)
                absst = absst.remove_flagged()

            p3start = datetime.utcnow()

            archive = False
            if starttime < datetime.utcnow()-timedelta(days=30):
                print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print ("Eventually not enough data in database for full coverage")
                print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                statusmsg[name3c] = 'critical coverage for QD analysis'
                print (" -> Accessing archive files instead")
                archive = True

            print (" g) getting variometer data, applying flags, offsets, WGS coordinates {}".format(varioinst))
            #name3b = "{}-step3b".format(name)
            if not archive:
                vario = readDB(db,varioinst,starttime=starttime,endtime=endtime)
            else:
                print ("Getting archive")
                vario = read(os.path.join('/srv/archive/WIC',variosens,varioinst,'*'),starttime=starttime,endtime=endtime)
                # Get meta info
                vario.header = dbfields2dict(db,varioinst)
            if (vario.length()[0]) > 0:
                # Apply existing flags from DB
                varioflag = db2flaglist(db,variosens,begin=datetime.strftime(starttime,"%Y-%m-%d %H:%M:%S"))
                print ("     -- getting flags from DB: {}".format(len(varioflag)))
                if len(varioflag) > 0:
                    vario = vario.flag(varioflag)
                    vario = vario.remove_flagged()
                vario = applyDeltas(db,vario)
                # Apply compensation offsets
                offdict = {}
                xcomp = vario.header.get('DataCompensationX','0')
                ycomp = vario.header.get('DataCompensationY','0')
                zcomp = vario.header.get('DataCompensationZ','0')
                if not float(xcomp) == 0:
                    offdict['x'] = -1*float(xcomp)*1000.
                if not float(ycomp) == 0:
                    offdict['y'] = -1*float(ycomp)*1000.
                if not float(zcomp) == 0:
                    offdict['z'] = -1*float(zcomp)*1000.
                print ('  - applying compensation fields: x={}, y={}, z={}'.format(xcomp,ycomp,zcomp))
                vario = vario.offset(offdict)
                # Apply rotation data
                rotstring = vario.header.get('DataRotationAlpha','')
                #rotstring = vario.header.get('DataRotationAlpha','')
                #rotdict = string2dict(rotstring,typ='listofdict')
                # new in October 2019
                try:
                    rotdict = string2dict(rotstring,typ='listofdict')[0]
                except:
                    rotdict = string2dict(rotstring,typ='listofdict')
                #print ("     -> rotation dict: {}".format(rotdict))
                try:
                    lastrot = sorted(rotdict)[-1]
                    rotangle = float(rotdict.get(lastrot,0))
                except:
                    rotangle = 0.0
                    lastrot = '0000'
                vario = vario.rotation(alpha=rotangle)
                print ('  - applying rotation: alpha={} determined in {}'.format(rotangle,lastrot))
                #convert latlong
                vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude'] = convertGeoCoordinate(vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude'],'epsg:31253','epsg:4326')
                #print vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude']
                vario.header['DataLocationReference'] = 'WGS84, EPSG:4326'
                #statusmsg[name3b] = 'variometer data loaded'
            else:
                print ("Did not find variometer data - aborting")
                #statusmsg[name3b] = 'loading variometer data failed'

            print (" h) getting scaladata, flags and offsets: {}".format(scalainst))
            #name3c = "{}-step3c".format(name)
            if not archive:
                scalar = readDB(db,scalainst,starttime=starttime,endtime=endtime)
            else:
                print ("Getting archive")
                scalar = read(os.path.join('/srv/archive/WIC',scalasens,scalainst,'*'),starttime=starttime,endtime=endtime)
                # Get meta info
                scalar.header = dbfields2dict(db,scalainst)

            if (scalar.length()[0]) > 0:
                print ("     -- obtained data")
                scalarflag = db2flaglist(db,scalasens,begin=datetime.strftime(starttime,"%Y-%m-%d %H:%M:%S"))
                print ("     -- getting flags from DB: {}".format(len(scalarflag)))
                #mp.plot(scalar)
                if len(scalarflag) > 0:
                    scalar = scalar.flag(scalarflag)
                    scalar = scalar.remove_flagged()
                #mp.plot(scalar)
                scalar = applyDeltas(db,scalar)
                #statusmsg[name3c] = 'scalar data loaded'
            else:
                print ("Did not find scalar data - aborting")
                #statusmsg[name3c] = 'loading scalar data failed - aborting'


            print (" i) perform baseline correction")
            # Get baseline
            # ##############################################
            # -- get most recent startdate from baseline tab
            #baselst = getBaseline(db,va)
            #print baselst
            baselst = getBaseline(db,variosens,date=date)
            try:
                print (baselst)  # valid for old dummy
                startabs = baselst[1][0]
                func = baselst[3][0]
                fitdeg = int(baselst[4][0])
                try:
                    fitknot = float(baselst[5][0])
                except:
                    fitknot = 0.3
            except:
                print (baselst)  # valid for true input and new dummy (> magpy 0.4.6)
                startabs = baselst[1][0]
                func = baselst[4][0]
                fitdeg = int(baselst[5][0])
                try:
                    fitknot = float(baselst[6][0])
                except:
                    fitknot = 0.3
            print (startabs)
            startabsdatetime = absst._testtime(startabs)
            if startabsdatetime < datetime.utcnow()-timedelta(days=365):
                startabs = datetime.strftime(datetime.utcnow()-timedelta(days=365),"%Y-%m-%d")
            print (startabs)
            print ("  - using function {} with knots at {} intervals beginning at {}".format(func,fitknot,startabs))
            absst = absst._drop_nans('dx')
            absst = absst._drop_nans('dy')
            absst = absst._drop_nans('dz')
            baselinefunc = vario.baseline(absst,startabs=startabs,fitfunc=func,fitdegree=fitdeg,knotstep=fitknot)
            #baselinefunc = vario.baseline(absst,startabs=baselst[1][0])
            #mp.plot(absst, ['dx','dy','dz'],symbollist=['o','o','o'],function=baselinefunc, plottitle='Baseline (until %s)' % (datetime.utcnow().date()))
            #print ("Baselinefile", absst.ndarray)
            #absst = absst.removeduplicates()
            #mp.plot(absst,['dx','dy','dz'],function=baselinefunc)
            vario = vario.bc()

            print (" j) merge data and save QD data set")
            # Produce QD dataset
            # ##############################################
            scalar = scalar.resample(['f'],period=1)
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
            qd.write(qpathcdf,format_type='IMAGCDF')
            qdmin = qd.filter()
            qdmin.write(qpathmin,filenamebegins="wic",dateformat="%Y%m%d",filenameends="qmin.min",format_type='IAGA')
            #mp.plot(qdmin)
            print ("  - Saving one minute quasidefinitive data to database")
            prelimmin.header['DataID'] = "WIC_quasidefinitive_0001_0002"
            prelimmin.header['SensorID'] = "WIC_quasidefinitive_0001"
            variocol = np.asarray([varioinst for el in prelimmin.ndarray[0]])
            scalacol = np.asarray([scalainst for el in prelimmin.ndarray[0]])
            prelimmin = prelimmin._put_column(variocol, 'str1')
            prelimmin = prelimmin._put_column(scalacol, 'str2')
            print ("Writing adjusted data to secondary DB")
            writeDB(db,prelimmin,tablename="WIC_quasidefinitive_0001_0002")
            if secdb:
                print ("Writing adjusted data to secondary DB")
                writeDB(secdb,prelimmin,tablename="WIC_quasidefinitive_0001_0002")
            #statusmsg[name3e] = 'all data files saved'

            print (" k) update current.data information and define a qd uploadlist")
            tmin,tmax = qdmin._find_t_limits()
            # Get daylist
            qddays = (tmax-tmin).days
            qdlist = [datetime.strftime(tmax-timedelta(days=i),"%Y%m%d") for i in range(0,qddays,1)]
            #lastday = datetime.strftime(tmax,"%Y-%m-%d")

            if os.path.isfile(currentvaluepath):
                with open(currentvaluepath, 'r') as file:
                    fulldict = json.load(file)
                    valdict = fulldict.get('magnetism')
                    valdict['QD analysis date'] = [date,'']
                    fulldict[u'magnetism'] = valdict
                with open(currentvaluepath, 'w',encoding="utf-8") as file:
                    file.write(unicode(json.dumps(fulldict)))
                    print ("last QD analysis date has been updated from {} to {}".format(lastQDdate,date))
            

            nameqd = "{}-quasidefinitive".format(name)
            statusmsg[name3b] = "QD data between {} and {} calculated and published (parameter: rotangle={})".format(starttime, endtime, rotangle)

            #statusmsg[name3b] = "quasidefinitive calculation successful"
        except:
            statusmsg[name3b] = "quasidefinitive calculation performed but failed - check current.data before redoing"


    p3end = datetime.utcnow()
    print "-----------------------------------"
    print "Part3 needs", p3end-p3start
    print "-----------------------------------"


if part4 and part3 and runqd:
    """
    Upload QD data diagrams
    """

    print ("----------------------------------------------------------------")
    print ("Part 4: upload quasi definitive data")
    print ("----------------------------------------------------------------")

    name4 = "{}-step4".format(name)

    try:
        #ok = True
        #if ok:
        # Upload QD data to WDC
        print ("Uploading QD data for {}".format(qdlist))
        for da in qdlist:
            # Send in background mode
            print ("Uploading QD data for {}".format(da))
            print ("  -- THREAD for IAGA qsec data to FTP: {}".format(da))
            Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(qpathsec,'wic'+da+'qsec.sec'),'ftppath':'/data/magnetism/wic/quasidefinitive/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/qsec-transfer.log'}).start()
            print ("  -- THREAD for IAGA qmin data to FTP: {}".format(da))
            Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(qpathmin,'wic'+da+'qmin.min'),'ftppath':'/data/magnetism/wic/quasidefinitive/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/qmin-transfer.log'}).start()
            if submit2gin:
                if not active_pid('curl'):
                    print ("  -- Uploading second data to GIN - active now")
                    uploadsec = ginupload(os.path.join(qpathsec,'wic'+da+'qsec.sec'), ginuser, ginpasswd, ginaddress,stdout=False)
                    print ("  -> Answer: {}".format(uploadsec))
                print ("  -- Uploading minute data to GIN: {}".format(da))
                uploadmin = ginupload(os.path.join(qpathmin,'wic'+da+'qmin.min'), ginuser, ginpasswd, ginaddress,stdout=False)
                #Thread(target=ginupload, kwargs={'localfile':os.path.join(qpathmin,'wic'+da+'qmin.min'),'user':ginuser, 'passwd': ginpasswd, 'address': ginaddress, 'stdout':False}).start()
                print ("  -> Answer: {}".format(uploadmin))

        statusmsg[name4] = 'upload of QD successful: {}'.fromat(uploadmin)
    except:
        print (" !!!!!!!!!!!!!!! QD data upload failed")
        statusmsg[name4] = 'upload of QD failed'

if part5 and part1:
    """
    Create diagrams and upload them - Eventually move that to a different job
    """
    print ("----------------------------------------------------------------")
    print ("Part 5: create plots and upload them to webpage")
    print ("----------------------------------------------------------------")
    #print (prelimmin.length()[0])
    name5 = "{}-step5".format(name)
    name5b = "{}-step5b".format(name)
    name5a = "{}-step5a".format(name)

    # Create realtime diagram and upload to webpage  WDC
    ok = True
    try:
      #if ok:
      if prelimmin.length()[0] > 0:
        # Filter prelim and upload minute data
        try:
            print ("Part5 - Creating plot:", varioinst, scalainst)
            pnd = prelimmin._select_timerange(starttime=yesterd2)
            pst = DataStream([LineStruct()],prelimmin.header,pnd)
            pst = pst.xyz2hdz()
            mp.plotStreams([pst],[['x','y','z','f']], gridcolor='#316931',fill=['x','z','f'],confinex=True, fullday=True, opacity=0.7, plottitle='Geomagnetic variation (until %s)' % (datetime.utcnow().date()),noshow=True)
            print (" - Saving diagram to products folder")
            pltsavepath = "/srv/products/graphs/magnetism/magvar_%s.png" % date
            plt.savefig(pltsavepath)
            statusmsg[name5a] = 'creating and saving graph successful'
        except:
            statusmsg[name5a] = 'failed to save data - remount necessary?'
        ## What about k values ??
        print (" - kvalues")
        try:
            #ok = True
            #if ok:
            if prelimmin.length()[0] > 1600:
                # Only perform this job if enough minute data is available
                # Thus there won't be any calculation between 0.00 and 1:30 
                kvals = prelimmin.k_fmi(k9_level=500)
                kvals = kvals._drop_nans('var1')
                # use cut (0.4.6) to get last 60% of data
                try:
                    kvals = kvals.cut(70)
                except:
                    pass
                #then write kvals to DB
                # get index of last kval before now
                index = len(kvals)
                print ("Checking kvals")
                for idx,t in enumerate(kvals.ndarray[0]):
                    print ("Index: {}, time: {}, kval: {}".format(idx,num2date(t),kvals.ndarray[7][idx]))
                    if num2date(t).replace(tzinfo=None) <= datetime.utcnow()+timedelta(hours=1):
                        index = idx
                print (index)
                #print (kvals.length())
                print (kvals.header.get('DataID'))
                kvaltime = datetime.strftime(num2date(kvals.ndarray[0][index]).replace(tzinfo=None),"%Y-%m-%d %H:%M")
                #print (datetime.strftime(num2date(kvals.ndarray[0][-2]).replace(tzinfo=None),"%Y-%m-%d %H:%M"))
                kval = kvals.ndarray[7][index]
                print ("Expect k of {} until {} + 1.5 hours UTC (current time = {})".format(kval,kvaltime,datetime.utcnow()))
                # Update K value
                if os.path.isfile(currentvaluepath):
                    with open(currentvaluepath, 'r') as file:
                        fulldict = json.load(file)
                        valdict = fulldict.get('magnetism')
                        ## set k values and k time
                        valdict['k'] = [kval,'']
                        valdict['k-time'] = [kvaltime,'']
                        fulldict[u'magnetism'] = valdict
                    with open(currentvaluepath, 'w',encoding="utf-8") as file:
                        file.write(unicode(json.dumps(fulldict))) 
                    print ("K value has been updated to {}".format(kval))
            try:
                print ("  -> Now writing kvals to database")
                writeDB(db,kvals,tablename="WIC_k_0001_0001")
                print ("  -> Done")
                if secdb:
                    print ("  -> Now writing kvals to secondary database")
                    writeDB(secdb,kvals,tablename="WIC_k_0001_0001")
                    print ("  -> Done")
            except:
                pass
            statusmsg[name5b] = 'determinig k successfull'
        except:
            statusmsg[name5b] = 'determinig k failed'

        #ok = True
        #if ok:
        try:
           # to send with 664 permission use a temporary directory
           tmppath = "/tmp"
           tmpfile= os.path.join(tmppath,os.path.basename(pltsavepath))
           #from shutil import copyfile
           #copyfile(pltsavepath,tmpfile)
           #remotepath = 'zamg/images/graphs/magnetism/'
           #timeout = 300
           #print (" -- Starting scptransfer with timeout {}".format(timeout))
           #scptransfer(tmpfile,'94.136.40.103:'+remotepath,passwd,timeout=timeout)
           #print (" -- now removing temporary file...")
           #os.remove(tmpfile)
           #print ('  - Uploading of plots successful')
           statusmsg[name5] = 'uploading plots successful'
           # update upload time in current data file
           fulldict = getcurrentdata(currentvaluepath)
           valdict = fulldict.get('logging',{})
           uploadtime = datetime.strftime(datetime.utcnow(),"%Y-%m-%d %H:%M")
           valdict['magnetismplots'] = [uploadtime,'']
           fulldict[u'logging'] = valdict
           writecurrentdata(currentvaluepath, fulldict)
        except:
           print (" !!!!!!!!!!! Uploading plots failed")
           message = True
           fulldict = getcurrentdata(currentvaluepath)
           valdict = fulldict.get('logging',{})
           try:
               lastupload = datetime.strptime(valdict.get('magnetismplots',['',''])[0],"%Y-%m-%d %H:%M")
               if not lastupload < datetime.utcnow()-timedelta(hours=uploadcheck):
                   message = False
           except:
               message = True

           # Only change status if failing for more than 'uploadcheck' e.g. 8 hours
           # Upload at 2:55 frequently fails because of traffic and server resets
           if message:
               statusmsg[name5] = 'uploading plots failed'
           else:
               # assume that the upload is ok -TODO  might produce flapping states if upload is really not working any more
               # Eventually it is better to get existing state and only change value if constant e.g. three times in a row
               statusmsg[name5] = 'uploading plots successful'
    except:
        print (" !!!!!!!!!!! Error in step 5")
        statusmsg[name5] = 'Found an error in step5 not related to upload and k'

    print "-----------------------------------"
    print "All Parts needed:", datetime.utcnow()-p1start
    print "-----------------------------------"


print (statusmsg)
martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)

if statusmsg[name4] == 'upload of QD failed':
    import shutil
    shutil.copyfile('/home/cobs/ANALYSIS/Logs/5min.log','/home/cobs/ANALYSIS/Logs/5min_QD.log')
