#!/usr/bin/env python
# coding=utf-8

"""
DESCRIPTION

    Magnetism products and graphs. The new version making use of a configuration file

PREREQUISITES

    Needs the GetConf2 function of martas.core acquisitionsupport

principal idea: make it easy...

    use a main function and 

"""

from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred
import io, pickle
import getopt
import pwd
import sys  # for sys.version_info()
import socket

import itertools
from threading import Thread
from subprocess import check_output   # used for checking whether send process already finished

coredir = os.path.abspath(os.path.join('/home/leon/Software/MARTAS', 'core'))
sys.path.insert(0, coredir)
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf


# ################################################
#    general inits
# ################################################

endtime = datetime.utcnow()



# ################################################
#            Local Methods
# ################################################

"""
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
            print ("Getprimary: error when converting database date to datetime")
            return datetime.utcnow()
    return value
"""

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
#         New Methods
# ################################################

def ValidityCheckConfig(config={}, debug=False):
    """
    DESCRIPTION:
        Check correctness of config data 
    """
    success = True
    return success

def ValidityCheckDirectories(config={},statusmsg={}, debug=False):
    """
    DESCRIPTION:
        Check availability of paths for saving data products 
    """
    name0 = "{}-obligatory directories".format(config.get('logname','Dummy')) 
    statusmsg[name0] = 'all accessible'
    successlist = [True,True]

    vpath = config.get('variationpath')
    qpath = config.get('quasidefinitivepath')
    figpath = config.get('magfigurepath')
    dipath = config.get('dipath')

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

    if not os.path.isdir(vpath) and not os.path.isdir(qpath) and not os.path.isdir(figpath):
        print ("directory for products not accessible?")
        statusmsg[name0] = 'products unavailable'
        successlist[0] = False
        # all other jobs cannot be performed
        try:
            print ("unmounting...")
            umount("/srv/products",dbpasswd)
            time.sleep(10)
            print ("mounting products again...")
            mount("-a",dbpasswd)
            print ("success...")
            successlist[0] = True
            statusmsg[name0] = 'products unavailable - remounting successful'
        except:
            statusmsg[name0] = 'products unavailable - remounting failed'
    if not os.path.isdir(dipath):
        print ("archive not accessible?")
        statusmsg[name0] = 'archive unavailable'
        successlist[1] = False
        try:
            print ("unmounting...")
            umount("/srv/archive",dbpasswd)
            time.sleep(10)
            print ("mounting archive again...")
            mount("-a",dbpasswd)
            print ("success...")
            statusmsg[name0] = 'archive unavailable - remounting successful'
            successlist[1] = True
        except:
            statusmsg[name0] = 'archive unavailable - remounting failed'

    success = all([i for i in successlist])

    return success, statusmsg


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

    config['conncetedDB'] = connectdict

    return config


def GetInstruments(config={}, statusmsg={}, debug = False):
    """
    DESCRIPTION
        Obtain the primary instruments from current data structure
    """
    if debug:
        print (" -> get primary instruments")
    currentvaluepath = config.get('currentvaluepath')
    name1a = "{}-step1a".format(config.get('logname','Dummy'))
    varioinst = ''
    scalainst = ''
    variosens = ''
    scalasens = ''
    try:
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
        statusmsg[name1a] = 'primary instrument assignment failed'
        print (" !!!!!! primary data read failed")

    config['primaryVario'] = variosens
    config['primaryScalar'] = scalasens
    config['primaryVarioInst'] = varioinst
    config['primaryScalarInst'] = scalainst
    config['lastQDdate'] = lastQDdate
    config['QDenddate'] = QDenddate

    return config, statusmsg


def DoVarioCorrections(db, variostream, variosens='', starttimedt=datetime.utcnow(), debug=False):
    """
    DESCRIPTION:
        Apply the following corrections to variometer data:
            - apply flags from database
            - apply compensation fields
            - apply delta values from DB
            - apply alpha rotations (horizontal)
            - perform coordinate transformation of location to WGS84
    """
    print ("  -> Variometer corrections")
    if (variostream.length()[0]) > 0 and db:
        # Apply existing flags from DB
        varioflag = db2flaglist(db,variosens,begin=datetime.strftime(starttimedt,"%Y-%m-%d %H:%M:%S"))
        print ("     -- getting flags from DB: {}".format(len(varioflag)))
        if len(varioflag) > 0:
            variostream = variostream.flag(varioflag)
            variostream = variostream.remove_flagged()
        print ("   -- applying deltas")
        variostream = applyDeltas(db,variostream)
        # Apply compensation offsets
        print ("   -- offsets")
        offdict = {}
        xcomp = variostream.header.get('DataCompensationX','0')
        ycomp = variostream.header.get('DataCompensationY','0')
        zcomp = variostream.header.get('DataCompensationZ','0')
        if not float(xcomp) == 0:
            offdict['x'] = -1*float(xcomp)*1000.
        if not float(ycomp) == 0:
            offdict['y'] = -1*float(ycomp)*1000.
        if not float(zcomp) == 0:
            offdict['z'] = -1*float(zcomp)*1000.
        print ('     -- applying compensation fields: x={}, y={}, z={}'.format(xcomp,ycomp,zcomp))
        variostream = variostream.offset(offdict)
        print ("     -- rotation")
        rotstring = variostream.header.get("DataRotationAlpha","0")
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
        print ("        -> found rotation angle of {}".format(rotangle))
        vario = vario.rotation(alpha=rotangle) 
        print ('     -- applying rotation: alpha={} determined in {}'.format(rotangle,lastrot))
        #convert latlong
        print ("     -- concerting lat and long to epsg 4326")
        vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude'] = convertGeoCoordinate(vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude'],'epsg:31253','epsg:4326')
        #print vario.header['DataAcquisitionLongitude'],vario.header['DataAcquisitionLatitude']
        vario.header['DataLocationReference'] = 'WGS84, EPSG:4326'

    return variostream

def DoScalarCorrections(db, scalarstream, scalarsens='', starttimedt=datetime.utcnow(), debug=False):
    """
    DESCRIPTION:
        Apply the following corrections to variometer data:
            - apply flags from database
            - apply compensation fields
            - apply delta values from DB
            - apply alpha rotations (horizontal)
            - perform coordinate transformation of location to WGS84
    """
    print ("  -> Scalar corrections")
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
        print ("     -- resampling at 1 sec steps")
        scalar = scalar.resample(['f'],period=1)
        print ("     -- all corrections performed -last F = {}".format(scalar.ndarray[4][-1]))

    return scalar


def DoBaselineCorrection(db, variostream, config={}, baselinemethod='simple', debug=False):
    """
    DESCRIPTION:
        Apply baseline correction
    """

    variosens = config.get('primaryVario')
    scalarsens = config.get('primaryScalar')
    dipath = config.get('dipath')
    primpier = config.get('primarypier')
    ndays = config.get('baselinedays')

    print ("  -> Baseline adoption")
    # Define BLV source:
    blvdata = 'BLVcomp_{}_{}_{}'.format(variosens,scalarsens,primpier) 
    blvpath = os.path.join(dipath,blvdata+'.txt')
    print ("     -- using BLV data in {}".format(blvdata))
    # Check if such a baseline is existing - if not abort and inform
    msg = 'baseline correction successful'

    if baselinemethod == 'simple':
        try:
            print ("     -- reading BLV data from file")
            starttime = endtime-timedelta(days=ndays)
            absr = read(blvpath, starttime=starttime)
            blvflagdata = blvdata.replace("comp","")
            blvflaglist = db2flaglist(db,blvflagdata, begin=datetime.strftime(starttime,"%Y-%m-%d %H:%M:%S"))
            print ("     -- found {} flags for baseline values of the last {} days".format(len(blvflaglist),ndays))
            if len(blvflaglist) > 0:
                absr = absr.flag(blvflaglist)
                absr = absr.remove_flagged()
            print ("     -- dropping all line with NAN in basevalues")
            absr = absr._drop_nans('dx')
            absr = absr._drop_nans('dy')
            absr = absr._drop_nans('dz')
            print ("     -- calculation medians for basevalues")
            bh, bhstd = absr.mean('dx',meanfunction='median',std=True)
            bd, bdstd = absr.mean('dy',meanfunction='median',std=True)
            bz, bzstd = absr.mean('dz',meanfunction='median',std=True)
            print ("       -> Basevalues for last 100 days:")
            print ("          Delta H = {a} +/- {b}".format(a=bh, b=bhstd))
            print ("          Delta D = {a} +/- {b}".format(a=bd, b=bdstd))
            print ("          Delta Z = {a} +/- {b}".format(a=bz, b=bzstd))
            print ("     -- Performing constant basevalue correction")
            variostream = variostream.simplebasevalue2stream([bh,bd,bz])
        except:
            msg = 'simple baseline correction failed'
            print ('  -> {}'.format(msg))
    else:
       try:
            print ("     -- reading BLV data from file")
            absst= read(blvpath)
            blvflagdata = blvdata.replace("comp","")
            absflag = db2flaglist(db,blvflagdata)
            print ("     -- found {} flags for basevalues".format(len(absflag)))
            if len(absflag) > 0:
                absst = absst.flag(absflag)
                absst = absst.remove_flagged()
            print ("     -- dropping all line with NAN in basevalues")
            absst = absst._drop_nans('dx')
            absst = absst._drop_nans('dy')
            absst = absst._drop_nans('dz')
            print ("     -- reading BLV parameter from database")
            baselst = getBaseline(db,variosens,date=datetime.strftime(endtime,"%Y-%m-%d"))
            if debug:
                print ("Obtained the following baseline parameters from DB: {}".format(baselst))
            try:
                # valid for old dummy
                startabs = baselst[1][0]
                func = baselst[3][0]
                fitdeg = int(baselst[4][0])
                try:
                    fitknot = float(baselst[5][0])
                except:
                    fitknot = 0.3
            except:
                # valid for true input and new dummy (> magpy 0.4.6)
                startabs = baselst[1][0]
                func = baselst[4][0]
                fitdeg = int(baselst[5][0])
                try:
                    fitknot = float(baselst[6][0])
                except:
                    fitknot = 0.3
            print ("       -> using function {} with knots at {} intervals beginning at {}".format(func,fitknot,startabs))
            print ("     -- checking and eventually adepting start of baseline")
            print ("       -> initial starttime: {}".format(startabs))
            startabsdatetime = absst._testtime(startabs)
            if startabsdatetime < datetime.utcnow()-timedelta(days=365):
                startabs = datetime.strftime(datetime.utcnow()-timedelta(days=365),"%Y-%m-%d")
            print ("       -> refined starttime: {}".format(startabs))
            print ("     -- calculating and applying adopted baseline")
            baselinefunc = variostream.baseline(absst,startabs=startabs,fitfunc=func,fitdegree=fitdeg,knotstep=fitknot)
            variostream = variostream.bc()
            msg = 'baseline correction using function {} and (knot: {}, degree: {})'.format(func,fitknot,fitdeg)
       except:
            msg = 'baseline correction failed'

    return variostream, msg

def DoCombination(db, variostream, scalarstream, config={}, publevel=2, date='', debug=False):
    """
    DESCRIPTION:
        Combine vario and scalar data set and add meta information
    """

    print ("  -> Combinations and Meta info")
    prelim = mergeStreams(vario,scalar,keys=['f'])
    print ("     -- preliminary data file after MERGE:", prelim.length()[0])
    prelim.header['DataPublicationLevel'] = str(publevel)
    prelim.header['DataPublicationDate'] = date
    # Eventually convert it to XYZ
    prelim = prelim.hdz2xyz()
    if len(prelim.header['DataComponents']) < 4:
        prelim.header['DataComponents'] += 'F'
    prelim = prelim._drop_column('flag')
    prelim = prelim._drop_column('comment')
    print ("     -- preliminary data finished")

    return prelim

def ExportData(datastream, config={}, publevel=2):
    """
    DESCRIPTION:
        Export data sets to selected directories and databases
    """

    success = True
    explist = config.get('magnetismexports')
    obscode = config.get('obscode')
    connectdict = config.get('conncetedDB')

    vpathsec = os.path.join(config.get('variationpath'),'sec')
    vpathmin = os.path.join(config.get('variationpath'),'min')
    vpathcdf = os.path.join(config.get('variationpath'),'cdf')

    if int(publevel)==2:
        pubtype = 'adjusted'
        pubshort = 'p'
    elif int(publevel)==3:
        pubtype = 'quasidefinitive'
        pubshort = 'q'

    print ("  -> Exporting data ")
    if 'IAGA' in explist:
        print ("     -- Saving one second data - IAGA")
        datastream.write(vpathsec,filenamebegins="wic",dateformat="%Y%m%d",filenameends="{}sec.sec".format(pubshort),format_type='IAGA')
        # supported keys of IMAGCDF -> to IMF format
        #supkeys = ['time','x','y','z','f','df']
    if 'CDF' in explist:
        print ("     -- Saving one second data - CDF")
        datastream.write(vpathcdf,filenamebegins="wic_",dateformat="%Y%m%d_%H%M%S",format_type='IMAGCDF',filenameends='_'+prelim.header['DataPublicationLevel']+'.cdf')
    if 'DBsec' in explist:
        oldDataID = datastream.header['DataID']
        oldSensorID = datastream.header['SensorID']
        datastream.header['DataID'] = "WIC_{}_0001_0001".format(pubtype)
        datastream.header['SensorID'] = "WIC_{}_0001".format(pubtype)
        # save
        for dbel in connectdict:
            db = connectdict[dbel]
            print ("     -- Writing {} data to DB {}".format(pubtype,dbel))
            writeDB(db,datastream,tablename="WIC_{}_0001_0001".format(pubtype))
        datastream.header['DataID'] = oldDataID
        datastream.header['SensorID'] = oldSensorID
    if 'IAGA' in explist:
        print ("     -- Saving one minute data - IAGA")
        prelimmin = datastream.filter()
        prelimmin.write(vpathmin,filenamebegins="wic",dateformat="%Y%m%d",filenameends="{}min.min".format(pubshort),format_type='IAGA')
        #mp.plot(prelimmin)
    if 'DBmin' in explist:
        print ("     -- Saving one minute {} data to database".format(pubtype))
        prelimmin.header['DataID'] = "WIC_{}_0001_0002".format(pubtype)
        prelimmin.header['SensorID'] = "WIC_{}_0001".format(pubtype)
        variocol = np.asarray([varioinst for el in prelimmin.ndarray[0]])
        scalacol = np.asarray([scalainst for el in prelimmin.ndarray[0]])
        prelimmin = prelimmin._put_column(variocol, 'str1')
        prelimmin = prelimmin._put_column(scalacol, 'str2')        
        for dbel in connectdict:
            db = connectdict[dbel]
            print ("     -- Writing {} data to DB {}".format(pubtype,dbel))
            writeDB(db,prelimmin,tablename="WIC_{}_0001_0002".format(pubtype))

    return prelimmin



def AdjustedData(config={},statusmsg = {}, debug=False):
    """
    DESCRIPTION
        Create and submit variation/adjusted data
        Use one specific (primary) variometer and scalar instrument with current data
        Done here to be as close as possible to data acquisition
    """
    prelim = DataStream()
    prelimmin = DataStream()
    success = True
    variosens = config.get('primaryVario')
    scalarsens = config.get('primaryScalar')
    varioinst = config.get('primaryVarioInst')
    scalarinst = config.get('primaryScalarInst')
    primpier = config.get('primarypier')
    daystodeal = config.get('daystodeal')
    db = config.get('primaryDB')

    if debug:
        p1start = datetime.utcnow()

    if debug:
        print ("-------------------------------------------------------")
        print ("Adjusted Data")
        print ("-------------------------------------------------------")
    print ("  Primary variometer {}:".format(varioinst))
    print ("  ----------------------")

    name1b = "{}-AdjustedVarioData".format(config.get('logname'))
    if not varioinst == '':
        vario = readDB(db,varioinst,starttime=datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d"))
        db = config.get('primaryDB',None)
        if vario.length()[0] > 0 and db:
            vario = DoVarioCorrections(db, vario, variosens=config.get('primaryVario'), starttimedt=endtime-timedelta(days=daystodeal))
            statusmsg[name1b] = 'variometer data loaded'
        else:
            print ("  -> Did not find variometer data - aborting")
            statusmsg[name1b] = 'variometer data loading failed - aborting'
            #sys.exit()
    else:
        print ("  -> No variometer - aborting")
        statusmsg[name1b] = 'no variometer specified - aborting'
        sys.exit()

    print ("  Primary scalar magnetometer {}:".format(scalarinst))
    print ("  ----------------------")
    name1c = "{}-AdjustedScalarData".format(config.get('logname','Dummy'))
    if not scalarinst == '':
        scalar = readDB(db,scalarinst,starttime=datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d"))
        if scalar.length()[0] > 0 and db:
            scalar = DoScalarCorrections(db, scalar, scalarsens=config.get('primaryScalar'), starttimedt=endtime-timedelta(days=daystodeal))
            statusmsg[name1c] = 'scalar data loaded'
        else:
            print ("  -> Did not find scalar data - aborting")
            statusmsg[name1c] = 'scalar data load faild - aborting'
            #sys.exit()
    else:
        print ("No scalar instrument - aborting")
        statusmsg[name1c] = 'no scalar instrument specified - aborting'
        sys.exit()

    print ("  Baseline correction for pier {}".format(primpier))
    print ("  ----------------------")
    print ("  Instruments: {} and {}".format(varioinst, scalarinst))

    name1d = "{}-AdjustedBaselineData".format(config.get('logname','Dummy'))
    vario, msg = DoBaselineCorrection(db, vario, config=config, baselinemethod='simple')
    statusmsg[name1d] = msg
    if not msg == 'baseline correction successful':
        success = False

    print ("  Combining all data sets")
    print ("  ----------------------")
    name1e = "{}-AdjustedDataCombination".format(config.get('logname','Dummy'))
    try:
        prelim = DoCombination(db, variostream, scalarstream, config=config, publevel=2, date=datetime.strftime(endtime,"%Y-%m-%d"), debug=debug)
        statusmsg[name1e] = 'combination finished'
    except:
        print (" !!!!!!!!!!! Combination failed")
        statusmsg[name1e] = 'combination failed'

    print ("  Exporting data sets")
    print ("  ----------------------")
    name1f = "{}-AdjustedDataExport".format(config.get('logname','Dummy'))
    statusmsg[name1f] = 'export of adjusted data successful'
    if not debug:
        try:
            prelimmin = ExportData(prelim, config={}, publevel=2)
        except:
            statusmsg[name1f] = 'export of adjusted data failed'
    else:
        print ("Debug selected: export disabled")

    if debug:
        p1end = datetime.utcnow()
        print ("  ---------------------------------")
        print ("  Adjusted data needed {}".format(p1end-p1start))
        print ("-----------------------------------")

    return prelimmin, statusmsg

def KValues(datastream,config={},statusmsg={}, debug=False):
    """
    DESCRIPTION
        Obtain K values and update current data structure
    """

    k9level = datastream.header.get('k9_level',500)
    name5b = "{}-KValueCurrentData".format(config.get('logname','Dummy'))
    currentdatapath = config.get('currentvaluepath')
    success = True

    ## What about k values ??
    print ("  K values")
    print ("  ----------------------")
    print ("     -- K9 level in stream: {}".format(datastream.header.get('k9_level')))
    try:
        if prelimmin.length()[0] > 1600:
            # Only perform this job if enough minute data is available
            # Thus there won't be any calculation between 0.00 and 1:30 
            kvals = datastream.k_fmi(k9_level=k9level)
            kvals = kvals._drop_nans('var1')
            # use cut (0.4.6) to get last 70% of data
            try:
                kvals = kvals.cut(70)
            except:
                pass
            # then write kvals to DB
            # get index of last kval before now
            index = len(kvals)
            print ("     -- Checking kvals:")
            for idx,t in enumerate(kvals.ndarray[0]):
                print ("         -> Index: {}, time: {}, kval: {}".format(idx,num2date(t),kvals.ndarray[7][idx]))
                if num2date(t).replace(tzinfo=None) <= datetime.utcnow()+timedelta(hours=1):
                    index = idx
            kvaltime = datetime.strftime(num2date(kvals.ndarray[0][index]).replace(tzinfo=None),"%Y-%m-%d %H:%M")
            kval = kvals.ndarray[7][index]
            print ("     -- Expecting k of {} until {} + 1.5 hours UTC (current time = {})".format(kval,kvaltime,datetime.utcnow()))
            # Update K value in current value path
            print ("     -- updating current data contents")
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
                print ("     -- K value has been updated to {}".format(kval))
            try:
                if not debug:
                    print ("     -- now writing kvals to database")
                    for dbel in connectdict:
                        db = connectdict[dbel]
                        print ("     -- Writing k values to DB {}".format(dbel))
                        writeDB(db,kvals,tablename="WIC_k_0001_0001")
            except:
                pass
        statusmsg[name5b] = 'determinig k successfull'
    except:
        statusmsg[name5b] = 'determinig k failed'
        success = False

    return success, statusmsg

def CreateDiagram(datastream,config={},statusmsg={}, debug=False):
    """
    DESCRIPTION
        Create diagram of adjusted data
    """

    figpath = config.get("magfigurepath")
    daystodeal = config.get("daystodeal")
    date = datetime.strftime(endtime,"%Y-%m-%d")
    yesterd2 = datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d")
    success = True
    name5 = "{}-CreateDiagramVario".format(config.get('logname','Dummy'))

    # Create realtime diagram and upload to webpage  WDC
    try:
            print ("  Creating plot")
            pnd = datastream._select_timerange(starttime=datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d"))
            pst = DataStream([LineStruct()],datastream.header,pnd)
            pst = pst.xyz2hdz()
            mp.plotStreams([pst],[['x','y','z','f']], gridcolor='#316931',fill=['x','z','f'],confinex=True, fullday=True, opacity=0.7, plottitle='Geomagnetic variation (until %s)' % (datetime.utcnow().date()),noshow=True)
            print ("     -- Saving diagram to products folder")

            pltsavepath = os.path.join(figpath,"magvar_{}.png".format(date))
            plt.savefig(pltsavepath)
            statusmsg[name5] = 'creating and saving graph successful'
    except:
            statusmsg[name5] = 'failed to save data - remount necessary?'
            success = False

    return success, statusmsg


def GetQDTimeslot(config={}, debug=False):
    """
    DESCRIPTION
        Check whether the current time slot justifies QD analysis
    RETURN
        True if valid
    """    
    runqd = True
    # Get the current weekday
    qdstart = datetime.utcnow()
    weekday = qdstart.weekday()
    qdweekday = config.get("qdweekday")
    qdstarthour = config.get("qdstarthour")
    qdendhour = config.get("qdendhour")

    print ("  -> Testing for a valid time slot")
    print ("     -- Current weekday: {}".format(weekday))
    print ("     -- QD calculation will only be performed on day {} between {}:00 and {}:00".format(qdweekday,qdstarthour,qdendhour))
    # run the analysis only once at the scheduled weekday between starthour and endhour

    # update current.data if quasidefinite data has been calculated
    ldate = datetime(qdstart.year, qdstart.month, qdstart.day,qdstarthour)  # 2
    udate = datetime(qdstart.year, qdstart.month, qdstart.day,qdendhour)  # 3

    if ldate<p4start<udate and weekday in [qdweekday,str(qdweekday)]:  # 5
        print ("  - Running Quasidefinitve data determinations - checking for new flags")
        runqd = True
    else:
        print ("  - Not time for Quasidefinitve data determinations - waiting for 2:00 am")
        runqd = False

    return runqd


def GetQDFlagcondition(lastQDdate='', debug=False):
    """
    DESCRIPTION
        Check whether flags have been updated recently to justify QD analysis
    RETURN
        True if valid
    """
    newQDenddate = ''
    flaglist = []
    runqd = True

    print ("  -> Checking actuality of flags")
    if debug:
        print ("     -- lastQDdate: {}, converted: {}".format(lastQDdate, datetime.strftime(datetime.strptime(lastQDdate,"%Y-%m-%d"),"%Y-%m-%d %H:%M:%S")))
    varioflag = db2flaglist(db,variosens,begin=datetime.strftime(datetime.strptime(lastQDdate,"%Y-%m-%d"),"%Y-%m-%d %H:%M:%S"))
    scalaflag = db2flaglist(db,scalasens,begin=datetime.strftime(datetime.strptime(lastQDdate,"%Y-%m-%d"),"%Y-%m-%d %H:%M:%S"))

    if len(varioflag) > 0:
        flaglist = varioflag
        if len(scalaflag) > 0:
            flaglist.extend(scalaflag)
    elif len(scalaflag) > 0:
        flaglist = scalaflag
    if len(flaglist) > 0:
        print ("     -- found flags: {}".format(len(flaglist)))
        # checking last input date (modifications dates)
        moddates = [el[-1] for el in flaglist if el[3] in [0,2,3,'2','3']]
        print ("     -- last flag modification with ID 2 or 3 at {}".format(max(moddates)))
        newQDenddate = datetime.strftime(max(moddates)-timedelta(days=7),"%Y-%m-%d")
        # now get the last flag date and define lastflagdate -7 days as the new QD enddate
        print ("     -- found new flags -> assuming QD conditions for the week before") 
    else:
        runqd = False

    return runqd, newQDenddate


def GetQDDonealready(lastQDdate='', QDenddate='', newQDenddate='', debug=False):
    """
    DESCRIPTION
        Check whether qd range is new and not already existing
    RETURN
        True if valid
    """
    date = datetime.strftime(endtime,"%Y-%m-%d")
    runqd = True

    print ("  -> checking whether QD determination has already been performed within the current time period")
    if debug:
        print ("     -- last analysis performed on {} (today: {})".format(lastQDdate, date))
    if lastQDdate == date:
        print ("       -> QD determination already performed (or tried) in this period")
        runqd = False

    if runqd:
        print ("     -- checking whether an analysis is already existing for the period in question")
        if debug:
            print ("     -- QDendate in currentvalues: {}, new QDenddate: {}".format(QDenddate, newQDenddate))
        if QDenddate == newQDenddate or newQDenddate == '':
            print ("       -> the projected period has already been analyzed")
            runqd = False

    return runqd


def QuasidefinitiveData(config={}, statusmsg = {}, debug=False):
    """
    DESCRIPTION
        Create and submit quasidefinitive data
        Use one specific (primary) variometer and scalar instrument with current data
        Done here to be as close as possible to data acquisition
    """

    lastQDdate = config.get('lastQDdate')
    QDenddate = config.get('QDenddate')
    currentvaluepath = config.get('currentvaluepath')
    archivepath = config.get('archivepath')
    variosens = config.get('primaryVario')
    scalarsens = config.get('primaryScalar')
    varioinst = config.get('primaryVarioInst')
    scalarinst = config.get('primaryScalarInst')

    rotangle = 0.0
    runqd = False

    if debug:
        print ("----------------------------------------------------------------")
        print ("Determine quasidefinite data")
        print ("----------------------------------------------------------------")
        p3start = datetime.utcnow()

    # QDtype:
    #name3b = "{}-step3b".format(config.get('logname','Dummy'))
    #name4 = "{}-step4".format(config.get('logname','Dummy'))
    #statusmsg[name3b] = "last quasidefinitive calculation successful" # will be newly set if conducted
    #statusmsg[name3c] = "qd coverage ok"
    #statusmsg[name4] = 'last upload of QD successful' # will be set to failed in case of error in  step 4

    print ("  Suitability test for QD analysis")
    print ("  ----------------------")

    name3a = "{}-QDanalysis".format(config.get('logname','Dummy'))
    statusmsg[name3a] = "last suitability test for quasidefinitive finished"
    try:
        runqd = GetQDTimeslot(config=config, debug=debug)
        if runqd:
            runqd, newQDenddate = GetQDFlagcondition(lastQDdate=lastQDdate, debug=debug)
        if runqd:
            runqd = GetQDDonealready(lastQDdate=lastQDdate, QDenddate=QDenddate, newQDenddate=newQDenddate, debug=debug)
    except:
        statusmsg[name3a] = "suitability test for quasidefinitive failed"

    if runqd:
        print ("  Running QD analysis")
        print ("  ----------------------")
        try:
            # first time condition
            qdendtime = datetime.strptime(newQDenddate,"%Y-%m-%d") + timedelta(days=1)
            if not QDenddate == '':
                # QDenddate is 8 days before newQDenddate
                qdstarttime = datetime.strptime(QDenddate,"%Y-%m-%d") - timedelta(days=1)
            else:
                qdstarttime = datetime.strptime(newQDenddate,"%Y-%m-%d") - timedelta(days=8)
            print ("  -> all conditions met - running QD analysis")
            print ("     -- Analyzing data between:")
            print ("     -- Start: {}".format(qdstarttime))
            print ("     -- End:   {}".format(qdendtime))

            print ("  -> Updating current value file with new QD enddate")
            # QDenddate should be updated already now with newQDenddate in current.data file to prevent restart of job in next schedule, if running the analysis is not yet finished
            if os.path.isfile(currentvaluepath):
                with open(currentvaluepath, 'r') as file:
                    fulldict = json.load(file)
                    valdict = fulldict.get('magnetism')
                    valdict['QD enddate'] = [newQDenddate,'']
                    fulldict[u'magnetism'] = valdict
                with open(currentvaluepath, 'w',encoding="utf-8") as file:
                    file.write(unicode(json.dumps(fulldict))) # use `json.loads` to $
                    print ("     -- QDenddate has been updated from {} to {}".format(QDenddate,newQDenddate))

            # Running analysis
            # ##############################################
            print ("  -> Checking whether database has a suitable coverage")
            name3c = "{}-QDdatasource".format(config.get('logname','Dummy'))
            archive = False
            if qdstarttime < datetime.utcnow()-timedelta(days=30):
                print ("     -- Eventually not enough data in database for full coverage")
                print ("       -> Accessing archive files instead")
                statusmsg[name3c] = 'using data from file archive for QD analysis'
                archive = True
            else:
                statusmsg[name3c] = 'using database for QD analysis'

            print ("  -> Reading raw vario data and applying corrections")
            name3d = "{}-QDvario".format(config.get('logname','Dummy'))
            if not archive:
                vario = readDB(db,varioinst,starttime=qdstarttime,endtime=qdendtime)
            else:
                print ("     -- getting vario file archive")
                vario = read(os.path.join(archivepath,variosens,varioinst,'*'),starttime=qdstarttime,endtime=qdendtime)
                # Get meta info
                vario.header = dbfields2dict(db,varioinst)
            if (vario.length()[0]) > 0:
                vario = DoVarioCorrections(db, vario, variosens=config.get('primaryVario'), starttimedt=qdstarttime)
                statusmsg[name3d] = 'variometer data loaded'
            else:
                print ("     -- Did not find variometer data - aborting")
                statusmsg[name3d] = 'variometer data load failed'
                sys.exit()

            print ("  -> Reading raw scalar data and applying corrections")
            name3e = "{}-QDscalar".format(config.get('logname','Dummy'))
            if not archive:
                scalar = readDB(db,scalainst,starttime=qdstarttime,endtime=qdendtime)
            else:
                print ("     -- getting scalar file archive")
                scalar = read(os.path.join(archivepath,scalasens,scalainst,'*'),starttime=qdstarttime,endtime=qdendtime)
                # Get meta info
                scalar.header = dbfields2dict(db,scalainst)
            if (scalar.length()[0]) > 0:
                scalar = DoScalarCorrections(db, scalar, scalarsens=config.get('primaryScalar'), starttimedt=qdstarttime)
                statusmsg[name3e] = 'scalar data loaded'
            else:
                print ("  -> Did not find scalar data - aborting")
                statusmsg[name3e] = 'scalar data load failed'

            print ("  -> Doing baseline correction")
            name3f = "{}-QDBaselineData".format(config.get('logname','Dummy'))
            vario, msg = DoBaselineCorrection(db, vario, config=config, baselinemethod='normal')
            statusmsg[name3f] = msg
            if msg == 'baseline correction failed':
                success = False

            print ("  -> Combining all data sets")
            name3g = "{}-QDDataCombination".format(config.get('logname','Dummy'))
            try:
                qddata = DoCombination(db, vario, scalar, config=config, publevel=3, date=datetime.strftime(endtime,"%Y-%m-%d"), debug=debug)
                statusmsg[name3g] = 'combination finished'
            except:
                print ("      !!!!!!!!!!! Combination failed")
                statusmsg[name3g] = 'combination failed'

            print ("  -> Exporting data sets")
            name3h = "{}-QDDataExport".format(config.get('logname','Dummy'))
            statusmsg[name3h] = 'export of adjusted data successful'
            if not debug:
                try:
                    qdmin = ExportData(qddata, config={}, publevel=3)
                except:
                    statusmsg[name3h] = 'export of adjusted data failed'
            else:
                print ("Debug selected: export disabled")

            print ("  -> Updating current data")
            if os.path.isfile(currentvaluepath):
                with open(currentvaluepath, 'r') as file:
                    fulldict = json.load(file)
                    valdict = fulldict.get('magnetism')
                    valdict['QD analysis date'] = [date,'']
                    fulldict[u'magnetism'] = valdict
                with open(currentvaluepath, 'w',encoding="utf-8") as file:
                    file.write(unicode(json.dumps(fulldict)))
                    print ("    -- last QD analysis date has been updated from {} to {}".format(lastQDdate,date))

            nameqd = "{}-quasidefinitive".format(config.get('logname','Dummy'))
            statusmsg[name3b] = "QD data between {} and {} calculated and published (parameter: rotangle={})".format(qdstarttime, qdendtime, rotangle)
        except:
            statusmsg[name3b] = "quasidefinitive calculation performed but failed - check current.data before redoing"

    if debug:
        p3end = datetime.utcnow()
        print ("-----------------------------------")
        print ("QDanalysis needed {}".format(p3end-p3start))
        print ("-----------------------------------")

    return statusmsg


def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    debug=False

    try:
        opts, args = getopt.getopt(argv,"hc:j:D",["config=","joblist=","debug=",])
    except getopt.GetoptError:
        print ('magnetism_products.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- magnetism_products.py will analyse magnetic data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python magnetism_products.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-------------------------------------')
            print ('Application:')
            print ('python magnetism_products.py -c /etc/marcos/analysis.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-j", "--joblist"):
            # get a list of jobs (adjusted, quasidefinitive,upload,plots)
            joblist = arg.split(',')
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    if debug:
        print ("Running ")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    if debug:
        print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)
    success = ValidityCheckConfig(config)

    #if not success:
    #    sys.exit(1)
    # #############################################################
    if debug:
        print ("2. Activate logging scheme as selected in config")
    # create statusmsgname
    category = "DataProducts"
    host = socket.gethostname()
    jobname = os.path.splitext(os.path.basename(__file__))[0]
    name = "{}-{}-{}".format(host,category,jobname)
    # extract loggingpath from config 
    #logpath = config.get(logfile) # '/var/log/magpy/mm-dp-magnetism.log'
    # add name to config dict
    config['logname'] = name

    if debug:
        print (" -> Config contents:")
        print (config)

    if debug:
        print ("3. Check all paths and eventually remount directories")
    success,statusmsg = ValidityCheckDirectories(config=config, statusmsg=statusmsg, debug=debug)

    if debug:
        print ("4. Loading current.data and getting primary instruments")
    config, statusmsg = GetInstruments(config=config, statusmsg=statusmsg, debug=debug)

    if debug:
        print ("5. Connect to databases")
    config = ConnectDatabases(config=config, debug=debug)

    if debug:
        print ("6. Obtain adjusted data")
    mindata,statusmsg = AdjustedData(config=config, statusmsg=statusmsg, debug=debug)

    if mindata.length()[0]>0:
        if debug:
            print ("8. Diagrams")
        suc,statusmsg = CreateDiagrams(mindata, config=config,statusmsg=statusmsg)
        if debug:
            print ("9. K Values")
        suc,statusmsg = KValues(mindata, config=config,statusmsg=statusmsg)

    if debug:
        print ("10. Obtain quasidefinitive data")
    statusmsg = QuasidefinitiveData(config=config, statusmsg=statusmsg, debug=debug)


    if not debug:
        martaslog = ml(logfile=logpath,receiver='telegram')
        martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])



"""
# ##############################################################
#                     Configuration data for analysis jobs
# ##############################################################


# Observatory
# --------------------------------------------------------------

obscode                :      WIC


# Basic analysis parameters
# --------------------------------------------------------------

daystodeal             :      2

submit2gin = True
submit2app = True
submitlist = ['submitGINasIAGA', 'submitGINasIMCDF', 'submitZAMGFTPasIAGA', 'submitZAMGFTPasIMCDF', 'submitAPPasIAGA']

#  - MAGNETISM

variometerinstruments  :      LEMI036_1_0002_0002,LEMI025_22_0003_0002,FGE_S0252_0001_0001
scalarinstruments      :      GSM90_14245_0002_0002,GSM90_6107631_0001_0001,GP20S3NSS2_012201_0001_0001
magnetismexports       :      IAGA,CDF,DBmin
qdstarthour            :      3
qdendhour              :      4
# analyze quasidefinitive data only on 5=Saturday
qdweekday              :      5
# baseline anaylsis
primarypier            :      A2
baselinedays           :      100



# Databases
# --------------------------------------------------------------

dbcredentials          :      list


# Paths and Directories
# --------------------------------------------------------------

#  - MAGNETISM

variationpath          :       /srv/products/data/magnetism/variation/
quasidefinitivepath    :       /srv/products/data/magnetism/quasidefinitive/
dipath                 :       /srv/archive/WIC/DI/data
archivepath            :       /srv/archive/WIC

#  - GENERAL

currentvaluepath       :       /srv/products/data/current.data
magfigurepath          :       /srv/products/graphs/magnetism/


# Logging and notification
# --------------------------------------------------------------

# Logfile (a json style dictionary, which contains statusmessages) 
logfile              :   /var/log/magpy/mm-dp-magnetism.log

# Notifaction (uses martaslog class, one of email, telegram, mqtt, log) 
notification         :   telegram
notificationconfig   :   /myconfpath/mynotificationtype.cfg

"""

"""
# ################################################
#             Part 2
# ################################################

if part2:
    #""
    #Publish adjusted data
    #- requires an uploadlist for the specific time range
    #""
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


if part4 and part3 and runqd:
    #""
    #Upload QD data diagrams
    #""

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

"""
