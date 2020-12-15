#!/usr/bin/env python
# coding=utf-8

from magpy.stream import *
from magpy.database import *
import magpy.opt.cred as mpcred
import socket


print ("Importing anaylsis methods")

def DefineLogger(config={}, category="DataProducts", job='anaylsismethods', newname='', debug=False):
    host = socket.gethostname()
    jobname = os.path.splitext(job)[0]
    name = "{}-{}-{}".format(host.upper(),category,jobname)
    # extract loggingpath from config
    if not newname == '':
        logdir = config.get('loggingdirectory')
        logpath = os.path.join(logdir,newname)
        config['logfile'] = logpath
        if debug:
            print ("    - Saving logs to {}".format(logpath))
    # add name to config dict
    config['logname'] = name

    return config


def active_pid(name):
     # Part of Magpy starting with version ??
    try:
        pids = map(int,check_output(["pidof",name]).split())
    except:
        return False
    return True


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


def GetPrimaryInstruments(config={}, statusmsg={}, fallback=True, debug=False):
    """
    DESCRIPTION
        Obtain the primary instruments from current data structure
    """
    if debug:
        print (" -> get primary instruments started")

    currentvaluepath = config.get('currentvaluepath')
    name1a = "{}-PrimaryInstrumentSelection".format(config.get('logname','Dummy'))

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
            try:
                lastdec = valdict.get('Declination','')[0]  # format 2019-11-22
                lastinc = valdict.get('Incliniation','')[0]  # format 2019-11-22
                lastf = valdict.get('Fieldstrength','')[0]  # format 2019-11-22
            except:
                lastdec = ''
                lastinc = ''
                lastf = ''

        if not varioinst == "":
            print ("Found {} as primary variometer and {} as scalar instrument".format(varioinst,scalainst))
            print ("Current Values: Declination={}, Inclination={}, Intensity={}".format(lastdec, lastinc, lastf))
        elif fallback:
            varioinst = config.get('variometerinstruments')[0]
            scalarinst = config.get('scalarinstruments')[0]
            variosens = "_".join(varioinst.split('_')[:-1])
            scalasens = "_".join(scalainst.split('_')[:-1])
            statusmsg[name1a] = 'primary instrument could not be assigned automatically - using first in list'
        else:
            statusmsg[name1a] = 'primary instrument could not be assigned - found none'
    except:
        statusmsg[name1a] = 'primary instrument assignment failed'
        print (" !!!!!! primary data read failed")

    config['primaryVario'] = variosens
    config['primaryScalar'] = scalasens
    config['primaryVarioInst'] = varioinst
    config['primaryScalarInst'] = scalainst
    config['lastQDdate'] = lastQDdate
    config['QDenddate'] = QDenddate
    config['Dec'] = lastdec
    config['Inc'] = lastinc
    config['FieldStrength'] = lastf
    if debug:
        print (" -> get primary instruments finished")

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
        variostream = variostream.rotation(alpha=rotangle)
        print ('     -- applying rotation: alpha={} determined in {}'.format(rotangle,lastrot))
        #convert latlong
        print ("     -- concerting lat and long to epsg 4326")
        variostream.header['DataAcquisitionLongitude'],variostream.header['DataAcquisitionLatitude'] = convertGeoCoordinate(variostream.header['DataAcquisitionLongitude'],variostream.header['DataAcquisitionLatitude'],'epsg:31253','epsg:4326')
        variostream.header['DataLocationReference'] = 'WGS84, EPSG:4326'

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
    if (scalarstream.length()[0]) > 0:
        print ("     -- obtained data - last F = {}".format(scalarstream.ndarray[4][-1]))
        scalarflag = db2flaglist(db,scalarsens,begin=datetime.strftime(starttimedt,"%Y-%m-%d %H:%M:%S"))
        print ("     -- getting flags from DB: {}".format(len(scalarflag)))
        if len(scalarflag) > 0:
            scalarstream = scalarstream.flag(scalarflag)
            scalarstream = scalarstream.remove_flagged()
        print ("     -- applying deltasB:")
        scalarstream = applyDeltas(db,scalarstream)
        print ("     -- resampling at 1 sec steps")
        scalarstream = scalarstream.resample(['f'],period=1)
        print ("     -- all corrections performed -last F = {}".format(scalarstream.ndarray[4][-1]))

    return scalarstream


def DoBaselineCorrection(db, variostream, config={}, baselinemethod='simple', endtime=datetime.utcnow(), debug=False):
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


