#!/usr/bin/env python
# coding=utf-8

from magpy.stream import *
from magpy.database import *
import magpy.opt.cred as mpcred
import socket


print ("Importing analysis methods")

def DefineLogger(config={}, category="DataProducts", job='anaylsismethods', newname='', debug=False):
    host = socket.gethostname()
    jobname = os.path.splitext(job)[0]
    name = "{}-{}-{}".format(host.upper(),category,jobname)
    # extract loggingpath from config
    if not newname == '':
        logdir = config.get('loggingdirectory')
        if logdir:
            logpath = os.path.join(logdir,newname)
            config['logfile'] = logpath
            if debug:
                print ("    - Saving logs to {}".format(logpath))
        else:
            print ("    - no logging directory specified - skipping logging")
    # add name to config dict
    config['logname'] = name.replace('_','')

    return config


def combinelists(l1,l2):
    if len(l1) > 0 and len(l2) > 0:
        l1.extend(l2)
    elif not len(l1) > 0 and len(l2) > 0:
        l1 = l2
    return l1


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



def WriteMemory(memorypath, memdict):
        """
        DESCRIPTION
             write memory
        """
        try:
            with open(memorypath, 'w', encoding='utf-8') as f:
                json.dump(memdict, f, ensure_ascii=False, indent=4)
        except:
            return False
        return True


#WriteMemory("/home/cobs/ANALYSIS/PeriodicGraphs/mytest_plot.json", test)


def ReadMemory(memorypath,debug=False):
        """
        DESCRIPTION
             read memory
        -> Same function as used for imbot (imbotcore)
        """
        memdict = {}
        if os.path.isfile(memorypath):
            if debug:
                print ("Reading memory: {}".format(memorypath))
            with open(memorypath, 'r') as file:
                memdict = json.load(file)
        else:
            print ("Memory path not found - please check (first run?)")
        if debug:
            print ("Found in Memory: {}".format([el for el in memdict]))
        return memdict

def load_current_data_sub(currentvaluepath, group):
    """
    Loads a speciffic group of  current data json
    """
    valdict = {}
    fulldict = {}
    if os.path.isfile(currentvaluepath):
        # read log if exists and exentually update changed information
        # return changes
        with open(currentvaluepath, 'r') as file:
            fulldict = json.load(file)
            valdict = fulldict.get(group)
    if not valdict:
        valdict = {}
    if not fulldict:
        fulldict = {}

    return fulldict, valdict


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
    lastdec = ''
    lastinc = ''
    lastf = ''
    lastQDdate = ''
    QDenddate = ''

    try:
        if currentvaluepath and os.path.isfile(currentvaluepath):
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
                lastinc = valdict.get('Inclination','')[0]  # format 2019-11-22
                lastf = valdict.get('Fieldstrength','')[0]  # format 2019-11-22
            except:
                lastdec = ''
                lastinc = ''
                lastf = ''
        else:
            print ("No current value path found - using first vario and scalar as primary")
            varioinst = ''

        if not varioinst == "":
            print ("Found {} as primary variometer and {} as scalar instrument".format(varioinst,scalainst))
            print ("Current Values: Declination={}, Inclination={}, Intensity={}".format(lastdec, lastinc, lastf))
        elif fallback:
            varioinst = config.get('variometerinstruments')[0]
            scalainst = config.get('scalarinstruments')[0]
            print ("Using fallback:  {} as primary variometer and {} as scalar instrument".format(varioinst,scalainst))
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
        betastring = variostream.header.get("DataRotationBeta","0")
        # alpha rotation data
        try:
            rotdict = string2dict(rotstring,typ='listofdict')[0]
        except:
            try:
                rotdict = string2dict(rotstring,typ='listofdict')
            except:
                rotdict = {}
        try:
            lastrot = sorted(rotdict)[-1]
            rotangle = float(rotdict.get(lastrot,0))
        except:
            rotangle = 0.0
            lastrot = '0000'
        # beta rotation data
        try:
            betadict = string2dict(betastring,typ='listofdict')[0]
        except:
            try:
                betadict = string2dict(betastring,typ='listofdict')
            except:
                betadict = {}
        try:
            lastbeta = sorted(betadict)[-1]
            betaangle = float(betadict.get(lastbeta,0))
        except:
            betaangle = 0.0
            lastbeta = '0000'
        print ("        -> found rotation angles: alpha={}, beta={}".format(rotangle,betaangle))
        variostream = variostream.rotation(alpha=rotangle,beta=betaangle)
        print ('     -- applying rotation: alpha={} determined in {} and beta={} determined in {}'.format(rotangle,lastrot,betaangle,lastbeta))
        #convert latlong
        print ("     -- concerting lat and long to epsg 4326")
        try:
            variostream.header = dbfields2dict(db,variostream.header.get('DataID'))
            epsg = variostream.header.get('DataLocationReference','GK34, epsg:31253')
            epsg = epsg.split(',')[1].replace(' ','').lower()
            print ("        -> found epsg code {}".format(epsg))
        except:
            epsg = 'epsg:31253'
        variostream.header['DataAcquisitionLongitude'],variostream.header['DataAcquisitionLatitude'] = convertGeoCoordinate(variostream.header['DataAcquisitionLongitude'],variostream.header['DataAcquisitionLatitude'],epsg,'epsg:4326')
        variostream.header['DataLocationReference'] = 'WGS84, EPSG:4326'
        print ("   -> Vario correction done")

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
        print ("     -- got {} flags from DB stating from {}".format(len(scalarflag),datetime.strftime(starttimedt,"%Y-%m-%d %H:%M:%S")))
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
    blvdata1 = 'BLVcomp_{}_{}_{}'.format(variosens,scalarsens,primpier)
    blvdata2 = 'BLV_{}_{}_{}'.format(variosens,scalarsens,primpier)
    if os.path.isfile(os.path.join(dipath,blvdata1+'.txt')):
        blvpath = os.path.join(dipath,blvdata1+'.txt')
        blvdata = blvdata1
    elif os.path.isfile(os.path.join(dipath,blvdata2+'.txt')):
        blvpath = os.path.join(dipath,blvdata2+'.txt')
        blvdata = blvdata2
    else:
        print ("     -- could not identify an appropriate BLV file - method will fail")
    print ("     -- using BLV data in {}".format(blvpath))
    # Check if such a baseline is existing - if not abort and inform
    msg = 'baseline correction successful'

    if baselinemethod == 'simple':
        try:
            print ("     -- reading BLV data from file")
            starttime = endtime-timedelta(days=ndays)
            #print ("     -- starttime looks like {}, path={}".format(starttime,blvpath))
            absr = read(blvpath)
            absr = absr.trim(starttime=starttime)
            print ("     -- found {} basevalues".format(absr.length()[0]))
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


def Quakes2Flags(config={}, endtime=datetime.utcnow(), timerange=5, sensorid=None, keylist=[], distancedict={"global" : [12,7,40000], "strong": [7,6,6000], "moderate": [6,4.5,3000], "significant" : [4.5, 3.0, 500] }, debug=False):
    """
    DESCRIPTION
        Creates a flaglist from earthquakes using a distance/strength selector.
    PARAMETER
        sensorid : provide the sensorid to be used in the flagging output
        keys     : provide the keys to be used in the flagging output
        distancedict : extract all earthquakes below magitude and above secondmag at distances below x km
        e.g.   {"global" : [12,7,40000], "strong": [7,6,6000], "moderate": [6,4.5,3000], "significant" : [4.5, 3.0, 500] }

    """

    flaglist = []

    print ("  - extracting quakes and construct flaglist")
    db = config.get('primaryDB')

    if debug:
        print ("  - reading QUAKES table from database for selected time range between {} and {}".format(endtime-timedelta(days=timerange),endtime))
    stream = readDB(db,'QUAKES',starttime=endtime-timedelta(days=timerange), endtime=endtime)
    if debug:
        print ("    -> found {} records".format(stream.length()[0]))

    #distancedict = {"global" : [12,7,40000], "strong": [7,6,6000], "moderate": [6,4.5,3000], "significant" : [4.5, 3.0, 500] } #, "near" : [3,1,50]

    def ExtractValues(stream, distancelist, debug=False):
        if debug:
            print ("    -> extract all earthquakes below {} and above {} at distances below {} km".format(distancelist[0],distancelist[1],distancelist[2]))
        ext = stream.extract('var5',distancelist[2],'<')
        ext = ext.extract('f',distancelist[0],'<')
        ext = ext.extract('f',distancelist[1],'>=')
        return ext

    st = DataStream()
    if stream.length()[0] > 0:
        for selector in distancedict:
            if debug:
                print ("  - extracting {} quakes".format(selector))
            ext = ExtractValues(stream, distancedict[selector], debug=debug)
            if debug:
                print ("    -> found {} quakes meeting this criteria".format(ext.length()[0]))
            if ext.length()[0] > 0:
                st = appendStreams([st,ext])
            if debug:
                print ("    -> new selection length is {}".format(st.length()[0]))

    if st.length()[0] > 0:
        if debug:
            print ("  - creating flaglist from the quakelist")
        flaglist = st.stream2flaglist(comment='f,str3',sensorid=sensorid, userange=False, keystoflag=keylist)
        print ("  - new flaglist with {} inputs created  - finished".format(len(flaglist)))
        return flaglist
    else:
        print ("  - no quakes found - finished")
        return flaglist

def magn(d, a=2.4,c=-0.43):
    # (Hauksson and Goddard, 1981, JGR)
    return a*np.log10(d) + c

def distancefilter(stream,distancecolumn,magnitudecolumn,func,a=2.0,c=-0.43,debug=False):
    """
    Application
        fquakes = distancefilter(quakes,"var5","f",magn,a=2.4)
        # Flagging data sets
        fl = fquakes.stream2flaglist(comment='f,str3',sensorid=nstream.header["SensorID"], userange=False, keystoflag="x")
    """
    if debug:
        print ("All data", len(stream.ndarray[0]))
    res = stream.copy()
    droplist = []
    for idx, el in enumerate(stream.ndarray[0]):
        distind = KEYLIST.index("var5")
        magnind = KEYLIST.index("f")
        d = stream.ndarray[distind][idx]
        m = stream.ndarray[magnind][idx]
        minmagn = func(d,a=a,c=c)
        if m < minmagn:
            droplist.append(idx)
    if debug:
        print ("Dropping", len(droplist))

    array = [np.asarray([]) for elem in KEYLIST]
    for index,tkey in enumerate(KEYLIST):
        if len(stream.ndarray[index]) > 0:
            array[index] = np.delete(stream.ndarray[index], droplist)
    res.ndarray = np.asarray(array, dtype=object)
    if debug:
        print ("Remaining Quakes", res.length()[0])
    return res

def quakes2flags_new(config={}, endtime=datetime.utcnow(), timerange=5, sensorid=None, keylist=[], a=2.0, c=-0.43, debug=False):
    """
    DESCRIPTION
        Creates a flaglist from earthquakes using a distance/strength selector.
    PARAMETER
        sensorid : provide the sensorid to be used in the flagging output
        keys     : provide the keys to be used in the flagging output
        distancedict : extract all earthquakes below magitude and above secondmag at distances below x km
        e.g.   {"global" : [12,7,40000], "strong": [7,6,6000], "moderate": [6,4.5,3000], "significant" : [4.5, 3.0, 500] }

    """

    flaglist = []

    print ("  - extracting quakes and construct flaglist")
    db = config.get('primaryDB')

    if debug:
        print ("  - reading QUAKES table from database for selected time range between {} and {}".format(endtime-timedelta(days=timerange),endtime))
    quakes = readDB(db,'QUAKES',starttime=endtime-timedelta(days=timerange), endtime=endtime)
    if debug:
        print ("    -> found {} records".format(quakes.length()[0]))

    st = distancefilter(quakes,"var5","f",magn,a=a,c=c)

    if st.length()[0] > 0:
        if debug:
            print ("  - creating flaglist from the quakelist")
        flaglist = st.stream2flaglist(comment='f,str3',sensorid=sensorid, userange=False, keystoflag=keylist)
        print ("  - new flaglist with {} inputs created  - finished".format(len(flaglist)))
        return flaglist
    else:
        print ("  - no quakes found - finished")
        return flaglist
