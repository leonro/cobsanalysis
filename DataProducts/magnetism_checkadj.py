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
logpath = '/var/log/magpy/mm-dp-magdatacheck.log'
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
name = "{}-DataProducts-magdatacheck".format(sn)


# Status checks:
namecheck0 = "{}-check-general".format(name)
statusmsg[namecheck0] = "cross check of data sets ok"
namecheck1 = "{}-check-adjusted".format(name)
statusmsg[namecheck1] = "baseline corrected data sets of all instruments are similar"
namecheck2 = "{}-check-f".format(name)
statusmsg[namecheck2] = "scalar data sets of all instruments are similar"

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


part0 = True # primary instruments
part1 = True # load data from all variometer sensors and caluclate diffs
part2 = True # load data from all scalar sensors and caluclate differences 

endtime = datetime.utcnow()
daystodeal = 2
primpier = 'A2'

currentvaluepath = '/srv/products/data/current.data'

#Instrument Lists
variolist = ['LEMI036_1_0002_0002','LEMI025_22_0003_0002','FGE_S0252_0001_0001']
scalalist = ['GP20S3NSS2_012201_0001_0001','GSM90_14245_0002_0002','GSM90_6107631_0001_0001']

# ################################################
#             Paths and derived time ranges
# ################################################

dipath = '/srv/archive/WIC/DI/data/'
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

# ################################################
#            Local Methods
# ################################################


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


# Get Primary 

part0 = True
if part0:
    print ("----------------------------------------------------------------")
    print ("Part 0: get primary instruments")
    print ("----------------------------------------------------------------")
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
                primvarioinst = valdict.get('primary vario','')[0]
                primvariosens = "_".join(primvarioinst.split('_')[:-1])
                primscalainst = valdict.get('primary scalar','')[0]
                primscalasens = "_".join(primscalainst.split('_')[:-1])
            except:
                primvarioinst = ''
                primscalainst = ''
            try:
                lastdec = valdict.get('Declination','')[0]  # format 2019-11-22
                lastinc = valdict.get('Incliniation','')[0]  # format 2019-11-22
                lastf = valdict.get('Fieldstrength','')[0]  # format 2019-11-22
            except:
                lastdec = ''
                lastinc = ''
                lastf = ''
        print ("Selected primary variometer: {}".format(primvariosens))
        print ("Selected primary scalar: {}".format(primscalasens))
        print ("Current Values: Declination={}, Inclination={}, Intensity={}".format(lastdec, lastinc, lastf))
    except:
        statusmsg[checkname0] = 'primary instrument assignment failed'
        print (" !!!!!! primary data read failed")

# ################################################
#             Part 1
# ################################################

p1start = datetime.utcnow()
if part1:
    """
    Load Load variation/adjusted data
    Use one specific (primary) variometer and scalar instrument with current data
    Done here to be as close as possible to data acquisition
    """
    print ("----------------------------------------------------------------")
    print ("Part 1a: Create adjusted one minute data from all instruments")
    print ("----------------------------------------------------------------")
    streamlist = []
    try:
      for varioinst in variolist:
        variosens = "_".join(varioinst.split('_')[:-1])
        print ("Variosens", variosens)
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
            #statusmsg[name1b] = 'variometer data loaded'

            # Doing baseline correction

            blvdata = 'BLVcomp_{}_{}_{}'.format(variosens,primscalasens,primpier) 
            blvpath = os.path.join(dipath,blvdata+'.txt')
            print ("    using BLV data".format(blvdata))
            absr = read(blvpath, starttime = endtime-timedelta(days=365))
            blvflagdata = blvdata.replace("comp","")
            blvflaglist = db2flaglist(db,blvflagdata)
            print ("  - Found {} flags for baseline values".format(len(blvflaglist)))

            if len(blvflaglist) > 0:
                absr = absr.flag(blvflaglist)
                absr = absr.remove_flagged()
            if absr.length()[0] > 0:
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
                startabsdatetime = absr._testtime(startabs)
                if startabsdatetime < datetime.utcnow()-timedelta(days=365):
                    startabs = datetime.strftime(datetime.utcnow()-timedelta(days=365),"%Y-%m-%d")
                print (startabs)
                print ("  - using function {} with knots at {} intervals beginning at {}".format(func,fitknot,startabs))
                absr = absr._drop_nans('dx')
                absr = absr._drop_nans('dy')
                absr = absr._drop_nans('dz')
                baselinefunc = vario.baseline(absr,startabs=startabs,fitfunc=func,fitdegree=fitdeg,knotstep=fitknot)
                vario = vario.bc()
                variomin = vario.filter()
                print (" -> Adding variometer data from {} with length {} to streamlist".format(variosens,variomin.length()[0]))
                streamlist.append(variomin)
                if variosens == primvariosens:
                    # Calculate dif
                    print ("HERE")            
        else:
            print ("No variometer data for {}".format(variosens))

      print ("----------------------------------------------------------------")
      print ("Part 1b: Compare adjusted one minute data")
      print ("----------------------------------------------------------------")
      if len(streamlist) > 0:
        # Get the means
        meanstream = stackStreams(streamlist,get='mean',uncert='True')
        mediandx = meanstream.mean('dx',meanfunction='median')
        mediandy = meanstream.mean('dy',meanfunction='median')
        mediandz = meanstream.mean('dz',meanfunction='median')
        print ("Medians", mediandx,mediandy,mediandz)
        maxmedian = max([mediandx,mediandy,mediandz])
        if maxmedian > 0.1:
            statusmsg[namecheck1] = "variometer check - significant differences between instruments - please check"
      else:
        print ("No variometer data found")
        statusmsg[namecheck1] = "variometer check failed - no data found for any variometer"

    except:
      statusmsg[namecheck0] = "variometer check generally failed"

# ################################################
#             Part 2
# ################################################

if part2:
    """
    Publish adjusted data
    - requires an uploadlist for the specific time range
    """
    print ("----------------------------------------------------------------")
    print ("Part 2a: Check scalar data")
    print ("----------------------------------------------------------------")
    name2 = "{}-step2".format(name)

    streamlist = []
    for scalainst in scalalist:
        scalasens = "_".join(scalainst.split('_')[:-1])
        print ("- getting scaladata, flags and offsets: {}".format(scalainst))
        if not scalainst == '':
            scalar = readDB(db,scalainst,starttime=yesterd2)
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
            scalarmin = scalar.filter()
            streamlist.append(scalarmin)
        else:
            print ("Did not find scalar data - aborting")


    print ("----------------------------------------------------------------")
    print ("Part 2b: Compare scalar one minute data")
    print ("----------------------------------------------------------------")
    if len(streamlist) > 0:
        # Get the means
        meanstream = stackStreams(streamlist,get='mean',uncert='True')
        mediandf = meanstream.mean('df',meanfunction='median')
        print ("Medians", mediandf)
        if mediandf > 0.3:
            statusmsg[namecheck2] = "scalar check - large differences between instruments - please check"
    else:
        print ("No scalar data found")
        statusmsg[namecheck2] = "scalar check failed - no data found for any variometer"


    """
     # eventually update average D, I and F values in currentvalue
     if os.path.isfile(currentvaluepath):
                with open(currentvaluepath, 'r') as file:
                    fulldict = json.load(file)
                    valdict = fulldict.get('magnetism')
                    valdict['Declination'] = [dec,'deg']
                    valdict['Inclination'] = [inc,'deg']
                    valdict['Fieldstrength'] = [f,'nT']
                    fulldict[u'magnetism'] = valdict
                with open(currentvaluepath, 'w',encoding="utf-8") as file:
                    file.write(unicode(json.dumps(fulldict)))
                    print ("last QD analysis date has been updated from {} to {}".format(lastQDdate,date))
    """

print (statusmsg)
martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)
