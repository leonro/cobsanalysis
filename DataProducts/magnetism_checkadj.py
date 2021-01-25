#!/usr/bin/env python

"""
DESCRIPTION
   Compares adjusted data sets from all sensors for similarity. If not similar then a
   waring is send out. The same comparison is done for all F sensors using the 
   offsets contained in the database.

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -j joblist             :   list    :  vario,scalar
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"

APPLICATION
    PERMANENTLY with cron:
        python magnetism_checkadj.py -c /etc/marcos/analysis.cfg
    REDO analysis for a time range:
        (startime is defined by endtime - daystodeal as given in the config file 
        python magnetism_checkadj.py -c /etc/marcos/analysis.cfg -e 2020-11-22

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

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, DoVarioCorrections, DoBaselineCorrection, DoScalarCorrections,ConnectDatabases, GetPrimaryInstruments, getcurrentdata, writecurrentdata
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__



def GetDirections(datastream, debug=False):
    """
    DESCRIPTION:
        Calculate last hourly mean declination, inclination and F
    """
    dec = float(nan)
    inc = float(nan)
    f = float(nan)
    hourly = datastream.filter(filter_width=timedelta(minutes=60), resampleoffset=timedelta(minutes=30), filter_type='flat', missingdata='iaga')
    if hourly.length()[0] > 0:
        hourly = hourly.hdz2xyz()
        hourly = hourly.xyz2idf()
        dec = hourly._get_column('y')[-1]
        inc = hourly._get_column('x')[-1]
        f = hourly._get_column('z')[-1]
        if debug:
            print ("     -- got dec={} deg , inc={} deg and f={} nT at {}".format(dec,inc,f, num2date(hourly._get_column('time')[-1]))) 
    return (dec, inc, f)

def CompareAdjustedVario(config={}, endtime=datetime.utcnow(), debug=False):
    """
    DESCRIPTION:
        Load Load variation/adjusted data
        Use one specific (primary) variometer and scalar instrument with current data
        Done here to be as close as possible to data acquisition
    """
    if debug:
        p1start = datetime.utcnow()

    msg = "variometer check performed successfully - everything ok"
    variolist = config.get('variometerinstruments')
    db = config.get('primaryDB',None)
    primpier = config.get('primarypier')
    daystodeal = config.get('daystodeal')
    streamlist = []
    variochecklist = []

    try: # assigning streamlist
        for varioinst in variolist:
            variosens = "_".join(varioinst.split('_')[:-1])
            vario = readDB(db,varioinst,starttime=datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d"))
            if vario.length()[0] > 0 and db:
                vario = DoVarioCorrections(db, vario, variosens=variosens, starttimedt=endtime-timedelta(days=daystodeal))
                vario, msg = DoBaselineCorrection(db, vario, config=config, baselinemethod='full',endtime=endtime)
                variomin = vario.filter()
                print ("    -> Adding variometer data from {} with length {} to streamlist".format(variosens,variomin.length()[0]))
                streamlist.append(variomin)
                variochecklist= variochecklist.append(varioinst)
                if variosens == config.get('primaryVario'):
                    # Calculate dif
                    print ("HERE")
                    (dec,inc,f) = GetDirections(variomin)
                    print ("    -> Obtained Declination={}, Inclination={} and F={}".format(dec,inc,f))
                    if not debug:
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
                                print ("Magnetic directions have been updated to {}".format(lastQDdate,date))
            else:
                print ("    -> No data for {}".format(varioinst))
    except:
        msg = "variometer check - selection of variometers failed"

    if debug:
        print ("   -> The following variometer sensors are checked:")
        print ("   -> {}".format(variochecklist))

    try: # getting means
       if len(streamlist) > 0:
            # Get the means
            meanstream = stackStreams(streamlist,get='mean',uncert='True')
            mediandx = meanstream.mean('dx',meanfunction='median')
            mediandy = meanstream.mean('dy',meanfunction='median')
            mediandz = meanstream.mean('dz',meanfunction='median')
            print ("Medians", mediandx,mediandy,mediandz)
            maxmedian = max([mediandx,mediandy,mediandz])
            if maxmedian > 0.2:
                msg = "variometer check - significant differences between instruments exceeding 0.2 nT - please check"
        else:
            print ("No variometer data found")
            msg = "variometer check failed - no data found for any variometer"
    except:
        msg = "variometer check - calculation of means failed"

    return msg

def CompareFieldStrength(config={}, endtime=datetime.utcnow(), debug=False):
    """
    DESCRIPTION
        compare field strengths and check scalar data
    """
    if debug:
        print (" - Checking scalar data")

    msg = "scalar data check performed successfully - everything ok"

    scalalist = config.get('scalarinstruments')
    db = config.get('primaryDB',None)
    primpier = config.get('primarypier')
    daystodeal = config.get('daystodeal')
    medianf = 999
    scalarchecklist = []
    streamlist = []

    try:
        for scalainst in scalalist:
            scalasens = "_".join(scalainst.split('_')[:-1])
            print ("    -- getting scaladata, flags and offsets: {}".format(scalainst))
            if not scalainst == '':
                scalar = readDB(db,scalainst,starttime=datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d"))
            if (scalar.length()[0]) > 0:
                scalar = DoScalarCorrections(db, scalar, scalarsens=scalasens, starttimedt=endtime-timedelta(days=daystodeal), debug=debug)
                scalarmin = scalar.filter()
                streamlist.append(scalarmin)
                scalarchecklist.append(scalasens)
    except:
        msg = "scalar check - selection of sensors failed"

    if debug:
        print ("   -> The following scalar sensors are checked:")
        print ("   -> {}".format(scalarchecklist))

    try:
        if len(streamlist) > 0:
            # Get the means
            meanstream = stackStreams(streamlist,get='mean',uncert='True')
            mediandf = meanstream.mean('df',meanfunction='median')
            if mediandf > 0.3:
                msg = "scalar check - large differences between instruments - please check"
        else:
            print ("    -> No scalar data found")
            msg = "scalar check failed - no data found for any sensor"
    except:
        msg = "scalar check - general error"

    if debug:
        print ("   -> The total median difference between all sensors is:")
        print ("   -> {}".format(mediandf))

    return msg

def main(argv):
    try:
        version = __version__
    except:
        version = "1.0.0"
    configpath = ''
    statusmsg = {}
    joblist = ['vario','scalar']
    debug=False
    endtime = None

    try:
        opts, args = getopt.getopt(argv,"hc:j:D",["config=","joblist=","endtime=","debug=",])
    except getopt.GetoptError:
        print ('magnetism_checkadj.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- magnetism_checkadj.py will analyse magnetic data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python magnetism_checkadj.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-j            : vario, scalar')
            print ('-e            : endtime')
            print ('-------------------------------------')
            print ('Application:')
            print ('python magnetism_checkadj.py -c /etc/marcos/analysis.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-j", "--joblist"):
            # get a list of jobs (vario, scalar)
            joblist = arg.split(',')
        elif opt in ("-e", "--endtime"):
            endtime = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    if debug:
        print ("Running magnetism_checkadj version {} - debug mode".format(version))
        print ("---------------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    if endtime:
        try:
            endtime = DataStream()._testtime(endtime)
        except:
            print ("Endtime could not be interpreted - Aborting")
            sys.exit(1)
    else:
        endtime = datetime.utcnow()

    print ("1. Read configuration data")
    config = GetConf(configpath)
    config = ConnectDatabases(config=config, debug=debug)


    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname='mm-dp-magdatacheck.log', debug=debug)

    if debug:
        print (" -> Config contents:")
        print (config)

    print ("3. Get basic information of current data")
    config,statusmsg = GetPrimaryInstruments(config=config, statusmsg=statusmsg, fallback=False, debug=debug)

    if 'vario' in joblist:
        print ("4. Variometer analysis")
        namecheck1 = "{}-check-adjusted".format(config.get('logname'))
        msg = CompareAdjustedVario(config=config, endtime=endtime, debug=debug)
        statusmsg[namecheck1] = msg

    if 'scalar' in joblist:
        print ("5. Scalar analysis")
        namecheck2 = "{}-check-f".format(config.get('logname'))
        msg = CompareFieldStrength(config=config, endtime=endtime, debug=debug)
        statusmsg[namecheck2] = msg


    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])

