#!/usr/bin/env python

"""
DESCRIPTION
   Checks all variometers and scalar sensors as defined in a configuration file.
   The first instrument in the list, which fulfills criteria on data availablility
   will be selected as primary instrument. Both primary instruments will be stored
   within a current.data json structure.

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)

APPLICATION
    PERMANENTLY with cron:
        python getprimary.py -c /etc/marcos/analysis.cfg
"""

from magpy.stream import *
from magpy.database import *
import magpy.opt.cred as mpcred
import json
import getopt
import pwd
import socket
import sys  # for sys.version_info()

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, getstringdate
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf


"""
Methods in several scripts:
ConnectDatabases()
DefineLogger()
getstringdate()

"""

def PrimaryVario(db, variolist, endtime=datetime.utcnow(), logname='', statusmsg={}, debug=False):
    """
    Identify currently active variometer and f-instrument, which are recording now and have at least one day of data
    """
    varioinst = ''
    ## Step 1: checking available data for variometers (need to cover one day and should not be older than one hour 
    ##         the first instrument fitting these conditions is selected
    for inst in variolist:
        if debug:
            print ("    -- Checking ", inst)
        last = dbselect(db,'time',inst,expert="ORDER BY time DESC LIMIT 86400")
        if len(last) > 0:
            # convert last to datetime
            lastval = getstringdate(last[0])
            firstval = getstringdate(last[-1])
            print (lastval,firstval)
            if lastval > endtime-timedelta(minutes=60) and firstval > endtime-timedelta(minutes=1500):
                varioinst = inst
                if debug:
                    print ("    -- Coverage OK - using {}".format(inst))
                break
            else:
                print ("    -- Coverage not OK")
    print ("  -> Selected: {}".format(varioinst))

    if varioinst == '':
        print (" -- Did not find variometer instrument")
        varioinst = variolist[0]
        print ("  -> Selected fallback: {}".format(varioinst)) 
        statusmsg[logname] = 'No recent variometer data found - switching to fallback - will automatically continue without further notice until DB/collector are online again'
    else:
        statusmsg[logname] = '{}'.format(varioinst.replace('_','-'))

    return varioinst, statusmsg


def PrimaryScalar(db, scalarlist, endtime=datetime.utcnow(), logname='', statusmsg={}, debug=False):
    """
    Identify currently active variometer and f-instrument, which are recording now and have at least one day of data
    """
    scalainst=''
    ## Step 2: checking available scalar data (need to be valid data and cover one day)
    ##         the first instrument fitting these conditions is selected
    for inst in scalarlist:
        print ("    -- Checking ", inst)
        try:
            sr = dbselect(db,'DataSamplingRate','DATAINFO','DataID LIKE "{}"'.format(inst))
            sr = float(sr[0])
            CNT = int(86400./sr)
        except:
            CNT = 86400
        last = dbselect(db,'time',inst,expert="ORDER BY time DESC LIMIT "+str(CNT))
        #if inst.startswith('GP20S3'):
        #    lval = dbselect(db,'y',inst,expert="ORDER BY time DESC LIMIT 1")
        #else:
        lval = dbselect(db,'f',inst,expert="ORDER BY time DESC LIMIT 1")
        if len(last) > 0 and lval and not lval[0] in [0,np.nan]:
            # convert last to datetime
            if debug:
                print ("    -- Test 1 OK: data  available and last value not null")
            lastval = getstringdate(last[0])
            firstval = getstringdate(last[-1])
            if debug:
                print (lastval, firstval)
            if lastval > endtime-timedelta(minutes=60) and firstval > endtime-timedelta(minutes=1500):
                if debug:
                    print ("    -- Test 2 OK: correct amount of data covering the last day")
                scalainst = inst
                break
            else:
                if debug:
                    print ("    -- Test 2 failed: {} is smaller than {} or {} is smaller than {}".format(lastval,endtime-timedelta(minutes=60),firstval,endtime-timedelta(minutes=1500)))
    print ("  -> Selected: {}".format(scalainst))

    if scalainst == '':
        print ("  !! Did not find scalar instrument")
        scalainst = scalalist[0]
        print ("  -> Selected fallback: {}".format(scalainst)) 
        statusmsg[logname] = 'No recent scalar data found - switching to fallback - will automatically continue without further notice until DB/collector are online again'
    else:
        statusmsg[logname] = '{}'.format(scalainst.replace('_','-'))

    return scalainst, statusmsg


def UpdateCurrentValuePath(currentvaluepath, varioinst='', scalainst='', logname='', statusmsg={}, debug=False):
    try:
        if os.path.isfile(currentvaluepath):
            # read log if exists and exentually update changed information
            # return changes
            with open(currentvaluepath, 'r') as file:
                fulldict = json.load(file)
                valdict = fulldict.get('magnetism',{})
        else:
            valdict = {}
            fulldict = {}
        print ("    -- Got", valdict)
        valdict[u'primary vario'] = [varioinst,'']
        valdict[u'primary scalar'] = [scalainst,'']

        fulldict[u'magnetism'] = valdict
        with open(currentvaluepath, 'w',encoding="utf-8") as file:
            file.write(unicode(json.dumps(fulldict))) # use `json.loads` to do $
        print ("  -> current.value succesfully updated")
        statusmsg[logname] = 'values written'
    except:
        statusmsg[logname] = 'problem when writing current values '

    return statusmsg


def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    debug=False
    endtime = datetime.utcnow()
    varioinst = ''
    scalainst = ''

    try:
        opts, args = getopt.getopt(argv,"hc:e:D",["config=","endtime=","debug=",])
    except getopt.GetoptError:
        print ('getprimary.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- getprimary.py will determine the primary instruments --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python getprimary.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-------------------------------------')
            print ('Application:')
            print ('python getprimary.py -c /etc/marcos/analysis.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-e", "--endtime"):
            # get an endtime
            endtime = arg.split(',')
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running getprimary version {}".format(version))
    print ("--------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category="DataProducts", job=os.path.basename(__file__), newname='mm-dp-getprimary.log')

    name1 = "{}-process".format(config.get('logname'))
    name2 = "{}-vario".format(config.get('logname'))
    name3 = "{}-scalar".format(config.get('logname'))
    name4 = "{}-write".format(config.get('logname'))
    statusmsg[name1] = 'successful'

    print ("3. Connect databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        if debug:
            print ("   -- success")
    except:
        if debug:
            print ("   -- database failed")
        statusmsg[name1] = 'database failed'

    print ("4. Checking variometer instruments")
    try:
        varioinst, statusmsg = PrimaryVario(db, config.get('variometerinstruments'), endtime=endtime,  logname=name2, statusmsg=statusmsg, debug=debug)
        print ("   -> Using {}".format(varioinst))
    except:
        if debug:
            print ("   -- vario failed")
        statusmsg[name1] = 'vario failed'

    print ("5. Checking scalar instruments")
    try:
        scalainst, statusmsg = PrimaryScalar(db, config.get('scalarinstruments'), endtime=endtime,  logname=name3, statusmsg=statusmsg, debug=debug)
        print ("   -> Using {}".format(scalainst))
    except:
        if debug:
            print ("   -- scalar failed")
        statusmsg[name1] = 'scalar failed'

    print ("6. Updating Current values")
    try:
        if not debug:
            statusmsg = UpdateCurrentValuePath(config.get('currentvaluepath'), varioinst=varioinst, scalainst=scalainst, logname=name4, statusmsg=statusmsg, debug=debug)
    except:
        if debug:
            print ("   -- update current failed")
        statusmsg[name1] = 'current value update failed'


    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])


