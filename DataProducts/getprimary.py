#!/usr/bin/env python

"""
Magnetism - primary instruments
"""

from magpy.stream import *   
from magpy.database import *   
import magpy.opt.cred as mpcred
import json
# old
from pickle import dump


## New Logging features 
from martas import martaslog as ml
logpath = '/var/log/magpy/mm-dp-getprimary.log'
#import socket
#sn = socket.gethostname().upper()
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
name1 = "{}-DataProducts-getprimary-process".format(sn)
name2 = "{}-DataProducts-getprimary-vario".format(sn)
name3 = "{}-DataProducts-getprimary-scalar".format(sn)
name4 = "{}-DataProducts-getprimary-write".format(sn)


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

endtime = datetime.utcnow()
daystodeal = 1
starttime=datetime.strftime(endtime-timedelta(days=daystodeal),"%Y-%m-%d")
dbdateformat = "%Y-%m-%d %H:%M:%S.%f"

#Instrument Lists
variolist = ['LEMI036_1_0002_0002','LEMI025_22_0003_0002','FGE_S0252_0001_0001']
scalalist = ['GP20S3NSS2_012201_0001_0001','GSM90_14245_0002_0002','GSM90_6107631_0001_0001','POS1_N432_0001_0001']

currentvaluepath = '/srv/products/data/current.data'
varioinst = ''
scalainst = ''
#old
priminst = '/home/cobs/ANALYSIS/Logs/primaryinst.pkl'

def getstringdate(input):
    dbdateformat1 = "%Y-%m-%d %H:%M:%S.%f"
    dbdateformat2 = "%Y-%m-%d %H:%M:%S"
    try:
        value = datetime.strptime(input,dbdateformat1)
    except:
        try:
            value = datetime.strptime(input,dbdateformat2)
        except:
            print ("Get primary: error when converting database date to datetime")
            return datetime.utcnow()
    return value

#part1 = True
try:
    #if part1:
    """
    Identify currently active variometer and f-instrument, which are recording now and have at least one day of data
    """
    ## Step 1: checking available data for variometers (need to cover one day and should not be older than one hour 
    ##         the first instrument fitting these conditions is selected
    for inst in variolist:
        print ("Checking ", inst)
        last = dbselect(db,'time',inst,expert="ORDER BY time DESC LIMIT 86400")
        print (len(last), last[0], last[-1])
        if len(last) > 0:
            # convert last to datetime
            lastval = getstringdate(last[0]) #datetime.strptime(last[0],dbdateformat)
            firstval = getstringdate(last[-1]) #datetime.strptime(last[-1],dbdateformat)
            print (lastval,firstval)
            if lastval > endtime-timedelta(minutes=60) and firstval > endtime-timedelta(minutes=1500):
                varioinst = inst
                print ("Coverage OK - using {}".format(inst))
                break
            else:
                print ("Coverage not OK")
    statusmsg[name2] = '{}'.format(varioinst.replace('_','-'))

    ## Step 2: checking available scalar data (need to be valid data and cover one day)
    ##         the first instrument fitting these conditions is selected
    for inst in scalalist:
        print ("Checking ", inst)
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
        print (len(last), getstringdate(last[0]))
        if len(last) > 0 and lval and not lval[0] in [0,np.nan]:
            # convert last to datetime
            print ("Test 1 OK: data  available and last value not null")
            lastval = getstringdate(last[0]) # datetime.strptime(last[0],dbdateformat)
            firstval = getstringdate(last[-1]) # datetime.strptime(last[-1],dbdateformat)
            print (lastval, firstval)
            if lastval > endtime-timedelta(minutes=60) and firstval > endtime-timedelta(minutes=1500):
                print ("Test 2 OK: correct amount of data covering the last day")
                scalainst = inst
                break

    statusmsg[name3] = '{}'.format(scalainst.replace('_','-'))
    # Fallback
    if varioinst == '':
        print (" -- Did not find variometer instrument")
        varioinst = variolist[0]
    if scalainst == '':
        print (" -- Did not find scalar instrument")
        scalainst = scalalist[0]
        statusmsg[name3] = 'No recent scalar data found - check collector jobs and DB - switching to default - will automatically continue without further notice as soon as DB or collector is online again'

    print ("Get primary -> Using Variometer {} and Scalar {}".format(varioinst, scalainst))

    try:
        print ("Dumping instruments as usual in the old way...")
        #old
        """
        /home/cobs/ANALYSIS/TitleGraphs/mag_graph.py:62:    priminst = '/home/cobs/ANALYSIS/Logs/primaryinst.pkl'
        /home/cobs/ANALYSIS/DataProducts/GIC/src/gic_dailyplot.py:58:        priminst = '/home/cobs/ANALYSIS/Logs/primaryinst.pkl'
        /home/cobs/ANALYSIS/DataProducts/StormDetection/StormDetector_brain.py:860:    priminst = '/home/cobs/ANALYSIS/Logs/primaryinst.pkl'
        /home/cobs/ANALYSIS/DataProducts/StormDetection/StormDetector_graph.py:216:        priminst = '/home/cobs/ANALYSIS/Logs/primaryinst.pkl'
        /home/cobs/ANALYSIS/DataProducts/magnetism_products.py:228:    #priminst = '/home/cobs/ANALYSIS/Logs/primaryinst.pkl'
        /home/cobs/ANALYSIS/Info/tg_base.py:43:priminst = '/home/cobs/ANALYSIS/Logs/primaryinst.pkl'
        """
        lst = [varioinst, scalainst]
        dump(lst,open(priminst,'wb'))
        print ("... done")
    except:
        pass

    try:
        #ok = True
        #if ok:
        # writing data to current.data json 
        if os.path.isfile(currentvaluepath):
            # read log if exists and exentually update changed information
            # return changes
            with open(currentvaluepath, 'r') as file:
                fulldict = json.load(file)
                valdict = fulldict.get('magnetism',{})
        else:
            valdict = {}
            fulldict = {}
            #fulldict['meteo'] = valdict
        print ("Got", fulldict)
        valdict[u'primary vario'] = [varioinst,'']
        valdict[u'primary scalar'] = [scalainst,'']

        fulldict[u'magnetism'] = valdict
        with open(currentvaluepath, 'w',encoding="utf-8") as file:
            file.write(unicode(json.dumps(fulldict))) # use `json.loads` to do $
        statusmsg[name4] = 'values written'
    except:
        statusmsg[name4] = 'problem when writing current values '
    statusmsg[name1] = 'successful'
except:
    statusmsg[name1] = 'failed'

martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)

