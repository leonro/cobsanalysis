#!/usr/bin/env python

"""
Skeleton for graphs
"""

from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred

import itertools
from threading import Thread

from martas import martaslog as ml
#import socket
logpath = '/var/log/magpy/mm-per-gamma.log'
#sn = socket.gethostname().upper()
sn = 'SAGITTARIUS'
statusmsg = {}
name = "{}-PeriodicPlot-gamma".format(sn)

currentvaluepath = '/srv/products/data/current.data'

# ####################
#  Importing database
# ####################

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



def gamma_plot():
    """
    Plot radon data
    """
    try:
        meteopath = '/srv/products/data/meteo'
        gamma = readDB(db,'GAMMA_SFB867_0001_0001',starttime=starttime,endtime=endtime)
        meteo = read(os.path.join(meteopath,'meteo-1min*'),starttime=starttime,endtime=endtime)

        print ("... reading finished.")

        rainmax = meteo._get_max('y')
        if rainmax < 10:
            rainmax = 10
        snowmax = meteo._get_max('z')
        if snowmax < 50:
            snowmax = 50
        filter_width=timedelta(minutes=1)
        gamma = gamma.multiply({'x':0.016666667})
        gamma.header['col-x'] = 'Counts'
        gamma.header['unit-col-x'] = '1/s'
        gamma.header['col-t1'] = 'T'
        gamma.header['unit-col-t1'] = 'C'

        print ("Plotting ...")

        mp.plotStreams([gamma,meteo],[['x','t1'],['var5','f','y']], gridcolor='#316931',labels=[['Bq','T (SGO-tunnel)'],['pressure','T outside','rain bucket']], specialdict=[{},{'var1':[0,rainmax]}], padding=[[20,0.3],[5,1,0,0]],fill=['t1','y','var1'], colorlist=['k','g','m','r','b'], confinex=True, opacity=0.7, plottitle='Radon variation and environment',noshow=True)
        #mp.plot(meteo,["var5","f"])
        #plt.show()
        filename = "radon_%s.png" % date
        savepath = os.path.join(figpath,"radon_%s.png" % date)
        plt.savefig(savepath)

        remotepath = 'zamg/images/graphs/radon/'
        path2log='/home/cobs/ANALYSIS/Logs/radgraph-transfer.log'
        #ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)

    except:
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print ("     gamma step 1 failed        ")
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        failure = True
        statusmsg[name] = 'Gamma plot failed'

    fulldict = getcurrentdata(currentvaluepath)
    valdict = fulldict.get('logging',{})

    try:
        tmppath = "/tmp"
        tmpfile= os.path.join(tmppath,os.path.basename(savepath))
        from shutil import copyfile
        copyfile(savepath,tmpfile)
        scptransfer(tmpfile,'94.136.40.103:'+remotepath,passwd)
        os.remove(tmpfile)

        p1end = datetime.utcnow()
        print "-----------------------------------"
        print "Part1 needs", p1end-p1start
        print "-----------------------------------"
        statusmsg[name] = 'Gamma plot successfull'
        valdict['failedupload2homepage'] = 0
    except:
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print ("     gamma step 1 failed        ")
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        failure = True
        faileduploadcount = int(valdict.get('failedupload2homepage',0))
        print ("Current failed-upload count = {}".format(faileduploadcount))
        faileduploadcount += 1
        if faileduploadcount >= 4:
           # Only report if upload failed at least 4 times in row -> approximately after 2 hours... as the same counter is used by tilt
           statusmsg[name] = 'Gamma plot upload failed'
        else:
           statusmsg[name] = 'Gamma plot successfull'
        print ("Writing new count to currentdata")
        valdict['failedupload2homepage'] = faileduploadcount

    fulldict[u'logging'] = valdict
    writecurrentdata(currentvaluepath, fulldict)


def main(argv):
    try:
        version = __version__
    except:
        version = "1.0.0"
    configpath = ''
    statusmsg = {}
    debug=False
    starttime = None
    endtime = None
    source = 'database'

    try:
        opts, args = getopt.getopt(argv,"hc:e:s:D",["config=","endtime=","starttime=","debug=",])
    except getopt.GetoptError:
        print ('gamma_plot.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- gamma_plot.py will determine the primary instruments --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python gamma_plot.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-e            : endtime - default is now')
            print ('-s            : starttime -  default is three days from now')
            print ('-------------------------------------')
            print ('Application:')
            print ('python gamma_plot.py -c /etc/marcos/analysis.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-s", "--starttime"):
            # get an endtime
            starttime = arg
        elif opt in ("-e", "--endtime"):
            # get an endtime
            endtime = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running current_weather version {}".format(version))
    print ("--------------------------------")

    if endtime:
         try:
             endtime = DataStream()._testtime(endtime)
         except:
             print (" Could not interprete provided endtime. Please Check !")
             sys.exit(1)
    else:
         endtime = datetime.utcnow()

    if starttime:
         try:
             starttime = DataStream()._testtime(starttime)
         except:
             print (" Could not interprete provided starttime. Please Check !")
             sys.exit(1)
    else:
         starttime = datetime.utcnow()-timedelta(days=3)

    if starttime >= endtime:
        print (" Starttime is larger than endtime. Please correct !")
        sys.exit(1)


    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "PeriodicGraphs", job=os.path.basename(__file__), newname='mm-pg-currentweatherchanges.log', debug=debug)
    name1 = "{}-weatherchange".format(config.get('logname'))
    statusmsg[name1] = 'weather change analysis successful'

    print ("3. Connect databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
    except:
        statusmsg[name1] = 'database failed'
    # it is possible to save data also directly to the brokers database - better do it elsewhere

    print ("4. Weather change analysis")
    success = weather_change(db, config=config, starttime=starttime, endtime=endtime, debug=debug)
    if not success:
        statusmsg[name1] = 'weather change analysis failed'

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)


if __name__ == "__main__":
   main(sys.argv[1:])

