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

dbpasswd = mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print ("Connecting to primary MARCOS...")
    db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")
    print db
except:
    print ("... failed")
    try:
        # Test MARCOS 2
        print ("Connecting to secondary MARCOS...")
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
        print db
    except:
        print ("... failed -- aborting")
        sys.exit()
print ("... success")

# ####################
#  Activate monitoring
# #################### 

try:
    from magpy.opt.analysismonitor import *
    analysisdict = Analysismonitor(logfile='/home/cobs/ANALYSIS/Logs/AnalysisMonitor_cobs.log')
    analysisdict = analysisdict.load()
except:
    print ("Analysis monitor failed")
    pass


# ####################
#  Basic definitions
# #################### 
failure = False

endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=20),"%Y-%m-%d")
date = datetime.strftime(endtime,"%Y-%m-%d")

figpath = '/srv/products/graphs/radon/'

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


part1 = True
if part1:
    """
    Plot radon data
    """
    p1start = datetime.utcnow()
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
 
if not failure:
    analysisdict.check({'script_periodic_gamma_graph': ['success','=','success']})
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
    print ("    gamma_graph successfully finished     ")
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
else:
    analysisdict.check({'script_periodic_gamma_graph': ['failure','=','success']})

martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)

