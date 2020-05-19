#!/usr/bin/env python

"""
Script for plotting graph of solar wind data relevant to
geomagnetic activity.
"""

import numpy as np
from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.cred as mpcred

# ####################
#  Activate monitoring
# #################### 
## New Logging features 
from martas import martaslog as ml
logpath = '/var/log/magpy/mm-per-spaceweather.log'
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
name = "{}-PeriodicPlot-spaceweather".format(sn)
nameupload = "{}-PeriodicPlot-spaceweather-upload".format(sn)

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
starttime = endtime - timedelta(days=3)
date = datetime.strftime(endtime,"%Y-%m-%d")

solarpath = '/srv/archive/external/esa-nasa/ace'

try:
    try:
        dscovr_plasma = read("http://services.swpc.noaa.gov/products/solar-wind/plasma-3-day.json")
        dscovr_mag = read("http://services.swpc.noaa.gov/products/solar-wind/mag-3-day.json")
    #    ace_1m = read(os.path.join(solarpath,'collected','ace_1m_*'),
    #        starttime=starttime,endtime=endtime)
    #    ace_5m = read(os.path.join(solarpath,'collected','ace_5m_*'),
    #        starttime=starttime,endtime=endtime)
    except Exception as e:
        print("Reading ACE data failed (%s)!" % e)

    startstr = num2date(dscovr_mag.ndarray[0][0])
    endstr = num2date(dscovr_mag.ndarray[0][-1])
    print("Plotting from %s to %s" % (startstr, endstr))

    dscovr_plasma.header['col-var1'] = 'Density'
    dscovr_plasma.header['unit-col-var1'] = 'p/cc'
    dscovr_plasma.header['col-var2'] = 'Speed'
    dscovr_plasma.header['unit-col-var2'] = 'km/s'
    dscovr_mag.header['col-z'] = 'Bz'
    dscovr_mag.header['unit-col-z'] = 'nT'

    kp = read(path_or_url=os.path.join('/srv/archive/external/gfz','kp','gfzkp*')) # changed by KOMPEIN N. 2018-10-02 due to missing data on ZAGTSVL47u
    #kp = read(path_or_url=os.path.join('sftp://138.22.188.195','//srv/archive/external/gfz','kp','gfzkp*'))
    kp = kp.trim(starttime=starttime,endtime=endtime)

    #print (kp.length()[0], dscovr_mag.length()[0], dscovr_plasma.length()[0])
    #mp.plot(kp)

    mp.plotStreams([kp, dscovr_mag, dscovr_plasma],[['var1'], ['z'],['var1','var2']],confinex=True,bartrange=0.06,symbollist=['z','-','-','-'],specialdict = [{'var1': [0,9]}, {}, {}],plottitle = "Solar and global magnetic activity (GFZ + DSCOVR data)",outfile=os.path.join(solarpath),noshow=True)

    savepath = "/srv/products/graphs/spaceweather/solarwindact_%s.png" % date
    plt.savefig(savepath)
    print("File saved to {}.".format(savepath))

    statusmsg[name] = 'Spaceweather plot successfull'
except:
    failure = True
    statusmsg[name] = 'Spaceweather plot failed'

try:
    #upload
    cred = 'cobshomepage'
    address=mpcred.lc(cred,'address')
    user=mpcred.lc(cred,'user')
    passwd=mpcred.lc(cred,'passwd')
    port=mpcred.lc(cred,'port')
    remotepath = 'zamg/images/graphs/magnetism/'
    path2log = '/home/cobs/ANALYSIS/Logs/magvar.log'
    oldremotepath = 'cmsjoomla/images/stories/currentdata/wic/'

    # to send with 664 permission use a temporary directory
    try:
        tmppath = "/tmp"
        tmpfile= os.path.join(tmppath,os.path.basename(savepath))
        from shutil import copyfile
        copyfile(savepath,tmpfile)
        scptransfer(tmpfile,'94.136.40.103:'+remotepath,passwd,timeout=60)
        os.remove(tmpfile)
    except:
        pass
    #statusmsg[nameupload] = 'Spaceweather plot uploaded'
    print ('Spaceweather plot uploaded')
except:
    print ('Spaceweather plot upload failed')
    #statusmsg[nameupload] = 'Spaceweather plot upload failed'


martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)

