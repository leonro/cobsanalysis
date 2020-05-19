#!/usr/bin/env python

from magpy.stream import *
import magpy.mpplot as mp

# Import Monitoring...

import sys

end = datetime.strftime(datetime.utcnow(),"%Y-%m-%d")
start = datetime.strftime(datetime.utcnow() - timedelta(days=30),"%Y-%m-%d")
tmppath = '/tmp'

# Paths
ionopath = '/srv/projects/gamma/iono/raw/IM_*'
gammapath = '/srv/projects/gamma/NaTl*'
meteopath = '/srv/products/data/meteo/meteo-1min_*'
bmppath = '/srv/projects/gamma/BE280_0X76I2C00001_0001/raw/BE280*'
mq135path = '/srv/projects/gamma/MQ135_FC2200001_0001/raw/MQ135*'

statusdict = {}

step1 = True # Read data and filter
step2 = True # Plot all data sets in a common diagram (only possible if step1 = True)
step3 = True # Monitor project specific data


# 1. Read data from different sensors and filter them
#    Required are: IONO Neg, Bi214 (Radon proxy), Outside T, Inside T, Pressure, CO2 
if step1:
    # TODO Availablility of data should be monitored elsewhere 
    # Read IONO:
    try:
        iono = read(ionopath, starttime=start, endtime=end)
        if iono.length()[0] > 0:
            ionotest = True
        else:
            ionotest = False
    except:
        ionotest = False
    # Read Gamma
    try:
        gamma = read(gammapath, starttime= start, endtime=end)
        if gamma.length()[0] > 0:
            gammatest = True
        else:
            gammatest = False
    except:
        gammatest = False
    # Read and filter outside T
    try:
        meteo = read(meteopath, starttime= start, endtime=end)
        meteo = meteo.filter()
        if meteo.length()[0] > 0:
            meteotest = True
        else:
            meteotest = False
    except: 
        meteotest = False
    # Read and filter inside T/P
    try:
        room = read(bmppath, starttime= start, endtime=end)
        room = room.filter()
        if room.length()[0] > 0:
            roomtest = True
        else:
            roomtest = False
    except:
        roomtest = False
    # Read and filter CO2
    try:
        co = read(mq135path, starttime= start, endtime=end)
        co = co.filter()
        if co.length()[0] > 0:
            cotest = True
        else:
            cotest = False
    except:
        cotest = False


if step2:
    if cotest and roomtest and meteotest and gammatest and ionotest:
        mp.plotStreams([iono,gamma,meteo,room,co],[ ['x'], ['t1'], ['f'], ['t1','var2'], ['var5'] ])
        statusdict['project_iono_graph'] = 'full data plot'
    elif cotest and roomtest and gammatest and ionotest:
        mp.plotStreams([iono,gamma,room,co],[ ['x'], ['t1'], ['t1','var2'], ['var5'] ])
        statusdict['project_iono_graph'] = 'reduced plot - check meteo'
    elif cotest and roomtest and gammatest and meteotest:
        mp.plotStreams([gamma,meteo,room,co],[ ['t1'], ['f'], ['t1','var2'], ['var5'] ])
        statusdict['project_iono_graph'] = 'reduced plot - check iono'
    elif cotest and roomtest and gammatest:
        mp.plotStreams([gamma,room,co],[ ['t1'], ['t1','var2'], ['var5'] ])
        statusdict['project_iono_graph'] = 'minimal plot - check iono and meteo'
    else:
        statusdict['project_iono_graph'] = 'not enough data'

if step3:
    start = datetime.strftime(datetime.utcnow() - timedelta(days=2),"%Y-%m-%d")
    try:
        iono = read(ionopath, starttime=start, endtime=end)
        if iono.length()[0] > 0:
            statusdict['project_iono_ionodata'] = 'available'
        else:
            statusdict['project_iono_ionodata'] = 'not available since 48 hours'
    except:
        statusdict['project_iono_ionodata'] = 'not available '
    try:
        gamma = read(gammapath, starttime=start, endtime=end)
        # TODO Trim does not work. Why??
        #print (gamma.length(), start, end)
        #print (gamma._find_t_limits())
        #mp.plot(gamma)
        if gamma.length()[0] > 0:
            statusdict['project_iono_gammadata'] = 'available'
        else:
            statusdict['project_iono_gammadata'] = 'not available since 48 hours'
    except:
        statusdict['project_iono_gammadata'] = 'not available '
    try:
        room = read(bmppath, starttime=start, endtime=end)
        if room.length()[0] > 0:
            statusdict['project_iono_roomdata'] = 'available'
        else:
            statusdict['project_iono_roomdata'] = 'not available since 48 hours'
    except:
        statusdict['project_iono_roomdata'] = 'not available '
    try:
        co = read(mq135path, starttime=start, endtime=end)
        if co.length()[0] > 0:
            statusdict['project_iono_codata'] = 'available'
        else:
            statusdict['project_iono_codata'] = 'not available since 48 hours'
    except:
        statusdict['project_iono_codata'] = 'not available '


print (statusdict)
