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

# ####################
# Methods
# ####################

#from __future__ import print_function
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d

#### Add this code to gravity analysis
def calibneck(neck,calibpath='/home/cobs/ANALYSIS/PeriodicGraphs/OMEGA.DAT',calibplot=False):
    f = open(calibpath,'r')
    x = f.readlines()
    f.close()
    array = []
    # 1. analyse calibration data
    for el in x:
        l = el.strip().split()
        if len(l) == 2:
            array.append([float(i) for i in l])
    array = sorted([[ar[1],ar[0]] for ar in array])
    array = np.asarray(array)
    tar = array.transpose()
    x,y = tar[0],tar[1]

    # 2. check whether all neck values are within caibration range
    neck = np.asarray(neck)
    neck[neck>max(x)] = np.nan
    neck[neck<min(x)] = np.nan

    f = interp1d(x, y, kind='cubic')

    if calibplot:
        xnew = np.linspace(min(x), max(x), num=41, endpoint=True)
        plt.plot(x,y,'o', xnew, f(xnew), '-')
        plt.xlabel('V')
        plt.ylabel('K')
        plt.show()

    return f(neck)


# ####################
#  Importing database
# ####################

dbpasswd = mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print ("Connecting to primary MARCOS...")
    db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")
    print (db)
except:
    print ("... failed")
    try:
        # Test MARCOS 2
        print ("Connecting to secondary MARCOS...")
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
        print (db)
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
path2log = '/home/cobs/ANALYSIS/Logs/tilt_graph.log'

endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=7),"%Y-%m-%d")
date = datetime.strftime(endtime,"%Y-%m-%d")
starttime2=datetime.strftime(endtime-timedelta(days=7),"%Y-%m-%d")

font = {'family' : 'sans-serif',
        'weight' : 'normal',
        'size'   : 10}


part1=True   # Filtering (not used)
part2=True   # Flagging with earthquake data
part3=True   # Creating graphs
part4=True   # Creating status plots and threshold notification

if part1:
    # Read gravity data and filter it to seconds
    print ("Starting part 1:")
    p1start = datetime.utcnow()
    print ("No filtering required")
    p1end = datetime.utcnow()
    print ("-----------------------------------")
    print ("Part1 needs", p1end-p1start)
    print "-----------------------------------"

if part2:
    print ("Starting part 2:")
    try:
        stb = readDB(db,'QUAKES',starttime=datetime.utcnow()-timedelta(days=7))
        print ("Length", stb.length())
        ## <500(3.0-4.5), 500-3000(4.5-6), 3000-6000(6-7), >6000 (>7)
        stb1 = stb.extract('f',7,'>=')
        stb2 = stb.extract('var5',1000,'>')
        stb2 = stb2.extract('var5',6000,'<=')
        stb2 = stb2.extract('f',7,'<')
        stb2 = stb2.extract('f',5,'>=')
        stb3 = stb.extract('var5',300,'>')
        stb3 = stb3.extract('var5',1000,'<=')
        stb3 = stb3.extract('f',5,'<')
        stb3 = stb3.extract('f',4.5,'>=')
        stb4 = stb.extract('var5',0,'>')
        stb4 = stb4.extract('var5',300,'<=')
        stb4 = stb4.extract('f',4.5,'<')
        stb4 = stb4.extract('f',3.0,'>=')
        try:  # fails if no data is available
            print ("Found streams", stb1.length(), stb2.length(), stb3.length())
            st = appendStreams([stb1,stb2,stb3,stb4])
            if len(st.ndarray[0]) > 0:
                fl = st.stream2flaglist(comment='f,str3',sensorid='GWRSGC025_25_0002', userange=False, keystoflag=['x'])
        except:
            print ("Failed to annotate quakes")
    except:
        failure = True
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print ("  gravity_graph step2 failed ")
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

if part3:
    # Read seconds data and create plots
    print ("Starting part 3:")
    try:
        destinationpath= '/srv/products/data/gravity/SGO/GWRSGC025_25_0002/raw'
        grav = read(os.path.join(destinationpath,'F1*'),starttime=starttime, endtime=endtime)
        res = read(os.path.join(destinationpath,'C1*'),starttime=starttime, endtime=endtime)

        #g0 = readDB(db,'RCSG0_20160114_0001_0001',starttime=starttime2)
        # Light usv
        #g0.header['col-y'] = 'light(usv)'
        #mp.plot(g0)
        #try:
        #    flaglist = g0.bindetector('y',flagnum=0,keystoflag=['x'],sensorid='LM_TILT01_0001',text='tunnel light on')
        #    flaglist2db(db,flaglist)
        #    # Light power
        #    flaglist = g0.bindetector('x',flagnum=0,keystoflag=['x'],sensorid='LM_TILT01_0001',text='tunnel light on')
        #    flaglist2db(db,flaglist)
        #except:
        #    print ("bindetector: ValueError: total size of new array must be unchanged")

        #iwt = readDB(db,'IWT_TILT01_0001_0002',starttime=starttime2)
        #iwt = iwt.filter(filter_width=timedelta(seconds=1))
        #iwt.header['unit-col-x'] = 'i_phase'
        #iwt = iwt.multiply({'x':-1})

        #lm = readDB(db,'LM_TILT01_0001_0001',starttime=starttime2)
        flaglist = db2flaglist(db,'GWRSGC025_25_0002')
        grav = grav.flag(flaglist)
        try:
            grav = grav.flag(fl)
        except:
            print ("Flagging with earthquake data failed")
            pass

        # providing some Info on content
        #print data._get_key_headers()
        #matplotlib.rc('font', **font)

        print grav.length()
        grav.header['col-x'] = ''
        grav.header['col-z'] = ''
        res.header['col-z'] = ''
        grav.header['unit-col-x'] = ''
        grav.header['unit-col-z'] = ''
        res.header['unit-col-z'] = ''
        if grav.length()[0] > 1 and res.length()[0]> 1:
            mp.plotStreams([grav,res],[['x','z'],['z']], gridcolor='#316931',fill=['x'], annotate=[[True, False],[False]],confinex=True, fullday=True, opacity=0.7, plottitle='Gravity variation',noshow=True, labels=[['G [nm/s2]','Pressure [hPa]'],['Residual [nm/s2]']])
        elif grav.length()[0] > 1:
            mp.plotStreams([grav],[['x','z']], gridcolor='#316931',confinex=True, fullday=True,plottitle='Gravity variation' ,noshow=True, labels=[['G [nm/s2]','Pressure [hPa]']])
        else:
            print ("No data available")

        plt.rc('font', **font)

        #upload
        savepath = "/srv/products/graphs/gravity/gravity_%s.png" % date
        plt.savefig(savepath)

        cred = 'cobshomepage'
        address=mpcred.lc(cred,'address')
        user=mpcred.lc(cred,'user')
        passwd=mpcred.lc(cred,'passwd')
        port=mpcred.lc(cred,'port')
        remotepath = 'zamg/images/graphs/gravity/gwr/'

        #ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
        #scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
        # to send with 664 permission use a temporary directory
        tmppath = "/tmp"
        tmpfile= os.path.join(tmppath,os.path.basename(savepath))
        from shutil import copyfile
        copyfile(savepath,tmpfile)
        scptransfer(tmpfile,'94.136.40.103:'+remotepath,passwd)
        os.remove(tmpfile)

    except:
        failure = True
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print ("  gravity_graph step3 failed ")
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

if part4:
    # create statusplot
    # light gravity room
    print ("Starting part 4:")
    #try:
    mr = readDB(db,'RCSM6_20160114_0001_0001',starttime=starttime) # light is x
    print ("MR", mr.length())
    g0 = readDB(db,'RCSG0_20160114_0001_0001',starttime=starttime) # main pow is var1
    print ("G0", g0.length())
    aux = readDB(db,'GWRSGC025A_25_0002_0002',starttime=starttime)
    print ("AUX", aux.length())
    # x (level), z (tide), f (tx), var2 (ty), var4 (neck1), var5 (neck2), dz (He-flow)
    aux.ndarray[KEYLIST.index('var4')] = calibneck(aux.ndarray[KEYLIST.index('var4')])
    aux.header['unit-col-var4'] = 'K'
    aux.ndarray[KEYLIST.index('var5')] = calibneck(aux.ndarray[KEYLIST.index('var5')])
    aux.header['unit-col-var5'] = 'K'
    analysisdict.check({'data_threshold_neck1_GWRSGC025A_25_0002': [float(aux.ndarray[KEYLIST.index('var4')][-1]),'<',13]})
    analysisdict.check({'data_threshold_neck2_GWRSGC025A_25_0002': [float(aux.ndarray[KEYLIST.index('var5')][-1]),'<',78]})
    g0.header['col-var1'] = 'SGO pow'
    mr.header['col-x'] = 'light MR'
    plt.rc('font', **font)
    mp.plotStreams([mr,g0,aux],[['x'],['var1'],['z','f','var2','var4','var5','dz']], gridcolor='#316931', fill=['z','x','var1'], fullday=True, opacity=0.7, plottitle='Gravity status',noshow=True)
    savepath = "/srv/products/graphs/gravity/gravitystats_%s.png" % date
    plt.savefig(savepath)
    cred = 'cobshomepage'
    address=mpcred.lc(cred,'address')
    user=mpcred.lc(cred,'user')
    passwd=mpcred.lc(cred,'passwd')
    port=mpcred.lc(cred,'port')
    remotepath = 'zamg/images/graphs/gravity/gwr/'

    #ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
    #scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
    # to send with 664 permission use a temporary directory
    tmppath = "/tmp"
    tmpfile= os.path.join(tmppath,os.path.basename(savepath))
    from shutil import copyfile
    copyfile(savepath,tmpfile)
    scptransfer(tmpfile,'94.136.40.103:'+remotepath,passwd)
    os.remove(tmpfile)


if not failure:
    analysisdict.check({'script_periodic_gravity_graph': ['success','=','success']})
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
    print ("        gravity_graph successfully finished         ")
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
else:
    analysisdict.check({'script_periodic_gravity_graph': ['failure','=','success']})

