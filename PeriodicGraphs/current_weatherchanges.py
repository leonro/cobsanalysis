#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 11:26:04 2019

@author: N. Kompein

Code for creation of weather-change plot based on current_weather.py from sagitarius 2019-01-29 "if product1 and product3"

MagPy - RCS Analysis 
Analyze Weather measurements from different sources
Sources are RCST7, LNM, ULTRASONIC, PRESSURE 
Provides:
- General minute METEO date for plots and distribution 
- Provide some specific combinantions for certain projects
- Analysis checks of data validity in source data
"""
from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred
import json, os
from numpy import array
from itertools import chain

"""
# ################################################
#             Logging
# ################################################
"""

## New Logging features 
from martas import martaslog as ml
logpath = '/var/log/magpy/mm-dp-weatherchange.log'
#import socket
#sn = socket.gethostname().upper()
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
name = "{}-Periodicgraphs-weatherchange".format(sn)
"""
try: 
    from magpy.opt.analysismonitor import *
    analysisdict = Analysismonitor(logfile='/home/cobs/ANALYSIS/Logs/AnalysisMonitor_cobs.log')
    # next two line only necessary for first run
    #analysisdict['data_threshold_rain_RCST7_20160114_0001'] = [0.0,'<',0.3]
    #analysisdict.save('/home/cobs/ANALYSIS/Logs/AnalysisMonitor.pkl')
    analysisdict = analysisdict.load()
except:
    print ("Analysis monitor failed")
    pass
"""


"""
# ################################################
#             General configurations
# ################################################
"""

## STEPS
## -------
product1 = True  # creates one minute cumulative meteo file, one second bm35 file, and data table
product2 = True  # table inputs
product3 = True  # creates short term plots
product4 = True  # creates long term plots
product5 = True  # upload table to broker - change that 
product6 = True  # current weatherchange plot

## CONFIG
## -------
submit = True ## True: data should be be uploaded
showplots = False  ## True: all plots should be shown (plt.show())
cleanupRCSflags = False  ## True: delete duplicate flags from RCS T7 record
writefile = True ## True: data will be written in files
meteofilename = 'meteo-1min_'
currentvaluepath = '/srv/products/data/current.data'

## Definitions for part 1
## -------
# Define time frame  - for past data analysis use: starttime='2012-10-12'
dayrange = 3
endtime = datetime.utcnow()
starttime = endtime-timedelta(days=dayrange)
#starttime = datetime(2018,1,1)
#endtime = datetime(2018,3,1)
source = 'database'  # access database
#source = 'archive'  # access dataarchive using sgopath
#add2db = True #False

## PATHS
## -------
remotepath = 'zamg/images/graphs/meteorology'
remotedatapath = 'zamg/images/data'
imagepath = '/srv/products/graphs/meteo'
meteoproductpath = '/srv/products/data/meteo'
datapath = '/srv/products/data/meteo'
path2log = '/home/cobs/ANALYSIS/Logs/transfer_weather.log'
cred = 'cobshomepage'
brokercred = 'broker'
sgopath = '/srv/archive/SGO'

dbdateformat = "%Y-%m-%d %H:%M:%S.%f"

"""
# ################################################
#             Connect database
# ################################################
"""

dbpasswd=mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print ("Connecting to primary MARCOS...")
    db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")
    print ("... success")
except:
    print ("... failed")
    try:
        # Test MARCOS 2
        print ("Connecting to secondary MARCOS...")
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
        print ("... success")
    except:
        # Overall availablilty of data bases is logged in filter.py
        print ("... failed -- aborting")
        sys.exit()


"""
# ################################################
#             SYNOP Definitions
# ################################################
"""

synopdict = {"-1":"Sensorfehler",
                 "41":"Leichter bis maessiger Niederschlag (nicht identifiziert)",
                 "42":"Starker Niederschlag (nicht identifiziert, unbekannt)",
                 "00":"Kein Niederschlag",
                 "51":"Leichter Niesel",
                 "52":"Maessiger Niesel",
                 "53":"Starker Niesel",
                 "57":"Leichter Niesel mit Regen",
                 "58":"Maessiger bis starker Niesel mit Regen",
                 "61":"Leichter Regen",
                 "62":"Maessiger Regen",
                 "63":"Starker Regen",
                 "67":"Leichter Regen",
                 "68":"Maessiger bis starker Regen",
                 "77":"Schneegriesel",
                 "71":"Leichter Schneefall",
                 "72":"Maessiger Schneefall",
                 "73":"Starker Schneefall",
                 "74":"Leichte Graupel",
                 "75":"Maessige Graupel",
                 "76":"Starke Graupel",
                 "89":"Hagel"}
trans=''

"""
# ################################################
#             Time, Range and Credentials
# ################################################
"""

fd = datetime.utcnow # ABBREVATE actual timestamp request

e = fd() + timedelta(days=1)
highb = e - timedelta(days=1)
mb = e - timedelta(days=3)
hb = e - timedelta(days=365)
filedate = datetime.strftime(fd(),"%Y-%m-%d")
fileyear = datetime.strftime(fd(),"%Y")


# Getting credentials for homepage upload
address=mpcred.lc(cred,'address')
user=mpcred.lc(cred,'user')
passwd=mpcred.lc(cred,'passwd')
port=mpcred.lc(cred,'port')

# Getting credentials for cobs1 upload
brokeraddress=mpcred.lc(brokercred,'address')
brokeruser=mpcred.lc(brokercred,'user')
brokerpasswd=mpcred.lc(brokercred,'passwd')
brokerport=mpcred.lc(brokercred,'port')


def upload2homepage(source,destination,passwd,name):
    """
    Upload data to homepage by using 644 permissions
    """
#   try:
    # to send with 664 permission use a temporary directory
    tmppath = "/tmp"
    tmpfile= os.path.join(tmppath,os.path.basename(source))
    from shutil import copyfile
    copyfile(source,tmpfile)
    scptransfer(tmpfile,'94.136.40.103:'+destination,passwd,timeout=60)
    os.remove(tmpfile)
    #analysisname = 'upload_homepage_{}'.format(name)
    #analysisdict.check({analysisname: ['success','=','success']})
#    except:
    #analysisdict.check({analysisname: ['failure','=','success']})
#   print( 'Upload incomplete!')


"""
# ################################################
#             part 1
# ################################################
 plot weather changes in a automatically scaled plot.
 write first sample value of each timeseries as string in upper left corner and remove this bias from according graph.
"""
windsens = ''
lnmsens = ''

if product6:
    print ("Part 1 - short term weatherchange plot - started at {}".format(fd()))
    print ("-------------------------------------------------")
    name1 = name+'-1'
    try:
        print( 'Reading Meteo data from {}...\nStarttime is: {}\nEndtime: is: {}'.format( os.path.join( meteoproductpath, meteofilename + '*'), starttime, endtime))
        result = read( os.path.join( meteoproductpath, meteofilename + '*'), starttime = starttime, endtime = endtime)
        #print ('result is: {}'.format( result))
        print( '...done')
    except:
        print( '...NO DATA FOUND, ABORTING!')
        sys.exit()
    try:
        #import pylab # COMMENTED OUT DUE TO NEARLY NO MAINTENANCE REPLACEMENT IS matplotlib.pyplot
        import matplotlib.pyplot as plt
	from matplotlib.dates import DateFormatter
        from numpy import nansum, nanstd, shape#, abs, diff, median
        from scipy.signal import gaussian
        
        plt.rcParams.update({'font.size': 12}) # SETTING GENERAL FONTSIZE FOR PLOTS
        # ############################
        # # Create plots ???? -> move to plot
        # ############################
        print( 'Interpolating missing values...')
        result.ndarray[2] = result.missingvalue(result.ndarray[2],3600,threshold=0.05,fill='interpolate')
        result.ndarray[3] = result.missingvalue(result.ndarray[3],600,threshold=0.05,fill='interpolate')
        print( '...done')
        #longextract = result._select_timerange(starttime=starttime, endtime=endtime)
	result = result.trim(starttime = starttime, endtime = endtime)
        #for ind,elem in enumerate(result.ndarray):
        #    if len(elem) > 0 & KEYLIST[ind] == 'str1':
        #        synop = elem[-1]

        #transtmp = synopdict.get(str(synop))
	#print ("Test", result.ndarray[:])
	tind = KEYLIST.index( 'time')
        t = result.ndarray[tind].astype(float64)
	#print( 't is: ', t)
        weekdayst = [ el.strftime('%A-%H:%M') for el in num2date( t)]
        #print( 'weekdayst is: {}'.format( weekdayst))
        #print( '(t[-1] - t[0]).total_seconds() is: {}'.format((t[-1] - t[0])*86400.0))
	duration = (num2date(t[-1]) - num2date(t[0])).total_seconds()
	print( 'duration is: {}'.format( duration))
        #dt = ((t[-1] - t[0])*86400.0)/ ( float( len( t)) - 1.0)
	dt = duration/ ( float( len( t)) - 1.0)
        print( 'dt is: {} seconds'.format(dt))
        winsec = 1800.0
        winlen = int( winsec/ dt)
        if( winlen == 0):
            winlen = 1
        #print( 'window length therefore is: {} samples'.format(winlen))
        indices = [KEYLIST.index('y'), KEYLIST.index('z'), KEYLIST.index('f'), KEYLIST.index('var1')]
        print('indices is: {}'.format( indices))
        """
        EVALUATE SMOOTHED DATASET FOR PLOTS - SMOOTHING WINDOW LENGTH IS winlen with winsec SECONDS - SMOOTHING WINDOW WITH RUNNING GAUSSIAN WINDOW
        """
        print( 'Running gaussian taper over {} samples...'.format( winlen))
        ndata = []
        stdscalfc = 3.0 # FACTOR FOR SCALING THE SLOPE OF GAUSSIAN WINDOW
        #print( 'longextract is: '.format( longextract))
	#print( 'longextract.ndarray[indices] is: {}'.format(result.ndarray[indices]))
        dump = ( array( list( chain( result.ndarray[indices])))).reshape(len( indices),-1).astype(float64)
	#dump = list( chain( result.ndarray[indices])).reshape(len( indices),-1)
        #y4 = longextract.ndarray[4].astype(float64)
        #y7 = longextract.ndarray[7].astype(float64)
        #dump = np.array( (y3, y4, y7))
        #print('dump is: {}'.format( dump))
        for l, data in enumerate( dump[1:,:]):
            #temp = diff( data)
            #print( 'running index is: {}'.format( l))
	    #print( 'data is: {}'.format( data))
	    #print( 'nanstd( data) is: {}'.format( nanstd( data)))
            window = gaussian( winlen, std = max( [stdscalfc *nanstd( data), 1.0/ float(winlen)]))
            #print( 'nanstd of window is: {}'.format( nanstd( window)))
            #print( 'nanstd of data is: {}'.format( nanstd( data)))
            ndata.append( [nansum( data[k:k + winlen:] * window)/ nansum( window) for k in range(0,len(data) - winlen)])
            #print( 'nanstd of ndata[{}] is: {}'.format(  l, nanstd(ndata[l])))
        #print( 'ndata is: {}'.format( ndata))
        print( '...done')
        #t = longextract[0][:-winlen:]
        d3 = array( ndata[0])
        d4 = array( ndata[1])
        d7 = array( ndata[2])
        del ndata

        d3 = d3 - d3[0]
        d4 = d4# - d4[0]
        d7 = d7 * 3.6# - d7[0]
        #t = weekdayst
        t = t[:len( d3):] # IF CHANGES IN LENGTH ARE IMPORTANT

        y2 = dump[0,:len( d3)]
        y3 = dump[1,:len( d3)]
        y4 = dump[2,:len( d3)]
        y7 = dump[3,:len( d3)]
        
        """
        DERIVING TIMESERIES' MINUS FIRST SAMPLE IN TIMERANGE TO IMPROVE VISIBILITY OF CHANGES IN EACH TIMESERIES
        """
        
        #print d3, d4, d7
        
        #print( 'shape of t: {}\nshape of y2: {}\nshape of y3: {}\nshape of y4: {}\nshape of y7: {}'.format( shape( t), shape( y2), shape( y3), shape (y4), shape(y7)))
	#print( 'shape of t: {}\nshape of y2: {}\nshape of d3: {}\nshape of d4: {}\nshape of d7: {}'.format( shape( t), shape( y2), shape( d3), shape( d4), shape(d7)))
        max1a = 0
        max1b = 0
        max2a = 0

        #print (len(t), len(y2), len(y3), len(y4), len(y7))

        # YLIM MAX OF RAINFALL
        if len(y2) > 0:
            max1a = np.nanmax(y2)
        if np.isnan(max1a):
            max1a = 10
        # YLIM MAX OF SNOWHEIGHT
        if len(y3) > 0:
            displyvar = d3#/ dt
            max1b = np.nanmax(displyvar)
            min1b = np.nanmin(displyvar)
            avgy3 = 'snowheight 3days ago: ' + str( int( y3[0])) + '[cm]'
	    print( 'std( snowheight) is: {}'.format( nanstd( displyvar)))
        if np.isnan(max1b) or np.isnan(min1b):
            max1b = 5
            min1b = -5
        # YLIM MAX OF TEMPERATURE
        if len(y4) > 0:
            displyvar = d4#/ dt
            max1c = np.nanmax(displyvar)
            min1c = np.nanmin(displyvar)
            avgy4 = 'average temperature: ' + str( int( median( y4))) + '[$ \circ$C]'
            print( 'std( temperature) is: {}'.format( nanstd( displyvar)))
        if np.isnan(max1c) or np.isnan(min1c):
            max1c = 5
            min1c = -5
        # YLIM MAX OF WINDSPEED
        if len(y7) > 0:
            displyvar = d7#/ dt
            max2a = np.nanmax(displyvar)
            min2a = np.nanmin(displyvar)
            avgy7 = 'average wind: ' + str( int( median( d7))) + '[km/h]'
            print( 'std( wind) is: {}'.format( nanstd( displyvar)))
        if np.isnan(max2a) or np.isnan(min2a):
            max2a = 5
            min2a = -5
        print( 'limits succesfully derived')
        fig, axarr = plt.subplots(3, sharex=True, figsize=(15,9), dpi=200)
        # first plot (temperature)
        displyvar = d4#/ dt
        
        axarr[0].set_ylabel('TEMP [$ \circ$C]')
        #axarr[0].set_ylim([min1c,max1c])
	#print(' t is: {}\n displyvar is: {}'.format( ( t), ( displyvar)))
        axarr[0].plot_date( t, displyvar,'-',color='lightgray')
        axarr[0].fill_between(t,0, displyvar,where=displyvar<0,facecolor='blue',alpha=0.5)
        axarr[0].fill_between(t,0, displyvar,where=displyvar>=0,facecolor='red',alpha=0.5)
	axarr[0].set_title( 'Weatherchanges since: ' + num2date(t[0]).strftime(' %Y-%m-%d'))
        print( 'Temperature plot successful')
        #ax0 = axarr[0].twinx()
        #ax0.set_ylim([0,100])
        #ax0.set_ylabel('RH [%]')
        #ax0.plot_date(longextract[0],longextract[5],'-',color='green')
        displyvar = d3#/ dt
        
        axarr[1].set_ylabel('SNOW [cm]')
        #axarr[1].set_ylim([min1b,max1b])
        axarr[1].plot_date(t,displyvar,'-',color='gray')
        axarr[1].fill_between(t,0,displyvar,where=displyvar>=0,facecolor='cyan',alpha=0.5)
        axarr[1].fill_between(t,0,displyvar,where=displyvar<0,facecolor='magenta',alpha=0.5)
        print( 'Snowheight plot successful')
        
        
        ax1 = axarr[1].twinx()
        ax1.set_ylabel('N [mm/h]',color='blue')
        #ax1.set_ylim([ min( [axarr[1].get_ylim()[0], 0]), max( [axarr[1].get_ylim()[1],max1a])]) # DONE LATER
        ax1.plot_date(t,y2,'-',color='blue')
        ax1.fill_between(t,0,y2,where=y2>=0,facecolor='blue',alpha=0.5)
        print( 'Rainfall plot successful')
        
        displyvar = d7#/ dt
        
        axarr[2].set_ylabel('WIND [km/h]')
        #axarr[2].set_ylim([min2a,max2a])
        axarr[2].plot_date(t,displyvar,'-',color='gray')
        axarr[2].fill_between(t,0,displyvar,where=displyvar>=2.0*nanstd( d7),facecolor='orange',alpha=0.5)
        axarr[2].fill_between(t,0,displyvar,where=displyvar<2.0*nanstd( d7),facecolor='green',alpha=0.5)
        print( 'Windspeed plot successful')
        
        for ax, avg in zip( axarr, [ avgy4, avgy3, avgy7]):
            ax.grid(which='both')
	    maxchg = (ax.get_ylim()[1] - ax.get_ylim()[0])*0.1
            ax.text(t[10],ax.get_ylim()[1] - maxchg, avg + '+')
	    #if( ax == axarr[1]):
	        #ax.text(t[10],ax.get_ylim()[1] - 3.0 * maxchg, transtmp)
            ax.set_ylim([ax.get_ylim()[0] - maxchg, ax.get_ylim()[1] + maxchg])
	    ax.xaxis.set_major_formatter( DateFormatter('%A\n%H-%M') )
        ax1.set_ylim([ min( [axarr[1].get_ylim()[0], 0]), max( [axarr[1].get_ylim()[1],max1a])]) # SETTING LIMITS FOR TWINPLOT AFTER RESETING ALL OTHER PLOTS
        print( 'Setting axes limits successful. Placing average valus as text in plot successful.')
        #axarr[0].set_xlim([ t[0], t[-1]])
        savepath = os.path.join(imagepath,'MeteoChange_0_'+filedate+'.png')
        #pylab.savefig(savepath)
        plt.savefig( savepath)
        print( 'File {} saved.'.format( savepath))
        if showplots:
            plt.show()
        statusmsg[name1] = 'Step1: three day weatherchange plot finished'
        if submit:
            #ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
            #scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
            try:
                upload2homepage(savepath,remotepath,passwd,'meteochange0_graph')
                #statusmsg[name1] = 'Step1: three day weatherchange plot finished'
                print("Short term weatherchange plot - finished at {}". format( fd()))
            except:
                print ("Step 1 failed")
                print ('Step1: three day weatherchange plot failed - upload failed')
                #statusmsg[name1] = 'Step1: three day weatherchange plot failed - upload failed'
        else:
            #statusmsg[name1] = 'Step1: three day weatherchange plot finished'
            print("Short term weatherchange plot - finished at {}". format( fd()))
    except:
        print ("Step 1 failed")
        statusmsg[name1] = 'Step1: three day weatherchange plot failed - plot generation failed'
    print( 'Statusmsg are: {}'.format( statusmsg))

martaslog = ml(logfile=logpath,receiver='telegram') # receiver can be "telegram", "email" or "logfile"
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf' # config file with channel info
martaslog.msg(statusmsg) # upload of message to messenger


