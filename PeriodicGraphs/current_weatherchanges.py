#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 10 12:40:04 2021

@author: N. Kompein


DESCRIPTION
   blablabla

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"

APPLICATION
    PERMANENTLY with cron:
        python weather_products.py -c /etc/marcos/analysis.cfg
    REDO analysis for a time range:
        (startime is defined by endtime - daystodeal as given in the config file
        python weather_products.py -c /etc/marcos/analysis.cfg -e 2020-11-22
    RECREATE archive files:
        python3 weather_products.py -c ~/CONF/wic.cfg -e 2020-05-18 -r 20 -a



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

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from numpy import nansum, nanstd, shape#, abs, diff, median
from scipy.signal import gaussian

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
from version import __version__



def weather_change(db,config={},starttime=datetime.utcnow()-timedelta(days=3),endtime=datetime.utcnow(), debug=False):

    # Get configuration data
    meteoproductpath = config.get('meteoproducts')
    imagepath = config.get('meteoimages')
    meteofilename = 'meteo-1min_'
    succ = True

    #source = 'database'  # access database
    source = 'archive'  # access dataarchive using sgopath

    filedate = datetime.strftime(endtime,"%Y-%m-%d")

    # variables: fd, meteofilename, name

    if debug:
        print ("Part 1 - short term weatherchange plot - started at {}".format(datetime.utcnow()))
        print ("-------------------------------------------------")

    if source=='database':
        print( 'Reading Meteo data from database...\nStarttime is: {}\nEndtime: is: {}'.format( starttime,endtime))
        result = readDB(db, 'METEOSGO_adjusted_0001_0001', starttime = starttime, endtime = endtime)
    else:
        print( 'Reading Meteo data from {}...\nStarttime is: {}\nEndtime: is: {}'.format( os.path.join( meteoproductpath, meteofilename + '*'), starttime, endtime))
        result = read( os.path.join( meteoproductpath, meteofilename + '*'), starttime = starttime, endtime = endtime)
        
    if result.length()[0] > 0:
        print( '...done - obtained {} datapoints'.format(result.length()[0]))
    else:
        print( '...NO DATA FOUND, ABORTING!')
        return False

    try:
        plt.rcParams.update({'font.size': 12}) # SETTING GENERAL FONTSIZE FOR PLOTS
        # ############################
        # # Create plots ???? -> move to plot
        # ############################
        print( 'Interpolating missing values...')
        result.ndarray[2] = result.missingvalue(result.ndarray[2],3600,threshold=0.05,fill='interpolate')
        result.ndarray[3] = result.missingvalue(result.ndarray[3],600,threshold=0.05,fill='interpolate')
        print( '...done')
        result = result.trim(starttime = starttime, endtime = endtime)

        tind = KEYLIST.index( 'time')
        t = result.ndarray[tind].astype(float64)
        if debug:
            print( 't is: ', t)
        weekdayst = [ el.strftime('%A-%H:%M') for el in num2date( t)]
        if debug:
            print( 'weekdayst is: {}'.format( weekdayst))
            print( '(t[-1] - t[0]).total_seconds() is: {}'.format((t[-1] - t[0])*86400.0))
        duration = (num2date(t[-1]) - num2date(t[0])).total_seconds()
        if debug:
            print( 'duration is: {}'.format( duration))
        dt = duration/ ( float( len( t)) - 1.0)
        if debug:
            print( 'dt is: {} seconds'.format(dt))
        winsec = 1800.0
        winlen = int( winsec/ dt)
        if( winlen == 0):
            winlen = 1
        if debug:
            print( 'window length therefore is: {} samples'.format(winlen))
        indices = [KEYLIST.index('y'), KEYLIST.index('z'), KEYLIST.index('f'), KEYLIST.index('var1')]
        if debug:
            print('indices is: {}'.format( indices))
        """
        EVALUATE SMOOTHED DATASET FOR PLOTS - SMOOTHING WINDOW LENGTH IS winlen with winsec SECONDS - SMOOTHING WINDOW WITH RUNNING GAUSSIAN WINDOW
        """
        print( 'Running gaussian taper over {} samples...'.format( winlen))
        ndata = []
        stdscalfc = 3.0 # FACTOR FOR SCALING THE SLOPE OF GAUSSIAN WINDOW
        dump = ( array( list( chain( result.ndarray[indices])))).reshape(len( indices),-1).astype(float64)
        if debug:
            print('dump is: {}'.format( dump))
        for l, data in enumerate( dump[1:,:]):
            window = gaussian( winlen, std = max( [stdscalfc *nanstd( data), 1.0/ float(winlen)]))
            ndata.append( [nansum( data[k:k + winlen:] * window)/ nansum( window) for k in range(0,len(data) - winlen)])
        if debug:
            print( 'ndata is: {}'.format( ndata))
        print( '...done')
        d3 = array( ndata[0])
        d4 = array( ndata[1])
        d7 = array( ndata[2])
        del ndata

        d3 = d3 - d3[0]
        d4 = d4# - d4[0]
        d7 = d7 * 3.6# - d7[0]
        t = t[:len( d3):] # IF CHANGES IN LENGTH ARE IMPORTANT
        
        y2 = dump[0,:len( d3)]
        y3 = dump[1,:len( d3)]
        y4 = dump[2,:len( d3)]
        y7 = dump[3,:len( d3)]
        
        """
        DERIVING TIMESERIES' MINUS FIRST SAMPLE IN TIMERANGE TO IMPROVE VISIBILITY OF CHANGES IN EACH TIMESERIES
        """
        
        if debug:
            print( 'shape of t: {}\nshape of y2: {}\nshape of d3: {}\nshape of d4: {}\nshape of d7: {}'.format( shape( t), shape( y2), shape( d3), shape( d4), shape(d7)))

        max1a = 0
        max1b = 0
        max2a = 0

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
        fig, axarr = plt.subplots(3, sharex=True, figsize=(15,9), dpi=100) #dpi=200)
        displyvar = d4#/ dt

        axarr[0].set_ylabel('TEMP [$ \circ$C]')
        axarr[0].plot_date( t, displyvar,'-',color='lightgray')
        axarr[0].fill_between(t,0, displyvar,where=displyvar<0,facecolor='blue',alpha=0.5)
        axarr[0].fill_between(t,0, displyvar,where=displyvar>=0,facecolor='red',alpha=0.5)
        axarr[0].set_title( 'Weatherchanges since: ' + num2date(t[0]).strftime(' %Y-%m-%d'))
        print( 'Temperature plot successful')
        displyvar = d3#/ dt

        axarr[1].set_ylabel('SNOW [cm]')
        axarr[1].plot_date(t,displyvar,'-',color='gray')
        axarr[1].fill_between(t,0,displyvar,where=displyvar>=0,facecolor='cyan',alpha=0.5)
        axarr[1].fill_between(t,0,displyvar,where=displyvar<0,facecolor='magenta',alpha=0.5)
        print( 'Snowheight plot successful')

        ax1 = axarr[1].twinx()
        ax1.set_ylabel('N [mm/h]',color='blue')
        ax1.plot_date(t,y2,'-',color='blue')
        ax1.fill_between(t,0,y2,where=y2>=0,facecolor='blue',alpha=0.5)
        print( 'Rainfall plot successful')

        displyvar = d7#/ dt

        axarr[2].set_ylabel('WIND [km/h]')
        axarr[2].plot_date(t,displyvar,'-',color='gray')
        axarr[2].fill_between(t,0,displyvar,where=displyvar>=2.0*nanstd( d7),facecolor='orange',alpha=0.5)
        axarr[2].fill_between(t,0,displyvar,where=displyvar<2.0*nanstd( d7),facecolor='green',alpha=0.5)
        print( 'Windspeed plot successful')

        for ax, avg in zip( axarr, [ avgy4, avgy3, avgy7]):
            ax.grid(which='both')
            maxchg = (ax.get_ylim()[1] - ax.get_ylim()[0])*0.1
            ax.text(t[10],ax.get_ylim()[1] - maxchg, avg + '+')
            ax.set_ylim([ax.get_ylim()[0] - maxchg, ax.get_ylim()[1] + maxchg])
        ax.xaxis.set_major_formatter( DateFormatter('%A\n%H-%M') )
        ax1.set_ylim([ min( [axarr[1].get_ylim()[0], 0]), max( [axarr[1].get_ylim()[1],max1a])]) # SETTING LIMITS FOR TWINPLOT AFTER RESETING ALL OTHER PLOTS
        print( 'Setting axes limits successful. Placing average valus as text in plot successful.')
        savepath = os.path.join(imagepath,'MeteoChange_0_'+filedate+'.png')
        if debug:
            plt.show()
        else:
            plt.savefig( savepath)
            print( 'File {} saved.'.format( savepath))
    except:
        print ("failed")
        succ = False

    return succ


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
        print ('current_weatherchanges.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- current_weatherchanges.py will determine the primary instruments --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python weather_products.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-e            : endtime - default is now')
            print ('-s            : starttime -  default is three days from now')
            print ('-------------------------------------')
            print ('Application:')
            print ('python current_weatherchanges.py -c /etc/marcos/analysis.cfg')
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
