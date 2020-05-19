#!/usr/bin/env python
#********************************************************************
# This is the main program for data analysis of Sparkling Geomagnetic
# Field data. Runs once per day and produces output in day files
# and plots.
#
# FUNCTIONS:
# 1. Download raw data files
#	- Save locally:
#		...	/srv/archive/magnetism/CODE/INST/raw
#	- Save to ftp server:
#		...	www.zamg.ac.at
# 2. Filter data
# 3. Do baseline correction
# 4. Plot all data
#	- Save locally:
#		...	/srv/archive/magnetism/CODE/INST/plots
# 5. Save data in IAGA-2002 min files
#	- Save locally:
#		...	/srv/archive/magnetism/CODE/INST/filtered
# 6. Upload files
#
# Adapted from tse_fetch.py script by RLB on 2014-04-15.
# Updated to work on Vega 2016-02-29.
#
#********************************************************************

import os, urllib, sys
import logging
from magpy.stream import *
from magpy.transfer import * 
import magpy.mpplot as mp
import magpy.opt.cred as mpcred

#--------------------------------------------------------------------
# I. Preamble: General
#--------------------------------------------------------------------

#############
TSE = True  
IPL = False 
GQU = False 
#############

basepath = 	'/srv/archive'

IAGA_headers = ['StationInstitution','StationName','StationIAGAcode',
		'DataAcquisitionLatitude','DataAcquisitionLongitude',
		'DataElevation','DataSensorOrientation']

logging.basicConfig(level = logging.INFO)
formatter = 	logging.Formatter('%(asctime)s [%(name)s] %(levelname)s - %(message)s')

def process_data(station, sensortype, sensors, basepath, fileext, date, plotvariables,**kwargs):
    '''
    DEFINITION:
        STANDARD: Reads data, writes prelim data, plots & writes final data 
	OPTIONAL: Baseline & declination correct, multiply stream, upload, 
	produce plot of last 7 days.
	NOTE: Files must be saved under proper data format:
	--> /BASEPATH/TO/DATA/IAGA-ST-CODE/INSTRUMENT...
	(e.g: /srv/archive/magnetism/tse/lemi...)
	.../raw
	.../plots
	.../filtered
	Must also have file with header information under BASEPATH/CODE/CODE_headers.txt.

    PARAMETERS:
    Variables:
        - station: 		(str) IAGA-code for station
	- sensortype:		(str) 'combined' or 'normal'
				combined = only for magnetic variometer + magnetometer
				normal = all other data
	- sensors:		(str/list) Sensor name, 'ENV05_1_0001'
				Note: for 'combined' this is a list. ['LEMI025_22_0001', 'POS1_N432_0001']
	- basepath:		(str) Path to where data is stored.
	- fileext:		(str/list) File extension of data file, e.g. 'bin', 'cdf', 'txt'
				Note: for 'combined' this is a list. ['bin', 'cdf']
	- date:			(str) Date of data in format %Y-%m-%d / YYYY-MM-DD.
        - plotvariables: 	(list) List of magpy keys to plot.
    Kwargs:
	- logger:		(logging.Logger object) Logger for logging purposes.
	- decl:			(float) Will rotate data by this value, if given
	- mult_factors:		(dict) Will multiply corresponding stream keys with these factors
	- baseline:		(dict) Will baseline correct corresponding keys with these factors
	- upload:		(bool) If True, will upload files
	- sevendayplot:		(bool) If True, will produce plot of last 7 days
	- prelim:		(bool) If True, will save prelim file

    RETURNS:
        - True / False

    EXAMPLE:
        >>> 

    APPLICATION:

    '''
    decl = kwargs.get('decl')
    mult_factors = kwargs.get('mult_factors')
    baseline = kwargs.get('baseline')
    upload = kwargs.get('upload')
    prelim = kwargs.get('prelim')
    sevendayplot = kwargs.get('sevendayplot')
    logger = kwargs.get('logger')

    if not logger:
        logging.basicConfig(level=logging.INFO)
	logger = logging.getLogger(' %s ' % station.upper())

    header_data = {}
    headersfile = os.path.join(basepath,station,'%s_headers.txt' % station)
    headers = open(headersfile, 'r')
    for line in headers:
        hdata = line.split()
        header_data[hdata[0].strip(':')] = hdata[1]

#--------------------------------------------------------------------
# 1. READ DATA, REMOVE OUTLIERS, FILTER

    if sensortype == 'combined':
        vario_sensor = sensors[0]
        magn_sensor = sensors[1]
        v_datafile = '%s_%s.%s' % (vario_sensor, date, fileext[0])
        m_datafile = '%s_%s.%s' % (magn_sensor, date, fileext[1])
        v_datapath = os.path.join(basepath,station,vario_sensor,'raw',v_datafile)
        m_datapath = os.path.join(basepath,station,magn_sensor,'raw',m_datafile)
	logger.info("Reading files %s and %s..." % (v_datafile,m_datafile))
        v_stream = read(v_datapath)
        m_stream = read(m_datapath)
        #v_stream.remove_outlier()
        #v_stream.remove_flagged()
        #m_stream.remove_outlier()
        #m_stream.remove_flagged()
        v_stream = v_stream.filter()
        m_stream = m_stream.filter()
        stream = mergeStreams(v_stream,m_stream)
        stream.header['col-f'] = 'F'
        stream.header['unit-col-f'] = 'nT'
        sensor = vario_sensor
	title = '%s-%s' % (vario_sensor, magn_sensor)

    elif sensortype == 'normal':
	sensor = sensors
        datafile = '%s_%s.%s' % (sensor, date, fileext)
	logger.info("Reading file %s..." % datafile)
        datapath = os.path.join(basepath,station,sensor,'raw',datafile)
        stream = read(datapath)
        #stream.remove_outlier()
        #stream.remove_flagged()
        stream = stream.filter()
	title = sensor

    else:
	logger.error("Wrong sensortype (%s). Options are 'combined' and 'normal'." % sensortype)


    for data_header in IAGA_headers:
        stream.header[data_header] = header_data[data_header]

    if sensor[:3].lower() == 'lem':
        stream.header['DataType'] = 'Magnetic'
        stream.header['DataComponents'] = 'x, y, z, F [nT]'
        stream.header['DataDigitalSampling'] = '0.1s, 5s'
        dx = 1000.* stream.header['DataCompensationX']
        dy = 1000.* stream.header['DataCompensationY']
        dz = 1000.* stream.header['DataCompensationZ']
        stream.header['DataSensorOrientation'] = "%s, %s, %s" % (dx,dy,dz)
    elif sensor[:3].lower() == 'env':
        stream.header['DataType'] = 'Environmental'
        stream.header['DataComponents'] = 'T (ambient) [C], RH [%], T (dewpoint) [C]'
        stream.header['DataDigitalSampling'] = '1s'
        stream._move_column(plotvariables[0],'x')
        stream._move_column(plotvariables[1],'y')
        stream._move_column(plotvariables[2],'z')
	plotvariables = ['x','y','z']
    elif sensor[:3].lower() == 'cs1':
        stream.header['DataType'] = 'Magnetic'
        stream.header['DataComponents'] = 'F [nT]'
        stream.header['DataDigitalSampling'] = '1s'

    filenamebegins = '%s_0002' % (title)
    #filenamebegins = '%s_%s_' % (station,title)

    if prelim:
	prelim_path = os.path.join(basepath,station,sensor,'prelim')
	stream.write(prelim_path,filenamebegins=filenamebegins+'_',format_type='IAGA')
	logger.info("Preliminary data written to %s." % prelim_path)

#--------------------------------------------------------------------
# 2. (OPTIONAL) ROTATE, MULTIPLY, BASELINE CORRECT
#    Steps for PRELIMINARY --> FINAL

    if decl:
        stream.rotation(alpha=decl)

    if mult_factors:
        stream.multiply(mult_factors)

    if baseline:
        stream.offset(baseline)

#--------------------------------------------------------------------
# 3. PLOT

    sensorpadding =   {'env': 0.5,
		       'pos': 10,
		       'lem': 5,
		       'cs1': 10}

    plotname = '%s_%s.png' % (filenamebegins,date)
    outfile = os.path.join(basepath,station,sensor,'plots',plotname)
    mp.plot(stream, plotvariables,
		plottitle = '%s %s (%s)' % (station.upper(),title,date),
		bgcolor = 'white',
		confinex = True,
		fullday = True,
		outfile = outfile,
		padding = sensorpadding[sensor[:3].lower()])

    logger.info("Data plotted to %s." % outfile)

#--------------------------------------------------------------------
# 4. SAVE & WRITE STREAM TO MINUTE FILE

    #filenamebegins = '%s_%s_' % (station,title)
    finalpath = os.path.join(basepath,station,sensor,filenamebegins)
    stream.write(finalpath,filenamebegins=filenamebegins+'_',format_type='IAGA')

    logger.info("Final data written to %s." % finalpath)

#--------------------------------------------------------------------
# 5. UPLOAD (plot + filtered data)

    cred = 'cobshomepage'
    myproxy = mpcred.lc(cred,'address')
    login = mpcred.lc(cred,'user')
    passwd = mpcred.lc(cred,'passwd')
    #passwd = 'ku7tag8!haus' # TODO CHANGE THIS BACK
    port = mpcred.lc(cred,'port')
    ftppath = 	'cmsjoomla/images/stories/currentdata/'

    upload=False
    if upload:
	try:
            filtered_file = '%s_%s.txt' % (filenamebegins,date)
            filtered_path = os.path.join(basepath,station,sensor,filenamebegins,filtered_file)
            logger.info("Uploading %s..." % filtered_path)
            ftpdatatransfer(localfile=filtered_path, 
		ftppath=ftppath, 	# TODO 
		myproxy=myproxy, port=port, 
		login=login, passwd=passwd, raiseerror=True,
		logfile=os.path.join(basepath,station,'%s-transfer.log' % station))
	except:
	    logger.error("Uploading failed.")

	try:
            plot_file = '%s_%s.png' % (filenamebegins,date)
            plot_path = os.path.join(basepath,station,title,'plots',plotfile)
	    logger.info("Uploading %s..." % plot_path)
            ftpdatatransfer(localfile=plotpath, 
		ftppath=ftppath,	# TODO 
		myproxy=myproxy, port=port, 
		login=login, passwd=passwd,  raiseerror=True,
		logfile=os.path.join(basepath,station,'%s-transfer.log' % station))
	except:
            logger.error("Uploading failed.")

#--------------------------------------------------------------------
# 6. CREATE 7-DAY PLOT (x, y, z, F) & UPLOAD

    if sevendayplot:
        today = datetime.utcnow()
        date = datetime.strftime(today, "%Y-%m-%d")
        datapath = os.path.join(basepath,station,sensor,filenamebegins,'*')
        startdate = datetime.strptime(date,'%Y-%m-%d') - timedelta(days=7)
        start = datetime.strftime(startdate, "%Y-%m-%d")+' 00:00:00'
        end = date+' 00:00:00'

        last7days = read(path_or_url=datapath,starttime=start,endtime=end)
        plotname = 'TSE_last7days.png'
        plotpath = os.path.join(basepath,station,'7dayplots',plotname)
        diff = eval(last7days.header['DataSensorOrientation'])
        last7days = last7days.offset(offsets={'x':-float(diff[0]),'y':-float(diff[1]),'z':-float(diff[2])})
        last7days = last7days.calc_f()

        fig = mp.plot(last7days, ['x','y','z','f'],
		plottitle = '%s Magnetic Data (%s - %s)' % (station,start[:10],end[:10]),
		bgcolor = 'white',
                noshow = True,
		padding = 5)

        axes = gcf().get_axes()

        day = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        while day <= datetime.strptime(end, '%Y-%m-%d %H:%M:%S'):
            if day.weekday() in [5,6]:	# Saturday or Sunday
                t_start = day
                t_end = day + timedelta(days=1)
                for ax in axes:
                    ax.axvspan(t_start, t_end, facecolor='green', alpha=0.3, linewidth=0)
            day += timedelta(days=1)

        ax.get_xaxis().set_major_formatter(matplotlib.dates.DateFormatter('%d.%b\n%H:%M'))

        plt.savefig(plotpath,savedpi=80)
        ftppath = 'zamg/images/graphs/magnetism/'
        oldftppath = 'cmsjoomla/images/stories/currentdata/tse'

        scptransfer(plotpath,'94.136.40.103:'+ftppath,passwd)
        #ftpdatatransfer(localfile=plotpath, 
	#	ftppath=ftppath, 
	#	myproxy=myproxy, port=port, 
	#	login=login, passwd=passwd, 
	#	logfile=os.path.join(basepath,station,'%s_processing.log' % station))

#####################################################################
# MAIN PROGRAM
#####################################################################

if __name__ == '__main__':
    today = datetime.utcnow()
    yesterday = today - timedelta(days=1) # Default = 1
    date = datetime.strftime(yesterday, "%Y-%m-%d")
    starttime = date+' 00:00:00'
    endtime = datetime.strftime(today, '%Y-%m-%d') +' 00:00:00'

#####################################################################
# A. TSE (Tamsweg-Sedna) DATA
#####################################################################

    if TSE:

        station = 'TSE'
        stationname = 'sedna'
        sensors = ['LEMI025_27_0001', 'POS1_N457_0001', 'ENV05_12_0001']
        decl = 3.02

	# Start logger
	logger_tse = logging.getLogger('TSE-DP')
	handler = logging.FileHandler(os.path.join(basepath,station,'%s_processing.log' % station))
	handler = logging.StreamHandler(sys.stdout)
	handler.setLevel(logging.INFO)
	handler.setFormatter(formatter)
	logger_tse.addHandler(handler)

        # Correct for westerly orientation:
	# TODO: This is probably wrong and it actually needs a 180 degree rotation...
        factors =  {'x': -1,
	    	    'y': -1,
	    	    'z': +1}
        # Correct values with IGRF values:
        baseline = {'x': 21492.4,
	    	    'y': 1134.8,
	    	    'z': 42941.5}
            
        try:
            process_data(station,'normal','LEMI025_27_0001',basepath,'bin',date,['x','y','z'],
                    logger = logger_tse, sevendayplot = True)
        except Exception as e:
            logger_tse.error("ERROR reading LEMI file!------------------------------")
            logger_tse.error(e)
  
        try:
            process_data(station,'normal','POS1_N457_0001',basepath,'bin',date,['f'],
                    logger = logger_tse)
        except Exception as e:
            logger_tse.error("ERROR reading POS1 file!------------------------------")
            logger_tse.error(e)

        try:
            process_data(station,'normal','ENV05_12_0001',basepath,'bin',date,['t1','var1','t2'],
                    logger = logger_tse)
        except Exception as e:
            logger_tse.error("ERROR reading ENV file!------------------------------")
            logger_tse.error(e)


#####################################################################
# B. IPL (Innsbruck-Pluto) DATA
#####################################################################

    if IPL:
	station = 'IPL'
        sensors = ['lemi', 'pos1', 'Env05']
	logger_ipl = logging.getLogger('IPL')
        decl = 2.49

	# Start logger
	logger_tse = logging.getLogger('IPL-DP')
	handler = logging.FileHandler(os.path.join(basepath,station,'%s_processing.log' % station))
	handler.setLevel(logging.INFO)
	handler.setFormatter(formatter)
	logger_tse.addHandler(handler)

        # Correct for westerly orientation:
        factors =  {'x': -1,
	    	    'y': -1,
	    	    'z': -1}

        # Correct values with IGRF values:
        baseline = {'x': 21404.6,
	    	    'y': 932.2,
	    	    'z': 42731.5}

#####################################################################
# C. GQU (Graz-Quaoar) DATA
#####################################################################

    if GQU:
	station = 'GQU'
        sensors = ['cs1','env1']
	logger_gqu = logging.getLogger('GQU')
	print "So..."


