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

## New Logging features 
from martas import martaslog as ml
logpath = '/var/log/magpy/mm-dp-scaradon.log'
#import socket
#sn = socket.gethostname().upper()
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
name = "{}-DataProducts-SCAradon".format(sn)


dbpasswd = mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print "Connecting to primary MARCOS..."
    db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")
    print db
except:
    print "... failed"
    try:
        # Test MARCOS 2
        print "Connecting to secondary MARCOS..."
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
        print db
    except:
        print "... failed -- aborting"
        sys.exit()

endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=1),"%Y-%m-%d")
date = datetime.strftime(endtime,"%Y-%m-%d")
dbdateformat = "%Y-%m-%d %H:%M:%S.%f"
today1 = datetime.strftime(endtime,"%Y%m%d")
yesterd1 = datetime.strftime(endtime-timedelta(days=1),"%Y%m%d")
yesterd2 = datetime.strftime(endtime-timedelta(days=1),"%Y-%m-%d")
weekago = datetime.strftime(endtime-timedelta(days=6),"%Y-%m-%d")

## Data Tables
end = endtime
#start = '2017-01-01'
start = datetime.strftime(endtime-timedelta(days=7),"%Y-%m-%d")

rawradpath = '/srv/archive/SGO/GAMMA_SFB867_0001/raw/'
tablepath = '/srv/projects/radon/tables/'
envboxpath = '/srv/projects/radon/confinedExp/environment/'
figpath = '/srv/products/graphs/radon/'

zamgcred = 'zamg'
zamgaddress=mpcred.lc(zamgcred,'address')
zamguser=mpcred.lc(zamgcred,'user')
zamgpasswd=mpcred.lc(zamgcred,'passwd')
zamgport=mpcred.lc(zamgcred,'port')

part1 = True # upload SCA data to ZAMG FTP Server
part2 = True # filter SCA data and create full tables
part3 = False # Box experiments, auxiliary data
part4 = True  # outside temperture data

def convertGeoCoordinate(lon,lat,pro1,pro2):
    try:
        from pyproj import Proj, transform
        p1 = Proj(init=pro1)
        x1 = lon
        y1 = lat
        # projection 2: WGS 84
        p2 = Proj(init=pro2)
        # transform this point to projection 2 coordinates.
        x2, y2 = transform(p1,p2,x1,y1)
        return x2, y2
    except:
        return lon, lat


if part1:
    """
    Upload radon raw data to zamg server
    """
    try:
        p1start = datetime.utcnow()

        for da in [date,yesterd2]:
            # Send in background mode
            Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(rawradpath,'COBSEXP_2_'+da+'.txt'),'ftppath':'/data/radon/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/rawrad-transfer.log'}).start()

        p1end = datetime.utcnow()
        print "-----------------------------------"
        print "Part1 needs", p1end-p1start
        print "-----------------------------------"
        statusmsg[name] = 'SCA Radon step1 success'
    except:
        statusmsg[name] = 'SCA Radon step1 failed'
        pass

if part2:
    """
    prepare mean tables ready for analysis
    """
    try:
        p2start = datetime.utcnow()
        print ("-----------------------------------")
        print ("PART 2:")
        print ("Reading SCA Gamma data...")
        gammasca = read(os.path.join(rawradpath,'COBSEXP_2_*'), starttime=start, endtime=end)
        print (gammasca.ndarray)
        gammasca.write(tablepath, filenamebegins='sca-tunnel-1min_',dateformat='%Y',coverage='year', mode='replace',format_type='PYCDF')
        gammasca = gammasca.filter(filter_type='gaussian', resample_period=900 )
        gammasca.write(tablepath, filenamebegins='sca-tunnel-15min_',dateformat='%Y', coverage='year', mode='replace',format_type='PYCDF')
        print ("...finished")
        p2end = datetime.utcnow()
        print "-----------------------------------"
        print "Part2 needs", p2end-p2start
        print "-----------------------------------"
        statusmsg[name] = 'SCA Radon step2 success'
    except:
        statusmsg[name] = 'SCA Radon step2 failed'

if part3:
    """
    Get data from tethys and prepare mean tables 1min and hourly filtered
    """
    try:
        p3start = datetime.utcnow()
        print ("-----------------------------------")
        print ("PART 3:")
        print ("Upload managed by tethys system - crontab")
        print ("-----------------------------------")
        # 44 13 * * * root python /home/cobs/MARTAS/DataScripts/senddata.py -c vega -s scp -d 20 -l /srv/ws/ -r /srv/projects/radon/confinedExp/environment/
        # Don't forget to set up addcred for vega
        print ("Filtering data now")
        #end = '2017-03-01'
        #start = '2017-01-01'
        envboxout = read(os.path.join(envboxpath,'SHT75_RASHT002_*'), starttime=start, endtime=end)
        envboxout = envboxout.filter()
        envboxout.write(tablepath, filenamebegins='env-box-outside-sht-1min_',dateformat='%Y',coverage='year', mode='replace',format_type='PYCDF')
        envboxin = read(os.path.join(envboxpath,'SHT75_RASHT001_*'), starttime=start, endtime=end)
        envboxin = envboxin.filter()
        envboxin.write(tablepath, filenamebegins='env-box-inside-sht-1min_',dateformat='%Y',coverage='year', mode='replace',format_type='PYCDF')
        #gammasca = gammasca.filter(filter_type='gaussian', resample_period=900 )
        #gammasca.write(tablepath, filenamebegins='sca-tunnel-15min',dateformat='%Y', coverage='year', mo$
        # GetData: Done by senddata.py on MARTAS
        bmpboxin = read(os.path.join(envboxpath,'BMP085_10085001_*'), starttime=start, endtime=end)
        bmpboxin = bmpboxin.filter(filter_width=timedelta(seconds=3.33), resample_period=1)
        bmpboxin.write(tablepath, filenamebegins='env-box-inside-bmp-1sec_',dateformat='%Y%m',coverage='month', mode='replace',format_type='PYCDF')
        bmpboxin = bmpboxin.filter(filter_width=timedelta(minutes=2), resample_period=60)
        bmpboxin.write(tablepath, filenamebegins='env-box-inside-bmp-1min_',dateformat='%Y',coverage='year', mode='replace',format_type='PYCDF')
        bmpboxout = read(os.path.join(envboxpath,'BMP085_10085002_*'), starttime=start, endtime=end)
        bmpboxout = bmpboxout.filter(filter_width=timedelta(seconds=3.33), resample_period=1)
        bmpboxout.write(tablepath, filenamebegins='env-box-outside-bmp-1sec_',dateformat='%Y%m',coverage='month', mode='replace',format_type='PYCDF')
        bmpboxout = bmpboxout.filter(filter_width=timedelta(minutes=2), resample_period=60)
        bmpboxout.write(tablepath, filenamebegins='env-box-outside-bmp-1min_',dateformat='%Y',coverage='year', mode='replace',format_type='PYCDF')
        p3end = datetime.utcnow()
        print "-----------------------------------"
        print "Part3 needs", p3end-p3start
        print "-----------------------------------"
        statusmsg[name] = 'SCA Radon step3 success'
    except:
        statusmsg[name] = 'SCA Radon step3 failed'

if part4:
    """
    Get further additional data 
    """
    try:
        # Temperature from all positions within the SGO
        #end = '2016-10-01'
        #start = '2016-09-01'
        p4start = datetime.utcnow()
        print ("-----------------------------------")
        print ("PART 4:")
        print ("Loading and filetering RCSG0temp data")
        rcsg0path = '/srv/archive/SGO/RCSG0temp_20161027_0001/raw/'
        tempsgo = read(os.path.join(rcsg0path,'*'), starttime=start, endtime=end)
        tempsgo = tempsgo.filter()
        tempsgo.write(tablepath, filenamebegins='temp-sgo-1min_',dateformat='%Y', coverage='year', mode='replace',format_type='PYCDF')

        p4end = datetime.utcnow()
        print "-----------------------------------"
        print "Part4 needs", p4end-p4start
        print "-----------------------------------"
        statusmsg[name] = 'SCA Radon step4 success'
    except:
        statusmsg[name] = 'SCA Radon step4 failed'


martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)

