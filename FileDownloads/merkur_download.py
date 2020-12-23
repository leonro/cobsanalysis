#!/home/leon/Software/anaconda2/bin/env python
"""
MagPy - Basic Runtime tests including durations  
"""

# INSTRUCTIONS
# using anaconda

# 1.) install anaconda
#         - https://docs.continuum.io/anaconda/install
# 2.) install obspy
#         - https://github.com/obspy/obspy/wiki/Installation-via-Anaconda
# 3.) install magpy
#         - if you want to use CDF formats like ImagCDF: 
#             optional: install NasaCDF (http://cdf.gsfc.nasa.gov/)
#             optional: - pip install spacepy
#         - run 'pip install geomagpy' within the anaconda path
#               possible issues: MySQL-python problem -> install libmysqlclient-dev on linux, brew mysql install on mac
# 4.) start python -> the anaconda python, not system!


### Alternative with rsync
# 1. Get data from merkur (once per hour)
# rsync -avze ssh 'find -type f -iname "CALY.[LH]F[NZE].*" -mtime 10' 
# rsyn -avze ssh  /srv/archive/WIC/BGSINDCOIL_1_0001/raw

# 2. Sync to BROKER (depth 2)

# the same for CONA and CSNH

# to log use (as for other rsync features)
#python3 logfiledates.py -c ../conf/wic.cfg -p /srv/archive/WIC/BGSINDCOIL_1_0001/raw -a 2 -i day


from magpy.stream import *
from magpy.transfer import *
#from obspy import read as obsread
import magpy.opt.cred as mpcred
from threading import Thread


indload = True
uploadzamg = True
if indload:
    depth = 2
    dateformat = '%Y.%j'
    current = datetime.utcnow() # make that a variable
    year = current.year
    doy = datetime.strftime(current,"%j")
    datelist = []
    for elem in range(depth):
        datelist.append(datetime.strftime(current,dateformat))
        current = current-timedelta(days=elem+1)

    print ("inddate", datelist)
    cred = 'merkur'
    user = mpcred.lc(cred,'user') #leon
    address = mpcred.lc(cred,'address') #138.22.188.203
    password = mpcred.lc(cred,'passwd')
    basepath = os.path.join('/Users/cobs/antelope/db')
    destinationpath = '/srv/archive/WIC/BGSINDCOIL_1_0001/raw'
    zcred = 'zamg'
    zamgaddress=mpcred.lc(zcred,'address')
    zamguser=mpcred.lc(zcred,'user')
    zamgpasswd=mpcred.lc(zcred,'passwd')
    zamgport=mpcred.lc(zcred,'port')

    for date in datelist:
        print ("Dealing with date:", date)
        remotepath = os.path.join(basepath,str(date.split('.')[0]),str(date.split('.')[1]))
        print ("Remotepath", remotepath)
        filepat = 'CALY.[LH]F[NZE].'+date
        filelist = ssh_remotefilelist(remotepath, filepat, user,address,password)
        filelist = [elem for elem in filelist if elem.find(date) > 0 and not elem == '' and not elem == ' ']
        for fi in filelist:
            print ("indload", fi)
            scptransfer(user+'@'+address+':'+fi,destinationpath,password,timeout=30)
            if uploadzamg:  # upload this data only once per hour
                ti = datetime.utcnow()
                minute = int(ti.minute)
                if 52 < minute < 58 and fi.find('ALY.HF') > 0:
                    localfile = os.path.join(destinationpath, os.path.split(fi)[1])
                    Thread(target=ftpdatatransfer, kwargs={'localfile':localfile,'ftppath':'/data/magnetism/wic/induction/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/induction-transfer.log'}).start()


seisload = True
if seisload:
    depth = 2
    dateformat = '%Y.%j'
    current = datetime.utcnow() # make that a variable
    year = current.year
    doy = datetime.strftime(current,"%j")
    datelist = []
    for elem in range(depth):
        datelist.append(datetime.strftime(current,dateformat))
        current = current-timedelta(days=elem+1)

    cred = 'merkur'
    user = mpcred.lc(cred,'user') #leon
    address = mpcred.lc(cred,'address') #138.22.188.203
    password = mpcred.lc(cred,'passwd')
    basepath = os.path.join('/Users/cobs/antelope/db')
    destinationpath = '/srv/archive/SGO/CONACSNA_1_0001/raw'

    for date in datelist:
        print ("Dealing with date:", date)
        remotepath = os.path.join(basepath,str(date.split('.')[0]),str(date.split('.')[1]))
        print ("Remotepath", remotepath)
        filepatlst = ['CONA.HN[NZE].'+date,'CSNA.HN[NZE].'+date]
        for filepat in filepatlst:
            filelist = ssh_remotefilelist(remotepath, filepat, user,address,password)
            filelist = [elem for elem in filelist if elem.find(date) > 0 and not elem == '' and not elem == ' ']
            for fi in filelist:
                print ("seisload", fi)
                scptransfer(user+'@'+address+':'+fi,destinationpath,password,timeout=30)


"""
seedx = obsread('/home/leon/Dropbox/Daten/CALY.BFN.2016.278.00.00.00')
seedy = obsread('/home/leon/Dropbox/Daten/CALY.BFE.2016.278.00.00.00')
seedz = obsread('/home/leon/Dropbox/Daten/CALY.BFZ.2016.278.00.00.00')
seed = seedx+seedy+seedz
print(seed)
#  Do whatever you want in ObsPy
obj = obspy2magpy(seed,keydict={'OE.CALY..BFN': 'x','OE.CALY..BFE': 'y','OE.CALY..BFZ': 'z'})
print obj.length()
#  Do whatever you want in MagPy as mpobj is now a MagPy object
import magpy.mpplot as mp
mp.plot(obj)
mp.plotSpectrogram(obj,['y'])
#mpobj.write('/home/myuser/mypath',format_type='PYASCII')
"""

