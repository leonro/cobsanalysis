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

from magpy.stream import *
from magpy.transfer import *
from obspy import read as obsread
import magpy.opt.cred as mpcred


# Get filelist
filelst = True
if filelst:
    path = '/srv/archive/WIC/BGSINDCOIL_1_0001/raw'
    #path = '/srv/archive/SGO/CONACSNA_1_0001/raw'
    depth = 1
    dateformat = '%Y.%j'
    current = datetime.utcnow() # make that a variable
    year = current.year
    doy = datetime.strftime(current,"%j")
    datelist = []
    for elem in range(depth):
        datelist.append(datetime.strftime(current,dateformat))
        current = current-timedelta(days=elem+1)

    #filelist = [os.path.join(path,'CONA.HNZ.'+date+'.00.00.00') for date in datelist]
    filelist = [os.path.join(path,'CALY.HFE.'+date+'.00.00.00') for date in datelist]
    print filelist


for fi in filelist:
    #seedx = obsread('/home/leon/Dropbox/Daten/CALY.BFN.2016.278.00.00.00')
    seedy = obsread(fi)
    #seedz = obsread('/home/leon/Dropbox/Daten/CALY.BFZ.2016.278.00.00.00')
    #seed = seedx+seedy+seedz
    #print(seed)
    #  Do whatever you want in ObsPy
    #obj = obspy2magpy(seed,keydict={'OE.CALY..BFN': 'x','OE.CALY..BFE': 'y','OE.CALY..BFZ': 'z'})
    comp = 'y'
    obj = obspy2magpy(seedy,keydict={'OE.CALY..HFE': comp})
    #obj = obspy2magpy(seedy)
    print obj.length(), obj.header
    #  Do whatever you want in MagPy as mpobj is now a MagPy object
    import magpy.mpplot as mp
    #mp.plot(obj)
    mp.plotSpectrogram(obj,[comp])
    #mpobj.write('/home/myuser/mypath',format_type='PYASCII')


