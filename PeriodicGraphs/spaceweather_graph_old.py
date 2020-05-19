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

endtime = datetime.utcnow()
starttime = endtime - timedelta(days=4)
date = datetime.strftime(endtime,"%Y-%m-%d")

solarpath = '/srv/archive/external/esa-nasa/ace'

try:
    ace_1m = read(os.path.join(solarpath,'collected','ace_1m_*'),
        starttime=starttime,endtime=endtime)
    ace_5m = read(os.path.join(solarpath,'collected','ace_5m_*'),
        starttime=starttime,endtime=endtime)
except Exception as e:
    print("Reading ACE data failed (%s)!" % e)

startstr = num2date(ace_1m.ndarray[0][0])
endstr = num2date(ace_1m.ndarray[0][-1])
print("Plotting from %s to %s" % (startstr, endstr))

ace_1m.header['col-var1'] = 'Proton density'
ace_1m.header['unit-col-var1'] = 'p/cc'
ace_1m.header['col-var2'] = 'Solar wind speed'
ace_1m.header['unit-col-var2'] = 'km/s'
pflux = ace_5m._get_column('var1').astype(float)
logpflux = np.log10(pflux)
ace_5m._put_column(np.asarray(logpflux), 'var1')
ace_5m.header['col-var1'] = 'log10(P-flux 47-68 keV)\n   '
ace_5m.header['unit-col-var1'] = 'p/cm2-s-ster-MeV'

kp = read(path_or_url=os.path.join('/srv/archive/external/gfz','kp','gfzkp*'))
kp = kp.trim(starttime=starttime,endtime=endtime)

mp.plotStreams([kp, ace_5m, ace_1m],[['var1'], ['var1'],['var1','var2']],confinex=True,bartrange=0.06,symbollist=['z','-','-','-'],specialdict = [{'var1': [0,9]}, {}, {}],plottitle = "Solar and global magnetic activity",outfile=os.path.join(solarpath),noshow=True)

#mp.plotStreams([vario,kvals],[['x'],['var1']],specialdict = [{},{'var1':[0,9]}],symbollist=['-','z'],bartrange=0.06, gridcolor='#316931',fill=['x','var1'],confinex=True, opacity=0.7, plottitle='Geomagnetic activity (until %s)' % (datetime.utcnow().date()),noshow=True)

savepath = "/srv/products/graphs/spaceweather/solarwindact_%s.png" % date
plt.savefig(savepath)

#upload
cred = 'cobshomepage'
address=mpcred.lc(cred,'address')
user=mpcred.lc(cred,'user')
passwd=mpcred.lc(cred,'passwd')
port=mpcred.lc(cred,'port')
remotepath = 'zamg/images/graphs/magnetism/'
path2log = '/home/cobs/ANALYSIS/Logs/magvar.log'
oldremotepath = 'cmsjoomla/images/stories/currentdata/wic/'

ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
ftpdatatransfer(localfile=savepath,ftppath=oldremotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
