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

dbpasswd = mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print "Connecting to primary MARCOS..."
    db = mysql.connect(host="localhost",user="cobs",passwd=dbpassed,db="cobsdb")
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
starttime=datetime.strftime(endtime-timedelta(days=20),"%Y-%m-%d")
date = datetime.strftime(endtime,"%Y-%m-%d")

gamma = readDB(db,'GAMMA',starttime=starttime)
#iwt = iwt.filter(filter_width=timedelta(seconds=1))
#lm = readDB(db,'LM_TILT01_0001_0001',starttime=starttime) 

# providing some Info on content
#print data._get_key_headers()

#mp.plot(data,['x','t1'],bgcolor = '#d5de9c', gridcolor = '#316931',fill=['t1'],confinex=True)
mp.plotStreams([gamma,lm],[['x'],['x','t1','var2']], gridcolor='#316931',fill=['t1','var2'],confinex=True, fullday=True, opacity=0.7, plottitle='Tilts (until %s)' % (datetime.utcnow().date()),noshow=True)


#upload
savepath = "/srv/products/graps/tilt/tilt_%s.png" % date
plt.savefig(savepath)

cred = 'cobshomepage'
address=mpcred.lc(cred,'address')
user=mpcred.lc(cred,'user')
passwd=mpcred.lc(cred,'passwd')
port=mpcred.lc(cred,'port')
remotepath = 'zamg/images/graphs/gravity/tilt/'

ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)

