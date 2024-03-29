#!/usr/bin/env python
from magpy.stream import read
import magpy.mpplot as mp
import magpy.absolutes as di
from magpy.database import *
import magpy.opt.cred as mpcred

dbpasswd = mpcred.lc('cobsdb','passwd')
db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")


absresult = di.absoluteAnalysis('/srv/archive/WIC/DI/raw','/srv/archive/WIC/FGE_S0252_0001/FGE_S0252_0001_0001/*','/srv/products/data/magnetism/definitive/2015/GSM90_14245_0002_0002_scalar_oc_min*',deltaF=-0.258,diid='A2_WIC.txt',pier='A2', expD=4.0, expI=64.0,starttime='2015-06-01',endtime='2016-02-01',db=db, dbadd=True)

print absresult.length()

absresult.write('/srv/archive/WIC/DI/data',coverage='all',filenamebegins="BLV_FGE_S0252_0001_GSM90_14245_0002_A2_oc_min",format_type="PYSTR",mode='replace')

data1 = read('/srv/archive/WIC/DI/data/BLV_FGE_S0252_0001_GSM90_14245_0002_A2_oc_min.txt')

mp.plot(data1,['dx','dy','dz'],symbollist=['o','o','o'])

data2 = read('/srv/archive/WIC/DI/data/BLV_FGE_S0252_0001_GSM90_14245_0002_A2.txt')

sub = subtractStreams(data1,data2)
mp.plot(sub,['dx','dy','dz'],symbollist=['o','o','o'])
#writeDB(db,absresult,'BLV_FGE_S0252_0001_GSM90_14245_0002_A2')

