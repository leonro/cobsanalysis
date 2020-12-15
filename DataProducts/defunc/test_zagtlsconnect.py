#!/usr/bin/env python
"""
Get files from a remote server (to be reached by nfs, samba, ftp, html or local directory) 
File content is directly added to a data bank (or local file if preferred).
"""
try:
    from magpy.stream import *
    from magpy.database import *
    from magpy.opt import cred as mpcred
except:
    import sys
    sys.path.append('/home/leon/Software/magpy/trunk/src')
    from stream import *
    from database import *
    from opt import cred as mpcred
import getopt
import fnmatch
import pwd
import zipfile

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

product3 = True
if product3:
    locationlist = []
    # Get a sensor list for data of interest
    senslist = dbselect(db, 'SensorID', 'SENSORS','SensorGroup LIKE "%agnetism"')
    senslist = ["'"+elem+"'" for elem in senslist]
    where = "FlagNum IN (2,3) AND SensorID IN ({})".format(','.join(senslist))
    #where = "FlagNum IN (2,3)"
    #where = "FlagNum IN (2,3) AND SensorID IN ('FGE_S0252_0001','GSM90_14245_0002','LEMI025_22_0002','POS1_N432_0001')"
    print (where)
    quasidefval = dbselect(db,'ModificationDate','FLAGS',where)
    tmp = DataStream()
    quasidefval = [tmp._testtime(el) for el in quasidefval]
    qdendtime = max(quasidefval)
    print qdendtime
    #qdendtime = datetime.strptime(qdendtime,dbdateformat)
    #senslist = dbselect(db, 'SensorID', 'DATAINFO','ColumnContents LIKE "%T%"')
    #senslist = list(set(senslist)) # remove duplicates
    # For each sensor get location and value
    print senslist

