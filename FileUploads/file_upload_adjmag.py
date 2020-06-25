#!/usr/bin/env python

"""
Magnetism products and graphs
"""

from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred
import io, pickle

import itertools
from threading import Thread
from subprocess import check_output   # used for checking whether send process already finished

# ################################################
#             Logging
# ################################################

## New Logging features 
from martas import martaslog as ml
logpath = '/var/log/magpy/mm-fu-uploads.log'
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
name = "{}-FileUploadsAdjusted".format(sn)


# ################################################
#             Methods
# ################################################

def getcurrentdata(path):
    """
    usage: getcurrentdata(currentvaluepath)
    example: update kvalue
    >>> fulldict = getcurrentdata(currentvaluepath)
    >>> valdict = fulldict.get('magnetism',{})
    >>> valdict['k'] = [kval,'']
    >>> valdict['k-time'] = [kvaltime,'']
    >>> fulldict[u'magnetism'] = valdict
    >>> writecurrentdata(path, fulldict) 
    """
    if os.path.isfile(path):
        with open(path, 'r') as file:
            fulldict = json.load(file)
        return fulldict
    else:
        print ("path not found")

def writecurrentdata(path,dic):
    """
    usage: writecurrentdata(currentvaluepath,fulldict)
    example: update kvalue
    >>> see getcurrentdata
    >>>
    """
    with open(path, 'w',encoding="utf-8") as file:
        file.write(unicode(json.dumps(dic)))


def active_pid(name):
     # Part of Magpy starting with version ??
    try:
        pids = map(int,check_output(["pidof",name]).split())
    except:
        return False
    return True


def uploaddata(localpath, destinationpath, typus='ftp', address='', user='', pwd='', port=None, logfile='stdout'):
    """
    DEFINITION:
        upload data method.
        Supports file upload to servers using the following schemes:
        ftp
        ftpback (background process)
        scp  (please consider using rsync)  scp transfer requires a established key, therefor connect to the server once using ssh to create it
        gin   (curl based ftp upload to data gins)    
    """
    success = True
    print ("Running upload to {} (as {}) via {}: {} -> {}, logging to {}".format(address, user, typus, localpath, destinationpath, logfile)) 
    #typus = "TEST"
    if typus == 'ftpback':
           Thread(target=ftpdatatransfer, kwargs={'localfile':localpath,'ftppath':destinationpath,'myproxy':address,'port':port,'login':user,'passwd':pwd,'logfile':logfile}).start()
    elif typus == 'ftp':
           ftpdatatransfer(localfile=localpath,ftppath=destinationpath,myproxy=address,port=port,login=user,passwd=pwd,logfile=logfile)
    elif typus == 'scp':
           timeout = 300
           destina = "{}:{}".format(address,destinationpath)
           scptransfer(localpath,destina,passwd,timeout=timeout)
    elif typus == 'gin':
        if not active_pid('curl'):
            print ("  -- Uploading minute data to GIN - active now")
            stdout = False
            if logfile == 'stdout':
                stdout = True
            success = ginupload(localpath, user, passwd, address, stdout=stdout)
        else:
            print ("curl is active")
    else:
        print ("Selected type of transfer is not supported")

    return success


def getchangedfiles(basepath,memory,startdate=datetime(1840,4,4),enddate=datetime.utcnow(), add="newer"):
    """
    DESCRIPTION
        Will compare contents of basepath and memory and create a list of paths with changed information
        This method will work a bit like rsync without accessing the zieldirectory. It just checks whether
        such data has been uploaded already
    VARIABLES
        basepath  (String)      :  contains the basepath in which all files will be checked
        memory    (list/dict)   :  a dictionary with filepath and last change date (use getcurrentdata)
        startdate (datetime)    :  changes after startdate will be considered
        enddate   (datetime)    :  changes before enddate will be considered
        add       (string)      :  either "all" or "newer" (default)   
    RETURNS
        dict1, dict2   : dict1 contains all new data sets to be uploaded, dict2 all analyzed data files for storage
    """

    filelist=[]
    try:
        for file in os.listdir(basepath):
            fullpath=os.path.join(basepath, file)
            if os.path.isfile(fullpath):
                filelist.append(fullpath)
    except:
        print ("Directory not found")
        return {}, {}

    retrievedSet={}
    for name in filelist:
        mtime = datetime.fromtimestamp(os.path.getmtime(name))
        stat=os.stat(os.path.join(basepath, name))
        mtime=stat.st_mtime
        #ctime=stat.st_ctime
        #size=stat.st_size
        if datetime.utcfromtimestamp(mtime) > startdate and datetime.utcfromtimestamp(mtime) <= enddate:
            retrievedSet[name] = mtime

    if memory:
        if sys.version_info >= (3,):
            newdict = dict(retrievedSet.items() - memory.items())
        else:
            newdict = dict(filter(lambda x: x not in memory.items(), retrievedSet.items()))
    else:
        newdict = retrievedSet.copy()

    return newdict, retrievedSet


# ################################################
#             Configuration
# ################################################


part1 = True # check for availability of paths

uploadpath = '/srv/products/data/lastupload.json'
#uploadpath = '/home/leon/Tmp/lastupload.json'

#Basic path lists
pathlist = ['/srv/products/data/magnetism/quasidefinitive']

workdictionary = {'wicadjmin': { 'path' : '/srv/products/data/magnetism/quasidefinitive/sec',
                                'destinations'  : {'gleave' : {'type' : 'scp', 'path' : '/uploads/all-obs'}
                                                  },   #destinations contain the credential as key and type of transfer as value (for scp use rsync)
                                'log'  : '/home/cobs/ANALYSIS/Logs/wicadjart.log', 
                                'endtime'  : datetime.utcnow(),
                                'starttime'  : datetime.utcnow()-timedelta(days=2),
                              }
                  }


#'gin' : {'type' : 'gin', 'path' : '/data/magnetism/wic/variation/', }, 
#'zamg' : {'type' : 'ftpback', 'path' : '/data/magnetism/wic/variation/','logfile': '/home/cobs/ANALYSIS/Logs/wicadj.log'},

vpathsec = '/srv/products/data/magnetism/variation/sec/'
vpathmin = '/srv/products/data/magnetism/variation/min/'
vpathcdf = '/srv/products/data/magnetism/variation/cdf/'
qpathsec = '/srv/products/data/magnetism/quasidefinitive/sec/'
qpathcdf = '/srv/products/data/magnetism/quasidefinitive/cdf/'
qpathmin = '/srv/products/data/magnetism/quasidefinitive/min/'



# ################################################
#             Part 0
# ################################################
#try:
if part1:
    """
    Test    
    """
    for key in workdictionary:
        print ("DEALING with ", key)
        lastfiles = {}
        fulldict = {}
        if os.path.isfile(uploadpath):
            with open(uploadpath, 'r') as file:
                fulldict = json.load(file)
                lastfiles = fulldict.get(key)
                # lastfiles looks like: {'/path/to/my/file81698.txt' : '2019-01-01T12:33:12', ...}

        if not lastfiles == {}:
            print ("write memory")
            pass

        sourcepath = workdictionary.get(key).get('path')
        starttime = workdictionary.get(key).get('starttime')
        endtime = workdictionary.get(key).get('endtime')
        newfiledict, alldic = getchangedfiles(sourcepath, lastfiles, starttime, endtime)

        print ("Found new: {} and all {}".format(newfiledict, alldic))

        for dest in workdictionary.get(key).get('destinations'):
            print ("  -> Destination: {}".format(dest))
            address=mpcred.lc(dest,'address')
            user=mpcred.lc(dest,'user')
            passwd=mpcred.lc(dest,'passwd')
            port=mpcred.lc(dest,'port')
            destdict = workdictionary.get(key).get('destinations')[dest]
            #print (destdict)
            if address and user and newfiledict:
                for nfile in newfiledict:
                    print ("    -> Uploading {} to dest {}".format(nfile, dest))
                    success = uploaddata(nfile, destdict.get('path'), destdict.get('type'), address, user, passwd, port, logfile=destdict.get('logfile','stdout'))
                    print ("    -> Success", success)
                    if not success:
                        #remove nfile from alldic 
                        # thus it will be retried again next time
                        print (" !---> upload of {} not successful: keeping it in todo list".format(nfile))
                        del alldic[nfile]

        fulldict[key] = alldic
        writecurrentdata(uploadpath, fulldict)
"""
    statusmsg[name] = "uploading data succesful"
except:
    statusmsg[name] = "error when uploading files - please check"

print (statusmsg)
martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)

"""
