#!/usr/bin/env python

from magpy.stream import read
from magpy.database import * 
import magpy.opt.cred as mpcred

dbpasswd = mpcred.lc('cobsdb','passwd')

## New Logging features 
from martas import martaslog as ml
logpath = '/var/log/magpy/mm-fd-earthquake.log'
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
namea = "{}-FileDownload-quakes-AT".format(sn)
nameb = "{}-FileDownload-quakes-NEIC".format(sn)

serverlist = ["138.22.188.195","138.22.188.191"]

currentvaluepath = '/srv/products/data/current.data'
errorcntAT = 0
errorcntNE = 0
uploadcheck = 1

# 1. Download data
stb = DataStream()
sta = DataStream()


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
    if os.path.isfile(currentvaluepath):
        with open(currentvaluepath, 'r') as file:
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
    with open(currentvaluepath, 'w',encoding="utf-8") as file:
        file.write(unicode(json.dumps(dic)))


for server in serverlist:
    cont = True
    try:
        # Test MARCOS 1
        print ("Connecting to server {}...".format(server))
        db = mysql.connect(host=server,user="cobs",passwd=dbpasswd,db="cobsdb")
        print ("{} ... connected".format(server))
    except:
        print "... failed"
        cont = False

    if cont:
      try:

        (startlong, startlat) = dbcoordinates(db, 'A2')

        print ("Getting Austrian data")
        statusmsg[namea] = 'Austrian data added'
        if not stb.length()[0] > 0:  # only load it once
            print (" - getting Austrian data from geoweb")
            stb = read('http://geoweb.zamg.ac.at/static/event/lastweek.csv')
        stb.header['DataFormat'] = 'NEICCSV'
        stb.header['DataSource'] = 'Austrian Seismological Service'
        stb.header['DataReferences'] = 'http://geoweb.zamg.ac.at/static/event/lastmonth.csv'

        dct = stb._get_key_names()
        poslon = KEYLIST.index(dct.get('longitude'))
        poslat = KEYLIST.index(dct.get('latitude'))
        lonar = stb.ndarray[poslon]
        latar = stb.ndarray[poslat]
        # calculate distance between points
        from pyproj import Geod
        g = Geod(ellps='WGS84')
        ar = []
        for idx,el in enumerate(lonar):
            (az12, az21, dist) = g.inv(startlong, startlat, el, latar[idx])
            ar.append(dist/1000.)
        pos = KEYLIST.index('var5')
        stb.header['col-var5'] = 'distance from COBS'
        stb.header['unit-col-var5'] = 'km'
        stb.ndarray[pos] = np.asarray(ar)
        # Add to DB like BLV data, Kp data
        # Please Note: you must manually insert a DATAID in DATAINFO to upload header data
        # insert into DATAINFO (DataID, SensorID) VALUES ('QUAKES','QUAKES');
        stb.header['StationID'] = 'SGO'
        stb = stb.extract('f',2,'>=')
        dbupdateDataInfo(db, 'QUAKES', stb.header)
        for idx,key in enumerate(KEYLIST):
            if key in NUMKEYLIST:
                stb.ndarray[idx] = np.asarray([float(elem) if not elem == '' else float(nan) for elem in stb.ndarray[idx]])
                stb.ndarray[idx] = stb.ndarray[idx].astype(float64)    
        writeDB(db,stb,tablename='QUAKES',StationID='SGO')
        print ("Austrian data has been added")
        print ("----------------------------")
        #statusmsg[namea] = 'Austrian data added'
        errorcntAT = 0
        # update upload time in current data file
        fulldict = getcurrentdata(currentvaluepath)
        valdict = fulldict.get('logging',{})
        uploadtime = datetime.strftime(datetime.utcnow(),"%Y-%m-%d %H:%M")
        valdict['seismoATdata'] = [uploadtime,'']
        fulldict[u'logging'] = valdict
        writecurrentdata(currentvaluepath, fulldict)
      except:
        errorcntAT += 1
        if errorcntAT > 1:
            message = True
            #statusmsg[namea] = 'Austrian data failed'
            fulldict = getcurrentdata(currentvaluepath)
            valdict = fulldict.get('logging',{})
            try:
                lastupload = datetime.strptime(valdict.get('seismoATdata',['',''])[0],"%Y-%m-%d %H:%M")
                if not lastupload < datetime.utcnow()-timedelta(hours=uploadcheck):
                    message = False
            except:
                message = True
            if message:
                statusmsg[namea] = 'Austrian data failed'

      try:
        print ("Downloading NEIC data")
        statusmsg[nameb] = 'NEIC data added'
        if not sta.length()[0] > 0: # only load it once
            print (" - getting NEIC data from usgs")
            os.system('curl https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.csv -s > /home/cobs/ANALYSIS/Seismo/neic_quakes.d')
            sta = read('/home/cobs/ANALYSIS/Seismo/neic_quakes.d')
        sta.header['DataFormat'] = 'NEICCSV'
        #sta.header['DataSource'] = 'US National Earthquake information center'
        #sta.header['DataReferences'] = 'http://earthquake.usgs.gov'

        dct = sta._get_key_names()
        poslon = KEYLIST.index(dct.get('longitude'))
        poslat = KEYLIST.index(dct.get('latitude'))
        lonar = sta.ndarray[poslon]
        latar = sta.ndarray[poslat]
        # calculate distance between points
        from pyproj import Geod
        g = Geod(ellps='WGS84')
        ar = []
        for idx,el in enumerate(lonar):
            (az12, az21, dist) = g.inv(startlong, startlat, el, latar[idx])
            ar.append(dist/1000.)

        pos = KEYLIST.index('var5')
        sta.header['col-var5'] = 'distance from COBS'
        sta.header['unit-col-var5'] = 'km'
        sta.ndarray[pos] = np.asarray(ar)
        # Add to DB like BLV data, Kp data
        # Please Note: you must manually insert a DATAID in DATAINFO to upload header data
        # insert into DATAINFO (DataID, SensorID) VALUES ('QUAKES','QUAKES');
        sta.header['StationID'] = 'SGO'
        sta = sta.extract('f',5,'>=')
        dbupdateDataInfo(db, 'QUAKES', stb.header)
        for idx,key in enumerate(KEYLIST):
            if key in NUMKEYLIST:
                sta.ndarray[idx] = np.asarray([float(elem) if not elem == '' else float(nan) for elem in sta.ndarray[idx]])
                sta.ndarray[idx] = sta.ndarray[idx].astype(float64)
        writeDB(db,sta,tablename='QUAKES',StationID='SGO')
        print ("NEIC data has been added")
        print ("----------------------------")
        #statusmsg[nameb] = 'NEIC data added'
        errorcntNE = 0
        # update upload time in current data file
        fulldict = getcurrentdata(currentvaluepath)
        valdict = fulldict.get('logging',{})
        uploadtime = datetime.strftime(datetime.utcnow(),"%Y-%m-%d %H:%M")
        valdict['seismoNEICdata'] = [uploadtime,'']
        fulldict[u'logging'] = valdict
        writecurrentdata(currentvaluepath, fulldict)
      except:
        errorcntNE+=1
        if errorcntNE > 1:
            message = True
            fulldict = getcurrentdata(currentvaluepath)
            valdict = fulldict.get('logging',{})
            try:
                lastupload = datetime.strptime(valdict.get('seismoNEICdata',['',''])[0],"%Y-%m-%d %H:%M")
                if not lastupload < datetime.utcnow()-timedelta(hours=uploadcheck):
                    message = False
            except:
                message = True
            if message:
                statusmsg[nameb] = 'NEIC data failed'

print (statusmsg)
martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)
