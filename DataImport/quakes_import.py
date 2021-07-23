#!/usr/bin/env python

from magpy.stream import read
from magpy.database import * 
import magpy.opt.cred as mpcred
from pyproj import Geod
import getopt
import pwd
import socket


scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, DoVarioCorrections, DoBaselineCorrection, DoScalarCorrections,ConnectDatabases, GetPrimaryInstruments, getcurrentdata, writecurrentdata
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__


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

def fix_datastream_for_db(datastream):
    """
    DESCRIPTION
       Reads a data stream and converts all coulumns in data types necessary for writing to DB
       Please note: this method will drop non-ascii characters from strings which will change
       names of locations.
    IMPORTANT
       this method changes the supplied datastream
    """
    # 1. convert values forl NUMERICAL columns
    for idx,key in enumerate(KEYLIST):
        if key in NUMKEYLIST:
            datastream.ndarray[idx] = np.asarray([float(elem) if not elem == '' else float(nan) for elem in datastream.ndarray[idx]])
            datastream.ndarray[idx] = datastream.ndarray[idx].astype(float64)
        else:
            datastream.ndarray[idx] = np.asarray([str(el).encode().decode('ascii', 'ignore') for el in datastream.ndarray[idx]])
            datastream.ndarray[idx] = datastream.ndarray[idx].astype(object)
    return datastream


def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    debug=False
    endtime = datetime.utcnow()
    joblist = ['NEIC','AT']
    stb = DataStream()
    sta = DataStream()
    errorcntAT = 0
    errorcntNE = 0
    uploadcheck = 1
    path = '/tmp/neic_quakes.d' 

    try:
        opts, args = getopt.getopt(argv,"hc:e:j:p:s:o:D",["config=","endtime=","joblist=","debug="])
    except getopt.GetoptError:
        print ('neic_download.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- neic_download.py will determine the primary instruments --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python neic_download.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-e            : endtime, default is now')
            print ('-j            : joblist: NEIC, AT')
            print ('-p            : path for neic data')
            print ('-------------------------------------')
            print ('Application:')
            print ('python neic_download.py -c /etc/marcos/analysis.cfg -p /home/cobs/ANALYSIS/Seismo/neic_quakes.d')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-e", "--endtime"):
            # get an endtime
            endtime = arg.split(',')
        elif opt in ("-j", "--joblist"):
            # get an endtime
            joblist = arg.split(',')
        elif opt in ("-p", "--path"):
            path = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running flagging version {}".format(version))
    print ("--------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname='mm-di-quakes.log', debug=debug)

    namea = "{}-quakes-AT".format(config.get('logname'))
    nameb = "{}-quakes-NEIC".format(config.get('logname'))
    currentvaluepath = config.get('currentvaluepath')

    print ("3. Connect databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        connectdict = config.get('conncetedDB')
    except:
        statusmsg[name1] = 'database failed'
    print (" -> connected databases: {}".format(connectdict))

    proxy = ''
    prox = config.get('proxy','')
    proxport = config.get('proxyport')
    if prox:
        proxy = "--proxy http://{}:{} ".format(prox,proxport)


    (startlong, startlat) = dbcoordinates(db, 'A2')
    if 'AT' in joblist:
      try:
        print ("Getting Austrian data")
        print ("---------------------")
        statusmsg[namea] = 'Austrian data added'
        if not stb.length()[0] > 0:  # only load it once
            print (" - getting Austrian data from geoweb")
            stb = read('http://geoweb.zamg.ac.at/static/event/lastweek.csv')
        stb.header['DataFormat'] = 'NEICCSV'
        stb.header['DataSource'] = 'Austrian Seismological Service'
        stb.header['DataReferences'] = 'http://geoweb.zamg.ac.at/static/event/lastmonth.csv'
        if debug:
            print ("  - Found :", stb.length())

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
        if debug:
            print ("  - Found :", stb.length())
        if not debug:
            dbupdateDataInfo(db, 'QUAKES', stb.header)

        stb = fix_datastream_for_db(stb)

        if not debug:
            for dbel in connectdict:
                dbt = connectdict[dbel]
                print ("  -- Writing AT Quakes to DB {}".format(dbel))
                writeDB(dbt,stb,tablename='QUAKES',StationID='SGO')
                print ("     -> Done")
        else:
            print ("   - Debug selected: ")
            print ("     last line of AT {}".format(stb.length()))

        print (" -> Austrian data has been added to all databases")
        print ("----------------------------")
        print (" Now writing last AT update into current value") 
        #statusmsg[namea] = 'Austrian data added'
        errorcntAT = 0
        # update upload time in current data file
        fulldict = getcurrentdata(currentvaluepath)
        valdict = fulldict.get('logging',{})
        uploadtime = datetime.strftime(datetime.utcnow(),"%Y-%m-%d %H:%M")
        valdict['seismoATdata'] = [uploadtime,'']
        fulldict[u'logging'] = valdict
        if not debug:
            print (" Writing update time to current data")
            writecurrentdata(currentvaluepath, fulldict)
            print (" -> Updating time has been written to current data")
            print ("----------------------------")
        print (" DONE")
        print ("----------------------------")
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
                print ('Austrian data failed')
                statusmsg[namea] = 'Austrian data failed'

    if 'NEIC' in joblist:
        #try:
        print ("Downloading NEIC data")
        print ("---------------------")
        statusmsg[nameb] = 'NEIC data added'
        if not sta.length()[0] > 0: # only load it once
            print (" - getting NEIC data from usgs")
            os.system('curl https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.csv {}-s > {}'.format(proxy,path))
            sta = read(path)
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
        print (" got {} quakes above magnitude 5".format(sta.length()[0]))
        if not debug:
            dbupdateDataInfo(db, 'QUAKES', sta.header)

        sta = fix_datastream_for_db(sta)

        if not debug:
            for dbel in connectdict:
                dbt = connectdict[dbel]
                print ("  -- Writing NEIC Quakes to DB {}".format(dbel))
                writeDB(dbt,sta,tablename='QUAKES',StationID='SGO')
                print ("    -> done")
        else:
            print ("   - Debug selected: ")
            print ("     last line of NEIC {}".format(stb.length()))

        print ("NEIC data has been added")
        print ("----------------------------")
        print (" Now writing last NEIC update into current value")
        #statusmsg[nameb] = 'NEIC data added'
        errorcntNE = 0
        # update upload time in current data file
        fulldict = getcurrentdata(currentvaluepath)
        valdict = fulldict.get('logging',{})
        uploadtime = datetime.strftime(datetime.utcnow(),"%Y-%m-%d %H:%M")
        valdict['seismoNEICdata'] = [uploadtime,'']
        fulldict[u'logging'] = valdict
        if not debug:
            writecurrentdata(currentvaluepath, fulldict)
        """
      except:
        print ("  error encountered")
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
        """
    print ("------------------------------------------")
    print ("  neic_download finished")
    print ("------------------------------------------")
    print ("SUCCESS")

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
        pass
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])


