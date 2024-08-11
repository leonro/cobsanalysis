#!/usr/bin/env python
# coding=utf-8

"""
Execute remote RCS commands and obtain sensor lists and data from rcs archive
Running on powehi

Options:
- get sensor lists
- search for specfic measurements
- extract data from selected sensors and export to file or any magpy strcuture
- automize the call and save to database
- extract specific recent data and add to status tables
- create and update a RCS sensor database table

RCS Structure:
RCS server -> RCS fieldpoints (G0, T7, ...) each with up to 255 channels -> RCS channel with timeseries sensor data
Basically two data sets: bools (on/off) or float, timeseries is non-harmonic

Sensor table (similar to DATAINFO):
SensorID -> "RCS"+Fieldpoint_ChannelName(s)_0001, DataID  (contains StationID, type (bool or float), FieldPoint, ChannelNumber(s), units and all other TCS info
-> can contain a single channel or multiple channels summarized (i.e. all temperature measurements of one fieldpoint)
-> Amount of SensorID's equals field points

Underlying commands from Richards rcs scripts:
- rcsListIni
- rcs2csv

rcs                         rcsMessages
rcs2csv                     rcsPlot
rcsListIni                  rcsStatus
rcsMailIfNoDataTransferred  rcsUpdate
cobs@geosphgt-utech:~$ rcs
rcs                         rcsMessages
rcs2csv                     rcsPlot
rcsListIni                  rcsStatus
rcsMailIfNoDataTransferred  rcsUpdate
cobs@geosphgt-utech:~$ rcs
rcsMessages     zeigt die RCS-Meldungen eines Zeitraums

Skripts im Detail mit Beispielen

rcsListIni
rcsListIni -v   (verbose: gesamte Zeile aus der .ini-Datei)
rcsListIni YYYY-MM-DD [hh:mm:ss]
Bsp.:
rcsListIni | grep °C
rcsListIni 2023-08-16
rcsListIni | grep T7
rcsListIni | grep Stollen
rcsListIni | grep STOLLEN

rcs2csv today FP signals
rcs2csv YYYY-MM-DD now FP signals
rcs2csv YYYY-MM-DD [hh:mm:ss] YYYY-MM-DD [hh:mm:ss] FP signals [outfile]
Bsp.:
rcs2csv 2012-02-12 now G0 30-31,120-128

rcsPlot today FP signals


(fieldpoint  channel datatype  fuse   description)
rcsListIni output
G0      1       binary  3Q2     FI ÜSP Netz1
...


Config contains:
- host of RCS software provider 138.22.188.45 and login info
- db credential shortcuts

Applications:
# get sensor list (-l)
rcs_extractor -c config -f "fieldpoint" -s "search term" -l
# extrcat and eventually save data (-o can also be stdout or db (making use of config)
rcs_extractor -c config -f "fieldpoint" -s "search term" -a "channels" -b "begin" -e "end" -o "outputpath" -t "outputformat"
# extrcat status data (requires a statusjson) and eventually add to db (if -o, otherwise stdout)
rcs_extractor -c config -f "fieldpoint" -s "search term" -j "statusjson" -o db
# update table definition in RCSINFO, eventually DATAINFO (and SensorID)
rcs_extractor -c config -u


"""
import subprocess
import sys
import getopt
import os
import json
from magpy.stream import *


def _execute_remote_call(host, command, tool="ssh", debug=False):
    """
    call: ["ssh", "cobs@138.22.188.45", "bin/rcsListIni"]
    """
    ssh = subprocess.Popen([tool, "%s" % host, command],
                           shell=False,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    result = ssh.stdout.readlines()
    if result == [] and tool == 'ssh':
        error = ssh.stderr.readlines()
        if debug:
             print ("rcs_extracctor: ERROR in remote call: {}".format(error))
    else:
        if debug:
             print ("Obtained {} lines".format(len(result)))
    return result


def _create_fp_data_call(host, fp, channellist, sensid, begin, end, headerdict={}, debug=False):
    """
    rcs2csv YYYY-MM-DD [hh:mm:ss] YYYY-MM-DD [hh:mm:ss] FP signals [outfile]
    will save to temporary directory and
    will create MagPy datastream objects

    """
    stream = DataStream()
    s = datetime.strftime(begin, "%Y-%m-%d %H:%M:%S")
    e = datetime.strftime(end, "%Y-%m-%d %H:%M:%S")
    outname = "/tmp/{}.csv".format(sensid)
    if len(channellist) > 15:
        channellist = channellist[:15]
        print("Too many channels ({}) selected for a MagPy datastream output - reducing to 15".format(len(channellist)))
    call = "bin/rcs2csv {} {} {} {} {}".format(s, e, fp, ",".join(channellist), outname)
    if debug:
        print (call)
    res = _execute_remote_call(host, call, debug=debug)
    cp = _execute_remote_call("{}:{}".format(host,outname), outname, tool="scp", debug=debug)
    stream = read(outname)
    stream.header["SensorID"] = sensid
    return stream


def _export_data(tmppath, outpath, format_type, debug=False):
    """
    will export temporary saved data
    """
    pass


def _full_list_call(host, debug=False):
    """
    DESCRIPTION
       extracts the full rcsListIni list and converts it
       to a dictionary with fieldpoints (SensorName) as key and channels (SensorRevision) as
       subdictionary.
    """
    fpdict = {}
    listcommand = "bin/rcsListIni"
    res = _execute_remote_call(host, listcommand, debug=debug)
    for el in res:
        ll = el.decode().strip().split("\t")
        fpcont = fpdict.get(ll[0], {})
        contl = [e for idx, e in enumerate(ll) if idx > 0]
        if ll and len(contl) > 2:
            if len(contl) > 3:
                fpcont[contl[0]] = {'datatype': contl[1], 'fuse': contl[2], 'description': contl[3]}
            elif len(contl) > 2:
                fpcont[contl[0]] = {'datatype': contl[1], 'fuse': contl[2]}
            else:
                fpcont[contl[0]] = {'datatype': contl[1]}
        fpdict[ll[0]] = fpcont
    return fpdict


def _fpdict_extract(fpdict, fieldpoints=[], channels=[], searchlist=[], notlist=[], debug=False):
    """
    DESCRIPTION
        Extract descriptions and channels from fieldpointdictionary
    RETURNS
        A reduced dictionary containing only fieldpoints and channels matching the searchcriteria
    """
    newdict = {}
    if not fieldpoints:
        fieldpoints = [key for key in fpdict]
    for fp in fieldpoints:
        if debug:
            print ("rcs_extractor: dealing with ", fp)
        newcont = {}
        subd = fpdict.get(fp)
        if not channels:
            channels = [key for key in subd]
        for ch in channels:
            take = True
            contd = subd.get(ch)
            if contd:
                dt = contd.get('datatype',' ')
                fu = contd.get('fuse',' ')
                de = contd.get('description',' ')
                if searchlist:
                    take = False
                    for se in searchlist:
                        if dt.find(se) > -1 or fu.find(se) > -1 or de.find(se) > -1:
                            take = True
                if notlist:
                    for no in notlist:
                        if dt.find(no) > -1 or fu.find(no) > -1 or de.find(no) > -1:
                            take = False
                if take:
                    newcont[ch] = contd
        newdict[fp] = newcont

    return newdict


def _isnum(string):
    try:
        num = float(string)
        return True
    except:
        return False
    return False


def _make_list(stringvar):
    var = stringvar.split(',')
    if not isinstance(var, list):
        var = [var]
    var = [el.strip() for el in var]
    # check for 81-85
    if stringvar.find('-') > -1:
        newvar = []
        for el in var:
            if el.find('-') > -1:
                inds = el.split('-')
                if len(inds) == 2 and _isnum(inds[0]) and _isnum(inds[1]):
                    li = list(range(int(inds[0]), int(inds[1]) + 1, 1))
                    li = [str(e) for e in li]
                    newvar.extend(li)
            else:
                newvar.append(el)
        var = newvar
    return var


def main(argv):
    conf = ''
    fieldpoints = []
    searchlist = []
    notlist = []
    l = False
    channels = []
    begin = datetime.utcnow() - timedelta(days=1)
    end = datetime.utcnow()
    outpath = ''
    format_type = 'PYCDF'
    SUPPORTED_FORMATS = ['PYCDF','CSV']
    jsonpath = ''
    statusdict = {}
    update = False
    debug = False
    usage = 'rcs_extractor.py -c <config> -f <fieldpoint> -s <search> -n not-search -l <list> -a <channels> -b <begin> -e <end> -o <output> -t <format> -j <statusjson> -u <update>'
    try:
        opts, args = getopt.getopt(argv, "hc:f:s:n:la:b:e:o:t:j:uD",
                                   ["config=", "fieldpoint=", "search=", "notsearch=", "list=", "channels=", "begin=",
                                    "end=", "output=", "format=", "statusjson=", "update=", "debug=", ])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('-------------------------------------')
            print('Description:')
            print('-------------------------------------')
            print('Usage:')
            print(usage)
            print('-------------------------------------')
            print('Options:')
            print('-c (required) : provide a path to a configuartion file')
            print('-f            : fieldpoints (or list of fieldpoints)')
            print('-s            : search term')
            print('-n            : not-search term')
            print('-l            : list existing sensors to stdout')
            print('-a            : define channel ranges or list of channels (81-85,106)')
            print('-b            : begin of timeseries')
            print('-e            : end of timeseries')
            print('-o            : output path')
            print('-t            : output format')
            print('-j            : output of status data as defined in statusjson - provide path to statusjson')
            print('-u            : update cobsdb database')
            print('-------------------------------------')
            print('Example:')
            sys.exit()
        elif opt in ("-c", "--config"):
            conf = os.path.abspath(arg)
        elif opt in ("-f", "--fieldpoints"):
            fieldpoints = _make_list(arg)
        elif opt in ("-s", "--search"):
            searchlist = _make_list(arg)
        elif opt in ("-n", "--notsearch"):
            notlist = _make_list(arg)
        elif opt in ("-l", "--list"):
            l = True
        elif opt in ("-a", "--channels"):
            channels = _make_list(arg)
        elif opt in ("-b", "--begin"):
            begin = _get_datetime(arg)
        elif opt in ("-e", "--end"):
            end = _get_datetime(arg)
        elif opt in ("-o", "--output"):
            outpath = os.path.abspath(arg)
        elif opt in ("-t", "--format"):
            format_type = arg
        elif opt in ("-j", "--statusjson"):
            jsonpath = os.path.abspath(arg)
        elif opt in ("-u", "--update"):
            update = True
        elif opt in ("-D", "--debug"):
            print ("DEBUG mode selected")
            debug = True

    if not conf:
        print("rcs_extractor: configuration file is required")
        # sys.exit()
    if not begin or not end or begin >= end:
        print("rcs_extractor: check your input dates")
        sys.exit()

    if not format_type in SUPPORTED_FORMATS:
        print("rcs_extractor: selected outputformat is not supported")
        print("               choose one of {}".format(SUPPORTED_FORMATS))
        sys.exit()

    # 1) read config
    config = {}
    config['rcshost'] = "cobs@138.22.188.45"
    if not config.get('rcshost'):
        print("rcs_extractor: RCS host not defined in config file - aborting")
        sys.exit()

    # 2) create "rcsListIni" call        
    fpdict = _full_list_call(config.get('rcshost'), debug=debug)
    selected = _fpdict_extract(fpdict, fieldpoints=fieldpoints, channels=channels, searchlist=searchlist, notlist=notlist, debug=debug)
    if debug:
        count = 0
        for el in selected:
            for e in selected.get(el):
                count = count +1
        print ("rcs_extractor: Amount of signals in dictionary:", count)

    if l:
        for el in selected:
            print ("Fieldpoint:", el) 
            print ("-------------------------") 
            for e in selected.get(el):
                print ("Channel {}: {}".format(e, selected.get(el).get(e)))
        
    if update:
        print("Updating database information table contents")
    if outpath:
        print("data output selected")
        # Only supported for single fieldpoint and less then 15 channels
        # create rcs2csv call - save to temporary and then create selected format
        for fp in selected:
            channellist = [e for e in selected.get(fp)]
            print ("Fieldpoint:", fp, channellist)
            sensid = "RCS{}_{}_0001".format(fp,len(channellist))
            datastream = _create_fp_data_call(config.get('rcshost'), fp, channellist, sensid, begin, end, headerdict={}, debug=debug)
        import magpy.mpplot as mp
        mp.plot(datastream)


if __name__ == "__main__":
    main(sys.argv[1:])
