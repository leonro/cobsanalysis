#!/usr/bin/env python

"""
Uploading and Checking AutoDIF and Basevalue data
"""

from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred

import itertools
from threading import Thread
import json

from martas import martaslog as ml
logpath = '/var/log/magpy/mm-dp-autodif.log'
sn = 'SAGITTARIUS'
statusmsg = {}

dbpasswd = mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print ("Connecting to primary MARCOS...")
    db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")
except:
    print "... failed"
    try:
        # Test MARCOS 2
        print ("Connecting to secondary MARCOS...")
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
    except:
        print ("... failed -- aborting")
        sys.exit()

endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=1),"%Y-%m-%d")
date = datetime.strftime(endtime,"%Y-%m-%d")
dbdateformat = "%Y-%m-%d %H:%M:%S.%f"
today1 = datetime.strftime(endtime,"%Y%m%d")
yesterd1 = datetime.strftime(endtime-timedelta(days=1),"%Y%m%d")
yesterd2 = datetime.strftime(endtime-timedelta(days=1),"%Y-%m-%d")
weekago = datetime.strftime(endtime-timedelta(days=6),"%Y-%m-%d")

currentvaluepath = '/srv/products/data/current.data'
autodifpath = '/media/autodif/Data/'
pierlist = ['A16','A7','H1']

zamgcred = 'zamg'
zamgaddress=mpcred.lc(zamgcred,'address')
zamguser=mpcred.lc(zamgcred,'user')
zamgpasswd=mpcred.lc(zamgcred,'passwd')
zamgport=mpcred.lc(zamgcred,'port')


part1 = True  # Get primary instuments 
part2 = True  # Upload autodif raw data to FTP
part3 = True  # Analysze raw data (if part1 is successful)

if part1:
    # 1. Get variometer and scalarinstrument
    print ("Step 1: Getting primary instruments") 
    varioinst = ''
    scalainst = ''
    name = "{}-DataProducts-Autodif-Step1".format(sn)
    #try:
    ok = True
    if ok:
        if os.path.isfile(currentvaluepath):
            # read log if exists and exentually update changed information
            # return changes
            with open(currentvaluepath, 'r') as file:
                fulldict = json.load(file)
                valdict = fulldict.get('magnetism')
            varioinst = valdict.get('primary vario','')[0]
            scalainst = valdict.get('primary scalar','')[0]
    #except:
    #   pass
    if varioinst == "" or scalainst == "":
        statusmsg[name] = 'primary instruments not fully assigned - skipping AutoDIF analysis'
        part3 = False
    else:
        statusmsg[name] = 'fine'
        print ("Instruments", varioinst, scalainst)

if part2:
    """
    Upload autodif files 
    """
    #p1start = datetime.utcnow()
    for da in [today1,yesterd1]:
         # Send in background mode
        Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(autodifpath,da+'.txt'),'ftppath':'/data/magnetism/wic/autodif/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/autodif-transfer.log'}).start()

    #p1end = datetime.utcnow()
    #print "-----------------------------------"
    #print "Part1 needs", p1end-p1start
    #print "-----------------------------------"


if part3 and part1:
    """
    Check content and analysis of AUTODIF and other piers
    """
    p2start = datetime.utcnow()
    print ("Instruments", varioinst, scalainst)

    for pier in pierlist:
        absr = readDB(db,'BLV_'+varioinst[:-5]+'_'+scalainst[:-5]+'_'+pier, starttime = endtime-timedelta(days=100))
        blvflaglist = db2flaglist(db,'BLV_'+varioinst[:-5]+'_'+scalainst[:-5]+'_'+pier)
        absr = absr.flag(blvflaglist)
        absr = absr.remove_flagged()
        absr = absr._drop_nans('dx')
        absr = absr._drop_nans('dy')
        absr = absr._drop_nans('dz')
        bh, bhstd = absr.mean('dx',meanfunction='median',std=True)
        bd, bdstd = absr.mean('dy',meanfunction='median',std=True)
        bz, bzstd = absr.mean('dz',meanfunction='median',std=True)

        print (" Basevalues for last 100 days:")
        print (" Delta H = {a} +/- {b}".format(a=bh, b=bhstd))
        print (" Delta D = {a} +/- {b}".format(a=bd, b=bdstd))
        print (" Delta Z = {a} +/- {b}".format(a=bz, b=bzstd))
        
        print ("Running Analysis jobs ... ")
        analysisname1 = 'data_threshold_amount_BLV_'+varioinst[:-5]+'_'+scalainst[:-5]+'_'+pier
        analysisname2 = 'data_actuality_time_BLV_'+varioinst[:-5]+'_'+scalainst[:-5]+'_'+pier
        analysisname3 = 'data_threshold_base_BLV_'+varioinst[:-5]+'_'+scalainst[:-5]+'_'+pier
        if pier in ['A16']:
            solldatum = endtime - timedelta(days=2)
            #analysisdict.check({analysisname1: [absr.length()[0],'>',10]})
        elif pier in ['H1','H2','H3','A7']:
            solldatum = endtime - timedelta(days=40)
            #analysisdict.check({analysisname1: [absr.length()[0],'>',3]})
        else:
            solldatum = endtime - timedelta(days=14)
            #analysisdict.check({analysisname1: [absr.length()[0],'>',10]})
        istdatum = num2date(absr.ndarray[0][-1])
        #analysisdict.check({analysisname2: [istdatum,'>',solldatum]})
        #analysisdict.check({analysisname3: [bhstd+bzstd,'<',5]})
    p2end = datetime.utcnow()
    print "-----------------------------------"
    print "Part2 needs", p2end-p2start
    print "-----------------------------------"

