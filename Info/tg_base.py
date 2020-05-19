#!/usr/bin/env python
# coding=utf-8

"""
MagPy - Weekly baseline/value information  
"""
from __future__ import print_function
from __future__ import unicode_literals

# Define packges to be used (local refers to test environment) 
# ------------------------------------------------------------
from magpy.stream import *   
from magpy.database import *
import magpy.mpplot as mp
import magpy.opt.cred as mpcred
from pickle import load as pload
import telegram_send
from os import listdir
from os.path import isfile, join

# Connect to database 
# ------------------------------------------------------------
dbpasswd = mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print("Connecting to primary MARCOS...")
    db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")
    print("success")
except:
    print("... failed")
    try:
        print("Connecting to secondary MARCOS...")
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
        print("success")
    except:
        sys.exit()


# SOME DEFINITIONS:
dipath = '/srv/archive/WIC/DI'
analyzepath = os.path.join(dipath,'analyze')
datapath = os.path.join(dipath,'data')
priminst = '/home/cobs/ANALYSIS/Logs/primaryinst.pkl'
plotdir = '/home/cobs/ANALYSIS/Info/plots'
endtime = datetime.utcnow()
starttime = endtime-timedelta(days=380)
pier = "A2"
caption = ''

# 1. get primary instruments:
# ###########################
lst = pload(open(priminst,'rb'))
print ("PRIMARY INSTRUMENTS: {}".format(lst))


# 2. define BLV filename
# ###########################
blvname = "BLVcomp_"+lst[0][:-5]+"_"+lst[1][:-5]+"_"+pier+".txt"
blvdata = os.path.join(datapath,blvname)
print ("BASEVALUE SOURCE: {}".format(blvdata))

# 3. Read BLV fiel and create BLV plot for the last year
# ###########################
absresult = read(blvdata,starttime=starttime,endtime=endtime)
try:
    blvflagname = blvname.replace("comp","").replace(".txt","")
    flags = db2flaglist(db,blvflagname)
    print (len(flags))
    if len(flags) > 0:
        absresult = absresult.flag(flags)
        absresult = absresult.remove_flagged()
except:
    print ("flagging failed")
try:
    absresult = absresult._drop_nans('dx')
    absresult = absresult._drop_nans('dy')
    absresult = absresult._drop_nans('dz')
    func = absresult.fit(['dx','dy','dz'],fitfunc='spline', knotstep=0.3)
    mp.plot(absresult,['dx','dy','dz'],symbollist=['o','o','o'],padding=[2.5,0.005,2.5],function=func,plottitle="{}: {} and {}".format(pier,lst[0][:-5],lst[1][:-5]),outfile=os.path.join(plotdir,'basegraph.png'))
    caption = "{}: Basevalues and adopted baseline".format(datetime.strftime(endtime,"%Y-%m-%d")) 
except:
    caption = "Not enough data points for creating new baseline graph"
    pass

# 4. read file list of *.txt files remaining in DI/analyse
# ###########################
onlyfiles = [f for f in listdir(analyzepath) if isfile(join(analyzepath, f))]
print ("FAILED ANALYSES: {}".format(onlyfiles))
failedmsg = ''
if len(onlyfiles) > 0:
    failedmsg = '*Failed analyses:*\n'.format(datetime.strftime(endtime,"%Y-%m-%d %H:%M"))
    for f in onlyfiles:
        failedmsg += '{}\n'.format(f)
    failedmsg += 'at *{}*'.format(datetime.strftime(endtime,"%Y-%m-%d %H:%M"))
    failedmsg = failedmsg.replace("_","")

print (failedmsg)

# 5. send all info
# ###########################
with open(os.path.join(plotdir,'basegraph.png'), "rb") as f:
    telegram_send.send(images=[f],captions=[caption],conf="/home/cobs/ANALYSIS/Info/conf/tg_base.cfg",parse_mode="markdown")
if not failedmsg == '':
    telegram_send.send(messages=[failedmsg],conf="/home/cobs/ANALYSIS/Info/conf/tg_base.cfg",parse_mode="markdown")

print ("tg_base successfully finished")

