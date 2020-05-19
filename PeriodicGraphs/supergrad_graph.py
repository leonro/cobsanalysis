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
from inspect import stack, getmodule # ADDED BY N. KOMPEIN 2018-09-27 due to missing info on start of supergrad_graph.py in logfile

## New Logging features 
from martas import martaslog as ml
logpath = '/var/log/magpy/mm-per-supergrad.log'
#import socket
#sn = socket.gethostname().upper()
sn = 'SAGITTARIUS' # servername ### Get that automatically??
statusmsg = {}
name = "{}-PeriodicPlot-supergrad".format(sn)

# ####################
#  Importing database
# ####################

dbpasswd = mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print ("Connecting to primary MARCOS...")
    db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")
    print ("... DB on vega connected")
except:
    print ("... failed")
    try:
        # Test MARCOS 2
        print ("Connecting to secondary MARCOS...")
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
        print ("... DB on sol connected")
    except:
        print ("... failed -- aborting")
        sys.exit()
print ("... success")
# please note: DB availability is monitored elsewhere


# ####################
#  Activate monitoring
# #################### 
# discontinued
try:
    from magpy.opt.analysismonitor import *
    analysisdict = Analysismonitor(logfile='/home/cobs/ANALYSIS/Logs/AnalysisMonitor_cobs.log')
    analysisdict = analysisdict.load()
except:
    print ("Analysis monitor failed")
    pass


# ####################
#  Basic definitions
# #################### 
failure = False
path2log = '/home/cobs/ANALYSIS/Logs/supergrad_graph.log'

endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=4), "%Y-%m-%d")
starttimedata = endtime-timedelta(days=3)
date = datetime.strftime(endtime,"%Y-%m-%d")

print ("-----------------------------------")
print ("Starting Supergrad analysis script:")
print ("-----------------------------------")
print ('datetime of processing is: {}'.format(endtime)) 
#print 'calling script is: ', stack()[0] # ADDED BY N. KOMPEIN 2018-09-27 due to missing info on start of supergrad_graph.py in logfile

nsmissing = False
ewmissing = False
vmissing = False

try:
    try:
        print (" - Reading Status information")
        nsstat = readDB(db,'GP20S3NSstatus_012201_0001_0001',starttime=starttimedata)
    except:
        print ("...no status information from NS-System")
        statusmsg[name] = 'Supergrad NS-Statusinfo failed'
    try:
        ewstat = readDB(db,'GP20S3EWStatus_111201_0001_0001',starttime=starttimedata)
    except:
        print ("...no status information from EW-System")
        statusmsg[name] = 'Supergrad EW-Statusinfo failed'
    try:
        vstat = readDB(db,'GP20S3VStatus_911005_0001_0001',starttime=starttimedata)
    except:
        print ("...no status information from V-System")
        statusmsg[name] = 'Supergrad V-Statusinfo failed'
    
    print (" - Reading data files")
    try:
        ns = readDB(db,'GP20S3NS_012201_0001_0001',starttime=starttimedata)
        print( 'NS-gradient...read')
        if( ns.length()[0] == 0):
            nsmissing = True
    except:
        print( 'NS-gradient...no data')
        statusmsg[name] = 'Supergrad NS-dataread failed'
        nsmissing = True
    try:
        ew = readDB(db,'GP20S3EW_111201_0001_0001',starttime=starttimedata)
        print( 'EW-gradient...read')
        if( ew.length()[0] == 0):
            ewmissing = True
    except:
        print( 'EW-gradient...no data')
        statusmsg[name] = 'Supergrad EW-dataread failed'
        ewmissing = True
    try:
        v = readDB(db,'GP20S3V_911005_0001_0001',starttime=starttimedata)
        print( 'V-gradient...read')
        if( v.length()[0] == 0):
            vmissing = True
    except:
        print( 'V-gradient...no data')
        statusmsg[name] = 'Supergrad V-dataread failed'
        vmissing = True
    
    try:
        if ns.length()[0] > 0 and ew.length()[0] > 0 and v.length()[0] > 0:
            statusmsg[name] = 'Data available'
        else:
            #statusmsg[name] = 'Necessary data not available'
            print( 'At least one dataset is missing...trying to move on...')
            statusmsg[name] = 'At least one dataset is missing...trying to move on...'
    except:
        pass
    
    # Statustext:
    print (" - Preparing status information")
    try:
        line1 = r'$\mathrm{T_{s1}:\ %s^\circ C,\ T_{s2}:\ %s^\circ C,\ T_{s3}:\ %s^\circ C}$' % (nsstat.ndarray[1][-1],nsstat.ndarray[2][-1],nsstat.ndarray[3][-1])
        line2 = r'$\mathrm{L_1:\ %s\mu A,\ L_2:\ %s\mu A,\ L_3:\ %s\mu A}$' % (nsstat.ndarray[12][-1],nsstat.ndarray[13][-1],nsstat.ndarray[14][-1])
        statusmsg[name] = 'Status lines generated'
    except:
        line1 = ""
        line2 = ""
        statusmsg[name] = 'Status line generation failed'
    #print line1
    goodlvl = 12500.0 # MAXIMUM DIFFERENCE WHICH IS ASSUMED TO BE A REAL DIFFERENCE NOT BASED ON EQUIPMENT MALFUNCTION N. KOMPEIN 2019-05-15
    print (" - Removing prominent outliers")
    if( not ewmissing):
        ew = ew.flag_outlier(threshold=5.0,timerange=timedelta(minutes=10))
        #flew = ew.flag_range(keys=['dy'], above = goodlvl, below = -goodlvl)
        #ew = ew.flag(flew)
        ew = ew.remove_flagged()
        print (" - EW-outliers removed...")
    if( not nsmissing):
        ns = ns.flag_outlier(threshold=5.0,timerange=timedelta(minutes=10))
        #flns = ns.flag_range(keys=['dy'], above = goodlvl, below = -goodlvl)
        #ns = ns.flag(flns)
        ns = ns.remove_flagged()
        print (" - NS-outliers removed...")
    if( not vmissing):
        v = v.flag_outlier(threshold=5.0,timerange=timedelta(minutes=10))
        #flv = v.flag_range(keys=['dz'], above = goodlvl, below = -goodlvl)
        #v = v.flag(flv)
        v = v.remove_flagged()
        print (" - V-outliers removed...")
    
    #print ("Here I found", nsstat.ndarray[14][-1])
    #print ("Here I found", ewstat.ndarray)
    #print ("Here I found", vstat.ndarray)
    
    print (" - Checking threshold values - removed ...")
    
    """
    try:
        analysisdict.check({'data_threshold_t1_GP20S3EW_111201_0001': [float(ewstat.ndarray[1][-1]),'>',30]})
        analysisdict.check({'data_threshold_t2_GP20S3EW_111201_0001': [float(ewstat.ndarray[2][-1]),'>',30]})
        analysisdict.check({'data_threshold_t3_GP20S3EW_111201_0001': [float(ewstat.ndarray[3][-1]),'>',30]})
        analysisdict.check({'data_threshold_l1_GP20S3EW_111201_0001': [float(ewstat.ndarray[12][-1]),'>',2.3]})
        analysisdict.check({'data_threshold_l2_GP20S3EW_111201_0001': [float(ewstat.ndarray[13][-1]),'>',2.3]})
        analysisdict.check({'data_threshold_l3_GP20S3EW_111201_0001': [float(ewstat.ndarray[14][-1]),'>',2.3]}) 
        analysisdict.check({'data_threshold_f1_GP20S3EW_111201_0001': [abs( float(ew.ndarray[1][-1])),'>',30000000]}) # abs added by N KOMPEIN 2018-04-16
        analysisdict.check({'data_threshold_f2_GP20S3EW_111201_0001': [abs( float(ew.ndarray[2][-1])),'>',30000000]}) # abs added by N KOMPEIN 2018-04-16
        analysisdict.check({'data_threshold_f3_GP20S3EW_111201_0001': [abs( float(ew.ndarray[3][-1])),'>',30000000]}) # abs added by N KOMPEIN 2018-04-16
        analysisdict.check({'data_threshold_t1_GP20S3NS_012201_0001': [float(nsstat.ndarray[1][-1]),'>',30]})
        analysisdict.check({'data_threshold_t2_GP20S3NS_012201_0001': [float(nsstat.ndarray[2][-1]),'>',30]})
        analysisdict.check({'data_threshold_t3_GP20S3NS_012201_0001': [float(nsstat.ndarray[3][-1]),'>',30]})
        analysisdict.check({'data_threshold_l1_GP20S3NS_012201_0001': [float(nsstat.ndarray[12][-1]),'>',2.3]})
        analysisdict.check({'data_threshold_l2_GP20S3NS_012201_0001': [float(nsstat.ndarray[13][-1]),'>',2.3]})
        analysisdict.check({'data_threshold_l3_GP20S3NS_012201_0001': [float(nsstat.ndarray[14][-1]),'>',2.3]})
        analysisdict.check({'data_threshold_f1_GP20S3NS_012201_0001': [abs( float(ns.ndarray[1][-1])),'>',30000000]}) # abs added by N KOMPEIN 2018-04-16
        analysisdict.check({'data_threshold_f2_GP20S3NS_012201_0001': [abs( float(ns.ndarray[2][-1])),'>',30000000]}) # abs added by N KOMPEIN 2018-04-16
        analysisdict.check({'data_threshold_f3_GP20S3NS_012201_0001': [abs( float(ns.ndarray[3][-1])),'>',30000000]}) # abs added by N KOMPEIN 2018-04-16
        analysisdict.check({'data_threshold_t1_GP20S3V_911005_0001': [float(vstat.ndarray[1][-1]),'>',30]})
        analysisdict.check({'data_threshold_t2_GP20S3V_911005_0001': [float(vstat.ndarray[2][-1]),'>',30]})
        analysisdict.check({'data_threshold_t3_GP20S3V_911005_0001': [float(vstat.ndarray[3][-1]),'>',30]})
        analysisdict.check({'data_threshold_l1_GP20S3V_911005_0001': [float(vstat.ndarray[12][-1]),'>',2.3]})
        analysisdict.check({'data_threshold_l2_GP20S3V_911005_0001': [float(vstat.ndarray[13][-1]),'>',2.3]})
        analysisdict.check({'data_threshold_l3_GP20S3V_911005_0001': [float(vstat.ndarray[14][-1]),'>',2.3]})
        analysisdict.check({'data_threshold_f1_GP20S3V_911005_0001': [abs( float(v.ndarray[1][-1])),'>',30000000]}) # abs added by N KOMPEIN 2018-04-16
        analysisdict.check({'data_threshold_f2_GP20S3V_911005_0001': [abs( float(v.ndarray[2][-1])),'>',30000000]}) # abs added by N KOMPEIN 2018-04-16
        analysisdict.check({'data_threshold_f3_GP20S3V_911005_0001': [abs( float(v.ndarray[3][-1])),'>',30000000]}) # abs added by N KOMPEIN 2018-04-16
    except:
        print ("Found error while creating analysisdict")
    """

    print (" - Filtering data for plot")
    
    if( not ewmissing):
        ew = ew.filter(filter_width=timedelta(minutes=1))
        print (" - EW-filtered to 1min-data...")
    if( not nsmissing):
        ns = ns.filter(filter_width=timedelta(minutes=1))
        print (" - NS-filtered to 1min-data...")
    if( not vmissing):
        v = v.filter(filter_width=timedelta(minutes=1))
        print (" - V-filtered to 1min-data...")
    
    # providing some Info on content
    #print data._get_key_headers()
    
    # Change to 
    #mp.plot(ns,['y','z','dy'], bgcolor = '#d5de9c', gridcolor = '#316931',fill=['x','y','z'],confinex=True,noshow=True,plottitle='East-West system')
    #ns.header['col-y'] = 'E-W' # - TO CHANGE LABELS ON PLOT
    #mp.plot(ns,['y','x','dz'], gridcolor = '#316931',fill=['x','y','z'],confinex=True,noshow=True,plottitle='East-West system')
###########################
# ACTIVATED DUE TO E-W AXES PROBLEM SOLVING BY Niko Kompein 7.2.2018
###########################
    #goodlvl = 7500.0 # MAXIMUM DIFFERENCE WHICH IS ASSUMED TO BE A REAL DIFFERENCE NOT BASED ON EQUIPMENT MALFUNCTION N. KOMPEIN 2019-05-15
    if( not ewmissing):
        ewgraph = ew.extract( 'dy', goodlvl, '<=') 
        ewgraph = ewgraph.extract( 'dy', -goodlvl, '>=')
        print( 'ewgraph with length {} extracted'.format( np.shape( ewgraph['dy'])[0]))
        ewgoodbool = True
        if( np.shape( ewgraph['dy'])[0] == 0):
            ewgraph = ew.extract( 'dx', goodlvl, '<=') # CAUSE ew['dy'] is empty for given goodlvl
            ewgraph = ewgraph.extract( 'dx', -goodlvl, '>=') # CAUSE ew['dy'] is empty for given goodlvl
            print( 'ewgraph with length {} extracted'.format( np.shape( ewgraph['dx'])[0]))	
            ewgoodbool = False
    else:
        ewgoodbool = False
    
    if( not nsmissing):
        nsgraph = ns.extract( 'dy', goodlvl, '<=')
        nsgraph = nsgraph.extract( 'dy', -goodlvl, '>=')
        print( 'nsgraph with length {} extracted'.format( np.shape( nsgraph['dy'])[0]))
        nsgoodbool = True
        if( np.shape( nsgraph['dy'])[0] == 0):
            nsgraph = ns.extract( 'dx', goodlvl, '<=') # CAUSE ns['dy'] is empty for given goodlvl
            nsgraph = nsgraph.extract( 'dx', -goodlvl, '>=') # CAUSE ns['dy'] is empty for given goodlvl
            print( 'nsgraph with length {} extracted'.format( np.shape( nsgraph['dx'])[0]))
            nsgoodbool = False
    else:
        nsgoodbool = False
    
    if( not vmissing):
        vgraph = v.extract( 'dz', goodlvl, '<=') # CAUSE dz is B-TA, dy = TB-B
        vgraph = vgraph.extract( 'dz', -goodlvl, '>=')
        print( 'vgraph with length {} extracted'.format( np.shape( vgraph['dz'])[0]))
        vgoodbool = True
        if( np.shape( vgraph['dz'])[0] == 0):
            vgraph = v.extract( 'dy', goodlvl, '<=') # CAUSE v['dz'] is empty for given goodlvl
            vgraph = vgraph.extract( 'dy', -goodlvl, '>=') # CAUSE v['dz'] is empty for given goodlvl
            print( 'vgraph with length {} extracted'.format( np.shape( vgraph['dy'])[0]))
            vgoodbool = False
    else:
        vgoodbool = False
    
    if( not ewmissing):
        if( ewgoodbool):
            testarrayew = np.array( ewgraph['dy'])
        else:
            testarrayew = np.array( ewgraph['dx'])
    if( not nsmissing):
        if( nsgoodbool):
            testarrayns = np.array( nsgraph['dy'])
        else:
            testarrayns = np.array( nsgraph['dx'])
    if( not vmissing):
        if( vgoodbool):
            testarrayv = np.array( vgraph['dz'])
        else:
            testarrayv = np.array( vgraph['dy'])
    
    if( not ewmissing):
        ewgoodind = np.where( np.logical_and( np.abs( testarrayew - np.nanmedian( testarrayew)) < goodlvl/ 10.0, ~np.isnan( testarrayew)))[0] # ONLY CALCULATE MINIMUM AND MAXIMUM LEVELS FOR PLOTS FROM 'ERROR-FREE' VALUES WHERE THE VARIATIONS ARROUND A GIVEN MEAN OF THE DIFFERENCES ARE BELOW goodlvl/10 pT N. KOMPEIN 2019-05-15
        ewleng = np.nanmax( np.shape( ewgoodind))
        devew = 3.0* np.nanstd( testarrayew[ewgoodind])#* float(ewleng)/ float( maxleng)
        avgew = np.nanmean( testarrayew[ewgoodind])
    else:
        ewleng = 0
        devew = 0.0
        avgew = 0.0
    
    if( not nsmissing):
        nsgoodind = np.where( np.logical_and( np.abs( testarrayns - np.nanmedian( testarrayns)) < goodlvl/ 10.0, ~np.isnan( testarrayns)))[0] # ONLY CALCULATE MINIMUM AND MAXIMUM LEVELS FOR PLOTS FROM 'ERROR-FREE' VALUES WHERE THE VARIATIONS ARROUND A GIVEN MEAN OF THE  DIFFERENCES ARE BELOW goodlvl/10 pT N. KOMPEIN 2019-05-15
        nsleng = np.nanmax( np.shape( nsgoodind))
        devns = 3.0* np.nanstd( testarrayns[nsgoodind])#* float(nsleng)/ float( maxleng)
        avgns = np.nanmean( testarrayns[nsgoodind])
    else:
        nsleng = 0
        devns = 0.0
        avgns = 0.0
    
    if( not vmissing):
        vgoodind = np.where( np.logical_and( np.abs( testarrayv - np.nanmedian( testarrayv)) < goodlvl/ 10.0, ~np.isnan( testarrayv)))[0] # ONLY CALCULATE MINIMUM AND MAXIMUM LEVELS FOR PLOTS FROM 'ERROR-FREE' VALUES WHERE THE VARIATIONS ARROUND A GIVEN MEAN OF THE DIFFERENCES ARE BELOW goodlvl/10 pT N. KOMPEIN 2019-05-15
        vleng = np.nanmax( np.shape( vgoodind))
        devv = 3.0* np.nanstd( testarrayv[vgoodind])#* float(vleng)/ float( maxleng)
        avgv = np.nanmean( testarrayv[vgoodind])
    else:
        vleng = 0
        devv = 0.0
        avgv = 0.0
    
    
    lenglist = [ewleng, nsleng, vleng]
    
    maxleng = np.nanmax( lenglist)
    dumpdata = DataStream()
    #dumpdata.ndarray[dumpdata.KEYLIST.index('time')] = dumpdata.ndarray[nsgraph.KEYLIST.index('time')]
    for el in lenglist:
        if( el == maxleng):
            dumpdata = nsgraph
    for k, el in enumerate( lenglist):
        if( k == 0):
            if( el == 0):
                ewgraph = dumpdata
        if( k == 1):
            if( el == 0):
                nsgraph = dumpdata
        if( k == 2):
            if( el == 0):
                vgraph = dumpdata
#    baddaybool = True
#    if( devv > 3.0* np.sqrt( devew* devns)):
#        testarrayv = np.array( v['dy'])
#        vgoodind = np.where( np.logical_and( np.abs( testarrayv - np.nanmedian( testarrayv)) < goodlvl/ 10.0, ~np.isnan( testarrayv)))[0]
#        devv = np.nanstd( testarrayv[vgoodind])
#        baddaybool = False
#    if( ~baddaybool):
#        avgv = np.nanmedian( testarrayv[vgoodind])
    print( 'Standard-dev EW: ', devew,' Standard-dev NS: ', devns, 'Standard-dev V: ', devv)
    print( 'mean EW: ', avgew,' mean NS: ', avgns, 'mean V: ', avgv)
    #mp.plotStreams([ns,ew,v],[['dx'],['dx'],['dx']], gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', noshow=True)
    if( ewgoodbool & nsgoodbool & vgoodbool):
        print('Three main differences are OK...')
        #mp.plotStreams([ns,ew,v],[['dx'],['dx'],['dx']], gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', specialdict = [{'dx': [avgns - 3.0* devns, avgns + 3.0* devns]}, {'dx': [avgew - 3.0* devew, avgew + 3.0* devew]}, {'dx': [avgv - 3.0* devv, avgv + 3.0* devv]}],noshow=True)
        mp.plotStreams([nsgraph,ewgraph,vgraph],[['dy'],['dy'],['dz']], gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', specialdict = [{'dy': [avgns - 3.0* devns, avgns + 3.0* devns]}, {'dy': [avgew - 3.0* devew, avgew + 3.0* devew]}, {'dz': [avgv - 3.0* devv, avgv + 3.0* devv]}],noshow=True)
    elif( ewgoodbool & nsgoodbool & ~vgoodbool):
        print('North-South difference is OK. East-West difference is OK. Vertical difference is BAD...')
        #mp.plotStreams([ns,ew,v],[['dx'],['dx'],['dy']], bgcolor = 'grey', gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', specialdict = [{'dx': [avgns - 3.0* devns, avgns + 3.0* devns]}, {'dx': [avgew - 3.0* devew, avgew + 3.0* devew]}, {'dy': [avgv - 3.0* devv, avgv + 3.0* devv]}],noshow=True)
        mp.plotStreams([nsgraph,ewgraph,vgraph],[['dy'],['dy'],['dy']], bgcolor = 'grey', gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', specialdict = [{'dy': [avgns - 3.0* devns, avgns + 3.0* devns]}, {'dy': [avgew - 3.0* devew, avgew + 3.0* devew]}, {'dy': [avgv - 3.0* devv, avgv + 3.0* devv]}],noshow=True)
    elif( ~ewgoodbool & nsgoodbool & vgoodbool):
        print('North-South difference is OK. East-West difference is BAD. Vertical difference is OK...')
        #mp.plotStreams([ns,ew,v],[['dx'],['dx'],['dy']], bgcolor = 'grey', gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', specialdict = [{'dx': [avgns - 3.0* devns, avgns + 3.0* devns]}, {'dx': [avgew - 3.0* devew, avgew + 3.0* devew]}, {'dy': [avgv - 3.0* devv, avgv + 3.0* devv]}],noshow=True)
        mp.plotStreams([nsgraph,ewgraph,vgraph],[['dy'],['dx'],['dz']], bgcolor = 'grey', gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', specialdict = [{'dy': [avgns - 3.0* devns, avgns + 3.0* devns]}, {'dx': [avgew - 3.0* devew, avgew + 3.0* devew]}, {'dz': [avgv - 3.0* devv, avgv + 3.0* devv]}],noshow=True)
    elif( ewgoodbool & ~nsgoodbool & vgoodbool):
        print('North-South difference is BAD. East-West difference is OK. Vertical difference is OK...')
        #mp.plotStreams([ns,ew,v],[['dx'],['dx'],['dy']], bgcolor = 'grey', gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', specialdict = [{'dx': [avgns - 3.0* devns, avgns + 3.0* devns]}, {'dx': [avgew - 3.0* devew, avgew + 3.0* devew]}, {'dy': [avgv - 3.0* devv, avgv + 3.0* devv]}],noshow=True)
        mp.plotStreams([nsgraph,ewgraph,vgraph],[['dy'],['dx'],['dz']], bgcolor = 'grey', gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', specialdict = [{'dy': [avgns - 3.0* devns, avgns + 3.0* devns]}, {'dx': [avgew - 3.0* devew, avgew + 3.0* devew]}, {'dz': [avgv - 3.0* devv, avgv + 3.0* devv]}],noshow=True)
    else:
        print('More thean two differnces are bad, fallback to worstcase scenario...')
        #mp.plotStreams([ns,ew,v],[['dx'],['dx'],['dy']], bgcolor = 'grey', gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', specialdict = [{'dx': [avgns - 3.0* devns, avgns + 3.0* devns]}, {'dx': [avgew - 3.0* devew, avgew + 3.0* devew]}, {'dy': [avgv - 3.0* devv, avgv + 3.0* devv]}],noshow=True)
        mp.plotStreams([nsgraph,ewgraph,vgraph],[['dx'],['dx'],['dy']], bgcolor = 'yellow', gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', specialdict = [{'dx': [avgns - 3.0* devns, avgns + 3.0* devns]}, {'dx': [avgew - 3.0* devew, avgew + 3.0* devew]}, {'dy': [avgv - 3.0* devv, avgv + 3.0* devv]}],noshow=True)
 

###########################
# ALTERNATIVE DEACTIVED UNTIL E-W GRADIENT PROBLEMS ARE FIXED BY Niko Kompein 7.2.2018
###########################
    #mp.plotStreams([ns,v],[['dx'],['dx']], gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), VERTICAL(pink)', noshow=True)
###########################
    #mp.plotStreams([ns,ew,vert],[['dy'],['dy'],['dy']],bgcolor='#d5de9c', gridcolor='#316931',fill=['dy'],confinex=True, fullday=True, opacity=0.7, plottitle='Field gradients (until %s)' % (datetime.utcnow().date()),noshow=True)
    #mp.plot(ns,['y','z','dy'], gridcolor = '#316931',fill=['x','y','z'],confinex=True,noshow=True,plottitle='North-South system')
    #mp.plotStreams([ns],[['dy']],bgcolor='#d5de9c', gridcolor='#316931',confinex=True, fullday=True, opacity=0.7, plottitle='Field gradients (until %s)' % (datetime.utcnow().date()),noshow=True)
    #maxval = max(ns.ndarray[KEYLIST.index('dz')])
    #minval = min(ns.ndarray[KEYLIST.index('dz')])
    #maxval = np.amax(np.concatenate(([maxval],[max(ew.ndarray[KEYLIST.index('dz')])]),axis=1))
    #maxval = np.amax(np.concatenate(([maxval],[max(v.ndarray[KEYLIST.index('dz')])]),axis=1))
    #minval = np.amin(np.concatenate(([minval],[min(ew.ndarray[KEYLIST.index('dz')])]),axis=1))
    #minval = np.amin(np.concatenate(([minval],[min(v.ndarray[KEYLIST.index('dz')])]),axis=1))
    #diff = maxval-minval
    #try:
    #    plt.text(nsstat.ndarray[0][0]+0.01,minval+0.3*diff,line1)
    #    plt.text(nsstat.ndarray[0][0]+0.01,minval+0.1*diff,line2)
    #except:
    #    pass
    #plt.show()
    #upload
    #plt.show()
    savepath = "/srv/products/graphs/magnetism/supergrad_%s.png" % date
    #savepath = "/home/cobs/ANALYSIS/PeriodicGraphs/tmpgraphs/supergrad_%s.png" % date
    plt.savefig(savepath)
    #mp.plot(ns,['y','z','dy'], gridcolor = '#316931',fill=['x','y','z'],confinex=True,noshow=True,plottitle='North-South system')
    #maxval = max(ns.ndarray[KEYLIST.index('dy')])
    #minval = min(ns.ndarray[KEYLIST.index('dy')])
    #diff = maxval-minval
    #plt.text(nsstat.ndarray[0][0]+0.01,minval+0.3*diff,line1)
    #plt.text(nsstat.ndarray[0][0]+0.01,minval+0.1*diff,line2)
    #savepath = "/home/cobs/ANALYSIS/PeriodicGraphs/tmpgraphs/supergradNS_%s.png" % date
    #plt.savefig(savepath)


    cred = 'cobshomepage'
    #address=mpcred.lc(cred,'address')
    user=mpcred.lc(cred,'user')
    passwd=mpcred.lc(cred,'passwd')
    #port=mpcred.lc(cred,'port')
    remotepath = 'zamg/images/graphs/magnetism/supergrad/'

    #scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
    #ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
    try:
        # to send with 664 permission use a temporary directory
        tmppath = "/tmp"
        tmpfile= os.path.join( tmppath, os.path.basename(savepath))
        from shutil import copyfile
        copyfile(savepath,tmpfile)
        scptransfer(tmpfile,'94.136.40.103:'+remotepath,passwd)
        os.remove(tmpfile)
        #scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
        analysisdict.check({'upload_homepage_supergradEWplot': ['success','=','success']})
    except:
        analysisdict.check({'upload_homepage_supergradEWplot': ['failure','=','success']})
        pass
    statusmsg[name] = 'Supergrad analysis successful'
except:
    failure = True
    statusmsg[name] = 'Supergrad analysis failed'

if not failure:
    analysisdict.check({'script_periodic_supergrad_graph': ['success','=','success']})
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
    print ("    supergrad_graph successfully finished     ")
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
else:
    analysisdict.check({'script_periodic_supergrad_graph': ['failure','=','success']})

martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)
