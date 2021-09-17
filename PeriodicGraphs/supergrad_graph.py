#!/usr/bin/env python

"""
Skeleton for graphs
--------------------------------------------------------------------------------------

DESCRIPTION
   Skeleton file for creating plots for a specific sensors/groups/etc.
PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods
PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -r range               :   int     :  default  2 (days)
    -s sensor              :   string  :  sensor or dataid
    -k keys                :   string  :  comma separated list of keys to be plotted
    -f flags               :   string  :  flags from other lists e.g. quakes, coil, etc
    -y style               :   string  :  plot style
    -l loggername          :   string  :  loggername e.g. mm-pp-tilt.log
    -e endtime             :   string  :  endtime (plots from endtime-range to endtime)

APPLICATION
    PERMANENTLY with cron:
        python webpage_graph.py -c /etc/marcos/analysis.cfg
    SensorID:
        python3 general_graph.py -c ../conf/wic.cfg -e 2019-01-15 -s GP20S3NSS2_012201_0001 -D
    DataID:
        python3 general_graph.py -c ../conf/wic.cfg -e 2019-01-15 -s GP20S3NSS2_012201_0001_0001 -D
"""


from magpy.stream import *
from magpy.database import *
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred
import io, pickle
import getopt
import pwd
import sys  # for sys.version_info()
import socket

import itertools
from threading import Thread
from subprocess import check_output   # used for checking whether send process already finished

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, Quakes2Flags, combinelists
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__










#>> EDIT >>>>>>>>>>>>>>>>>>>>>>>>
def CreateDiagram(config={}, endtime=datetime.utcnow(), dayrange=1, debug=False):
    """
    Skeleton
    """
    # basic parameter
    starttimedata = endtime-timedelta(days=dayrange)
    savepath = "/srv/products/graphs/tilt/tilt_%s.png" % date

    # READ data
    goodlvl = 12500
    savepath = "/srv/products/graphs/magnetism/supergrad_%s.png" % date

    nsmissing = False
    ewmissing = False
    vmissing = False

    # Read status information
    print (" - Reading Status information")
    try:
        nsstat = readDB(db,'GP20S3NSstatus_012201_0001_0001',starttime=starttimedata)
    except:
        print ("...no status information from NS-System")
    try:
        ewstat = readDB(db,'GP20S3EWStatus_111201_0001_0001',starttime=starttimedata)
    except:
        print ("...no status information from EW-System")
    try:
        vstat = readDB(db,'GP20S3VStatus_911005_0001_0001',starttime=starttimedata)
    except:
        print ("...no status information from V-System")

    # Read data files    
    print (" - Reading data files")
    try:
        ns = readDB(db,'GP20S3NS_012201_0001_0001',starttime=starttimedata)
        ns = ns.flag_outlier(threshold=5.0,timerange=timedelta(minutes=10))
        ns = ns.remove_flagged()
        print( 'NS-gradient...read')
        if not ns.length()[0] > 0:
            nsmissing = True
    except:
        print( 'NS-gradient...no data')
        statusmsg[name] = 'Supergrad NS-dataread failed'
        nsmissing = True
    try:
        ew = readDB(db,'GP20S3EW_111201_0001_0001',starttime=starttimedata)
        ew = ew.flag_outlier(threshold=5.0,timerange=timedelta(minutes=10))
        ew = ew.remove_flagged()
        print( 'EW-gradient...read')
        if not ew.length()[0] > 0:
            ewmissing = True
    except:
        print( 'EW-gradient...no data')
        statusmsg[name] = 'Supergrad EW-dataread failed'
        ewmissing = True
    try:
        v = readDB(db,'GP20S3V_911005_0001_0001',starttime=starttimedata)
        v = v.flag_outlier(threshold=5.0,timerange=timedelta(minutes=10))
        v = v.remove_flagged()
        print( 'V-gradient...read')
        if not v.length()[0] > 0:
            vmissing = True
    except:
        print( 'V-gradient...no data')
        statusmsg[name] = 'Supergrad V-dataread failed'
        vmissing = True
    
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

    print( 'Standard-dev EW: ', devew,' Standard-dev NS: ', devns, 'Standard-dev V: ', devv)
    print( 'mean EW: ', avgew,' mean NS: ', avgns, 'mean V: ', avgv)

    # MODIFY data


    # PLOT data
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
        try:
            mp.plotStreams([nsgraph,ewgraph,vgraph],[['dx'],['dx'],['dy']], bgcolor = 'yellow', gridcolor = '#316931',confinex=True, plottitle='Gradients: N-S(blue), E-W(green), VERTICAL(pink)', specialdict = [{'dx': [avgns - 3.0* devns, avgns + 3.0* devns]}, {'dx': [avgew - 3.0* devew, avgew + 3.0* devew]}, {'dy': [avgv - 3.0* devv, avgv + 3.0* devv]}],noshow=True)
        except:
            print (" ----> and even this case failed")
            noplot = True
 

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


    if not debug:
        print (" ... saving now")
        plt.savefig(savepath)
        print("Plot successfully saved to {}.".format(savepath))
    else:
        print (" ... debug mode - showing plot")
        plt.show()

    return True

#<<<<<<<<<<<<<<<<<<<<<<<< EDIT <<


def main(argv):
    version = __version__
    configpath = ''
    statusmsg = {}
    path=''
    dayrange = 1
    debug=False
    endtime = datetime.utcnow()


    try:
        opts, args = getopt.getopt(argv,"hc:r:e:l:D",["config=","range=","endtime=","debug=",])
    except getopt.GetoptError:
        print ('job.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- general_graph.py will plot sensor data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python general_graph.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-r            : range in days')
            print ('-e            : endtime')
            print ('-------------------------------------')
            print ('Application:')
            print ('python general_graph.py -c /etc/marcos/analysis.cfg')
            print ('python general_graph.py -c /etc/marcos/analysis.cfg')
            print ('# debug run on my machine')
            print ('python3 general_graph.py -c ../conf/wic.cfg -s debug -k x,y,z -f none -D')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-r", "--range"):
            # range in days
            dayrange = int(arg)
        elif opt in ("-e", "--endtime"):
            # endtime of the plot
            endtime = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    if debug:
        print ("Running ... graph creator version {}".format(version))

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check general_graph.py -h for more options and requirements')
        sys.exit(0)

    if endtime:
        try:
            endtime = DataStream()._testtime(endtime)
        except:
            print ("Endtime could not be interpreted - Aborting")
            sys.exit(1)
    else:
        endtime = datetime.utcnow()

    #>> EDIT >>>>>>>>>>>>>>>>>>>>>>>>
    newloggername = 'mm-pp-myplot'
    category = "MyPlot"
    #<<<<<<<<<<<<<<<<<<<<<<<< EDIT <<

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category=category, job=os.path.basename(__file__), newname=newloggername, debug=debug)
    monitorname = "{}-plot".format(config.get('logname'))

    print ("3. Connect to databases")
    config = ConnectDatabases(config=config, debug=debug)

    #try:
    #    print ("4. Read and Plot method")
    success = CreateDiagram(config=config, endtime=endtime, dayrange=dayrange, debug=debug)
    #    statusmsg[namecheck1] = "success"
    #     if not success:
    #         statusmsg 
    #except:
    #    statusmsg[namecheck1] = "failure"

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])


