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
    starttime = endtime-timedelta(days=dayrange)
    savepath = "/srv/products/graphs/tilt/tilt_%s.png" % date

    # READ data
    try:
        dscovr_plasma = read(os.path.join('/srv/archive/external/esa-nasa','dscovr','plasma-3-day.json'))
        dscovr_mag = read(os.path.join('/srv/archive/external/esa-nasa','dscovr','mag-3-day.json'))
        #dscovr_plasma = read("http://services.swpc.noaa.gov/products/solar-wind/plasma-3-day.json")
        #dscovr_mag = read("http://services.swpc.noaa.gov/products/solar-wind/mag-3-day.json")
    except Exception as e:
        print("Reading data failed ({})!".format(e))
        return False

    try:
        kp = read(path_or_url=os.path.join('/srv/archive/external/gfz','kp','gfzkp*'))
        kp = kp.trim(starttime=starttime,endtime=endtime)
    except Exception as e:
        print("Reading data failed ({})!".format(e))
        return False

    # MODIFY data
    startstr = num2date(dscovr_mag.ndarray[0][0])
    endstr = num2date(dscovr_mag.ndarray[0][-1])
    print("Plotting from %s to %s" % (startstr, endstr))

    dscovr_plasma.header['col-var1'] = 'Density'
    dscovr_plasma.header['unit-col-var1'] = 'p/cc'
    dscovr_plasma.header['col-var2'] = 'Speed'
    dscovr_plasma.header['unit-col-var2'] = 'km/s'
    dscovr_mag.header['col-z'] = 'Bz'
    dscovr_mag.header['unit-col-z'] = 'nT'


    # PLOT data
    mp.plotStreams([kp, dscovr_mag, dscovr_plasma],[['var1'], ['z'],['var1','var2']],confinex=True,bartrange=0.06,symbollist=['z','-','-','-'],specialdict = [{'var1': [0,9]}, {}, {}],plottitle = "Solar and global magnetic activity (GFZ + DSCOVR data)",outfile=os.path.join(solarpath),noshow=True)

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


