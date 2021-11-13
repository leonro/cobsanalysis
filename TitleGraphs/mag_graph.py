#!/usr/bin/env python

"""
DESCRIPTION
   Title graph with geomagnetic information

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"
    -s starttime           :   date    :  startdate for analysis
                                          default "datetime.utcnow() - 4 days"

APPLICATION
    PERMANENTLY with cron:
        python mag_graph.py -c /etc/marcos/analysis.cfg
"""

from magpy.stream import *
from magpy.database import *
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred

import urllib.request
import shutil

import getopt
import pwd
import socket
import sys  # for sys.version_info()

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, getstringdate, GetPrimaryInstruments
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__


def mag_graph(db,config={},starttime=datetime.utcnow()-timedelta(days=3),endtime=datetime.utcnow(), debug=False):

    msg = 'mag graph successful'
    variosens = config.get('primaryVario')
    scalarsens = config.get('primaryScalar')
    varioinst = config.get('primaryVarioInst')
    scalarinst = config.get('primaryScalarInst')

    if debug:
        print ('Getting Solare image')
        print ('TODO Move that job to aldebaran')

    sohopath = '/srv/archive/external/esa-nasa/soho/'
    sohoname = 'EIT304_latest.jpg'
    sohonewname = 'EIT304_{}.jpg'.format(datetime.strftime(datetime.utcnow(),"%Y-%m-%d_%H"))
    if debug:
        print ("New name:", sohonewname)
    try:
        urllib.request.urlretrieve("https://sohowww.nascom.nasa.gov/data/realtime/eit_304/512/latest.jpg", sohoname)
    except:
        msg = "Latest soho image not available"
    try:
        if not os.path.exists(sohopath):
            os.makedirs(sohopath)
        shutil.copyfile(sohoname, os.path.join(sohopath,sohonewname))
    except:
        print ("copying failed")

    print ('Reading data from primary instrument')
    try:
        data = readDB(db,varioinst,starttime=starttime)
        kvals = data.k_fmi(k9_level=500)
    except:
        msg = 'problem reading data'

    img = imread("/home/cobs/ANALYSIS/TitleGraphs/EIT304_latest.jpg")

    print ('Plotting streams')
    fig = mp.plotStreams([kvals,data],[['var1'],['x']],specialdict = [{'var1':[0,18]},{}],symbollist=['z','-'],bartrange=0.06, colorlist=['k','y'], gridcolor='#316931',singlesubplot=True,noshow=True,opacity=0.5)

    fig.set_size_inches(9.56, 2.19, forward=True)
    fig.patch.set_facecolor('black')
    fig.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)

    ax = fig.axes
    for a in ax:
        a.axis('off')
        #a.set_frame_on(False)
    print (" p1")
    maxk = kvals._get_max('var1')
    if maxk >= 6:
        img2 = imread("/home/cobs/ANALYSIS/TitleGraphs/polarlichter.v02.jpg")
        newax2 = fig.add_axes([0.0, 0.0, 1.0, 1.0], anchor='SE', zorder=-1)
        newax2.imshow(img2,origin='upper')
        newax2.axis('off')
    else:
        img2 = imread("/home/cobs/ANALYSIS/TitleGraphs/hyades.v03.jpg")
        newax2 = fig.add_axes([0.0, 0.0, 1.0, 1.0], anchor='SE', zorder=-1)
        newax2.imshow(img2,origin='upper')
        newax2.axis('off')

    #if debug:
    #print (" p2")
    newax = fig.add_axes([0.0, 0.0, 1.0, 1.0], anchor='SW', zorder=-1)
    newax.imshow(img,origin='upper')
    newax.axis('off')

    #if debug:
    #plt.show()
    savepath2 = "/srv/products/graphs/title/title_mag.png"
    plt.savefig(savepath2)
    print ("Save to {} done".format(savepath2))


def main(argv):
    try:
        version = __version__
    except:
        version = "1.0.0"
    configpath = ''
    statusmsg = {}
    debug=False
    starttime = None
    endtime = None
    source = 'database'

    try:
        opts, args = getopt.getopt(argv,"hc:e:s:D",["config=","endtime=","starttime=","debug=",])
    except getopt.GetoptError:
        print ('mag_graph.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- mag_graph.py plots title --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python weather_products.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-e            : endtime - default is now')
            print ('-s            : starttime -  default is three days from now')
            print ('-------------------------------------')
            print ('Application:')
            print ('python mag_graph.py -c /etc/marcos/analysis.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-s", "--starttime"):
            # get an endtime
            starttime = arg
        elif opt in ("-e", "--endtime"):
            # get an endtime
            endtime = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running mag_graph version {}".format(version))
    print ("--------------------------------")

    if endtime:
        try:
            endtime = DataStream()._testtime(endtime)
        except:
            print (" Could not interprete provided endtime. Please Check !")
            sys.exit(1)
    else:
        endtime = datetime.utcnow()

    if starttime:
        try:
            starttime = DataStream()._testtime(starttime)
        except:
            print (" Could not interprete provided starttime. Please Check !")
            sys.exit(1)
    else:
        starttime=endtime-timedelta(days=4)

    if starttime >= endtime:
        print (" Starttime is larger than endtime. Please correct !")
        sys.exit(1)

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "TitleGraphs", job=os.path.basename(__file__), newname='mm-tg-mag.log', debug=debug)
    name1 = "{}-graph".format(config.get('logname'))
    statusmsg[name1] = 'mag graph successful'

    print ("3. Connect databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
    except:
        statusmsg[name1] = 'database failed'
    # it is possible to save data also directly to the brokers database - better do it elsewhere

    print ("4. Loading current.data and getting primary instruments")
    config, statusmsg = GetPrimaryInstruments(config=config, statusmsg=statusmsg, debug=debug)

    print ("5. Mag graph")
    msg = mag_graph(db, config=config, starttime=starttime, endtime=endtime, debug=debug)
    statusmsg[name1] = msg

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)


if __name__ == "__main__":
   main(sys.argv[1:])
