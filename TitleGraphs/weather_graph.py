#!/usr/bin/env python

"""
DESCRIPTION
   Title graph with weather information

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
                                          default "datetime.utcnow() - 7 days"

APPLICATION
    PERMANENTLY with cron:
        python weather_graph.py -c /etc/marcos/analysis.cfg
"""

from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, getstringdate
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
from version import __version__


def weather_graph(db,config={},starttime=datetime.utcnow()-timedelta(days=4),endtime=datetime.utcnow(), debug=False):

    data = readDB(db,'METEO_T7_0001_0001',starttime=starttime)

    month = endtime.month

    if month in [12,1,2,3]:
        img = imread(os.path.join(path2images,"winter.jpg"))
    elif month in [4,5,6]:
        img = imread(os.path.join(path2images,"spring.jpg"))
    elif month in [7,8,9]:
        img = imread(os.path.join(path2images,"summer.jpg"))
    else:
        img = imread(os.path.join(path2images,"autumn.jpg"))

    clean = True
    if clean:
        res = data.steadyrise('dx', timedelta(minutes=60),sensitivitylevel=0.002)
        data = data._put_column(res, 't2', columnname='Niederschlag',columnunit='mm/1h')
        data = data.flag_outlier(keys=['f','t1','var5','z'],threshold=4.0,timerange=timedelta(hours=2))
        data = data.remove_flagged()
        snow = data._get_column('z')
        snowmax = np.max([el for el in snow if not isnan(el)])
        if snowmax < 200:
            snowmax = 200
        rain = data._get_column('t2')
        rainmax = np.max([el for el in rain if not isnan(el)])
        if rainmax < 15:
            rainmax = 15

    #month = 1
    if month in [11,12,1,2,3]:
        fig = mp.plotStreams([data],[['z','f']], fill=['z'], specialdict = [{'z':[0,snowmax]},{'f':[-10,40]}], colorlist=['w','r'], gridcolor='#316931',opacity=0.5,singlesubplot=True,noshow=True)
    else:
        fig = mp.plotStreams([data],[['t2','f']], fill=['t2'], specialdict = [{'t2':[0,rainmax]},{'f':[-30,20]}], colorlist=['b','r'], gridcolor='#316931',opacity=0.5,singlesubplot=True,noshow=True)

    fig.set_size_inches(9.56, 2.19, forward=True)
    fig.patch.set_facecolor('black')
    fig.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)

    # remove boundary and rotate image

    ax = fig.axes
    for a in ax:
        a.axis('off')

    newax = fig.add_axes([0.0, 0.0, 1.0, 1.0],anchor='SW',zorder=-1)
    newax.imshow(img,origin='upper')
    newax.axis('off')

    savepath2 = savepath = "/srv/products/graphs/title/title_weather.png"
    plt.savefig(savepath2)
    return True

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
        print ('weather_graph.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- weather_graph.py plots title --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python weather_graph.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-e            : endtime - default is now')
            print ('-s            : starttime -  default is three days from now')
            print ('-------------------------------------')
            print ('Application:')
            print ('python weather_graph.py -c /etc/marcos/analysis.cfg')
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

    print ("Running current_weather version {}".format(version))
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
        starttime=datetime.strftime(endtime-timedelta(days=7),"%Y-%m-%d")

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
    config = DefineLogger(config=config, category = "TitleGraphs", job=os.path.basename(__file__), newname='mm-tp-weather.log', debug=debug)
    name1 = "{}-graph".format(config.get('logname'))
    statusmsg[name1] = 'weather graph successful'

    print ("3. Connect databases and select first available")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
    except:
        statusmsg[name1] = 'database failed'
    # it is possible to save data also directly to the brokers database - better do it elsewhere

    print ("4. Weather graph")
    try:
        success = weather_graph(db, config=config, starttime=starttime, endtime=endtime, debug=debug)
    except:
        statusmsg[name1] = 'weather graph failed'

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)


if __name__ == "__main__":
   main(sys.argv[1:])
