#!/usr/bin/env python
# coding=utf-8

"""
MagPy - Basic Runtime tests including durations  
"""
from __future__ import print_function
from __future__ import unicode_literals

# Define packges to be used (local refers to test environment) 
# ------------------------------------------------------------

from magpy.stream import *   
from magpy.database import *   
import magpy.opt.cred as mpcred
from scipy.interpolate import interp1d
import telegram_send
# Get new data from API
import urllib, json

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, GetPrimaryInstruments, getstringdate, combinelists
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf


phamem = '/home/cobs/ANALYSIS/Logs/tg_pha.json'
#phamem = '/tmp/tg_pha.json'  # tmp will be deletet if Pc is restarted


'msghead':'*Erdnahes Objekt (z.B. Asteroid)*','msgnew':'Neues', 'msgupdate':'Update zu','msgfuture':'Nähert sich der Erde am','msgpast':'Näherte sich der Erde am','msgdist':'in einem Abstand von','msgsize':'Durchmesser','msgref':'Daten des'



languagedict = {'deutsch': {'msghead':'*Erdnahes Objekt (z.B. Asteroid)*','msgnew':'Neues', 
                            'msgupdate':'Update zu','msgfuture':'Nähert sich der Erde am',
                            'msgpast':'Näherte sich der Erde am','msgdist':'in einem Abstand von',
                            'msgsize':'Durchmesser','msgref':'Daten des','channeltype':'telegram',
                            'channelconfig':'/etc/martas/telegram.cfg'},
                'english': {'msghead':'*Near Earth Objekt*','msgnew':'New', 
                            'msgupdate':'Update to','msgfuture':'Approaching earth on',
                            'msgpast':'Approached earth on','msgdist':'within',
                            'msgsize':'Size','msgref':'Based on','channeltype':'telegram',
                            'channelconfig':'/etc/martas/telegram.cfg'}}

#data = read("https://www.minorplanetcenter.net/iau/MPCORB/PHA.txt")
#data = read("/home/leon/CronScripts/MagPyAnalysis/Asteroids/pha.txt")
#data = read("https://minorplanetcenter.net/Extended_Files/pha_extended.json.gz", dataformat='PHA')
#data = read("/home/leon/CronScripts/MagPyAnalysis/Asteroids/pha_extended.json.gz", format_type='PHA')


# Size estimation:
def getsize(h):
    # https://www.minorplanetcenter.net/iau/lists/Sizes.html
    sizearray = [[1050,2400,17.0],[840,1900,17.5],[670,1500,18.0],[530,1200,18.5],[420,940,19.0],[330,740,19.5],[260,590,20.0],[210,470,20.5],[170,370,21.0],    [130,300,21.5],[110,240,22.0],[85,190,22.5],[65,150,23.0],[50,120,23.5],[40,95,24.0],[35,75,24.5],[25,60,25.0],[20,50,25.5],[17,37,26.0],[13,30,26.5],[11,24,27.0],[8,19,27.5],[7,15,28.0],[5,12,28.5],[4,9,29.0],[3,7,29.5],[3,6,30.0],[2,5,30.5],[2,4,31.0],[1,3,31.5],[1,2,32.0],[1,2,32.5]]

    ma = np.asarray([el[1] for el in sizearray])
    mi = np.asarray([el[0] for el in sizearray])
    href = np.asarray([el[2] for el in sizearray])
    mif = interp1d(href, mi)
    maf = interp1d(href, ma)

    minval = np.round(float(mif(h)))
    maxval = np.round(float(maf(h)))

    sizerange = "{:.0f}-{:.0f} m".format(minval,maxval)
    return sizerange

def download_PHA_jpl(url,proxy=None, debug = False):
    datemax = datetime.strftime((datetime.now()+timedelta(days=730)),"%Y-%m-%d")
    #datemax = "2018-08-31"
    print ("Checking data until:", datemax)
    url = "https://ssd-api.jpl.nasa.gov/cad.api?dist-max=1LD&date-min=1900-01-01&date-max={}&sort=date".format(datemax)
    proxies = {'https': 'http://138.22.173.44:3128'}
    response = urllib.urlopen(url,proxies=proxies)
    data = json.loads(response.read())
    field = data.get(u'fields')
    alldata = data.get(u'data')

    print ("Database containing {} datasets".format(len(alldata)))
    return alldata, field, data

def get_PHA_mem(phamem, data, debug = False):
    # Check existig data on disk
    try:
        with open(phamem) as json_file:  
             existdata = json.load(json_file)
        existalldata = existdata['data']
        #print (existdata['fields'].index('des'))
        #print (field.index('des'))
        existname = [elem[field.index('des')] for elem in existalldata]
        print ("Existing data loaded ...")
        #print ("Names: {}".format(existname))
    except:
        existdata = data
        existalldata = []
        existname = []
    return existalldata, existname, existdata


def check_new_PHA(alldata,existalldata, languagedict={}, exceptlist=[], debug = False):
    print ("Checking for new objects ...")
    for language in languagedict:
        print (language)
        langdic = languagedict[language]
        channelconf = langdic.get('channelconfig')
        print (channelconfig)
        
        for elem in alldata:
            aname = elem[field.index('des')]
            # add a mail repetion two weeks before
            if not elem in existalldata and not elem[0] in exceptlist:
                print ("Found new data set")
                # Add new data to list
                existalldata.append(elem)
                # Creating new message if approach date is in the future:
                approachdate = datetime.strptime(elem[field.index('cd')],"%Y-%b-%d %H:%M") #TDB
                print ("Got approachdate", approachdate)
                if approachdate >= datetime.utcnow()-timedelta(days=2):  # ~30 secs diff between UTC and TDB
                    msg = langdic.get('msghead')
                    msg += "\n"
                    # Create message
                    if aname in existname:
                        msg += "{} [NEO](https://cneos.jpl.nasa.gov/ca/): *{}*\n".format(langdic.get('msgupdate'), elem[field.index('des')])
                    else:
                        msg += "{} [NEO](https://cneos.jpl.nasa.gov/ca/): *{}*\n".format(langdic.get('msgnew'), elem[field.index('des')])
                    msg += "(NEO = Near Earth object)\n"

                    msg += "Approaching earth on\n*{} UTC*\n".format(elem[field.index('cd')])
                    if approachdate >= datetime.utcnow():
                        msg += "{}\n*{} UTC*\n".format(langdic.get('msgfuture'),elem[field.index('cd')])
                    else:
                        msg += "{}\n*{} UTC*\n".format(langdic.get('msgpast'),elem[field.index('cd')])
                    distance = elem[field.index('dist')]
                    distancekm = float(distance)*149598073.
                    msg += "{} {:.6f} AU ({:d} km)\n".format(langdic.get('msgdist'),float(distance), int(distancekm))
                    amplitude = elem[field.index('h')]
                    if float(amplitude) < 32.5 and float(amplitude) > 17.0:
                        msg += "{}: {}\n".format(langdic.get('msgsize'),getsize(float(amplitude)))
                    msg += '_{} [Jet Propulsion Labs SBDB](https://ssd-api.jpl.nasa.gov/) (Small-Body DataBase)_'.format(langdic.get('msgref'))
                    msg += ""
                    if not debug:
                        if langdic.get('channeltype') == 'telegram':
                            telegram_send.send(messages=[msg],conf=langdic.get('channelconfig'),parse_mode="markdown")
                    else:
                        print (" Debug selected - not sending anything. Message would look like:")
                        print (msg)
                        print (" Break command issued - only first language")
                        break


def write_memory(phamem,data,debug=False):
    with open(phamem, 'wb') as outfile:
        json.dump(data, outfile,encoding="utf-8")



def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    debug=False
    channelconfig=''
    alldata, field, data = [],[],[]
    existalldata, existname, existdata = [],[],[]

    exceptlist = ["2020 SO"]
    # 2020 SO is not unique, two times the same name...


    try:
        opts, args = getopt.getopt(argv,"hc:t:D",["config=","telegram=","debug="])
    except getopt.GetoptError:
        print ('tg_base.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- tg_pha.py will obtain baseline plots --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('loading near earth objects database')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python tg_pha.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-t (required) : telegram channel configuration')
            print ('-------------------------------------')
            print ('Application:')
            print ('python tg_pha.py -c /etc/marcos/wic.cfg -t /etc/marcos/telegram.cfg')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-t", "--channel"):
            # delete any / at the end of the string
            channelconfig = os.path.abspath(arg)
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running tg_pha version {}".format(version))
    print ("--------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    # 1. conf and logger:
    # ###########################

    print ("Read and check validity of configuration data")
    config = GetConf(configpath)
    print (" -> Done")

    print ("Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "Info", job=os.path.basename(__file__), newname='mm-info-tgbase.log', debug=debug)
    print (" -> Done")

    # SOME DEFINITIONS:
    proxyadd = ''
    if config.get('proxy'):
        proxyadd = "http://{}:{}".format(config.get('proxy'),config.get('proxyport'))

    temporarypath = config.get('temporarydata')
    memorypath = os.path.join(temporarypath,'lastquake.npy')
    currentvaluepath = config.get('currentvaluepath')
    if not channelconfig:
        channelconfig = config.get('notificationconfig')
    if not channelconfig:
        print ("No message channel defined - aborting")
        sys.exit(1)

    continueproc = True
    # 2. Download PHA:
    # ###########################
    try:
        alldata, field, data = download_PHA_jpl(url,proxy=proxyadd, debug=debug)
        statusmsg['PHA'] = 'downloading new PHAs successful'
    except:
        statusmsg['PHA'] = 'downloading new PHAs failed' 
        continueproc = False

    # 3. Get local PHA memory:
    # ###########################
    if continueproc and len(data) > 0:
        try:
            existalldata, existname, existdata = get_PHA_mem(phamem, data, debug=debug)
            statusmsg['PHA'] = 'getting PHA memory successful'
        except:
            statusmsg['PHA'] = 'getting PHA memory failed' 
            continueproc = False

    # 4. sending PHA message:
    # ###########################
    if continueproc and len(alldata) > 0:
        try:
            check_new_PHA(alldata, existalldata, languagedict={}, exceptlist=exceptlist, debug=debug)
            statusmsg['PHA'] = 'sending PHA data successful'
        except:
            statusmsg['PHA'] = 'sending PHA data failed' 
            continueproc = False


    if continueproc and len(data) > 0 and not debug:
        try:
            write_memory(phamem,data,debug=debug)
            statusmsg['PHA'] = 'successfully finished'
        except:
            statusmsg['PHA'] = 'writing new PHA memory failed' 

    print ("tg_pha successfully finished")

    # 6. Logging section
    # ###########################
    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])

