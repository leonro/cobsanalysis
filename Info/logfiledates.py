#!/usr/bin/env python
# coding=utf-8

"""
Script to check the creation date of the latest file matching a certain structure
in a defined directory

TODO - is replaced by the monitor.py MARTAS script using the 'datafile' job.
""" 

from magpy.stream import *
from magpy.database import *
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.cred as mpcred

import glob
import getopt
import pwd
import socket
import sys  # for sys.version_info()


coredir = os.path.abspath(os.path.join('/home/cobs/MARTAS', 'core'))
coredir = os.path.abspath(os.path.join('/home/leon/Software/MARTAS', 'core'))
sys.path.insert(0, coredir)
from martas import martaslog as ml
from acquisitionsupport import GetConf2 as GetConf
scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, getstringdate, combinelists


def _latestfile(path, date=False, latest=True, debug=False):
    """
    DESCRIPTION
        get latest file
    """
    list_of_files = glob.glob(path) # * means all if need specific format then *.csv
    if debug:
        print (list_of_files)
    if len(list_of_files) > 0:
        if latest:
            latest_file = max(list_of_files, key=os.path.getctime)
        else:
            latest_file = min(list_of_files, key=os.path.getctime)
        ctime = os.path.getctime(latest_file)
        if date:
            return datetime.fromtimestamp(ctime)
        else:
            return latest_file
    else:
        return ""

def agerange(age, increment):
    mult = 1
    if not increment in ['day','hour','minute','second']:
        print ("   -> !! Unkown increments!! Use day, hour, minute or second")
        return 0
    if increment == 'minute':
        mult = 60
    if increment == 'hour':
        mult = 3600
    if increment == 'day':
        mult = 86400
    return mult*age


def main(argv):
    version = '1.0.0'
    configpath = ''
    statusmsg = {}
    debug=False
    age = 1
    iterate = 'day'
    structure = '*'
    logname = 'default'

    try:
        opts, args = getopt.getopt(argv,"hc:p:s:a:i:D",["config=","path=","structure=","age=","iterate=","debug=",])
    except getopt.GetoptError:
        print ('logfiledates.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- logfiledates.py will analyse magnetic data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python logfiledates.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-p            : path')
            print ('-s            : structure like "*.json" or "*A16.txt". Default is "*" ')
            print ('-a            : tolerated age (integer). Default is 1')
            print ('-i            : increment of age (day, hour, minute, second). Default is day')
            print ('-l            : name of the logger')
            print ('-------------------------------------')
            print ('Application:')
            print ('python3 logfiledates.py -c ../conf/wic.cfg -p /srv/archive/flags/uploads/ -a 1 -i day')
            sys.exit()
        elif opt in ("-c", "--path"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-p", "--path"):
            # delete any / at the end of the string
            path = os.path.abspath(arg)
        elif opt in ("-s", "--structure"):
            # get a list of jobs (adjusted, quasidefinitive,upload,plots)
            structure = arg
        elif opt in ("-a", "--age"):
            # get a list of jobs (adjusted, quasidefinitive,upload,plots)
            age = int(arg)
        elif opt in ("-i", "--iterate"):
            # get a list of jobs (adjusted, quasidefinitive,upload,plots)
            iterate = arg
        elif opt in ("-l", "--logger"):
            # get a list of jobs (adjusted, quasidefinitive,upload,plots)
            logname = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    if debug:
        print ("Running loggin file dates")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = GetConf(configpath)

    print ("2. Activate logging scheme as selected in config")
    config = DefineLogger(config=config, category = "DataProducts", job=os.path.basename(__file__), newname='mm-dp-logfiledate.log', debug=debug)
    name = "{}-{}".format(config.get('logname'),logname)
    statusmsg[name] = 'Latest file younger than {} {}'.format(age,iterate)

    print ("3. Create file search")
    filepath = os.path.join(path,structure)

    print ("4. Reading directory and getting latest file")
    fi = _latestfile(filepath, date=False, latest=True, debug=True)
    da = _latestfile(filepath, date=True, latest=True, debug=True)

    if os.path.isfile(fi):
        print ("   - Got {} created at {}".format(fi,da))
        diff = (datetime.utcnow()-da).total_seconds()
        print ("   - Diff {} sec".format(diff))
        accepteddiff = agerange(age, iterate)
        print ("   - Accepted: {} sec".format(accepteddiff))
        if not diff < accepteddiff:
            statusmsg[name] = 'Latest file older than {} {}'.format(age,iterate)
    else:
        print ("   - No file(s) found - check path and structure")
        statusmsg[name] = 'No file(s) found'

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])



"""
# ##############################################################
#                     Configuration data for analysis jobs
# ##############################################################


# Observatory
# --------------------------------------------------------------

obscode                :      WIC


# Basic analysis parameters
# --------------------------------------------------------------

meteorange             :       3
daystodeal             :      2


#  - MAGNETISM

variometerinstruments  :      LEMI036_1_0002_0002,LEMI025_22_0003_0002,FGE_S0252_0001_0001
scalarinstruments      :      GSM90_14245_0002_0002,GSM90_6107631_0001_0001,GP20S3NSS2_012201_0001_0001
magnetismexports       :      IAGA,CDF,DBmin
qdstarthour            :      3
qdendhour              :      4
# analyze quasidefinitive data only on 5=Saturday
qdweekday              :      5
# baseline anaylsis
primarypier            :      A2
baselinedays           :      100



# Databases
# --------------------------------------------------------------

dbcredentials          :      list


# Paths and Directories
# --------------------------------------------------------------

#  - METEOROLOGY

sgopath                :       /srv/archive/SGO
meteoproducts          :       /srv/products/data/meteo
meteoimages            :       /srv/products/graphs/meteo

#  - MAGNETISM

variationpath          :       /srv/products/data/magnetism/variation/
quasidefinitivepath    :       /srv/products/data/magnetism/quasidefinitive/
dipath                 :       /srv/archive/WIC/DI/data
archivepath            :       /srv/archive/WIC

#  - GAMMA

rcsg0rawdata           :       /srv/archive/SGO/RCSG0temp_20161027_0001/raw/
gammarawdata           :       /srv/archive/SGO/GAMMA_SFB867_0001/raw/
gammaresults           :       /srv/projects/radon/tables/


#  - GENERAL

currentvaluepath       :       /srv/products/data/current.data
magfigurepath          :       /srv/products/graphs/magnetism/


# Logging and notification
# --------------------------------------------------------------

# Logfile (a json style dictionary, which contains statusmessages) 
loggingdirectory       :   /var/log/magpy

# Notifaction (uses martaslog class, one of email, telegram, mqtt, log) 
notification         :   telegram
# Configuration for notification type, e.g. /home/cobs/SCRIPTS/telegram_notify.conf
notificationconfig   :   /myconfpath/mynotificationtype.cfg

"""

"""
# ################################################
#             Part 2
# ################################################

if part2:
    #""
    #Publish adjusted data
    #- requires an uploadlist for the specific time range
    #""
    print ("----------------------------------------------------------------")
    print ("Part 2: Publish adjusted data")
    print ("----------------------------------------------------------------")
    name2 = "{}-step2".format(name)

    print ("  uploading one second data to ZAMG Server and eventually to GIN")
    try:
        for da in uploadlist:
            #ok = True
            #if ok:
            print ("Uploading data for {}".format(da))
            print ("  -- THREAD for IAGA data to FTP: {}".format(da))
            if 'submitZAMGFTPasIAGA' in submitlist:
                # Send second data in background mode
                Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(vpathsec,'wic'+da+'psec.sec'),'ftppath':'/data/magnetism/wic/variation/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/psec-transfer.log'}).start()
            if 'submitZAMGFTPasIAGA' in submitlist:
                # Send minute data in background mode
                Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(vpathmin,'wic'+da+'pmin.min'),'ftppath':'/data/magnetism/wic/variation/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/pmin-transfer.log'}).start()

            if 'submitAPPasIAGA' in submitlist and submit2app:
                # Send second data in background mode
                print ("Uploading data to art project") 
                Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(vpathsec,'wic'+da+'psec.sec'),'ftppath':'/all-obs/','myproxy':artaddress,'port':artport,'login':artuser,'passwd':artpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/psec2app-transfer.log'}).start()

            if 'submitGINasIAGA' in submitlist and submit2gin:
                print ("Submitting to gin if no other curl job detected: active_pid = ", active_pid('curl'))
                print ("#################################")
                if not active_pid('curl'):
                    print ("  -- Uploading second data to GIN - active now")
                    uploadsec = ginupload(os.path.join(vpathsec,'wic'+da+'psec.sec'), ginuser, ginpasswd, ginaddress,stdout=True)
                    print (uploadsec)
                    print ("  -- Uploading minute data to GIN: {}".format(da))
                    uploadmin = ginupload(os.path.join(vpathmin,'wic'+da+'pmin.min'), ginuser, ginpasswd, ginaddress,faillog=True,stdout=True)
                    print (uploadmin)
                else:
                    print (" !!!!!!!!!!!!! curl still active sending data in next round")
        statusmsg[name2] = 'upload successful'
    except:
        print (" !!!!!!!!!!!!!!! data upload failed")
        statusmsg[name2] = 'upload failed'


if part4 and part3 and runqd:
    #""
    #Upload QD data diagrams
    #""

    print ("----------------------------------------------------------------")
    print ("Part 4: upload quasi definitive data")
    print ("----------------------------------------------------------------")

    name4 = "{}-step4".format(name)

    try:
        #ok = True
        #if ok:
        # Upload QD data to WDC
        print ("Uploading QD data for {}".format(qdlist))
        for da in qdlist:
            # Send in background mode
            print ("Uploading QD data for {}".format(da))
            print ("  -- THREAD for IAGA qsec data to FTP: {}".format(da))
            Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(qpathsec,'wic'+da+'qsec.sec'),'ftppath':'/data/magnetism/wic/quasidefinitive/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/qsec-transfer.log'}).start()
            print ("  -- THREAD for IAGA qmin data to FTP: {}".format(da))
            Thread(target=ftpdatatransfer, kwargs={'localfile':os.path.join(qpathmin,'wic'+da+'qmin.min'),'ftppath':'/data/magnetism/wic/quasidefinitive/','myproxy':zamgaddress,'port':zamgport,'login':zamguser,'passwd':zamgpasswd,'logfile':'/home/cobs/ANALYSIS/Logs/qmin-transfer.log'}).start()
            if submit2gin:
                if not active_pid('curl'):
                    print ("  -- Uploading second data to GIN - active now")
                    uploadsec = ginupload(os.path.join(qpathsec,'wic'+da+'qsec.sec'), ginuser, ginpasswd, ginaddress,stdout=False)
                    print ("  -> Answer: {}".format(uploadsec))
                print ("  -- Uploading minute data to GIN: {}".format(da))
                uploadmin = ginupload(os.path.join(qpathmin,'wic'+da+'qmin.min'), ginuser, ginpasswd, ginaddress,stdout=False)
                #Thread(target=ginupload, kwargs={'localfile':os.path.join(qpathmin,'wic'+da+'qmin.min'),'user':ginuser, 'passwd': ginpasswd, 'address': ginaddress, 'stdout':False}).start()
                print ("  -> Answer: {}".format(uploadmin))

        statusmsg[name4] = 'upload of QD successful: {}'.fromat(uploadmin)
    except:
        print (" !!!!!!!!!!!!!!! QD data upload failed")
        statusmsg[name4] = 'upload of QD failed'

"""
