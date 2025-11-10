#!/usr/bin/env python

"""
Flagging data

DESCRIPTION
   This method can be used to flag data regularly, to clean up the
   existing flagging database, to upload new flags from files and
   to archive "old" flags into json file structures. It is also possible
   to delete flags from the database. The delete method will always save
   a backup before removing flag data.

PREREQUISITES
   The following packages are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

   The upload method also works for pkl (pickle) files. However,
   a successful upload requires that the upload is performed with
   the same major python version as used for pkl creation. 

PARAMETERS
    flagdict          :  dict       :  currently hardcoded into the method
            { SensorNamePart : 
              [timerange, keys, threshold, window, markall, lowlimit, highlimit]
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -j joblist             :   list    :  jobs to be performed - default "flag"
                                          (flag, clean, upload, archive, delete)
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"
    -p path                :   string  :  upload - path to upload directory
    -s sensor              :   string  :  delete - sensor of which data is deleted 
    -o comment             :   string  :  delete - flag comment for data sets to be deleted

APPLICATION
    PERMANENT with cron:
        python flagging.py -c /etc/marcos/analysis.cfg
    YEARLY with cron:
        python flagging.py -c /etc/marcos/analysis.cfg -j archive
    DAILY with cron:
        python flagging.py -c /etc/marcos/analysis.cfg -j upload,clean -p /srv/archive/flags/uploads/
    REDO:
        python flagging.py -c /etc/marcos/analysis.cfg -e 2020-11-22
    DELETE data with comment:
        python flagging.py -c /etc/marcos/analysis.cfg -j delete -s MYSENSORID -o "my strange comment"
    DELETE data for FlagID Number (e.g. all automatic flags):
        python flagging.py -c /etc/marcos/analysis.cfg -j delete -s MYSENSORID -o "1"
    DELETE data all flags for key "f":
        python flagging.py -c /etc/marcos/analysis.cfg -j delete -s MYSENSORID -o "f"

"""

from magpy.stream import *
from magpy.core import database
from magpy.core import database
import magpy.opt.cred as mpcred
#import magpy.core.flagging as fl   # consecutive method

from shutil import copyfile
import itertools
import getopt
import pwd
import socket
import sys  # for sys.version_info()


from martas.version import __version__
from martas.core.methods import martaslog as ml
from martas.core import methods as mm
from martas.core import analysis



# ################################################
#            Flagging dictionary
# ################################################

## comments: switched from 5 to 6 for LEMI025 and LEMI036 at 01.08.2019

def main(argv):
    version = __version__
    configpath = ''
    statusmsg = {}
    statusmsg["Flags - uploads"] = "Flags: no new file for upload"
    statusmsg["Flags - archive"] = "Flags: archiving ok"
    debug=False
    endtime = datetime.utcnow()
    joblist = []
    varioinst = ''
    scalainst = ''
    #joblist = ['flag','clean','archive','update','delete']
    joblist = ['flag']
    flagfilearchivepath = '' # default:    flagarchive : /srv/archive/flags
    flagfilepath = ''
    consecutivethreshold = 100000
    delsensor = 'RCST7_20160114_0001'
    delcomment = 'aof - threshold 5.0 window 43200.0 sec'


    try:
        opts, args = getopt.getopt(argv,"hc:e:j:p:s:o:D",["config=","endtime=","joblist=","path=","sensor=","comment=","debug="])
    except getopt.GetoptError:
        print ('flagging.py -c <config>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- flags.py will flag your data sets --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python flags.py -c <config> -f <flagconf -j <joblist>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-e            : endtime, default is now')
            print ('-j            : joblist: flag,clean,archive,update,delete; default is flag,clean')
            print ('-p            : update - path to json files which end with flags.json')
            print ('-s            : delete - sensor')
            print ('-o            : delete - comment')
            print ('-------------------------------------')
            print ('Application:')
            print ('python flagging.py -c /etc/marcos/analysis.cfg')
            print ('Once per year:')
            print (' python flagging.py -c /etc/marcos/analysis.cfg -j archive')
            print ('Eventually always:')
            print (' python flagging.py -c /etc/marcos/analysis.cfg -j upload -p /srv/archive/flags/uploads/')
            print ('Once a day/week:')
            print (' python flagging.py -c /etc/marcos/analysis.cfg -j clean')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-e", "--endtime"):
            # get an endtime
            endtime = arg.split(',')
        elif opt in ("-j", "--joblist"):
            # get an endtime
            joblist = arg.split(',')
        elif opt in ("-p", "--path"):
            # delete any / at the end of the string
            flagfilepath = os.path.abspath(arg)
        elif opt in ("-s", "--sensor"):
            # hidden: delete sensor data
            delsensor = arg
        elif opt in ("-o", "--comment"):
            delcomment = arg
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running flagging version {}".format(version))
    print ("--------------------------------")

    if not os.path.exists(configpath):
        print ('Specify a valid path to configuration information')
        print ('-- check magnetism_products.py -h for more options and requirements')
        sys.exit()

    print ("1. Read and check validity of configuration data")
    config = mm.get_conf(configpath)
    flagdict = mm.get_json(flagconfpath)

    receiver  = config.get('notification',"")
    notificationcfg  = config.get('notificationcfg',"")
    logpath = config.get('logpath',"/tmp/filterstatus.log")

    mf = analysis.MartasAnalysis(config=config, flagdict=flagdict)

    if 'flag' in joblist:
      try:
          print ("Periodic flagging ...")
          fl = mf.periodically(debug=False)
          if fl and len(fl) > 0:
              print("    Writing flags to DB", len(fl))
              suc = mf.update_flags_db(fl, debug=True)
              statusmsg["Flags - periodically"] = 'Periodic flagging: success'
      except:
          print (" -> flagging failed")
          statusmsg["Flags - periodically"] = 'Periodic flagging: failed'


    if 'upload' in joblist and flagfilepath:
        print ("Upload flagging lists from files ...")
        fl = mf.upload(flagfilepath)
        if fl and len(fl) > 0:
            print("    Writing flags to DB", len(fl))
            suc = mf.update_flags_db(fl, debug=True)
            statusmsg["Flags - uploads"] = 'Flags: new flagging data sets uploaded'


    if 'delete' in joblist and flagfilearchivepath:
        print ("Not existing. Deleting content")
        # not yet available
        #delsensor = 'RCST7_20160114_0001'
        #delsensor = 'LEMI036_3_0001'
        #delcomment = 'aof - threshold 5.0 window 43200.0 sec'
        #delcomment = 'aof - threshold: 6, window: 600.0 sec'
        # Backup any data too bee deleted?
        parameter = 'comment'
        if delcomment in ['0','1','2','3','4']:
            parameter = 'flagnumber'
            print (" - found a valid flagnumber as value: removing flags with FlagID {}".format(delcomment))
        elif delcomment in KEYLIST:
            parameter = 'key'
            print (" - found a valid key as value: removing all flags for key {}".format(delcomment))
        print (" - selected sensor {}".format(delsensor))
        flaglist = db2flaglist(db,delsensor)
        print (" - got {} flags".format(len(flaglist)))
        toberemovedflaglist = DataStream().flaglistmod('select', flaglist, parameter=parameter, value=delcomment)
        print (" - will backup and then remove {} flags matching your criteria".format(len(toberemovedflaglist)))
        flagfile = os.path.join(flagfilearchivepath,'flags_{}_backup_{}.json'.format(delsensor,datetime.strftime(datetime.utcnow(),"%Y%m%d%H%M")))
        succ = saveflags(toberemovedflaglist, flagfile)
        if succ:
            print (" - backup saved to {}".format(flagfile))
            remainingflaglist = DataStream().flaglistmod('delete', flaglist, parameter=parameter, value=delcomment)
            print (" - remaining {} flags".format(len(remainingflaglist)))
            if not debug:
                for dbel in connectdict:
                    dbt = connectdict[dbel]
                    print ("  -- Writing flags to DB {}".format(dbel))
                    flaglist2db(dbt,remainingflaglist,mode='delete',sensorid=delsensor)
                print (" -> Done")
            else:
                print (" -> Debug selected - no changes made to DB")
        else:
            print (" Backup could not be saved - aborting - check directory permissions")


    if 'clean' in joblist:
        print ("6. Cleaning flagging list")
        try:
            print (" Cleaning up all records")
            cumflag = []
            stream = DataStream()
            flaglist = db2flaglist(db,'all')
            if debug:
                print ("   -> Found {} flags in database".format(len(flaglist)))
            print (" --------------------------------------")
            stream.flagliststats(flaglist, intensive=True)
            print (" --------------------------------------")
            currentyear = endtime.year
            yearlist = [i for i in range(2000,currentyear+2)]
            for year in yearlist:
                startyear = year -1
                print (" Checking data from {} until {}".format(startyear, year))
                beg = '{}-01-01'.format(startyear)
                end = '{}-01-01'.format(year)
                flaglist_tmp = db2flaglist(db,'all',begin=beg, end=end)
                print ("   -> Found {} flags in database between {} and {}".format(len(flaglist_tmp),startyear,year))
                if len(flaglist_tmp) > 0:
                    print ("  - Cleaning up flaglist")
                    clflaglist_tmp = stream.flaglistclean(flaglist_tmp,progress=True)
                    print ("   -> {} flags remaining".format(len(clflaglist_tmp)))
                    if len(clflaglist_tmp) < consecutivethreshold:
                        # TODO this method leads to a killed process sometimes...
                        print ("  - Combining consecutives")
                        coflaglist_tmp = consecutive_check(clflaglist_tmp) #debug=debug)
                    else:
                        coflaglist_tmp = clflaglist_tmp
                    print ("   -> {} flags remaining".format(len(coflaglist_tmp)))
                    if len(cumflag) == 0:
                        cumflag = coflaglist_tmp
                    else:
                        cumflag.extend(coflaglist_tmp)
            if debug:
                print ("   -> cleaned record contains {} flags".format(len(cumflag)))
            print (" --------------------------------------")
            stream.flagliststats(cumflag, intensive=True)
            print (" --------------------------------------")
            if not debug:
                for dbel in connectdict:
                    dbt = connectdict[dbel]
                    print ("  -- Writing flags to DB {}".format(dbel))
                    flaglist2db(dbt,cumflag,mode='delete',sensorid='all')
                print ("   -> cleaned flaglist uploaded to DB")
                statusmsg[name3] = 'Cleanup: cleaning database successful'
        except:
            print ("   -> failure while cleaning up")
            statusmsg[name3] = 'Cleanup: failure'


    # schedule with crontab at February 1st 6:00 (analyze yearly) flagging -c /wic.cfg -j archive
    if 'archive' in joblist and flagfilearchivepath:
        print ("7. Saving archive and deleting old db contents")
        print (" Archiving flags")
        print (" ---------------")
        print (" Every year in February - archive full year two years ago")
        print (" Delete all inputs older than two years from DB")
        ## e.g. Feb 2019 -> Keep only 2017 and 2018 in DB
        ## archive everything before 2017
        ## delete everything before 2017
        ## -> archive containsnow : flags_2016_final.pkl, # and 2015,etc
        ## ->                       flags_archive.pkl current backup (only monthly)
        ## -> DB contains 2017 to present ... 3 years max
        stream = DataStream()
        flaglist = db2flaglist(db,'all')
        if debug:
            print ("   -> Found {} flags in database".format(len(flaglist)))
        print (" --------------------------------------")
        stream.flagliststats(flaglist)
        print (" --------------------------------------")
        # Backup and export all old flags
        minyear = 2015
        succ = False
        currentyear = endtime.year
        if currentyear-3 > minyear:
             yearlist = [i for i in range(minyear,currentyear-2)]
        else:
             yearlist = [2015]
        flaglist_tmp = []
        for year in yearlist:
            startyear = 2000
            if year > 2015:
                startyear = year-1

            print (" Archiving flaglist until {}".format(year))
            flagfile = os.path.join(flagfilearchivepath,'flags_{}-{}.json'.format(startyear,year))
            beg = '{}-01-01'.format(startyear)
            end = '{}-01-01'.format(year)
            print ("  -> Checking database contents (flags) between {} and {}".format(beg,end))
            flaglist_tmp = db2flaglist(db,'all',begin=beg, end=end)
            if len(flaglist_tmp) > 0:
                print ("   -> Found {} flags in database between {} and {}".format(len(flaglist_tmp),startyear,year))
                if os.path.isfile(flagfile):
                    fileflaglist = loadflags(flagfile)
                    print ("   -> Found {} flags in file".format(len(fileflaglist)))
                    flaglist_tmp.extend(fileflaglist)
                # Cleaning has been done already
                print ("  - Saving flag archive to {}".format(flagfilepath))
                succ = saveflags(flaglist_tmp, flagfile, overwrite=True)
                print ("   -> Done")
            else:
                print ("    -> DB empty")

        if succ:
            # drop all flags from flaglist
            print (" Droping all flags until year {}".format(year))
            newflaglist = stream.flaglistmod('delete', flaglist, starttime='2000-01-01', endtime=end)
            print ("   -> remaining amount of flags: {}".format(len(newflaglist)))
            # Cleaning has been done already
            print (" Uploading new list to database and deleting all other inputs")
            print (" --------------------------------------")
            stream.flagliststats(newflaglist)
            print (" --------------------------------------")
            if not debug:
                for dbel in connectdict:
                    dbt = connectdict[dbel]
                    print ("  -- Writing flags to DB {}".format(dbel))
                    flaglist2db(dbt,newflaglist, mode='delete')
                print ("   -> Done")
            else:
                print ("   -> Debug selected - no changes made to DB")
            statusmsg[name4] = 'Archiving flags: done until {}'.format(year)
        else:
            print ("   -> Problem with saving files - aborting")
            statusmsg[name4] = 'Archiving flags: file saving problem'

    print ("------------------------------------------")
    print ("  flagging finished")
    print ("------------------------------------------")
    print ("SUCCESS")


    if not debug:
        martaslog = ml(logfile=logpath,receiver=receiver)
        if receiver == 'telegram':
            martaslog.telegram['config'] = notificationcfg
        elif receiver == 'email':
            martaslog.email['config'] = notificationcfg
        martaslog.msg(statusmsg)
    elif debug:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)


if __name__ == "__main__":
   main(sys.argv[1:])


