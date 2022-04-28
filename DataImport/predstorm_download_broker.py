#!/usr/bin/env python
# -*- coding: utf-8 -*-


from magpy.stream import *
import magpy.mpplot as mp
import getopt
import sys  # for sys.version_info()


def main(argv):
    version = "1.0.0"
    configpath = ''
    inputsource = ''
    outputpath = ''
    statusmsg = {}
    debug=False


    try:
        opts, args = getopt.getopt(argv,"hc:i:o:D",["config=","input=","output=","debug=",])
    except getopt.GetoptError:
        print ('try predstorm_extract.py -h for instructions')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- predstorm_extract.py will plot sensor data --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python predstorm_extract.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-c (required) : configuration data path')
            print ('-i            : input path')
            print ('-o            : output directory (or file) to save the graph')
            print ('-------------------------------------')
            print ('Application:')
            print ('python3 predstorm_extract.py -c ../conf/wic.cfg -i ../conf/sensordef_plot.json -e 2020-12-17')
            print ('python3 predstorm_download_broker.py -i https://helioforecast.space/static/sync/predstorm_real_1m.txt -o /home/cobs/SPACE/incoming/PREDSTORM/')
            sys.exit()
        elif opt in ("-c", "--config"):
            # delete any / at the end of the string
            configpath = os.path.abspath(arg)
        elif opt in ("-i", "--input"):
            # delete any / at the end of the string
            inputsource = arg
        elif opt in ("-o", "--output"):
            # delete any / at the end of the string
            outputpath = os.path.abspath(arg)
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    if debug:
        print ("Running predstorm loader version {}".format(version))

    # move the rest to sw_extractor
    #data = read_predstorm_data(os.path.join(outputpath,"PREDSTORM*"),debug=debug)

    if configpath:
        # requires martas import
        print ("1. Read and check validity of configuration data")
        config = GetConf(configpath)

        print ("2. Activate logging scheme as selected in config")
        config = DefineLogger(config=config, category = "GetPredstorm", job=os.path.basename(__file__), newname=newloggername, debug=debug)

    try:
        if debug:
            print ("Loading PREDSTORM data ...")
        # read and save data
        data = read(inputsource, starttime=datetime.utcnow())
        data.write(outputpath, filenamebegins="{}_".format(data.header.get('SensorID')), format_type='PYCDF', mode='replace')
        statusmsg['PREDSTORM download'] = 'success'
    except:
        statusmsg['PREDSTORM download'] = 'failure'

    if configpath and not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])

