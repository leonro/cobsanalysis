#!/usr/bin/env python3
# coding=utf-8

"""
Programm to extract GIC values for all stations covering a certain time range.
Analyzes availability, variability, extrema and current values.
Creates
"""
def read_gic_data(db,source='GICAUT',maxsensor=10, minutes=5, maxvals=5, debug=False):
    knewsql = ''
    gicdata = []
    status = {}
    amount = 0
    addcommlist = []
    start = datetime.utcnow()-timedelta(minutes=minutes)
    trange = datetime.strftime(start, "%Y-%m-%d %H:%M:%S")
    for i in range(1,maxsensor):
        name = "{}_GIC{:02d}_0001_0001".format(source,i)
        sn = "GIC{:02d}".format(i)
        try:
            if debug:
                print ("Checking table ", name)
            gicdat = dbselect(db,'x', name,'time > "{}"'.format(trange))
            if debug:
                print (" - obtained {} datapoints ".format(len(gicdat)))
            status[name] = len(gicdat)
            if len(gicdat) > 0:
                amount += 1
                addcommlist.append(sn)
            gicdata.extend(gicdat)
        except:
            pass

    def is_number(var):
        try:
            var = float(var)
            if np.isnan(var):
                return False
            return True
        except:
            return False

    """
    # remove nans and using absolutes
    cleangicdata = [np.abs(x) for x in gicdata if is_number(x)]
    if debug:
        print ("GIC data", cleangicdata)
    if len(cleangicdata) > 5:
        # get the 5 largest values and calculate median
        sortedgic = sorted(cleangicdata)
        gicvals = sortedgic[-maxvals:]
    else:
        gicvals = cleangicdata
    if len(cleangicdata) > 2:
        active = 1
        gicval = np.median(gicvals)
    else:
        active = 0
        gicval = 0.0
    if debug:
        print (gicval, active, amount)
    comment = "median of {} largest absolut values from {} stations ({})".format(maxvals, amount, ",".join(addcommlist))

    gicnewsql = _create_gicnow_sql(gicval,start,active, comment)

    return [gicnewsql]
    """

def main(argv):
    """
    METHODS:
        read_gic_data
    """
    version = "1.0.0"
    GICNUMS = [1,2,3,4,5,6,7,8,9]
    configpath = '' # is only necessary for monitoring
    sqllist = []
    gicrange = 360
    savepath = "/tmp/gicdata.csv"
    endtime = datetime.utcnow()
    debug = False
    init = False # create table if TRUE
    statusmsg = {}

    usage = 'gic_extractor.py -c <config>'
    try:
        opts, args = getopt.getopt(argv,"hc:r:e:s:ID",["config=","init=","range=","endtime=","savepath=","debug=",])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('-------------------------------------')
            print('Description:')
            print('gic_extractor.py extracts CME predictions from Scoreboard ')
            print('-------------------------------------')
            print('Usage:')
            print(usage)
            print('-------------------------------------')
            print('Options:')
            print('-c            : configuration data ')
            print('-r            : range in minutes ')
            print('-e            : endtime, default is utcnow')
            print('-s            : savepath for exporting csv file ')
            print('-------------------------------------')
            print('Examples:')
            print('---------')
            print('python3 gic_extractor.py -c /home/cobs/CONF/wic.cfg -D')
            print('---------')
            sys.exit()
        elif opt in ("-c", "--config"):
            configpath = os.path.abspath(arg)
        elif opt in ("-r", "--range"):
            gicrange = arg
        elif opt in ("-e", "--endtime"):
            endtime = arg
        elif opt in ("-s", "--savepath"):
            savepath = os.path.abspath(arg)
        elif opt in ("-I", "--init"):
            init = True
        elif opt in ("-D", "--debug"):
            debug = True

    if debug:
        print ("Running gic-extractor version:", version)

    # 1. conf and logger:
    # ###########################
    if debug:
        print ("Read and check validity of configuration data")
        print (" and activate logging scheme as selected in config")
    config = GetConf(configpath)
    config = DefineLogger(config=config, category = "Info", job=os.path.basename(__file__), newname='mm-info-sw.log', debug=debug)
    if debug:
        print (" -> Done")

    # 2. database:
    # ###########################
    try:
        config = ConnectDatabases(config=config, debug=debug)
        db = config.get('primaryDB')
        connectdict = config.get('conncetedDB')
    except:
        statusmsg[name1] = 'database failed'

    # 6. Read GIC data:
    # ###########################
    try:
        if debug:
            print(" -  Getting GIC:")
        newsql = read_gic_data(db,source='GICAUT',maxsensor=9, minutes=gicrange, debug=debug)
        sqllist.extend(newsql)
        statusmsg['GIC data access'] = 'success'
    except:
        statusmsg['GIC data access'] = 'failed'

    sqllist = [el for el in sqllist if el]

    """
    if debug:
        print ("Debug selected - sql call looks like:")
        print (sqllist)
    else:
      for dbel in connectdict:
        db = connectdict[dbel]
        print ("     -- Writing data to DB {}".format(dbel))
        if init:
            swtableinit(db)
        if len(sqllist) > 0:
            _execute_sql(db,sqllist, debug=debug)
    """

    # Logging section
    # ###########################
    if not debug and config:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

if __name__ == "__main__":
   main(sys.argv[1:])