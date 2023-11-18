#!/usr/bin/env python
"""
Importing data from warmbad villach
"""
from magpy.stream import *
import csv
import pandas as pd
from magpy.database import *
from magpy.opt import cred as mpcred
import getopt

scriptpath = os.path.dirname(os.path.realpath(__file__))
anacoredir = os.path.abspath(os.path.join(scriptpath, '..', 'core'))
sys.path.insert(0, anacoredir)
from analysismethods import DefineLogger, ConnectDatabases, load_current_data_sub
from martas import martaslog as ml
from martas import sendmail as sm
from acquisitionsupport import GetConf2 as GetConf


def walk_dir(directory_path, filename, date, dateformat):
    """
    Method to extract filename with wildcards or date patterns by walking through a local directory structure
    """
    # Walk through files in directory_path, including subdirectories
    print ("Scanning")
    pathlist = []
    if filename == '':
        filename = '*'
    if dateformat in ['','ctime','mtime']:
        filepat = filename
    else:
        filepat = filename % date
    #print ("Checking directory {} for files with {}".format(directory_path, filepat))
    for root, _, filenames in os.walk(directory_path):
        for filename in filenames:
            if fnmatch.fnmatch(filename, filepat):
                file_path = os.path.join(root,filename)
                if dateformat in ['ctime','mtime']:
                    if dateformat == 'ctime':
                        tcheck = datetime.fromtimestamp(os.path.getctime(file_path))
                    if dateformat == 'mtime':
                        tcheck = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if tcheck.date() > date.date():
                        pathlist.append(file_path)
                else:
                    pathlist.append(file_path)
    return pathlist

def read_friedmann(f, columns=['Time', 'Count'], debug=False):
    dataframe = None
    def to_number(value):
        value = str(value).replace(',','.')
        try:
            value = float(value)
        except:
            value = np.nan
        return value

    def to_date(value):
        try:
            d = datetime.strptime(value, "%d.%m.%Y %H")
        except:
            try:
                d = datetime.strptime(value, "%Y-%m-%d %H:%M")
            except:
                d = datetime.strptime(value, "%d.%m.%Y %H:%M")
        return d

    with open(f) as csv_file:
        if debug:
            print ("Processing file {} ".format(f))
        csv_reader = csv.reader(csv_file, delimiter=';')
        line_count = 0
        lines = []
        for row in csv_reader:
            #print (row)
            if row and len(row) >= 2 and not row[0]=='':
                try:
                    t = to_date(row[0])
                    v = to_number(row[1])
                    lines.append([t,v])
                except:
                    pass
            line_count += 1
        if debug:
            print(f'Processed {line_count} lines.')
            if not lines:
                print (" !!!!!!!!!! WARNING: empty structure - check read method")
        dataframe = pd.DataFrame(lines, columns=columns)
    return dataframe

def read_all_since(mpath="/home/leon/GeoSphereCloud/Daten/WBV",fn = "*Radon_MW*.csv", dt = datetime(1900,1,1),debug=False):
    tf = "ctime"
    paths = walk_dir(mpath,fn, dt,tf)
    fulldf = pd.DataFrame()
    for path in paths:
        df = read_friedmann(path,debug=debug)
        #fulldf = fulldf.append(df)
        fulldf = pd.concat([fulldf,df])
    fulldf.sort_values(by='Time', inplace=True)
    #print (list(fulldf.columns))
    return fulldf.drop_duplicates()

def read_xls_since(mpath="/home/leon/GeoSphereCloud/Daten/WBV",fn = "Freibad*.xls", dt = datetime(1900,1,1)):
    tf = "ctime"
    paths = walk_dir(mpath,fn, dt,tf)
    fulldf = pd.DataFrame()
    for path in paths:
        print (path)
        df = pd.read_excel(path)
        df.rename(columns = {'MM.TT.JJJJ/HH ':'Time'}, inplace = True)
        df.rename(columns = {'RADON':'Count'}, inplace = True)
        df.rename(columns = {'TEMPERATUR':'Temp'}, inplace = True)
        toremove = [col for col in list(df.columns) if not col in ["Time","Count","Temp"]]
        df = df.drop(columns=toremove)
        #print (df)
        #fulldf = fulldf.append(df)
        fulldf = pd.concat([fulldf,df])
    fulldf.sort_values(by='Time', inplace=True)
    return fulldf.drop_duplicates()

def read_xlsrow_since(mpath="/home/leon/GeoSphereCloud/Daten/WBV",fn = "Freibad*.xls", dt = datetime(1900,1,1),debug=False):
    """
    Description:
    reads excel files with data organized in rows instead of columns
    """
    tf = "ctime"
    paths = walk_dir(mpath,fn, dt,tf)
    fulldf = pd.DataFrame()
    for path in paths:
        if debug:
            print (path)
        try:
            df = pd.read_excel(path)
            succ = True
        except:
            succ = False
        if succ:
            ll = df.values.tolist()
            #print (ll)
            til,ral,tel,ntil =[],[],[],[]
            if len(ll)>0:
                for l in ll:
                    colname = str(l[0])
                    #print (colname)
                    if colname.find("Anzeige") >= 0:
                        sp=colname.split()
                        dt = None
                        for el in sp:
                            try:
                                dt = dparser.parse(el,fuzzy=True)
                            except:
                                pass
                        year = dt.year
                        month = dt.month
                    if colname.find("Temperatur") >= 0:
                        tel = l[1:]
                    if (colname.find("Radon") >= 0 or colname.find("RADON") >= 0) and len(til) > 0:
                        #after time row
                        ral = l[1:]
                    if colname.find("TT.MM") >= 0:
                        til = l[1:]
                        ntil = [datetime.strftime(datetime(year,month,int(el.split(".")[0]),int(el.split()[1])),"%d.%m.%Y %H") for el in til]
            if not tel:
                tel = [np.nan] * len(ral)
            if ntil:
                if debug:
                    print ("Adding", len(ntil))
                df = pd.DataFrame(list(zip(ntil, ral, tel)), columns =['Time', 'Count', 'Temp'])
                fulldf = pd.concat([fulldf,df])
    fulldf.sort_values(by='Time', inplace=True)
    return fulldf.drop_duplicates()

def pd2datastream(dataframe,sensorid="RADONWBV_9500_0001", columns=[], units=[], dateformat=None):
    array = dataframe.to_numpy()
    if not columns:
        columns = list(dataframe.columns)
    st = DataStream()
    st.header = {"SensorID":sensorid}
    ar = [[] for el in KEYLIST]
    for idx,el in enumerate(array.T):
        if KEYLIST[idx].find('time') > -1:
            if not dateformat:
                print (el)
                el = date2num(pd.to_datetime(el))
            else:
                el = date2num(pd.to_datetime(el, format=dateformat, errors='coerce'))
        col = 'col-{}'.format(KEYLIST[idx])
        uni = 'unit-col-{}'.format(KEYLIST[idx])
        if columns and idx < len(columns) and columns[idx]:
            st.header[col] = columns[idx]
        if units and idx < len(units) and units[idx]:
            st.header[uni] = units[idx]
        ar[idx] = el
    st.ndarray = np.asarray(ar,dtype=object)
    st = drop_nans(st,'time')
    return st

def drop_nans(stream,key, debug=False):
    """
        Same function (Datastream()._drop_nans) is available since MagPy 1.1.7
    """
    # Method only works with numerical columns and the time column
    searchlist = ['time']
    searchlist.extend(NUMKEYLIST)
    if debug:
        tstart = datetime.utcnow()

    if len(stream.ndarray[0]) > 0 and key in searchlist:
            # get the indicies with NaN's and then use numpy delete
            array = [np.asarray([]) for elem in KEYLIST]
            ind = KEYLIST.index(key)
            #indicieslst = [i for i,el in enumerate(self.ndarray[ind].astype(float)) if np.isnan(el) or np.isinf(el)]
            col = np.asarray(stream.ndarray[ind]).astype(float)
            indicieslst = []
            for i,el in enumerate(col):
                if np.isnan(el) or np.isinf(el):
                    indicieslst.append(i)
            for index,tkey in enumerate(KEYLIST):
                if len(stream.ndarray[index]) > 0 and len(stream.ndarray[index]) == len(col):
                    array[index] = np.delete(stream.ndarray[index], indicieslst)
    if debug:
        tend = datetime.utcnow()
        print ("Needed", (tend-tstart).total_seconds())
    return DataStream([LineStruct()],stream.header,np.asarray(array,dtype=object))

def export_data(datastream, config={}, exportdict={}, publevel=1, debug=False):
    """
    DESCRIPTION:
        Export data sets to selected directories and/or databases
    """

    success = True
    connectdict = config.get('conncetedDB')

    for extyp in exportdict:
        if extyp == 'CDF':
            print ("     -- Saving data to file - CDF")
            if debug:
                print (" DEBUG: Would write to ", exportdict.get(extyp))
                print (datastream.header.get("DataID"))
            else:
                datastream.write(os.path.join(exportdict.get(extyp),datastream.header.get("DataID")),filenamebegins=datastream.header.get("DataID")+"_",format_type='PYCDF',coverage='month')
            print ("       -> Done")
        if extyp == 'DB':
            print("     -- Saving data to Database")
            if connectdict:
                # save
                for dbel in connectdict:
                    if debug:
                        print (" DEBUG: Would write to {} in table {}".format(dbel,datastream.header.get("DataID")))
                    else:
                        db = connectdict[dbel]
                        print ("     -- Writing {} data to DB {}".format(datastream.header.get("DataID"),dbel))
                        writeDB(db,datastream,tablename=datastream.header.get("DataID"))
                print ("       -> Done")
            else:
                print ("       -> No database credentials provided")

    return success
def main(argv):
    version = "1.0.0"
    configpath = ''
    destination = ''
    output = ''
    statusmsg={}
    extlist=[]
    extdict={}
    mpath = ''
    begin = datetime(2023, 1, 1)
    end = datetime.utcnow()
    debug = False
    init = False # create table if TRUE

    usage = 'wbvimport.py -c <config> -s <source> -d <destination> -o <output> -b <begin> -e <end>'
    try:
        opts, args = getopt.getopt(argv,"hc:s:d:o:b:e:ID",["config=","source=","destination=","output=","begin=","end=","init=","debug=",])
    except getopt.GetoptError:
        print(usage)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('-------------------------------------')
            print('Description:')
            print('wbvimport.py extracts csv and excel data from ')
            print('radon measurements of warmbad villach. ')
            print('The tool requires pandas for reading such')
            print('datasets and converts them to magpy data structures.')
            print('Data is saved to a MagPy database or files.')
            print('Provided options will overwrite configuration data.')
            print('-------------------------------------')
            print('Usage:')
            print(usage)
            print('-------------------------------------')
            print('Options:')
            print('-c            : configuration data ')
            print('-b            : begin - default is 1900-01-01 ')
            print('-e            : end - default is now - not implemented so far')
            print('-s            : sourcepath - directory with csv/xls data')
            print('-d            : "file" (provide full path with option o) and/or "database" (provide credential with option o)')
            print('-o            : full path and/or "database" credentials')
            print('-------------------------------------')
            print('Examples:')
            print('---------')
            print('python3 wbvimport.py -c /home/cobs/CONF/wic.cfg -s /Users/leon/GeoSphereCloud/Daten/WBV -b 2023-04-01 -D')
            print('---------')
            sys.exit()
        elif opt in ("-c", "--config"):
            configpath = os.path.abspath(arg)
        elif opt in ("-s", "--sourcepath"):
            mpath = os.path.abspath(arg)
        elif opt in ("-b", "--begin"):
            begin = DataStream()._testtime(arg)
        elif opt in ("-e", "--end"):
            end = DataStream()._testtime(arg)
        elif opt in ("-d", "--destination"):
            destination = arg
        elif opt in ("-o", "--output"):
            output = arg
        elif opt in ("-I", "--init"):
            init = True
        elif opt in ("-D", "--debug"):
            debug = True

    if debug:
        print ("Running wbvimport version:", version)

    # 1. conf and logger:
    # ###########################
    if debug:
        print ("Read and check validity of configuration data")
        print (" and activate logging scheme as selected in config")
    config = GetConf(configpath)
    config = DefineLogger(config=config, category = "Import", job=os.path.basename(__file__), newname='mm-wbvimport-status.log', debug=debug)
    if debug:
        print (" -> Done")

    # 2. use conf if no parameters are provided:
    # ###########################
    if not mpath:
        mpath = config.get("gammarawdata")
        if not mpath or not os.path.isdir(mpath):
            print ("No approrpiate data source defined - aborting")
            if debug:
                print (mpath)
            sys.exit(1)

    if not destination:
        destination = config.get("gammadatadestination")
        if isinstance(destination, list):
             destination = ",".join(destination)
    if not destination:
        extlist = ["CDF"]
    else:
        destination = destination.replace("database","DB")
        destination = destination.replace("file", "CDF")
        tmpextlist = destination.split(",")
        extlist = [el.strip() for el in tmpextlist]
    if not len(extlist) > 0:
        extlist = ["CDF"]

    if output:
        op = output.split(",")
        if len(op) == len(extlist):
            for pos,el in enumerate(extlist):
                if el == "DB":
                    config["dbcredentials"] = op[pos]
                extdict[el] = op[pos]
        else:
            print (" provided output does not fit to destinations")
    else:
        for el in extlist:
            if el == "CDF":
                extdict[el] = config.get("gammaresults")
            if el == "DB":
                extdict[el] = config.get("dbcredentials")

    if debug:
        print ("Source: {}, Destination: {}".format(mpath,extdict))

    if debug:
        print ("Reading all existing CSV data...")
    # #############################
    counts = read_all_since(mpath=mpath, fn="*Radon_MW*.csv", dt=begin)
    if debug:
        print(counts)

    temps = read_all_since(mpath=mpath, fn="*Radon_Temp*.csv", dt=begin)
    if debug:
        print(temps)

    columns_to_merge_on = ['Time']
    df = pd.merge(counts, temps, how='outer', on=columns_to_merge_on)
    df.sort_values(by='Time', inplace=True)
    stream = pd2datastream(df, columns=['', 'Count', 'Temp'], units=['', '', 'deg C'])
    if debug:
        print ("    CSV - obtained {} datapoints".format(stream.length()[0]))

    print("   CSV -  Applying multiplier")
    tmpstream = stream.trim(starttime="2017-01-01", endtime="2017-03-10T15:30:00")
    tmpstream = tmpstream.multiply({'x': 0.1})
    # apply a radon multiplier of 0.1 between 2017-01-01T00:00:00 and 2017-03-10T15:30:00
    stream = joinStreams(tmpstream, stream)

    if debug:
        print ("Reading all existing XLS data organized in columns...")
    xlsdf = read_xls_since(mpath=mpath, fn="Freibad*.xls", dt=begin)
    xstream = pd2datastream(xlsdf, columns=['', 'Count', 'Temp'], units=['', '', 'deg C'], dateformat="%d.%m.%Y %H")
    xstream = xstream.sorting()
    if debug:
        print ("    XLS - obtained {} datapoints".format(xstream.length()[0]))

    mstream = joinStreams(xstream, stream)

    if debug:
        print ("Reading all existing XLS data organized in rows...")
    xlsrdf = read_xlsrow_since(mpath=mpath, fn="[hH]*.xls", dt=begin)
    xrstream = pd2datastream(xlsrdf, columns=['', 'Count', 'Temp'], units=['', '', 'deg C'], dateformat="%d.%m.%Y %H")
    xrstream = xrstream.sorting()
    if debug:
        print ("    XLS - obtained {} datapoints".format(xrstream.length()[0]))

    nstream = joinStreams(xrstream, mstream)

    nstream.header["SensorID"] = "RADONWBV_9500_0001"
    nstream.header["StationID"] = "WBV"
    nstream.header["DataID"] = "RADONWBV_9500_0001_0001"
    if debug:
        print (" -> Done - obtained {} unique datapoints in total".format(nstream.length()[0]))
        statusmsg['WBV amount'] = '{} data points'.format(nstream.length()[0])

    # 3. database:
    # ###########################
    if debug:
        print ("Connect to databases")
    try:
        config = ConnectDatabases(config=config, debug=debug)
        statusmsg['WBV destination database'] = 'database ok'
    except:
        statusmsg['WBV destination database'] = 'no database credentials'

    if debug:
        print("Writing data to selected output channel ...")

    try:
        succ = export_data(nstream, config=config, exportdict=extdict, publevel=1, debug=debug)
        statusmsg['WBV export'] = 'export fine'
    except:
        statusmsg['WBV export'] = 'export failed'

    if not debug:
        martaslog = ml(logfile=config.get('logfile'),receiver=config.get('notification'))
        martaslog.telegram['config'] = config.get('notificationconfig')
        martaslog.msg(statusmsg)
    else:
        print ("Debug selected - statusmsg looks like:")
        print (statusmsg)

    print ("----------------------------------------------------------------")
    print ("wbvimport SUCCESSFULLY finished")
    print ("----------------------------------------------------------------")

if __name__ == "__main__":
   main(sys.argv[1:])

