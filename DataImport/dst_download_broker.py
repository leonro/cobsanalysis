#!/usr/bin/env python

from magpy.stream import *
import getopt


"""
DESCRIPTION
   Downloads DST index data from Kyoto:
PREREQUISITES
   The following packegas are required:
      geomagpy >= 1.1.7
      martas.martaslog
      martas.acquisitionsupport
      analysismethods
PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -y year                :   int     :  default year is obtained from utcnow
    -m month               :   int     :  default month is obtained from utcnow

APPLICATION
    PERMANENTLY with cron:
        python3 dst_import.py -c /home/user/CONF/wic.cfg
"""

def get_dst(year=2023, month=11, baseurl='https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/',debug=False):
    dst = DataStream()
    sm = int((year/100. - int(year/100.))*100)
    url = '{c}{a}{b}/dst{d}{b}.for.request'.format(a=year, b=str(month).zfill(2), c=baseurl, d=sm)
    if debug:
        print ("URL = ", url)
    else:
        print ("reading URL = ", url)
        dst = read(url)
    return dst

def get_range(year,month, debug=False):
    now = datetime.utcnow()
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    if not year:
        years = [now.year]
    elif year in ['all','All','ALL']:
        years = range(now.year-5, now.year+1)
    else:
        try:
            years = [year]
        except:
            pass
    if not month and not year:
        months = [now.month]
    elif month and month in months:
        months = [month]
    if debug:
        print ("Getting data for ranges {} and {}".format(years, months))
    return years, months

def main(argv):
    version = '1.0.0'
    statusmsg = {}
    year, month = None, None
    baseurl = 'https://wdc.kugi.kyoto-u.ac.jp/dst_realtime/'
    savepath = '/home/cobs/SPACE/incoming/Kyoto/Dst'
    debug=False

    try:
        opts, args = getopt.getopt(argv,"hy:m:s:D",["year=","month=","savepath=","debug="])
    except getopt.GetoptError:
        print ('dst_import.py')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- dst_import.py will obtain dst values directly from kyoto obs --')
            print ('-----------------------------------------------------------------')
            print ('detailed description ..')
            print ('...')
            print ('...')
            print ('-------------------------------------')
            print ('Usage:')
            print ('python dst_import.py -c <config>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-y            : year, default is obtained from utcnow')
            print ('-m            : month, default is obtained from utcnow')
            print ('-s            : saving data to a specific path')
            print ('-------------------------------------')
            print ('Application:')
            print ('python dst_import.py')
            print ('python dst_import.py -y 2022 -m 11')
            print ('python dst_import.py -y all')
            sys.exit()
        elif opt in ("-y", "--year"):
            # get an endtime
            if arg in ['all','All','ALL']:
                year = arg
            else:
                year = int(arg)
        elif opt in ("-m", "--month"):
            # get an endtime
            month = int(arg)
        elif opt in ("-s", "--savepath"):
            # get an endtime
            savepath = os.path.abspath(arg)
        elif opt in ("-D", "--debug"):
            # delete any / at the end of the string
            debug = True

    print ("Running dst import version {}".format(version))
    print ("--------------------------------")

    years, months = get_range(year,month,debug=debug)

    if debug:
        print("Getting DST data")
        print("---------------------")
    for ye in years:
        for mo in months:
            dst = get_dst(year=ye, month=mo,debug=debug)
            if dst.length()[0] > 0:
                print ("Success for year {} and month {}".format(ye,mo))
                dst.header['DataSource'] = 'Kyoto Observatory'
                dst.header['DataReferences'] = baseurl
                print ("Saving to", savepath)
                dst.write(savepath, filenamebegins='Dst_', format_type='PYCDF',dateformat='%Y%m',coverage='month')


    print ("------------------------------------------")
    print ("  dst_import finished")
    print ("------------------------------------------")
    print ("SUCCESS")

if __name__ == "__main__":
   main(sys.argv[1:])
