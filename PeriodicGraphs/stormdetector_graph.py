#!/usr/bin/env python
#********************************************************************
# Plots development of storms continuously up till 24 hrs after storm
# initiation.
#
# Created by RLB on 2016-03-29.
#********************************************************************

import os
import sys
from datetime import datetime, timedelta
from dateutil import tz
import matplotlib
matplotlib.use("Agg")
from matplotlib.dates import DateFormatter
from subprocess import call

from magpy.stream import *
from magpy.transfer import *
from magpy.mpplot import *
import magpy.opt.cred as mpcred


def automated_storm_plot(magdata, satdata_1m, satdata_5m, mag_results, sat_results,
        plotvars=['x','var2', 'var1'], logpflux=True, estimate=False, savepath='',upload=False):
           
    date = datetime.strftime(mag_results['ssctime'],'%Y-%m-%d')
    ssctime = mag_results['ssctime']
    ssctime_st = datetime.strftime(ssctime,'%H:%M:%S')
    ssctime_st_long = datetime.strftime(ssctime,'%Y-%m-%dT%H:%M:%S')
    acetime = sat_results['satssctime']
    acetime_st = datetime.strftime(acetime,'%H:%M:%S')
    acetime_st_long = datetime.strftime(acetime,'%Y-%m-%dT%H:%M:%S')
    if logpflux:
        pflux = satdata_5m.ndarray[KEYLIST.index('var1')].astype(float)
        logpflux = np.log10(pflux)
        satdata_5m._put_column(np.asarray(logpflux), 'var1')
    satdata_5m.header['col-var1'] = 'log10(P-flux 47-68 keV)\n   '
    satdata_5m.header['unit-col-var1'] = 'p/cm2-s-ster-MeV'

    magdata = magdata.nfilter()
    dH = mag_results['amp']
    h_max = magdata._get_max(plotvars[0])
    h_min = magdata._get_min(plotvars[0])

    v_wind = sat_results['vwind']
    v_max = satdata_1m._get_max(plotvars[1])
    v_min = satdata_1m._get_min(plotvars[1])
    
    startdate = datetime.strftime(num2date(magdata.ndarray[0][0]), '%Y-%m-%d')
    enddate = datetime.strftime(num2date(magdata.ndarray[0][-1]), '%Y-%m-%d')
    if startdate == enddate:
        datestr = startdate
    else:
        datestr = startdate+' - '+enddate

    fig = plotStreams([magdata, satdata_1m, satdata_5m], [[plotvars[0]], [plotvars[1]], [plotvars[2]]],
            noshow=True,
            plottitle="Magnetic (Conrad Obs) and satellite (ACE SWEPAM+EPAM) data\n(%s)" % datestr)

    axes = gcf().get_axes()
    
    # Draw lines at detection points:
    axes[0].axvline(x=date2num(ssctime),color='red',ls='--',lw=2,zorder=0, clip_on=False)
    axes[1].axvline(x=date2num(acetime),color='gray',ls='--',lw=2,zorder=0, clip_on=False)
    axes[2].axvline(x=date2num(acetime),color='gray',ls='--',lw=2,zorder=0, clip_on=False)
    
    # Give larger borders to lines:
    axes[0].axvline(x=date2num(ssctime), ymin=0, ymax=0.94, color='red',ls='-',lw=15,zorder=0,alpha=0.3, clip_on=False)
    axes[1].axvline(x=date2num(acetime), ymin=0, ymax=0.94, color='gray',ls='-',lw=15,zorder=0,alpha=0.3, clip_on=False)
    axes[2].axvline(x=date2num(acetime), ymin=0.07, ymax=0.94, color='gray',ls='-',lw=15,zorder=0,alpha=0.3, clip_on=False)
    
    # Annotate with automatically detected variables:
    if estimate:
        axes[0].text(0.50, 0.90, "No SSC detected!",
                    verticalalignment='top', horizontalalignment='left',
                    transform=axes[0].transAxes, color='red', style='italic')
        magstring = "Expected SSC:\n%s UTC\ndH = ??? nT" % (ssctime_st)
    else:
        magstring = "SSC:\n%s UTC\ndH = %.1f nT" % (ssctime_st, dH)
        
    axes[0].text(0.78, 0.90, magstring,
                verticalalignment='top', horizontalalignment='left',
                transform=axes[0].transAxes,
                bbox=dict(boxstyle="square",fc='1.0')
                )
    axes[1].text(0.78, 0.90, "CME at ACE:\n%s UTC\nv = %.0f km/s" % (acetime_st, v_wind),
                verticalalignment='top', horizontalalignment='left',
                transform=axes[1].transAxes,
                bbox=dict(boxstyle="square",fc='1.0')
                )

    # Format time to fit nicely on x-axis:
    myFmt = DateFormatter('%H:%M')
    axes[2].xaxis.set_major_formatter(myFmt)

    # Save to output file:
    #plt.show()
    magoutfile = os.path.join(savepath, "stormplot_%s.png" % date)
    plt.savefig(magoutfile)

    if upload:
        cred = 'cobshomepage'
        address=mpcred.lc(cred,'address')
        user=mpcred.lc(cred,'user')
        passwd=mpcred.lc(cred,'passwd')
        port=mpcred.lc(cred,'port')
        remotepath = 'zamg/images/graphs/spaceweather/storms'

        scptransfer(magoutfile,'94.136.40.103:'+remotepath,passwd)

    command = ["cp", magoutfile, "/srv/products/graphs/spaceweather/storms/"]
    print "Executing command %s" % command
    call(command)
    upload = True


if __name__ == '__main__':

    utczone = tz.gettz('UTC')
    cetzone = tz.gettz('CET')
    scriptpath = "/home/cobs/ANALYSIS/DataProducts/StormDetection"
    filelist = os.listdir(os.path.join(scriptpath,'Reports'))
    dates = [f[7:17] for f in filelist]
    now = datetime.utcnow()
    # Use this for testing:
    #now = datetime.strptime('2016-07-20 23:00:00', '%Y-%m-%d %H:%M:%S')
    now = now.replace(tzinfo=utczone)
    today = datetime.strftime(now, '%Y-%m-%d')
    yesterday = datetime.strftime(now - timedelta(days=1), '%Y-%m-%d')
    
    # Check if report is recent enough to plot:
    if today in dates:
        filepath = os.path.join(scriptpath,'Reports/Report_'+today+'.txt')
    elif yesterday in dates:
        filepath = os.path.join(scriptpath,'Reports/Report_'+yesterday+'.txt')
    else:
        print("Nothing to plot here. Exiting.")
        sys.exit()
    
    # Read report file:
    report = open(filepath, 'r')
    reportdata = report.readlines()
    
    # Find index of SSC data in report file:
    if len(reportdata) > 25:
        split_idxs = [i for i, j in enumerate(reportdata) if j.startswith('-----')]
        if len(split_idxs) > 1:
            try:
                skip = [i for i, j in enumerate(reportdata) if j.startswith('Time of SSC')][0] - 1
            except:
                skip = 2
        else:
            skip = split_idxs[0] + 4
    else:
        skip = 2
    
    # Retrieve data from report file:
    if reportdata[skip+1].startswith('Time of SSC'):
        sscdata = reportdata[skip+1].split()
        cmedata = reportdata[skip+8].split()
        ssctime = datetime.strptime(sscdata[4]+'T'+sscdata[5], '%Y-%m-%dT%H:%M:%S')
        ssctime = ssctime.replace(tzinfo=cetzone)
        ssctime = ssctime.astimezone(utczone)
        cmetime = datetime.strptime(cmedata[6]+'T'+cmedata[7], '%Y-%m-%dT%H:%M:%S')
        cmetime = cmetime.replace(tzinfo=cetzone)
        cmetime = cmetime.astimezone(utczone)
        sscamp = float(reportdata[skip+2].split()[3])
        vwind = float(reportdata[skip+9].split()[4])
        estimate = False
    elif reportdata[skip+1].startswith('Time of CME'):
        ssctime = None
        cmedata = reportdata[skip+1].split()
        cmetime = datetime.strptime(cmedata[8]+'T'+cmedata[9], '%Y-%m-%dT%H:%M:%S')
        cmetime = cmetime.replace(tzinfo=cetzone)
        cmetime = cmetime.astimezone(utczone)
        sscdata = reportdata[skip+2].split()
        ssctime = datetime.strptime(sscdata[5]+'T'+sscdata[6], '%Y-%m-%dT%H:%M:%S')
        ssctime = ssctime.replace(tzinfo=cetzone)
        ssctime = ssctime.astimezone(utczone)
        sscamp = np.nan
        vwind = float(reportdata[skip+3].split()[4])
        estimate = True

    if (now - ssctime).seconds > 24.*60.*60.:
        print("Time since SSC exceeds 24 hours. Exiting.")
        sys.exit()
        
    basepath = '/srv/archive'
    dbcred = 'cobsdb'
    dbhost = mpcred.lc(dbcred, 'host')
    dbuser = mpcred.lc(dbcred, 'user')
    dbpasswd = mpcred.lc(dbcred, 'passwd')
    dbname = mpcred.lc(dbcred, 'db')

    try:
        inst = 'FGE_S0252_0001_0001'
        db = mysql.connect(host=dbhost,user=dbuser,passwd=dbpasswd,db=dbname)
        magdata = readDB(db, inst, starttime=cmetime-timedelta(hours=1), endtime=endtime)
        if len(magdata.ndarray[0]) == 0:
            raise ValueError("No data available from database.")
    except:
        print("Reading data from archive...")
        magdata = read(os.path.join(basepath,"WIC","FGE_S0252_0001","FGE_S0252_0001_0001","FGE_S0252_0001_0001_*"),
                   starttime=cmetime-timedelta(hours=1), endtime=now)
    satdata_1m = read(os.path.join(basepath,"external","esa-nasa","ace","collected","ace_1m_*"),
                                   starttime=cmetime-timedelta(hours=1), endtime=now)
    satdata_5m = read(os.path.join(basepath,"external","esa-nasa","ace","collected","ace_5m_*"),
                                   starttime=cmetime-timedelta(hours=1), endtime=now)

    sat_results = {'satssctime': cmetime, 'vwind': vwind}
    mag_results = {'ssctime': ssctime, 'amp': sscamp}
    
    #plotsavepath = "/srv/products/graphs/spaceweather/storms/"
    #plotsavepath = "/home/cobs/ANALYSIS/StormDetection/Reports/"
    plotsavepath = os.path.join(scriptpath, "Reports")
    automated_storm_plot(magdata, satdata_1m, satdata_5m, mag_results, sat_results, estimate=estimate,
                         savepath=plotsavepath, upload=True)
    #magdata = magdata.trim(starttime=cmetime-timedelta(minutes=10), endtime=ssctime+timedelta(minutes=20))
    #satdata_1m = satdata_1m.trim(starttime=cmetime-timedelta(minutes=10), endtime=ssctime+timedelta(minutes=20))
    #satdata_5m = satdata_5m.trim(starttime=cmetime-timedelta(minutes=10), endtime=ssctime+timedelta(minutes=20))
    #automated_storm_plot(magdata, satdata_1m, satdata_5m, mag_results, sat_results,
    #                     logpflux=False, closeup=True)
    



