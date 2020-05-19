#!/usr/bin/env python

"""
Skeleton for graphs
"""

from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred


dbpasswd = mpcred.lc('cobsdb','passwd')
try:
    # Test MARCOS 1
    print ("Connecting to primary MARCOS...")
    db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")
    print db
except:
    print ("... failed")
    try:
        # Test MARCOS 2
        print ("Connecting to secondary MARCOS...")
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
        print db
    except:
        print ("... failed -- aborting")
        sys.exit()
print ("... success")

# ####################
#  Activate monitoring
# #################### 

try:
    from magpy.opt.analysismonitor import *
    analysisdict = Analysismonitor(logfile='/home/cobs/ANALYSIS/Logs/AnalysisMonitor_cobs.log')
    analysisdict = analysisdict.load()
except:
    print ("Analysis monitor failed")
    pass


# ####################
#  Basic definitions
# #################### 

failure = False
path2log = '/home/cobs/ANALYSIS/Logs/title_weather.log'
path2images = '/home/cobs/ANALYSIS/TitleGraphs'

endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=7),"%Y-%m-%d")

try:
    data = readDB(db,'METEO_T7_0001_0001',starttime=starttime)

    #hight = data.extract('f',0,'>')
    #newdata = data.copy()
    #newdata = mergeStreams(newdata,hight,keys=['f'])

    #put max/min values on data

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

    savepath = "/home/cobs/ANALYSIS/TitleGraphs/title_weather.png"
    plt.savefig(savepath)

    cred = 'cobshomepage'
    address=mpcred.lc(cred,'address')
    user=mpcred.lc(cred,'user')
    passwd=mpcred.lc(cred,'passwd')
    port=mpcred.lc(cred,'port')
    remotepath = 'zamg/images/slideshow/'

    #ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
    scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
except:
    failure = True


if not failure:
    analysisdict.check({'script_title_weather_graph': ['success','=','success']})
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
    print ("      weather_graph successfully finished       ")
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
else:
    analysisdict.check({'script_title_weather_graph': ['failure','=','success']})

