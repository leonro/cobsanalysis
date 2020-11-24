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

path2log = '/home/cobs/ANALYSIS/Logs/title_gravity.log'
path2images = '/home/cobs/ANALYSIS/TitleGraphs'
#path2images = '/home/leon/CronScripts/MagPyAnalysis/Gravity'

endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=7),"%Y-%m-%d")

path = '/srv/products/data/gravity/SGO/GWRSGC025_25_0002/raw'

try:
    data = read(os.path.join(path,'F1*'),starttime=starttime, endtime=endtime)
    #data = read('/srv/products/data/gravity/SGO/GWRSHC025_25_0002/raw/F1161006.025')

    # get last values of filt
    lastval = data.ndarray[1][-1]

    print (lastval)

    if lastval < 0:
        img = imread(os.path.join(path2images,"ebbe_neu.jpg"))
    else:
        img = imread(os.path.join(path2images,"flut_neu.jpg"))

    fig = mp.plotStreams([data],[['x','z']], fill=['x'], colorlist=['g','w'], gridcolor='#316931', opacity=0.3,singlesubplot=True,noshow=True)

    fig.set_size_inches(9.56, 2.19, forward=True)
    #fig.set_size_inches(17.56, 2.19, forward=True)
    fig.patch.set_facecolor('black')
    fig.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)

    # remove boundary and rotate image

    ax = fig.axes
    for a in ax:
        a.axis('off')

    newax = fig.add_axes([0.0, 0.0, 1.0, 1.0],anchor='SW',zorder=-1)
    newax.imshow(img,origin='upper')
    newax.axis('off')

    #plt.show()

    savepath2 = savepath = "/srv/products/graphs/title/title_gravity.png"
    plt.savefig(savepath2)

    #savepath = "/home/cobs/ANALYSIS/TitleGraphs/title_gravity.png"
    #plt.savefig(savepath)

    #cred = 'cobshomepage'
    #address=mpcred.lc(cred,'address')
    #user=mpcred.lc(cred,'user')
    #passwd=mpcred.lc(cred,'passwd')
    #port=mpcred.lc(cred,'port')
    #remotepath = 'zamg/images/slideshow/'

    #ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=$
    #scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
except:
    failure = True

if not failure:
    analysisdict.check({'script_title_gravity_graph': ['success','=','success']})
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
    print ("      gravity_graph successfully finished       ")
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
else:
    analysisdict.check({'script_title_gravity_graph': ['failure','=','success']})


