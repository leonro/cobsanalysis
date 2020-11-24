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

import requests

# ####################
#  Importing database
# ####################

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
path2log = '/home/cobs/ANALYSIS/Logs/magtitle.log'
endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=4),"%Y-%m-%d")

ok =True
if ok:
    #try:

    from pickle import load as pload
    priminst = '/home/cobs/ANALYSIS/Logs/primaryinst.pkl'
    lst = pload(open(priminst,'rb'))
    varioinst = lst[0]

    print ('Reading data from primary instrument')
    data = readDB(db,varioinst,starttime=starttime)
    kvals = data.k_fmi(k9_level=500)

    print ('Getting Solare image')
    request = requests.get('http://sohowww.nascom.nasa.gov/data/realtime/eit_304/512/latest.jpg', timeout=20, stream=True, verify=False)
    # Open the output file and make sure we write in binary mode
    with open('/home/cobs/ANALYSIS/TitleGraphs/EIT304_latest.jpg', 'wb') as fh:
        # Walk through the request response in chunks of 1024 * 1024 bytes, so 1MiB
        for chunk in request.iter_content(1024 * 1024):
            # Write the chunk to the file
            fh.write(chunk)

    #urllib.urlretrieve("http://sohowww.nascom.nasa.gov/data/realtime/eit_304/512/latest.jpg","/home/cobs/ANALYSIS/TitleGraphs/EIT304_latest.jpg")
    img = imread("/home/cobs/ANALYSIS/TitleGraphs/EIT304_latest.jpg")

    print ('Plotting streams')
    fig = mp.plotStreams([kvals,data],[['var1'],['x']],specialdict = [{'var1':[0,18]},{}],symbollist=['z','-'],bartrange=0.06, colorlist=['k','y'], gridcolor='#316931',singlesubplot=True,noshow=True,opacity=0.5)

    fig.set_size_inches(9.56, 2.19, forward=True)
    fig.patch.set_facecolor('black')
    fig.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)

    ax = fig.axes
    for a in ax:
        a.axis('off')
        #a.set_frame_on(False)
    print (" p1")
    maxk = kvals._get_max('var1')
    if maxk >= 6: 
        img2 = imread("/home/cobs/ANALYSIS/TitleGraphs/polarlichter.v02.jpg")
        newax2 = fig.add_axes([0.0, 0.0, 1.0, 1.0], anchor='SE', zorder=-1)
        newax2.imshow(img2,origin='upper')
        newax2.axis('off')
    else:
        img2 = imread("/home/cobs/ANALYSIS/TitleGraphs/hyades.v03.jpg")
        newax2 = fig.add_axes([0.0, 0.0, 1.0, 1.0], anchor='SE', zorder=-1)
        newax2.imshow(img2,origin='upper')
        newax2.axis('off')

    print (" p2")
    newax = fig.add_axes([0.0, 0.0, 1.0, 1.0], anchor='SW', zorder=-1)
    newax.imshow(img,origin='upper')
    newax.axis('off')

    #plt.show()
    savepath = "/home/cobs/ANALYSIS/TitleGraphs/title_mag.png"
    plt.savefig(savepath)
    print ("Save 1 done")
    savepath2 = "/srv/products/graphs/title/title_mag.png"
    #/srv/products/graphs/title/
    plt.savefig(savepath2)
    print ("Save 2 done")

    # upload plot to homepage using credentials
    #cred = 'cobshomepage'
    #address=mpcred.lc(cred,'address')
    #user=mpcred.lc(cred,'user')
    #passwd=mpcred.lc(cred,'passwd')
    #port=mpcred.lc(cred,'port')
    #remotepath = 'zamg/images/slideshow/'

    #ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
    #scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
#except:
#    failure = True

#if not failure:
#    analysisdict.check({'script_title_mag_graph': ['success','=','success']})
#    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
#    print ("        mag_graph successfully finished         ")
#    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
#else:
#    analysisdict.check({'script_title_mag_graph': ['failure','=','success']})


