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
    from magpy.opt.analysismonitor import *
    analysisdict = Analysismonitor(logfile='/home/cobs/ANALYSIS/Logs/AnalysisMonitor_cobs.log')
    analysisdict = analysisdict.load()
except:
    print ("Analysis monitor failed")
    pass

try:
    # Test MARCOS 1
    print "Connecting to primary MARCOS..."
    db = mysql.connect(host="localhost",user="cobs",passwd=dbpasswd,db="cobsdb")
    print db
except:
    print "... failed"
    try:
        # Test MARCOS 2
        print "Connecting to secondary MARCOS..."
        db = mysql.connect(host="138.22.188.191",user="cobs",passwd=dbpasswd,db="cobsdb")
        print db
    except:
        print "... failed -- aborting"
        sys.exit()

path2log = '/home/cobs/ANALYSIS/Logs/supergrad_graph.log'

endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=3),"%Y-%m-%d")
date = datetime.strftime(endtime,"%Y-%m-%d")

ew = readDB(db,'GP20S3EW_111201_0001_0001',starttime=starttime)
nsstat = readDB(db,'GP20S3NSStatus_012201_0001_0001',starttime=starttime)
ewstat = readDB(db,'GP20S3EWStatus_111201_0001_0001',starttime=starttime)
vstat = readDB(db,'GP20S3VStatus_911005_0001_0001',starttime=starttime)

ns = readDB(db,'GP20S3NS_012201_0001_0001',starttime=starttime)
v = readDB(db,'GP20S3V_911005_0001_0001',starttime=starttime)

# Statustext:
line1 = r'$\mathrm{T_{s1}:\ %s^\circ C,\ T_{s2}:\ %s^\circ C,\ T_{s3}:\ %s^\circ C}$' % (nsstat.ndarray[1][-1],nsstat.ndarray[2][-1],nsstat.ndarray[3][-1])
line2 = r'$\mathrm{L_1:\ %s\mu A,\ L_2:\ %s\mu A,\ L_3:\ %s\mu A}$' % (nsstat.ndarray[12][-1],nsstat.ndarray[13][-1],nsstat.ndarray[14][-1])
#print line1
ew = ew.flag_outlier(threshold=5.0,timerange=timedelta(minutes=10))
ew = ew.remove_flagged()

ns = ns.flag_outlier(threshold=5.0,timerange=timedelta(minutes=10))
ns = ns.remove_flagged()

print ("Here I found", nsstat.ndarray[14][-1])
print ("Here I found", ewstat.ndarray)

try:
    analysisdict.check({'data_threshold_t1_GP20S3EW_111201_0001': [float(ewstat.ndarray[1][-1]),'>',30]})
    analysisdict.check({'data_threshold_t2_GP20S3EW_111201_0001': [float(ewstat.ndarray[2][-1]),'>',30]})
    analysisdict.check({'data_threshold_t3_GP20S3EW_111201_0001': [float(ewstat.ndarray[3][-1]),'>',30]})
    analysisdict.check({'data_threshold_l1_GP20S3EW_111201_0001': [float(ewstat.ndarray[12][-1]),'>',2.3]})
    analysisdict.check({'data_threshold_l2_GP20S3EW_111201_0001': [float(ewstat.ndarray[13][-1]),'>',2.3]})
    analysisdict.check({'data_threshold_l3_GP20S3EW_111201_0001': [float(ewstat.ndarray[14][-1]),'>',2.3]}) 
    #analysisdict.check({'data_threshold_f1_GP20S3EW_111201_0001': [float(ew.ndarray[1][-1]),'>',30000000]})
    #analysisdict.check({'data_threshold_f2_GP20S3EW_111201_0001': [float(ew.ndarray[2][-1]),'>',30000000]})
    #analysisdict.check({'data_threshold_f3_GP20S3EW_111201_0001': [float(ew.ndarray[3][-1]),'>',30000000]})
    analysisdict.check({'data_threshold_t1_GP20S3NS_012201_0001': [float(nsstat.ndarray[1][-1]),'>',30]})
    analysisdict.check({'data_threshold_t2_GP20S3NS_012201_0001': [float(nsstat.ndarray[2][-1]),'>',30]})
    analysisdict.check({'data_threshold_t3_GP20S3NS_012201_0001': [float(nsstat.ndarray[3][-1]),'>',30]})
    analysisdict.check({'data_threshold_l1_GP20S3NS_012201_0001': [float(nsstat.ndarray[12][-1]),'>',2.3]})
    analysisdict.check({'data_threshold_l2_GP20S3NS_012201_0001': [float(nsstat.ndarray[13][-1]),'>',2.3]})
    analysisdict.check({'data_threshold_l3_GP20S3NS_012201_0001': [float(nsstat.ndarray[14][-1]),'>',2.3]})
    analysisdict.check({'data_threshold_f1_GP20S3NS_012201_0001': [float(ns.ndarray[1][-1]),'>',30000000]})
    analysisdict.check({'data_threshold_f2_GP20S3NS_012201_0001': [float(ns.ndarray[2][-1]),'>',30000000]})
    analysisdict.check({'data_threshold_f3_GP20S3NS_012201_0001': [float(ns.ndarray[3][-1]),'>',30000000]})
    analysisdict.check({'data_threshold_t1_GP20S3V_911005_0001': [float(vstat.ndarray[1][-1]),'>',30]})
    analysisdict.check({'data_threshold_t2_GP20S3V_911005_0001': [float(vstat.ndarray[2][-1]),'>',30]})
    analysisdict.check({'data_threshold_t3_GP20S3V_911005_0001': [float(vstat.ndarray[3][-1]),'>',30]})
    analysisdict.check({'data_threshold_l1_GP20S3V_911005_0001': [float(vstat.ndarray[12][-1]),'>',2.3]})
    analysisdict.check({'data_threshold_l2_GP20S3V_911005_0001': [float(vstat.ndarray[13][-1]),'>',2.3]})
    analysisdict.check({'data_threshold_l3_GP20S3V_911005_0001': [float(vstat.ndarray[14][-1]),'>',2.3]})
    analysisdict.check({'data_threshold_f1_GP20S3V_911005_0001': [float(v.ndarray[1][-1]),'>',30000000]})
    analysisdict.check({'data_threshold_f2_GP20S3V_911005_0001': [float(v.ndarray[2][-1]),'>',30000000]})
    analysisdict.check({'data_threshold_f3_GP20S3V_911005_0001': [float(v.ndarray[3][-1]),'>',30000000]})
except:
    print ("Found error while creating analysisdict")

ew = ew.filter(filter_width=timedelta(minutes=1))
ns = ns.filter(filter_width=timedelta(minutes=1))

# providing some Info on content
#print data._get_key_headers()

# Change to 
#mp.plot(ns,['y','z','dy'], bgcolor = '#d5de9c', gridcolor = '#316931',fill=['x','y','z'],confinex=True,noshow=True,plottitle='East-West system')
#ns.header['col-y'] = 'E-W' # - TO CHANGE LABELS ON PLOT
mp.plot(ns,['y','x','dz'], gridcolor = '#316931',fill=['x','y','z'],confinex=True,noshow=True,plottitle='East-West system')
#mp.plotStreams([ns,ew,vert],[['dy'],['dy'],['dy']],bgcolor='#d5de9c', gridcolor='#316931',fill=['dy'],confinex=True, fullday=True, opacity=0.7, plottitle='Field gradients (until %s)' % (datetime.utcnow().date()),noshow=True)
#mp.plot(ns,['y','z','dy'], gridcolor = '#316931',fill=['x','y','z'],confinex=True,noshow=True,plottitle='North-South system')
#mp.plotStreams([ns],[['dy']],bgcolor='#d5de9c', gridcolor='#316931',confinex=True, fullday=True, opacity=0.7, plottitle='Field gradients (until %s)' % (datetime.utcnow().date()),noshow=True)
maxval = max(ns.ndarray[KEYLIST.index('dz')])
minval = min(ns.ndarray[KEYLIST.index('dz')])
diff = maxval-minval
try:
    plt.text(nsstat.ndarray[0][0]+0.01,minval+0.3*diff,line1)
    plt.text(nsstat.ndarray[0][0]+0.01,minval+0.1*diff,line2)
except:
    pass
#plt.show()
#upload
savepath = "/home/cobs/ANALYSIS/PeriodicGraphs/tmpgraphs/supergrad_%s.png" % date
plt.savefig(savepath)
#mp.plot(ns,['y','z','dy'], gridcolor = '#316931',fill=['x','y','z'],confinex=True,noshow=True,plottitle='North-South system')
#maxval = max(ns.ndarray[KEYLIST.index('dy')])
#minval = min(ns.ndarray[KEYLIST.index('dy')])
#diff = maxval-minval
#plt.text(nsstat.ndarray[0][0]+0.01,minval+0.3*diff,line1)
#plt.text(nsstat.ndarray[0][0]+0.01,minval+0.1*diff,line2)
#savepath = "/home/cobs/ANALYSIS/PeriodicGraphs/tmpgraphs/supergradNS_%s.png" % date
#plt.savefig(savepath)


cred = 'cobshomepage'
#address=mpcred.lc(cred,'address')
user=mpcred.lc(cred,'user')
passwd=mpcred.lc(cred,'passwd')
#port=mpcred.lc(cred,'port')
remotepath = 'zamg/images/graphs/magnetism/supergrad/'

#scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
#ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
try:
    scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
    analysisdict.check({'upload_homepage_supergradEWplot': ['success','=','success']})
except:
    analysisdict.check({'upload_homepage_supergradEWplot': ['failure','=','success']})
    pass
