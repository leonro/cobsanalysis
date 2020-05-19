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

path2log = '/home/cobs/ANALYSIS/Logs/tilt_graph.log'

endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=7),"%Y-%m-%d")
date = datetime.strftime(endtime,"%Y-%m-%d")
starttime2=datetime.strftime(endtime-timedelta(days=7),"%Y-%m-%d")

font = {'family' : 'sans-serif',
        'weight' : 'normal',
        'size'   : 10}


part1=False
if part1:
    # Read gravity data and filter it to seconds
    print ("Starting part 1:")
    p1start = datetime.utcnow()
    print ("No filtering required")
    p1end = datetime.utcnow()
    print "-----------------------------------"
    print "Part1 needs", p1end-p1start
    print "-----------------------------------"

part2 = False
if part2:
    print ("Starting part 2:")
    stb = readDB(db,'QUAKES',starttime=datetime.utcnow()-timedelta(days=7))
    print ("Length", stb.length())
    ## <500(3.0-4.5), 500-3000(4.5-6), 3000-6000(6-7), >6000 (>7)
    stb1 = stb.extract('f',7,'>=')
    stb2 = stb.extract('var5',1000,'>')
    stb2 = stb2.extract('var5',6000,'<=')
    stb2 = stb2.extract('f',7,'<')
    stb2 = stb2.extract('f',5,'>=')
    stb3 = stb.extract('var5',300,'>')
    stb3 = stb3.extract('var5',1000,'<=')
    stb3 = stb3.extract('f',5,'<')
    stb3 = stb3.extract('f',4.5,'>=')
    stb4 = stb.extract('var5',0,'>')
    stb4 = stb4.extract('var5',300,'<=')
    stb4 = stb4.extract('f',4.5,'<')
    stb4 = stb4.extract('f',3.0,'>=')
    try:  # fails if no data is available
        print ("Found streams", stb1.length(), stb2.length(), stb3.length())
        st = appendStreams([stb1,stb2,stb3,stb4])
        if len(st.ndarray[0]) > 0:
            fl = st.stream2flaglist(comment='f,str3',sensorid='GWRSGC025_25_0002', userange=False, keystoflag=['x'])
    except:
        print ("Failed to annotate quakes")
    #    pass

part3 = False
if part3:
    # Read seconds data and create plots
    print ("Starting part 3:")
    destinationpath= '/srv/products/data/gravity/SGO/GWRSGC025_25_0002/raw'
    grav = read(os.path.join(destinationpath,'F1*'),starttime=starttime, endtime=endtime)
    res = read(os.path.join(destinationpath,'C1*'),starttime=starttime, endtime=endtime)

    #g0 = readDB(db,'RCSG0_20160114_0001_0001',starttime=starttime2)
    # Light usv
    #g0.header['col-y'] = 'light(usv)'
    #mp.plot(g0)
    #try:
    #    flaglist = g0.bindetector('y',flagnum=0,keystoflag=['x'],sensorid='LM_TILT01_0001',text='tunnel light on')
    #    flaglist2db(db,flaglist)
    #    # Light power
    #    flaglist = g0.bindetector('x',flagnum=0,keystoflag=['x'],sensorid='LM_TILT01_0001',text='tunnel light on')
    #    flaglist2db(db,flaglist)
    #except:
    #    print ("bindetector: ValueError: total size of new array must be unchanged")

    #iwt = readDB(db,'IWT_TILT01_0001_0002',starttime=starttime2)
    #iwt = iwt.filter(filter_width=timedelta(seconds=1))
    #iwt.header['unit-col-x'] = 'i_phase'
    #iwt = iwt.multiply({'x':-1})

    #lm = readDB(db,'LM_TILT01_0001_0001',starttime=starttime2)
    flaglist = db2flaglist(db,'GWRSGC025_25_0002')
    grav = grav.flag(flaglist)
    try:
        grav = grav.flag(fl)
    except:
        print ("Flagging with earthquake data failed")
        pass

    # providing some Info on content
    #print data._get_key_headers()
    #matplotlib.rc('font', **font)

    print grav.length()
    grav.header['col-x'] = ''
    grav.header['col-z'] = ''
    res.header['col-z'] = ''
    grav.header['unit-col-x'] = ''
    grav.header['unit-col-z'] = ''
    res.header['unit-col-z'] = ''
    if grav.length()[0] > 1 and res.length()[0]> 1:
        mp.plotStreams([grav,res],[['x','z'],['z']], gridcolor='#316931',fill=['x'], annotate=[[True, False],[False]],confinex=True, fullday=True, opacity=0.7, plottitle='Gravity variation',noshow=True, labels=[['G [nm/s2]','Pressure [hPa]'],['Residual [nm/s2]']])
    elif grav.length()[0] > 1:
        mp.plotStreams([grav],[['x','z']], gridcolor='#316931',confinex=True, fullday=True,plottitle='Gravity variation' ,noshow=True, labels=[['G [nm/s2]','Pressure [hPa]']])
    else:
        print ("No data available")

    plt.rc('font', **font)

    #upload
    savepath = "/srv/products/graphs/gravity/gravity_%s.png" % date
    plt.savefig(savepath)

    cred = 'cobshomepage'
    address=mpcred.lc(cred,'address')
    user=mpcred.lc(cred,'user')
    passwd=mpcred.lc(cred,'passwd')
    port=mpcred.lc(cred,'port')
    remotepath = 'zamg/images/graphs/gravity/gwr/'

    ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)


#mr = read('/srv/archive/SGO/RCSM6_20160114_0001/raw/*',starttime='2016-05-01',endtime='2016-05-10')
#mp.plot(mr)
#mr = mr.filter()
#mr.plot(mr)

part4=True
if part4:
    # create statusplot
    # light gravity room
    aux = read('/srv/archive/SGO/GWRSGC025A_25_0002/raw/*',starttime='2016-05-01',endtime='2016-09-01')
    mr = read('/srv/archive/SGO/RCSM6_20160114_0001/raw/*',starttime='2016-05-01',endtime='2016-09-01')
    #mr = mr.filter()
    g0 = read('/srv/archive/SGO/RCSG0_20160114_0001/raw/*',starttime='2016-05-01',endtime='2016-09-01')
    #g0 = g0.filter()
    g0temp = read('/srv/archive/SGO/RCSG0temp_20161027_0001/raw/*',starttime='2016-05-01',endtime='2016-09-01')
    g0temp = g0temp.filter()
    #mr = readDB(db,'RCSM6_20160114_0001_0001',starttime=starttime) # light is x
    #g0 = readDB(db,'RCSG0_20160114_0001_0001',starttime=starttime) # main pow is var1
    #aux = readDB(db,'GWRSGC025A_25_0002',starttime=starttime)
    # x (level), z (tide), f (tx), var2 (ty), var4 (neck1), var5 (neck2), dz (He-flow)
    g0.header['col-var1'] = 'SGO pow'
    mr.header['col-x'] = 'light MR'
    plt.rc('font', **font)
    mp.plotStreams([mr,g0,aux,g0temp],[['x'],['var1'],['z','f','var2','var4','var5'],['var4']], gridcolor='#316931', fill=['z','x','var1'], fullday=True, opacity=0.7, plottitle='Gravity status',noshow=True)
    #savepath = "/srv/products/graphs/gravity/gravitystats_%s.png" % date
    #plt.savefig(savepath)
    plt.show()
    #cred = 'cobshomepage'
    #address=mpcred.lc(cred,'address')
    #user=mpcred.lc(cred,'user')
    #passwd=mpcred.lc(cred,'passwd')
    #port=mpcred.lc(cred,'port')
    #remotepath = 'zamg/images/graphs/gravity/gwr/'

    #ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
  
    #pass
