#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Skeleton for graphs
"""

from magpy.stream import *   
from magpy.database import *   
from magpy.transfer import *
import magpy.mpplot as mp
import magpy.opt.emd as emd
import magpy.opt.cred as mpcred

from martas import martaslog as ml
logpath = '/var/log/magpy/mm-per-tilt.log'
sn = 'SAGITTARIUS' # servername
statusmsg = {}


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

currentvaluepath = '/srv/products/data/current.data'

def getcurrentdata(path):
    """
    usage: getcurrentdata(currentvaluepath)
    example: update kvalue
    >>> fulldict = getcurrentdata(currentvaluepath)
    >>> valdict = fulldict.get('magnetism',{})
    >>> valdict['k'] = [kval,'']
    >>> valdict['k-time'] = [kvaltime,'']
    >>> fulldict[u'magnetism'] = valdict
    >>> writecurrentdata(path, fulldict)
    """
    if os.path.isfile(currentvaluepath):
        with open(currentvaluepath, 'r') as file:
            fulldict = json.load(file)
        return fulldict
    else:
        print ("path not found")

def writecurrentdata(path,dic):
    """
    usage: writecurrentdata(currentvaluepath,fulldict)
    example: update kvalue
    >>> see getcurrentdata
    >>>
    """
    with open(currentvaluepath, 'w',encoding="utf-8") as file:
        file.write(unicode(json.dumps(dic)))

# ####################
#  Method test section
# ####################

## TO BE REMOVED WITH 0.3.98

def stream2flaglist(stream, userange=True, flagnumber=None, keystoflag=None, sensorid=None, comment=None):
        """
        DESCRIPTION:
            Constructs a flaglist input dependent on the content of stream
        PARAMETER:
            comment    (key or string) if key (or comma separted list of keys) are 
                       found, then the content of this column is used (first input
            flagnumber (int) integer number between 0 and 4
            userange   (bool) if False, each stream line results in a flag, 
                              if True the full time range is marked
            
        """
        ### identify any given gaps and flag time ranges regarding gaps
        if not comment:
            print("stream2flag: you need to provide either a key or a text comment. (e.g. 'str1,str2' or 'Flagged'")
            return []
        if not flagnumber:
            flagnumber = 0
        if not keystoflag:
            print("stream2flag: you need to provide a list of keys to which you apply the flags (e.g. ['x','z']")
            return []
        if not sensorid:
            print("stream2flag: you need to provide a sensorid")
            return []

        commentarray = np.asarray([])
        uselist = False

        if comment in KEYLIST:
            pos = KEYLIST.index(comment)
            if userange:
                comment = stream.ndarray[pos][0]
            else:
                uselist = True
                commentarray = stream.ndarray[pos]
        else:
            lst,poslst = [],[]
            commentlist = comment.split(',')
            #try:
            ok = True
            if ok:
                for commkey in commentlist:
                    if commkey in KEYLIST:
                        #print(commkey)
                        pos = KEYLIST.index(commkey)
                        if userange:
                            lst.append(str(stream.ndarray[pos][0]))
                        else:
                            poslst.append(pos)
                    else:
                        # Throw exception
                        x= 1/0
                if userange:
                    comment = ' : '.join(lst)
                else:
                    uselist = True
                    resultarray = []
                    for pos in poslst:
                        resultarray.append(stream.ndarray[pos])
                    resultarray = np.transpose(np.asarray(resultarray))
                    print (resultarray)
                    commentarray = [''.join(str(lst)) for lst in resultarray]
            #except:
            #    #comment remains unchanged
            #    pass

        now = datetime.utcnow()
        res = []
        if userange:
            st = np.min(stream.ndarray[0])
            et = np.max(stream.ndarray[0])
            st = num2date(float(st)).replace(tzinfo=None)
            et = num2date(float(et)).replace(tzinfo=None)
            for key in keystoflag:
                res.append([st,et,key,flagnumber,comment,sensorid,now])
        else:
            for idx,st in enumerate(stream.ndarray[0]):
                for key in keystoflag:
                    st = num2date(float(st)).replace(tzinfo=None)
                    if uselist:
                        res.append([st,st,key,flagnumber,commentarray[idx],sensorid,now])
                    else:
                        res.append([st,st,key,flagnumber,comment,sensorid,now])
        return res



# ####################
#  Basic definitions
# #################### 
failure = False

path2log = '/home/cobs/ANALYSIS/Logs/tilt_graph.log'

endtime = datetime.utcnow()
starttime=datetime.strftime(endtime-timedelta(days=1),"%Y-%m-%d")
date = datetime.strftime(endtime,"%Y-%m-%d")
starttime2=datetime.strftime(endtime-timedelta(days=3),"%Y-%m-%d")


part1=False  # Read iwt data, drop wrong inputs and filter it to seconds - Done in filter.py
part2=True  # Get earthquake information and add flages to iwt
part3=True  # create periodic plot with RCS and LM data

if part1:
    # Read iwt data and filter it to seconds
    print ("Starting part 1:")
    name = '{}-PeriodicPlot-tilt1'.format(sn) 
    try:
        inst = 'IWT_TILT01_0001_0001'
        p1start = datetime.utcnow()

        # cleanup IWT Table - remove all too large dates
        cursor=db.cursor()
        delsql = "DELETE FROM {} WHERE time > NOW()".format(inst)
        cursor.execute(delsql)
        db.commit()
        cursor.close()
        
        #try:
        if part1:
            lasthour = dbgetlines(db,inst,70000)
            lasthour = lasthour.trim(endtime=datetime.utcnow())
            #lasthour.write('/home/cobs/', filenamebegins='iwttest_',format_type='PYCDF')
            #iwt = readDB(db,inst,starttime=starttime)
            print ("IWT HF Data obtained from database")
            iwtfilt = lasthour.filter(filter_width=timedelta(seconds=1),resample_period=1,missingdata='conservative')
            newtab = inst[:-5]+'_0002'
            print (newtab)
            writeDB(db,iwtfilt,tablename=newtab)
        #except:
        #    print ("Failed part1")

        p1end = datetime.utcnow()
        print "-----------------------------------"
        print "Part1 needs", p1end-p1start
        print "-----------------------------------"
        statusmsg[name] = 'Step 1 finished'
    except:
        failure = True
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print ("  tilt_graph step1 failed ")
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        statusmsg[name] = 'Step 1 failed (filtering iwt)'

if part2:
    print ("Starting part 2:")
    name = '{}-PeriodicPlot-tilt2'.format(sn) 
    try:
        stb = readDB(db,'QUAKES',starttime=datetime.utcnow()-timedelta(days=5))
        print ("Length", stb.length())
        ## <500(3.5-4.5), 500-3000(4.5-6), 3000-6000(6-7), >6000 (>7)
        if stb.length()[0] > 0:
            stb1 = stb.extract('f',7,'>=')
            stb2 = stb.extract('var5',3000,'>')
            stb2 = stb2.extract('var5',6000,'<=')
            stb2 = stb2.extract('f',7,'<')
            stb2 = stb2.extract('f',6,'>=')
            stb3 = stb.extract('var5',500,'>')
            stb3 = stb3.extract('var5',3000,'<=')
            stb3 = stb3.extract('f',6,'<')
            stb3 = stb3.extract('f',4.5,'>=')
            stb4 = stb.extract('var5',0,'>')
            stb4 = stb4.extract('var5',500,'<=')
            stb4 = stb4.extract('f',4.5,'<')
            stb4 = stb4.extract('f',3.0,'>=')
            try:  # fails if no data is available
                print ("Found quake streams", stb1.length(), stb2.length(), stb3.length())
                st = appendStreams([stb1,stb2,stb3,stb4])
                print ("Combined length: {}".format(st.length()[0]))
                if len(st.ndarray[0]) > 0:
                    fl = stream2flaglist(st, comment='f,str3',sensorid='LM_TILT01_0001', userange=False, keystoflag=['x'])
                    #fl = st.stream2flaglist(comment='f,str3',sensorid='LM_TILT01_0001', userange=False, keystoflag=['x'])
        
            except:
                pass
        else:
            print ("Failed to annotate quakes - no quakes found?")
        statusmsg[name] = 'Step 2 finished'
    except:
        failure = True
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print ("  tilt_graph step2 failed ")
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        statusmsg[name] = 'Step 2 failed (annotating earthquakes)'

if part3:
    # Read seconds data and create plots
    print ("Starting part 3:")
    name = '{}-PeriodicPlot-tilt3'.format(sn) 
    try:
        g0 = readDB(db,'RCSG0_20160114_0001_0001',starttime=starttime2)
        # Light usv
        g0.header['col-y'] = 'light(usv)'
        #mp.plot(g0)
        #try:
        #    flaglist = g0.bindetector('y',flagnum=0,keystoflag=['x'],sensorid='LM_TILT01_0001',text='tunnel light on')
        #    flaglist2db(db,flaglist)
        #    # Light power
        #    flaglist = g0.bindetector('x',flagnum=0,keystoflag=['x'],sensorid='LM_TILT01_0001',text='tunnel light on')
        #    flaglist2db(db,flaglist)
        #except:
        #    print ("bindetector: ValueError: total size of new array must be unchanged")

        iwt = readDB(db,'IWT_TILT01_0001_0002',starttime=starttime2)
        #iwt = iwt.filter(filter_width=timedelta(seconds=1))
        iwt.header['unit-col-x'] = 'i_phase'
        iwt = iwt.multiply({'x':-1})
        print ("loaded iwt data: {}".format(iwt.length()[0]))

        lm = readDB(db,'LM_TILT01_0001_0001',starttime=starttime2)
        flaglist = db2flaglist(db,lm.header['SensorID'])
        lm = lm.flag(flaglist)
        try:
            lm = lm.flag(fl)
        except:
            pass

        # providing some Info on content
        #print data._get_key_headers()
        #mp.plot(data,['x','t1'],bgcolor = '#d5de9c', gridcolor = '#316931',fill=['t1'],confinex=True)
        if lm.length()[0] > 1 and g0.length()[0]> 1 and iwt.length()[0]> 1:
            print ("Plot 1: rcs, lm and iwt")
            mp.plotStreams([g0,lm,iwt],[['y'],['t1','var2','x'],['x']], gridcolor='#316931',fill=['t1','var2','y'], padding=[[0.0],[0.2,0.5,0.0],[0.0]], annotate=[[False],[False,False,True],[True]], specialdict=[{'y':[0,1]},{},{}], fullday=True, opacity=0.7, plottitle='Tilts (until %s)' % (datetime.utcnow().date()),noshow=True)
        elif lm.length()[0] > 1 and iwt.length()[0]> 1:
            print ("Plot 2: lm and iwt")
            mp.plotStreams([lm,iwt],[['t1','var2','x'],['x']], gridcolor='#316931',fill=['t1','var2'], padding=[[0.2,0.5,0.0],[0.0]], annotate=[[False,False,True],[False]], confinex=True, fullday=True, opacity=0.7, plottitle='Tilts (until %s)' % (datetime.utcnow().date()),noshow=True)
        elif iwt.length()[0] > 1:
            print ("Plot 3: iwt only")
            mp.plotStreams([iwt],[['x']], gridcolor='#316931',confinex=True, fullday=True,plottitle='Tilts (until %s)' % (datetime.utcnow().date()),noshow=True)
        elif lm.length()[0] > 1 and g0.length()[0]> 11:
            print ("Plot 4: lm and rcs")
            mp.plotStreams([g0,lm],[['y'],['t1','var2','x']], gridcolor='#316931',fill=['t1','var2','y'], padding=[[0.0],[0.2,0.5,0.0]], annotate=[[False],[False,False,True]], specialdict=[{'y':[0,1]},{}],confinex=True, fullday=True, opacity=0.7, plottitle='Tilts (until %s)' % (datetime.utcnow().date()),noshow=True)
        elif lm.length()[0] > 1:
            print ("Plot 5: lm only")
            mp.plotStreams([lm],['t1','var2','x'], gridcolor='#316931',confinex=True, annotate=True, padding=[0.2,0.5,0.0], fullday=True,plottitle='Tilts (until %s)' % (datetime.utcnow().date()),noshow=True)
        else:
            print ("No data available")

        print ("Plot created .. saving now")
        #upload
        savepath = "/srv/products/graphs/tilt/tilt_%s.png" % date
        plt.savefig(savepath)
        # had an error once with "exceeds Locater.MAXTICKS" -> removed confinex=True to solve

        cred = 'cobshomepage'
        address=mpcred.lc(cred,'address')
        user=mpcred.lc(cred,'user')
        passwd=mpcred.lc(cred,'passwd')
        port=mpcred.lc(cred,'port')
        remotepath = 'zamg/images/graphs/gravity/tilt/'

    except:
        failure = True
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print ("   tilt_graph step3 failed ")
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        statusmsg[name] = 'Step 3 failed (creating plot)'

    print ("Plot created .. uploading now")

    fulldict = getcurrentdata(currentvaluepath)
    valdict = fulldict.get('logging',{})

    try:
        # to send with 664 permission use a temporary directory
        tmppath = "/tmp"
        tmpfile= os.path.join(tmppath,os.path.basename(savepath))
        from shutil import copyfile
        copyfile(savepath,tmpfile)
        scptransfer(tmpfile,'94.136.40.103:'+remotepath,passwd)
        os.remove(tmpfile)
        #ftpdatatransfer(localfile=savepath,ftppath=remotepath,myproxy=address,port=port,login=user,passwd=passwd,logfile=path2log)
        #scptransfer(savepath,'94.136.40.103:'+remotepath,passwd)
        statusmsg[name] = 'Step 3 finished'
        valdict['failedupload2homepage'] = 0
    except:
        failure = True
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print ("   tilt_graph step3 failed ")
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        faileduploadcount = int(valdict.get('failedupload2homepage',0))
        print ("Current failed-upload count = {}".format(faileduploadcount))
        faileduploadcount += 1
        if faileduploadcount >= 4:
           # Only report if upload failed at least 4 times in row -> approximat$
           statusmsg[name] = 'Step 3 failed (sending data to page)'
        else:
           statusmsg[name] = 'Step 3 finished'
        print ("Writing new count to currentdata")
        valdict['failedupload2homepage'] = faileduploadcount

    fulldict[u'logging'] = valdict
    writecurrentdata(currentvaluepath, fulldict)

if not failure:
    analysisdict.check({'script_periodic_tilt_graph': ['success','=','success']})
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
    print ("        tilt_graph successfully finished         ")
    print ("++++++++++++++++++++++++++++++++++++++++++++++++")
else:
    analysisdict.check({'script_periodic_tilt_graph': ['failure','=','success']})


martaslog = ml(logfile=logpath,receiver='telegram')
martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
martaslog.msg(statusmsg)

