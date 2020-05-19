#!/usr/bin/env python
"""
MagPy - Basic Runtime tests including durations  
"""

# Define packges to be used (local refers to test environment) 
# ------------------------------------------------------------
local = False
if local:
    import sys
    sys.path.append('/home/leon/Software/magpy/trunk/src')

    from stream import *   
    from database import *   
    import transfer as tr
    import absolutes as di
    import mpplot as mp
    import opt.emd as emd
    import opt.cred as cred
else:
    from magpy.stream import *   
    from magpy.database import *   
    import magpy.absolutes as di
    import magpy.transfer as tr
    import magpy.mpplot as mp
    import magpy.opt.emd as emd
    import magpy.opt.cred as cred

# Connect to test database 
# ------------------------------------------------------------
dbpasswd=cred.lc('cobsdb','passwd')

try:
    # Test MARCOS 1
    print "Connecting to primary MARCOS..."
    db = mysql.connect(host="localhost",user="cobs",passwd=dbpasswd,db="cobsdb")
    print db
except:
    print "... failed"
    sys.exit()


print " ----------------------------------------- "
print " --------------  Year 2015  -------------- "
print " ----------------------------------------- "

year = '2015'
ext = 'cdf'

datapath = '/srv/archive/WIC'
respath = '/srv/products/data/magnetism/definitive'

"""
data =readDB(db,'LEMI036_1_0002_0001')
data = data.filter(filter_width=timedelta(seconds=1))
archivepath = '/srv/archive/WIC/LEMI036_1_0002/LEMI036_1_0002_0002'
data.write(archivepath,filenamebegins='LEMI036_1_0002_0002_',format_type='PYCDF')
"""
"""
Step0: optional redo base value calculation
required information: delta F between provided scalar and DI Pier
usually you would do that with flagged data

current options: 
- done with minute data
- in 2014: using only POS1 as F
"""
step00 = False
if step00:
    # create overview diagrams of 1 minute data for respective year
    # flags should already be applied
    vario = read(os.path.join(datapath,'FGE_S0252_0001/FGE_S0252_0001_0002/*.'+ext),starttime=year+'-01-01',endtime=year+'-12-31 23:59:59.999')
    scalar = read(os.path.join(datapath,'POS1_N432_0001/POS1_N432_0001_0002/*.'+ext),starttime=year+'-01-01',endtime=year+'-12-31 23:59:59.999')
    #vaflaglist = db2flaglist(db,'FGE_S0252_0001')
    #scflaglist = db2flaglist(db,'POS1_N432_0001')
    print "Flagging data"
    #vario = vario.flag(vaflaglist)
    #scalar = scalar.flag(scflaglist)
    print "Removing flags"
    vario = vario.remove_flagged()
    scalar = scalar.remove_flagged()
    print vario.length() # expected length ?
    print scalar.length() # expected length ?
    print 365*1440, vario.length()[0]*100/(365*1440), scalar.length()[0]*100/(365*1440) 
    #mp.plot(scalar)
    mp.plotStreams([vario,scalar],variables=[['x','y','z','t1'],['f']])
    


step0 = False   # Usually not necessary - recalculate base values
if step0:
    absresult = di.absoluteAnalysis(os.path.join(datapath,'DI/raw'),os.path.join(datapath,'LEMI025_22_0002/LEMI025_22_0002_0002/*'),os.path.join(datapath,'POS1_N432_0001/POS1_N432_0001_0002/*'),diid='A2_WIC.txt',pier='A2', expD=4.0, expI=64.0,starttime='2015-08-01',endtime='2015-12-15',db=db, dbadd=True)
    absresult.write(os.path.join(datapath,'DI/data'),coverage='all',filenamebegins="BLV_LEMI025_22_0002_POS1_N432_0001_A2",format_type="PYSTR",mode='replace')
    writeDB(db,absresult,'BLV_LEMI025_22_0002_POS1_N432_0001_A2')

    absresult = di.absoluteAnalysis(os.path.join(datapath,'DI/raw'),os.path.join(datapath,'FGE_S0252_0001/FGE_S0252_0001_0001/*'),os.path.join(datapath,'GSM90_14245_0002/GSM90_14245_0002_0002/*'),diid='A2_WIC.txt',pier='A2', expD=4.0, expI=64.0,starttime='2015-08-01',endtime='2015-12-15',db=db, dbadd=True)
    absresult.write(os.path.join(datapath,'DI/data'),coverage='all',filenamebegins="BLV_FGE_S0252_0001_GSM90_14245_0002_A2",format_type="PYSTR",mode='replace')
    writeDB(db,absresult,'BLV_FGE_S0252_0001_GSM90_14245_0002_A2')

    absresult = di.absoluteAnalysis(os.path.join(datapath,'DI/raw'),os.path.join(datapath,'FGE_S0252_0001/FGE_S0252_0001_0001/*'),os.path.join(datapath,'GSM90_14245_0002/GSM90_14245_0002_0002/*'),diid='A2_WIC.txt',pier='A2', expD=4.0, expI=64.0,starttime='2015-04-01',endtime='2015-08-01',db=db, dbadd=True)
    absresult.write(os.path.join(datapath,'DI/data'),coverage='all',filenamebegins="BLV_FGE_S0252_0001_GSM90_14245_0002_A2",format_type="PYSTR",mode='replace')
    writeDB(db,absresult,'BLV_FGE_S0252_0001_GSM90_14245_0002_A2')

    #absresult = di.absoluteAnalysis('/media/SamsungArchive/archive/WIC/DI/analyze','/media/SamsungArchive/archive/WIC/FGE_S0252_0001/FGE_S0252_0001_0002/*','/media/SamsungArchive/archive/WIC/POS1_N432_0001/POS1_N432_0001_0002/*',diid='A7_WIC.txt',pier='A7', expD=4.0, expI=64.0,starttime='2013-09-01',endtime='2015-01-01',db=db, dbadd=True,movetoarchive='/media/SamsungArchive/archive/WIC/DI/raw')
    #absresult.write('/media/SamsungArchive/archive/WIC/DI/data',coverage='all',filenamebegins="BLV_FGE_S0252_0001_POS1_N432_0001_A7",format_type="PYSTR",mode='replace')

    #absresult = di.absoluteAnalysis('/media/SamsungArchive/archive/WIC/DI/analyze','/media/SamsungArchive/archive/WIC/FGE_S0252_0001/FGE_S0252_0001_0002/*','/media/SamsungArchive/archive/WIC/POS1_N432_0001/POS1_N432_0001_0002/*', azimuth=290.0748,diid='H1_WIC.txt',pier='H1', expD=4.0, expI=64.0,starttime='2013-09-01',endtime='2015-11-11',db=db, dbadd=True,movetoarchive='/media/SamsungArchive/archive/WIC/DI/raw')
    #absresult.write('/media/SamsungArchive/archive/WIC/DI/data',coverage='all',filenamebegins="BLV_FGE_S0252_0001_POS1_N432_0001_A7",format_type="PYSTR",mode='replace')

    #absresult = di.absoluteAnalysis('/media/SamsungArchive/archive/WIC/DI/analyze','/media/SamsungArchive/archive/WIC/FGE_S0252_0001/FGE_S0252_0001_0002/*','/media/SamsungArchive/archive/WIC/POS1_N432_0001/POS1_N432_0001_0002/*',diid='A16_WIC.txt',pier='A16',deltaF=2.145,starttime='2015-10-01',endtime='2015-11-11',db=db, dbadd=True, abstype='autodif', expD=4.0, expI=64.0, azimuth=267.36651, movetoarchive='/media/SamsungArchive/archive/WIC/DI/raw')
    #absresult.write('/media/SamsungArchive/archive/WIC/DI/data',coverage='all',filenamebegins="BLV_FGE_S0252_0001_POS1_N432_0001_A16",format_type="PYSTR",mode='replace')

"""
Step05: compare DI results from different BLV files
required information: 
"""
step05 = True
if step05:
    print " ----------------------------------------- "
    print " ---------  Compare DI data   --------- "
    print " ----------------------------------------- "

    ### TODO Subtract baselinefunctions and not the
    absr = readDB(db,'BLV_FGE_S0252_0001_POS1_N432_0001_A16')
    absres = readDB(db,'BLV_LEMI025_22_0002_GSM90_14245_0002_A2')
    #mp.plot(absr,['x','y','z','dx','dy','dz'],noshow=True)
    #mp.plot(absres,['x','y','z','dx','dy','dz'],noshow=True)
    absr = absr.remove_flagged()
    absr,newflaglist = mp.plotFlag(absr,variables=['x','y','z','dx','dy','dz'])
    flaglist2db(db, newflaglist)
    absres = absres.remove_flagged()
    absres,newflaglist = mp.plotFlag(absres,variables=['x','y','z','dx','dy','dz'])
    flaglist2db(db, newflaglist)
    diff = subtractStreams(absr,absres,keys=['x','y','z'])
    mp.plot(diff,['x','y','z'])


"""
Step1: get baseline corrected yearly files
required information: 
"""
step1 = False
if step1:
    print " ----------------------------------------------------------------------- "
    print " ---------  Checking and Filtering Vario, Baseline correction  --------- "
    print " ----------------------------------------------------------------------- "
    # ---------------------
    instlist = ['FGE_S0252_0001_0001','LEMI025_22_0002_0002']
    instlist = ['LEMI025_22_0002_0002']
    #instlist = ['FGE_S0252_0001_0001']


    # TODO: Check where I loose temperature !!

    # POS1: 15.06 (zwei peaks um 10:xx), 17.06 (5:55-8:38),

    # ?? 9.09...

    #### READ MONTHLY DATA
    for i in range(7,11):
        month = str(i+1).zfill(2)
        nextmonth = str(i+2).zfill(2)
        for inst in instlist:
            #print '/media/DAE2-4808/archive/WIC/'+inst[:-5]+'/'+inst+'/*'
            print year+'-'+month+'-01'
            try:
                va = read(os.path.join(datapath,inst[:-5],inst,'*'),starttime=year+'-'+month+'-01',endtime=year+'-'+nextmonth+'-01')
            except:
                va = DataStream()
            # get header from db
            if va.length()[0] > 1:
                va.header = dbfields2dict(db,inst)
                if va.header.get('SensorID','') == '':
                    va.header['SensorID'] = inst[:-5]
                # 2) flag it
                # ---------------------
                #  --a. add flaglist (from db)
                print "Flagging"
                flaglist = db2flaglist(db,va.header['SensorID'])
                va = va.flag(flaglist)
                #  --b. plotFlag - eventually extend and save extended flaglist
                #va,newflaglist = mp.plotFlag(va)  ## flags scheinen noch nicht uebernommen zu werden, test storing
                #flaglist2db(db, newflaglist)
                #writeDB(db,data)
                # 3) calc baseline
                # ---------------------
                # requirement: base value data has been cleaned up
                print "Getting baseline"
                absresult = readDB(db,'BLV_'+inst[:-5]+'_POS1_N432_0001_A2')
                # apply flags
                flaglist = db2flaglist(db,'BLV_'+inst[:-5]+'_POS1_N432_0001_A2')
                absresult = absresult.flag(flaglist)
                # eventually perform rotation to HDZ system

                # get baseline parameters from database in dependency of start and endtime of stream
                baselist = getBaseline(db,va.header['SensorID'])

                # Get baselines
                print 'L2',va.length()
                sts = va.baselineAdvanced(absresult,baselist)#,plotbaseline=True)
                for el in sts:
                    st = el[0]
                    #mp.plot(st)
                    timestr1 = st.header.get('DataAbsMinTime','')
                    timestr2 = st.header.get('DataAbsMaxTime','')
                    timeadd = datetime.strftime(num2date(timestr1),'%Y%m%d')+'-'+datetime.strftime(num2date(timestr2),'%Y%m%d')
                    savename = inst+'_vario_Base'+timeadd+'_'
                    # 4) save as PYCDF
                    # ---------------------
                    #   They will contain all flagging information and baseline parameters
                    print "Writing"
                    print 'L3',st.length()
                    st.write(respath,filenamebegins=savename,dateformat='%Y%m',coverage='month',format_type='PYCDF')

                    st = st.remove_flagged()
                    print 'L4',st.length()
                    st = st.bc()
                    print 'L5',st.length()
                    st = st.filter()
                    print 'L6',st.length()
                    st.header['DataPublicationLevel'] = 4
                    st.write(respath,filenamebegins=inst+'_vario_min_'+str(year),dateformat='%Y',mode='replace',coverage='all',format_type='PYCDF')



"""
Step2: check f files, flag them, and correct to main pier
required information: 
"""
step2 = False
if step2:
    print " ----------------------------------------- "
    print " --- Checking and Filtering F --- "
    print " ----------------------------------------- "
    # read all DIDD data (assuming its an independent F)

    scinstlist = ['POS1_N432_0001_0001','GSM90_14245_0002_0002']#,'GP20S3NS_012201_0001_0001']
    #scinstlist = ['GSM90_14245_0002_0002']
    #scinstlist = ['POS1_N432_0001_0001']
    f, dt = [],[]
    for i in range(11,12):
        month = str(i+1).zfill(2)
        nextmonth = str(i+2).zfill(2)
        nextyear = year
        if nextmonth == '13':
            nextmonth = '01'
            nextyear = str(int(year)+1)
        for scinst in scinstlist:
            print os.path.join(datapath,scinst[:-5],scinst,'*')
            print year+'-'+month+'-01'
            print nextyear+'-'+nextmonth+'-01'
            sc = read(os.path.join(datapath,scinst[:-5],scinst,'*'),starttime=year+'-'+month+'-01',endtime=nextyear+'-'+nextmonth+'-01')
            # get header from db
            if sc.length()[0] > 1:
                sc.header = dbfields2dict(db,scinst)
                # flag it
                print "Flagging"
                flaglist = db2flaglist(db,scinst[:-5]) #data.header['SensorID'])
                sc = sc.flag(flaglist)
                sc = applyDeltas(db,sc)
                sc = sc.remove_flagged()
                #print "Getting gaps" # if done before flagging then that for some reason flagging does not work any more
                #sc = sc.get_gaps()
                print "Filtering"
                sc = sc.filter()
                print "Writing data"
                sc.write(respath,filenamebegins=scinst+'_scalar_'+str(year),dateformat='%Y',coverage='all',mode='replace',format_type='PYCDF')
                

# to DataDeltaValues GS3NS: x_4.223,y_-1.547,z_2.137
# Sensor 3 changes in the night from 22.11 to 23.11 why??

"""
print "Reading 1"
data1 = read('/srv/products/data/magnetism/definitive/GSM90_14245_0002_0002_scalar_2015.cdf',starttime='2015-01-29',endtime='2015-10-22')
data2 = read('/srv/products/data/magnetism/definitive/GSM90_14245_0002_0002_scalar_2015.cdf',starttime='2015-10-27')
dataa=joinStreams(data1,data2)
dataa=dataa.get_gaps()
#mp.plot(dataa)
print "Reading 2"
data2 = read('/srv/products/data/magnetism/definitive/POS1_N432_0001_0001_scalar_2015.cdf',starttime='2015-08-07')
print "Reading 3"
data3 = read('/srv/products/data/magnetism/definitive/POS1_N432_0001_0001_scalar_2015.cdf',endtime='2015-01-29')
print "Reading 4"
data4 = read('/srv/products/data/magnetism/definitive/POS1_N432_0001_0001_scalar_2015.cdf',starttime='2015-01-29',endtime='2015-07-29')


print "Stacking"
stacked = stackStreams([dataa,data2],get='mean',uncert=True)
print stacked.length()
stacked = joinStreams(stacked,data3) #,keys=['f'],mode='insert')
print stacked.length()
#mp.plot(stacked)
stacked = mergeStreams(stacked,data4, mode='insert')
print stacked.length()
#mp.plot(stacked)
#print stacked.ndarray
stacked.write('/srv/products/data/magnetism/stacked/',filenamebegins='Stacked_scalar_',dateformat='%Y-%m',coverage='month',mode='replace',format_type='PYCDF')


data = read('/srv/products/data/magnetism/definitive/POS1_N432_0001_0001_scalar_2014.cdf')
data.write('/srv/products/data/magnetism/stacked/',filenamebegins='Stacked_scalar_',dateformat='%Y-%m',coverage='month',mode='replace',format_type='PYCDF')
"""

"""
Step3: reload f's, remove flagged, get delta's and construct combined record
required information: 
"""
step3 = False
if step3:
    print " -------------------------------------------------- "
    print " --- Checking  delta(v)F and merging/stacking F --- "
    print " -------------------------------------------------- "
    # ---------------------
    # checks whether lengths and contents are correct 
    streamlist = []
    instlist = ['POS1_N432_0001_0001','GSM90_14245_0002_0002','GP20S3NS_012201_0001_0001']
    instlist = ['POS1_N432_0001_0001','GSM90_14245_0002_0002']
    for idx, inst in enumerate(instlist):
        exec("data"+str(idx)+" = read('/srv/products/data/magnetism/definitive/'+inst+'*')")
        exec("data"+str(idx)+" = data"+str(idx)+".get_gaps()")
        streamlist.append(eval('data'+str(idx)))
    #for elem in streamlist:
    #    mp.plot(elem,annotate=True)
    #print streamlist[0]._find_t_limits()
    #print streamlist[1]._find_t_limits()

    # ###############################
    # Construct df plots for all F's 
    # ###############################

    #streamlist[2] = streamlist[2]._move_column('x','f')
    #streamlist[2] = streamlist[2].multiply({'f':0.001})
    import itertools
    #sub = []
    paralst = [['f']] * len(streamlist)
    v2ind = KEYLIST.index('var2')
    idxlst = [i for i in range(len(streamlist))]
    meanlst = []
    print idxlst
    for subset in itertools.combinations(idxlst,2):
         print subset
         print streamlist[subset[0]].length(),streamlist[subset[1]].length()
         sub = subtractStreams(streamlist[subset[0]],streamlist[subset[1]])
         ms = sub.mean('f',meanfunction='std')
         subset = [el for el in subset]
         print subset, ms
         subset.append(ms)
         meanlst.append(subset)
         mp.plotStreams([streamlist[subset[0]],streamlist[subset[1]],sub],[['f'],['f'],['f']])
         """
         col = sub._get_column('f')
         for elem in subset:
             print elem
             newind = v2ind+int(elem)
             sub = sub._put_column(col,KEYLIST[newind])
             print streamlist[int(elem)].length(), sub.length()
             test = mergeStreams(streamlist[int(elem)],sub,keys=[KEYLIST[newind]])
             #streamlist[int(elem)] = mergeStreams(streamlist[int(elem)],sub,keys=[KEYLIST[newind]])
             paralst[int(elem)] = paralst[int(elem)].append(KEYLIST[newind])
             print test.length()
         """
    print meanlst
    std = [el[2] for el in meanlst]
    mi = np.min(std)
    subs = meanlst[std.index(mi)]
    print subs
    print "use", subs[0],subs[1]
    streamlist = [el for idx,el in enumerate(streamlist) if idx in subs]
    print len(streamlist)

    #newstreamlist = []
    #for el in streamlist:
    #    newstreamlist.append(el.filter())
    # ###############################
    # Eventually perform flagging again, remove flags and stack all data/ merge primary
    # ###############################
    stacked = stackStreams(streamlist,get='mean',uncert=True)
    mp.plot(stacked)
    #print stacked.ndarray
    #print stacked.length()
    #stacked.write('/media/DAE2-4808/archive/WIC/YearlyAnalysis/',filenamebegins='Final_scalar_'+str(year),dateformat='%Y',coverage='all',mode='replace',format_type='PYCDF')
    #mp.plot(stacked)
    #use:
    # python FlagData.py -c testdb -p "/media/DAE2-4808/archive/WIC/POS1_N432_0001/POS1_N432_0001_0001/*" -b "2015-01-01" -e "2015-02-01" -r

    # ###############################
    # Write stacked record / merged primary record
    # ###############################

x=1/0

"""
Step4: reload combined f record and merge with all variodata
required information: 
"""
"""
sc1 = read('/media/DAE2-4808/archive/WIC/YearlyAnalysis/Final_scalar*')
mp.plot(sc1)
#sc2 = read('/media/DAE2-4808/archive/WIC/YearlyAnalysis/POS1_N432_0001_0001_scalar*')
sc2 = read('/media/DAE2-4808/archive/WIC/YearlyAnalysis/GSM90_14245_0002_0002_scalar*')
mp.plot(sc2)
sub = subtractStreams(sc1,sc2)
mp.plot(sub,variables=['f'])
"""
step4 = False
if step4:
    print " -------------------------------------------------- "
    print " --- Merging variodata with stacked F           --- "
    print " -------------------------------------------------- "
    #scalar = 'POS1_N432_0001_0001_scalar*'
    #scalar = 'GSM90_14245_0002_0002_scalar*'
    scalar = 'Final_scalar*'
    sc = read('/media/DAE2-4808/archive/WIC/YearlyAnalysis/'+scalar)
    sc = sc.remove_flagged()
    # ---------------------
    # checks whether lengths and contents are correct 
    instlist = ['FGE_S0252_0001_0001','LEMI025_22_0002_0002']
    for inst in instlist:
        vario = read('/media/DAE2-4808/archive/WIC/YearlyAnalysis/'+inst+'_vario_min*')
        #mp.plot(vario,annotate=True)
        print "Should be 0 or 1440:", len(vario.ndarray[0]) - 1440*365 # should be 1440*31
        if inst == 'LEMI025_22_0002_0002':
            vario = vario.bc()
        mp.plot(vario)
        #vario = vario.remove_flagged()
        #vario = vario.bc()    # flags???
        #mp.plot(vario,annotate=True)
        merge = mergeStreams(vario,sc,keys=['f'])
        # Drop columns t1,t2, var1 if existing
        for key in ['t1','t2','var1','var2','var3','var4','var5']:
            ind = KEYLIST.index(key)
            if len(merge.ndarray[ind]) > 0:
                merge = merge._drop_column(key)
        mp.plotFlag(merge)

        # ###################################################################################
        # Plot this data, analyse deltaF and eventually go back to high res data for flagging
        # Perform steps 1 to 4 until all outliers are marked and removed
        # ###################################################################################


"""
Step5: analyze variometer records and their diffs, construct a final single main variometer file
required information: 
"""
step5 = True
if step5:
    print " -------------------------------------------------- "
    print " --- Checking  delta of variometers             --- "
    print " -------------------------------------------------- "
    # ---------------------
    # checks whether lengths and contents are correct 
    streamlist = []
    instlist = ['FGE_S0252_0001_0001_vario_min_','LEMI025_22_0002_0002_vario_min_']

    for idx, inst in enumerate(instlist):
        exec("data"+str(idx)+" = read('/media/DAE2-4808/archive/WIC/YearlyAnalysis/'+inst+'*')")
        streamlist.append(eval('data'+str(idx)))
    #for elem in streamlist:
    #    mp.plot(elem,annotate=True)
    #print streamlist[0]._find_t_limits()
    #print streamlist[1]._find_t_limits()

    # ###############################
    # Construct dv plots for all F's 
    # ###############################
    sub = subtractStreams(streamlist[0],streamlist[1])
    #sub = sub.get_gaps()
    mp.plotStreams([streamlist[0],streamlist[1],sub],[['x',],['x'],['x','y','z']])



"""
Step6: calculate k values required information: 
"""
step6 = False
if step6:
    print " ------------------------------------------------ "
    print " --- Calculate k values  --- "
    print " ------------------------------------------------ "
    vario = read('/home/leon/CronScripts/MagPyAnalysis/YearlyAnalysis/DIDD_3121331_0002_vario_'+year+'*')
    vario = vario.remove_flagged()
    vario = vario.bc() 
    sc = read('/home/leon/CronScripts/MagPyAnalysis/YearlyAnalysis/DIDD_3121331_0002_scalar_'+year+'*')
    sc = sc.remove_flagged()
    #sc = sc.offset({'f': 4.1})
    merge = mergeStreams(vario,sc,keys=['f'])
    # Final flagging procedure
    merge,newflaglist = mp.plotFlag(merge)  ## flags scheinen noch nicht uebernommen zu werden
    #flaglist2db(db, newflaglist)
    # merge.write
    merge.write('/home/leon/CronScripts/MagPyAnalysis/YearlyAnalysis',filenamebegins='Final_mag_'+str(year),dateformat='%Y',coverage='all',format_type='PYCDF')
    merge = merge.remove_flagged()
    kvals = merge.k_fmi()
    kvals.write('/home/leon/CronScripts/MagPyAnalysis/YearlyAnalysis',filenamebegins='Final_kvals_'+str(year),dateformat='%Y',coverage='all',format_type='PYCDF')
    kvals.write('/home/leon/CronScripts/MagPyAnalysis/YearlyAnalysis',filenamebegins='Final_kvals_'+str(year),dateformat='%Y',coverage='all',format_type='PYASCII')
    

step7 = False
if step7:

    print " ----------------------------------------- "
    print " ------- Create Monthly ImagCDF's  ------- "
    print " ----------------------------------------- "
    # write monthly ImagCDF's/WDC/IAGA/IAF
    # checks whether lengths and contents are correct 

    print " ----------------------------------------- "
    print " ---------- Creating BLV files  ---------- "
    print " ----------------------------------------- "
    absresult = read('/home/leon/CronScripts/MagPyAnalysis/TestFiles/Output/BLV_WIK2.txt')
    absresult.write('/home/leon/CronScripts/MagPyAnalysis/TestFiles/Output',filenamebegins='BLVIMF', coverage='all',format_type='BLV', year='2014') # add daily means of deltaf

    print " ----------------------------------------- "
    print " ----- Full CD and consistency check ----- "
    print " ----------------------------------------- "
    # get everything necessary for submission

    print " ----------------------------------------- "
    print " -- Extended information e.g. activity  -- "
    print " ----------------------------------------- "
    # calc quiet days, sum of quiet days activity etc

    print " ----------------------------------------- "
    print " ------------ Year book pages ------------ "
    print " ----------------------------------------- "
    # create tables

    print " ----------------------------------------- "
    print " --------------  GIN upload -------------- "
    print " ----------------------------------------- "

