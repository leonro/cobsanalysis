from radonanalysismethods import *

graphdir = "/home/leon/CronScripts/MagPyAnalysis/RadonAnalysis/Report/graphs"

## ToDo:
## 1. Clean up outside temperature mesurements, fill small gaps remove long outliers
## 2. Figure of decay
## 3. Figure of tunnel, cross section and sensor position
## 4. Check function for relation of maxima and dT
## 5. Overview and details plot (with diurnal variation)
## 6. Frequency analysis of model???? -> Susana
## 7. Residuum and observatory usage, meteo
## 8. Discussion of the residuals, oversimplification of the model, other effects
## 9. Drop empirical model description
## 10. prepare bib file -> mendeley

#RUN the following processes:
timeseries = False      # timeseries and its graphs including relationships with dT
parameter = False      # some simple plots for showing physical parameter graphs
rangetest = False
empiricalmodel = False
physicalmodel = False
report = True          # update the latex file

# Defaultparameter
cmax = 60000.
tt = 2.       #

# Literature
D0 = 0.000011
thalfRn = 3.8235

# 2015
bg = 13886
E = 67824.716258033732
delay = 2645
a = 18831823211305596.0
m = 1180.     #->modified by timeseries
c = 13886
tm = 950.
tbg = -250000

# 2012-2017
bg = 13890
E = 53056.558728381482
delay = 2723
a = 40482539837383.703
m = 1170.     #->modified by timeseries
c = 13886


#Timeseries analysis finished. Found the following parameters:
#('Background level [counts]:     ', 13890)
#('Background - exp arrhenius:    ', -10126.995635457928)
#('Activation energy [J/mol]:     ', 16843.318616185352)
#('Average time delay [minutes]:  ', 2723)
#('Cnt0 (a, of exp fit) [counts]: ', 39419027.332327917)
#('Linear model m [1/$^\\circ$C]:  ', 1170)
#('Minimum count', 12971.0)

##summer 2015
#('Background level [counts]:     ', 14400)
#('Background - exp arrhenius:    ', 14184.943776624214)
#('Activation energy [J/mol]:     ', 62330.365396558875)
#('Average time delay [minutes]:  ', 2489)
#('Cnt0 (a, of exp fit) [counts]: ', 1986576246187737.5)
#('Linear model m [1/$^\\circ$C]:  ', 1114)



"""
# -> unkown parameters: cmax, D0, E
# -> other boundary cond: a) diffusion rate is always assumed to be maximal because of production
#                         b) relationship between dT and c0
#                         c) M is a function of c0

"""

if timeseries:
    #### ##########################################################################
    ##    Read gamma data, read and clean up meteo data
    #### ##########################################################################
    starttime = '2012-11-22'
    #starttime = '2013-07-27'
    endtime = '2017-01-01'
    #endtime = '2013-07-28'

    print ("Reading gamma data...")
    scapath = '/home/leon/Dropbox/Daten/RadonTables/sca-tunnel-1min*'
    #scapath = ('/home/leon/Dropbox/Projects/Radon/Data/gamma_count_tunnel/filt_gamma*')
    stream = read(scapath,starttime=starttime, endtime=endtime)
    stream.header['unit-col-x'] = ''
    stream = stream.extract('x',40000.,compare='<')
    print ("Now flagging")
    stream = stream.flag_stream('x', 3, 'cs', '2015-07-23T10:00:00', enddate='2015-07-23T18:00:00')
    stream = stream.remove_flagged()
    #mp.plot(stream,variables=['x','t1'],annotate=True)
    #mp.plot(stream,variables=['x','t1'],outfile=os.path.join(graphdir,'sca_timeseries.png'))

    #temppath = '/home/leon/Dropbox/Daten/RadonTables/temp-sgo-1min*'
    #tempstream = read(temppath,starttime=starttime, endtime=endtime)
    #print ("Temperature stream", tempstream.ndarray)

    recalcmeteo = False
    if recalcmeteo:
        print ("Reading old meteo data...")
        metpath1 = '/home/leon/Dropbox/Projects/Radon/Data/meteo_outside/meteo_out*'
        metstream = read(metpath1,starttime=starttime, endtime=endtime)
        sectcol = metstream._get_column('var3')
        secstream = metstream.copy()
        metstream = metstream.extract('f',-30.,compare='>')
        metstream = metstream.get_gaps()
        # Extract temperature from var3 (windsensor)
        secstream = secstream._put_column(sectcol,'f')
        metstream = mergeStreams(metstream, secstream, keys=['f'])
        metstream.write('/home/leon/Dropbox/Projects/Radon/Data/meteo_outside/', filenamebegins='meteo-1min_', coverage='year', dateformat='%Y',mode='insert',format_type='PYCDF')


    metpath2 = '/home/leon/Dropbox/Projects/Radon/Data/meteo_outside/meteo-1min*'
    metstream = read(metpath2,starttime=starttime, endtime=endtime)
    #met2stream = met2stream.extract('f',-30.,compare='>')
    #metstream = mergeStreams(metstream,met2stream, extend=True)
    mp.plot(metstream,variables=['f'])

    metstream.header['col-f'] = 'T'
    metstream.header['unit-col-f'] = 'deg C'
    metstream.header['col-var5'] = 'P'
    metstream.header['unit-col-var5'] = 'hPa'
    metstream.header['col-z'] = 'snow cover'
    metstream.header['unit-col-z'] = 'cm'

    #res = metstream.steadyrise('dx', datetime.timedelta(minutes=60),sensitivitylevel=0.002)
    #metstream = metstream._put_column(res, 't2', columnname='rain',columnunit='mm/1h')
    """
    fllist1 = metstream.flag_range(keys=['f'],below=-20)
    fllist2 = metstream.flag_range(keys=['var5'],below=800)
    fllist3 = metstream.flag_range(keys=['z'],above=100)
    metstream = metstream.flag(fllist1)
    metstream = metstream.flag(fllist2)
    metstream = metstream.flag(fllist3)
    metstream = metstream.remove_flagged()
    metstream = metstream.flag_outlier(keys=['t1','var5','z'],timerange=datetime.timedelta(days=5))
    metstream = metstream.remove_flagged()
    """

    #### ##########################################################################
    ##    create combined stream of gamma and meteo data, smooth it - 
    #### ##########################################################################

    newstream = mergeStreams(stream,metstream, keys=['f','z','var5'])
    diff = newstream.ndarray[4] - newstream.ndarray[5]
    diff = newstream.missingvalue(diff,120,threshold=0.05,fill='interpolation')
    outtemp = newstream.ndarray[4]
    outtemp = newstream.missingvalue(outtemp,120,threshold=0.05,fill='interpolation')
    count = newstream.ndarray[1]
    newstream = newstream._put_column(diff,'y')
    newstream = newstream._put_column(outtemp,'f')
    #mp.plot(newstream, variables=['x','y','f'])
    # create plot of timeseries here
    ## TODO add offset and scale for t1 (6 - 12)
    newstream.header['col-t1'] = 'T(i)'
    newstream.header['col-f'] = 'T(o)'


    #### ##########################################################################
    ##    create overview plot (full timerange with Cnt, T tunnel, T outside) -- not smmoothed
    #### ##########################################################################
    f, axarr = plt.subplots(2, sharex=True, figsize=(15,6))
    axarr[0].set_ylabel('Counts')
    axarr[0].plot_date(newstream.ndarray[0],newstream.ndarray[1],'-',color='darkgreen')
    #axarr[0].axhline(bg[0],linewidth=2, color='black')
    axarr[1].set_ylim([-20,35])
    axarr[1].set_ylabel('T [$^\circ$C]')
    axarr[1].fill_between(newstream.ndarray[0], 0, newstream.ndarray[4], where=newstream.ndarray[4] >= 0, facecolor='red', alpha=0.5, interpolate=True)
    axarr[1].fill_between(newstream.ndarray[0], 0, newstream.ndarray[4], where=newstream.ndarray[4] < 0, facecolor='blue', alpha=0.5, interpolate=True)
    axarr[1].plot_date(newstream.ndarray[0],newstream.ndarray[4],'-', linewidth=0.2, color='lightgray')
    #ax2 = axarr[1].twinx()
    #ax2.set_ylim([5.5,8.5])
    #ax2.set_ylabel('T(i)')
    axarr[1].plot_date(newstream.ndarray[0],newstream.ndarray[5],'-',color='magenta')
    axarr[1].set_xlabel('Date')
    pylab.savefig(os.path.join(graphdir,'sca_timeseries.png')) 
    plt.show()


    # gaps too fill (T) take from var3 (T of windsensor)
    # 31.07.2013 -8.08.2013
    # 20.11.2014
    # 20.02.-23.02.2015
    # 21.05.2015
    # 25.11. - 29.11.2016

    print ("Now check all gaps and clean up t data first")

    smoothstream = newstream.copy()
    smoothstream = smoothstream.smooth(['x'], window_len=2880)
    smoothstream = smoothstream._put_column(diff,'y')
    dTcol = running_mean(smoothstream.ndarray[2],5761,debug=True)
    smoothstream = smoothstream._drop_column('y')
    smoothstream = smoothstream._put_column(dTcol,'y')
    Tcol = running_mean(smoothstream.ndarray[4],5761,debug=True)
    smoothstream = smoothstream._drop_column('f')
    smoothstream = smoothstream._put_column(Tcol,'f')
    #smoothstream = smoothstream.smooth(['x','y'], window_len=1440)
    #mp.plot(smoothstream, variables=['x','y'])


    #### ##########################################################################
    ##    Find optimal parameters and constraints for transport model
    #### ##########################################################################

    # ################################################
    ## ### PARAMETERDEFINITION
    # ################################################
    # Parameters to adjust:
    # 1) grenztemperatur reduction <-> accumulation (diff+0 = sensor temp, diff+(2-4) average tunnel)
    # 2) zeitverzoegerung wann T_diff wirkt
    # 3) delay rate: wie schnell wird von einer bestimmten Konzentration abgebaut, bzw aufgebaut
    # 4) Concentration calib: welche maximal Konzentration wird bei dT erreicht
    # 5) one can eventually smooth the input data
    # 6) ---> for gidi: calculate power spec of model to show diurnal periods
    # 7) get optimal parameters --- minimize residuals
    # ################################################

    ## Background plus variation from raw data
    bg = TransportBackground(count)

    ## maxdT/maxCount relaionship, delay from smoothed data
    param = TransportBoundaryCond(smoothstream.ndarray[1], smoothstream.ndarray[2], smoothstream.ndarray[4], bg=bg)



    #### ##########################################################################
    ##    create dT/Cnt dependency plot (smoothed - limited window 2015)
    #### ##########################################################################

    select1 = smoothstream._select_timerange(starttime='2015-01-01',endtime='2016-01-01')
    x = select1[0]
    countarray = select1[1]
    deltatarray = select1[2]
    tarray = select1[4]
    doplot=True
    if doplot:
        fig = plt.figure(figsize=(15,5))
        ax = fig.add_subplot(1,1,1)
        plt.xlabel("Date")
        dt=False
        if dt:
            ax.set_ylim([-15,30])
            ax.set_ylabel(u'$\delta$T [$^\circ$C]')
            ax.plot_date(x,deltatarray,'-',color='lightgray',alpha=0.5)
            ax.fill_between(x, 0, deltatarray ,color='green',alpha=0.5)
            ax.fill_between(x, 0, deltatarray, where=deltatarray >= 0, facecolor='red', alpha=0.5, interpolate=True)
            ax.fill_between(x, 0, deltatarray, where=deltatarray < 0, facecolor='blue', alpha=0.5, interpolate=True)
        else:
            #ax.set_ylim([-15,30])
            ax.set_ylabel(u'T [$^\circ$C]')
            ax.plot_date(x,tarray,'-',color='red')
        #ax.plot(locmaxdt,deltatarray[locmaxdt],'o',color='darkgreen')
        ax2 = ax.twinx()
        ax2.set_ylim([5000,45000])
        ax2.set_ylabel("Counts", color='darkgreen')
        ax2.plot_date(x,countarray,'-',linewidth=1.5,color='darkgreen')
        pylab.savefig(os.path.join(graphdir,'sca_countdeltat.png')) 
        plt.show()

    #### ##########################################################################
    ##    Radioactive decay versus count rate reduction
    #### ##########################################################################

    select2 = newstream._select_timerange(starttime='2015-07-25T18:30:00',endtime='2015-08-01')
    x = select2[0]
    countarray = select2[1]
    decayrange = np.asarray(range(int(x[0]*1440.),int(x[-1]*1440.),10))
    decayval = np.asarray([decay(t, countarray[0]-bg[0], thalfRn*1440.) for t in decayrange-int(x[0]*1440.)])
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    plt.xlabel("time [min since start]")
    ax.set_ylabel("Counts", color='darkgreen')
    ax.set_ylim([15000,40000])
    ax.plot_date(x,countarray,'-',linewidth=2.0,color='darkgreen')
    ax.plot_date(decayrange/1440.,decayval+bg[0],'-',linewidth=2.0,color='red')
    pylab.savefig(os.path.join(graphdir,'sca_decaydecrease.png')) 
    plt.show()

    m  = int(param[0])
    delay  = int(param[1])
    E = param[2]
    a = param[3]
    c = param[4]
    bg = int(bg[0])
    tt = 2.
    tm = param[5]
    tbg = param[6]

    # For linear model use the 2015 values for E -> diffusion
    E = 67824.716258033732

    print ("-------------------------------------------------------------")
    print ("Timeseries analysis finished. Found the following parameters:")
    print ("Background level [counts]:     ", bg)
    print ("Background - exp arrhenius:    ", c)
    print ("Activation energy [J/mol] - will be fixed to 58000:     ", E)
    print ("Average time delay [minutes]:  ", delay)
    print ("Cnt0 (a, of exp fit) [counts]: ", a)
    print ("Linear model m [1/$^\circ$C]:  ", m)
    print ("Absolute model m [1/K]:        ", tm)
    print ("Absolute model t [counts]:     ", tbg)


if parameter:
    #### ##########################################################################
    ##    Show some parameter functions
    #### ##########################################################################
    F1 = M_func(E,D0,cmax,bg, m, a, c, tm, tbg, mode='nonlinear')
    F2 = M_func(E,D0,cmax,bg, m, a, c, tm, tbg, mode='linear')


    cntrange = np.asarray(range(14500, 40000, 1))
    fig = plt.figure(1)
    plt.title("M versus counts")
    ax = fig.add_subplot(1,1,1)
    plt.plot(cntrange,F1(cntrange),'-',color='darkred')
    plt.plot(cntrange,F2(cntrange),'-',color='darkblue')
    plt.show()

    dTrange = np.asarray(range(-15, 30, 1))
    cr1lst,cr2lst,cr3lst = [],[],[]
    Dlst = []
    for T in dTrange:
        D= diffusioncoefficient(D0, T, E)
        Dlst.append(D)
        cr = getCountDIF(F1(14300), 1, 1, D)
        cr1lst.append(cr)
        cr = getCountDIF(F1(20000), 1, 1, D)
        cr2lst.append(cr)
        cr = getCountDIF(F1(30000), 1, 1, D)
        cr3lst.append(cr)
    cr1lst = np.asarray(cr1lst)
    cr2lst = np.asarray(cr2lst)
    cr3lst = np.asarray(cr3lst)
    Dlst = np.asarray(Dlst)
    fig = plt.figure(1)
    plt.title("Diffusion - reduction rate at different countlevels")
    ax = fig.add_subplot(1,1,1)
    plt.plot(dTrange,cr1lst,'-',color='blue')
    plt.plot(dTrange,cr2lst,'-',color='purple')
    plt.plot(dTrange,cr3lst,'-',color='darkred')
    plt.show()

    fig = plt.figure(1)
    plt.title("Diffusion coefficient versus temperature")
    ax = fig.add_subplot(1,1,1)
    plt.plot(dTrange,Dlst,'-',color='blue')
    plt.show()



if rangetest:
    ratelst = [10] #[10,15,20,25,30] # counts/per min reduction
    t_tunnel = [0.,2.,4.]
    t_tunnel = [2.]
    delayrange = [delay] #range(0,7200,720) # delay in minutes (related to the drop /increase rate of radon)
    m_list= [m] #range(600,2000,100) # -> that should be determined by a extracting all maxima and corresponding Tdiffs na dline fit to the upper level
    bg = 14300
    print ("Minimum count", min(count))
    #E_list = [70000., 75000., 80000.]
    E_list = [70000.]
    #D0_list = [1000.]  # open air, Do, which is about 1.1 x 10-5 m2/s
    D0_list = [0.000011]  # open air, Do, which is about 1.1 x 10-5 m2/s
    cmax_list = [55000, 60000., 65000.]  ### Best fit for 06-10 2015 with E 3500, D0 50, cmax 32000, t 2 -> 0.94
    cmax_list = [55000.]
    ### Best choice is E=75000, cmax=60000. (Summer) -- Winter E 70000 cmax 55000-
    delayrange = [delay] #range(0,7200,720) # delay in minutes (related to the drop /increase rate of radon)
    a_list = [a]
else:
    bg = bg
    #print ("Minimum count", min(count))
    ratelst = [10] #[10,15,20,25,30] # counts/per min reduction
    t_tunnel = [2.]
    delayrange = [delay] #range(0,7200,720) # delay in minutes (related to the drop /increase rate of radon)
    m_list= [m] #range(600,2000,100) # -> that should be determined by a extracting all maxima and corresponding Tdiffs na dline fit to the upper level
    E_list = [E]
    E_list = [58000]
    D0_list = [D0]
    cmax_list = [cmax]  ### Best fit for 06-10 2015 with E 3500, D0 50, cmax 32000, t 2 -> 0.94
    a_list = [a]
    
if physicalmodel:
    #### ##########################################################################
    ##    Set up and analyze the diffusion model
    #### ##########################################################################

    mode = 5
    #fitmode='absolute'
    fitmode='linear'

    # requires diff or
    if fitmode == 'linear':
        temps = diff
    else:
        temps = outtemp+273.25
        t_tunnel = [0.]

    ### Physical model test
    resultarray = []
    maxpears,idx, bestidx = 0, 0, 0
    minstd = 1000000
    lastI = bg
    a = a_list[0]
    for D0 in D0_list:
        for E in E_list:
            for cmax in cmax_list:
                Mfunc = M_func(E,D0,cmax,bg, m, a, c, tm, tbg, mode=fitmode)
                for tt in t_tunnel:
                    minlst = np.asarray(range(0,len(temps),1))
                    dtlist = temps+tt
                    for delay in delayrange:
                        #tmplist = running_mean(dtlist,2*delay)
                        tmplist = dtlist
                        #tlen=len(tmplist)
                        #nl = [bg]*delay
                        #nl = [0.0]*delay
                        #nl.extend(tmplist)
                        #print ("Here1", len(nl))
                        #tmplist = nl[:tlen]
                        #print ("Here2", len(tmplist), np.asarray(tmplist))        
                        # add a offset to tmplist
                        for rate in ratelst:
                            for m in m_list:
                                print ("Running accumulation with the following parameter: m={}, rate={}, bg={}, delay={}, T_offset={}, E={}, D0={}, cmax={}, fitmode={}".format(m,rate,bg,delay,tt,E,D0,cmax,fitmode))
                                Ilst = []
                                #Ilst = [bg]*delay # timeshift
                                #print ("Here", len(Ilst))
                                for t in minlst:
                                    dT = tmplist[t]
                                    if t == 0:
                                        Ist = bg
                                    else:
                                        Ist = Ilst[t-1]
                                    I = accumulation(Ist, dT, rate=rate, m=m, mode=mode, bg=bg, E=E, cmax=cmax, D0=D0, Mfunc=Mfunc)
                                    if not np.isnan(I):
                                        lastI = I
                                    else:
                                        I = lastI
                                    Ilst.append(I)
                                #Itmp = [bg]*delay
                                #Itmp.extend(Ilst)
                                Itmp = Ilst
                                finallst = np.asarray(Itmp[:len(diff)]) #Ilst) #[:len(diff)])
                                orgdata = newstream.missingvalue(newstream.ndarray[1],10000,threshold=0.05,fill='interpolation')
                                # should thos be shifted by delay??? 
                                residuum = orgdata-finallst
                                std = np.std(residuum)
                                #finallst = np.insert(finallst,0,[bg]*delay)
                                #finallst = np.asarray(finallst[:len(diff)])
                                pears = pearson(orgdata, finallst)
                                print ("Pearson:", pears, std)
                                resultarray.append([std,pears[0],pears[1],m,delay,tt,rate,bg, E, cmax, D0])
                                #if pears[0] > maxpears:
                                #if std < minstd:
                                if pears[0] > maxpears:
                                     maxpears = pears[0]
                                     #minstd = std
                                     bestIlst = finallst
                                     bestidx = idx
                                idx = idx+1


    print resultarray[bestidx]
    newstream = newstream._put_column(bestIlst,'var2')
    #newstream = newstream.smooth(['var2'], window_length=12880)
    #mp.plot(newstream,variables=['x','var2','y','f','t1','z','var5'],fill=['y','z'])
    newstream.header['col-var2'] = 'model'
    newstream.header['col-y'] = 'T_diff'
    #mp.plot(newstream,variables=['x','var2','y'],fill=['y'])
    residuum = newstream.ndarray[1]-newstream.ndarray[8]
    newstream = newstream._put_column(residuum,'dx')

    #### ##########################################################################
    ##    Show model in comarison to data, temperature, with residuum
    #### ##########################################################################

    f, axarr = plt.subplots(4, sharex=True, figsize=(15,12))
    axarr[0].set_ylabel('Data [counts]')
    axarr[0].plot_date(newstream.ndarray[0],newstream.ndarray[1],'-',color='darkgreen')
    #axarr[0].axhline(bg[0],linewidth=2, color='black')
    #axarr[1].set_ylim([5.5,8.5])
    axarr[1].set_ylabel('Model [counts]')
    axarr[1].plot_date(newstream.ndarray[0],newstream.ndarray[8],'-',color='black')
    axarr[2].set_ylim([-20,35])
    axarr[2].set_ylabel('T(o)')
    axarr[2].plot_date(newstream.ndarray[0],newstream.ndarray[2],'-',linewidth=0.2, color='lightgray')
    axarr[2].fill_between(newstream.ndarray[0], 0, newstream.ndarray[2], where=newstream.ndarray[2] >= 0, facecolor='red', alpha=0.5, interpolate=True)
    axarr[2].fill_between(newstream.ndarray[0], 0, newstream.ndarray[2], where=newstream.ndarray[2] < 0, facecolor='blue', alpha=0.5, interpolate=True)
    #axarr[3].set_ylim([5.5,8.5])
    axarr[3].set_ylabel('Residuum')
    axarr[3].plot_date(newstream.ndarray[0],newstream.ndarray[12],'-',color='blue')
    axarr[3].set_xlabel('Date')
    pylab.savefig(os.path.join(graphdir,'sca_timeseriesmodel.png')) 
    plt.show()

    #### ##########################################################################
    ##    Show zoom of model in comarison to data
    #### ##########################################################################

    zoomarray = newstream._select_timerange(starttime='2015-08-01',endtime='2015-09-15')
    f, axarr = plt.subplots(2, sharex=True, figsize=(7,3))
    axarr[0].set_ylabel('Data [counts]')
    axarr[0].plot_date(zoomarray[0],zoomarray[1],'-',color='darkgreen')
    #axarr[0].axhline(bg[0],linewidth=2, color='black')
    #axarr[1].set_ylim([5.5,8.5])
    axarr[1].set_ylabel('Model [counts]')
    axarr[1].plot_date(zoomarray[0],zoomarray[8],'-',color='black')
    pylab.savefig(os.path.join(graphdir,'sca_zoom.png')) 
    plt.show()


    #mp.plot(newstream,variables=['x','var2','y','dx'],fill=['y'])
    #mp.plot(newstream,variables=['x','var2','y','dx'],fill=['y'],outfile=os.path.join(graphdir,'sca_timeseriesmodel.png'))


if empiricalmodel:
    ### Emperical model test
    resultarray = []
    maxpears,idx = 0, 0
    minstd = 1000000
    for tt in t_tunnel:
        minlst = np.asarray(range(0,len(diff),1))
        dtlist = diff+tt
        for delay in delayrange:
            tmplist = running_mean(dtlist,2*delay)
            tlen=len(tmplist)
            nl = [bg]*delay
            nl.extend(tmplist)
            print ("Here1", len(nl))
            tmplist = nl[:tlen]
            print ("Here2", len(tmplist))        
            # add a offset to tmplist
            for rate in ratelst:
                for m in m_list:
                    print ("Running accumulation with the following parameter: m={}, rate={}, bg={}, delay={}, T_offset={}".format(m,rate,bg,delay,tt))
                    Ilst = []
                    #Ilst = [bg]*delay
                    #print ("Here", len(Ilst))
                    for t in minlst:
                        dT = tmplist[t]
                        if t == 0:
                            Ist = bg
                        else:
                            Ist = Ilst[t-1]
                        I = accumulation(Ist, dT, rate=rate, m=m, mode = mode, bg=bg, E=E, cmax=cmax, D0=D0, Mfunc=Mfunc)
                        if not np.isnan(I):
                            lastI = I
                        else:
                            I = lastI
                        Ilst.append(I)
                    finallst = np.asarray(Ilst) #[:len(diff)])
                    residuum = newstream.ndarray[1]-finallst
                    std = np.std(residuum)
                    #finallst = np.insert(finallst,0,[bg]*delay)
                    #finallst = np.asarray(finallst[:len(diff)])
                    pears = pearson(newstream.ndarray[1], finallst)
                    print ("Pearson:", pears, std)
                    resultarray.append([std,pears[0],pears[1],m,delay,tt,rate,bg])
                    #if pears[0] > maxpears:
                    if std < minstd:
                         #maxpears = pears[0]
                         minstd = std
                         bestIlst = finallst
                         bestidx = idx
                    idx = idx+1

    if not physicalmodel: 
        print resultarray[bestidx]
        newstream = newstream._put_column(bestIlst,'var2')
        #newstream = newstream.smooth(['var2'], window_length=12880)
        #mp.plot(newstream,variables=['x','var2','y','f','t1','z','var5'],fill=['y','z'])
        newstream.header['col-var2'] = 'model'
        newstream.header['col-y'] = 'T_diff'
        #mp.plot(newstream,variables=['x','var2','y'],fill=['y'])
        residuum = newstream.ndarray[1]-newstream.ndarray[8]
        newstream = newstream._put_column(residuum,'dx')
        mp.plot(newstream,variables=['x','var2','y','dx'],fill=['y'])
        mp.plot(newstream,variables=['x','var2','y','dx'],fill=['y'],outfile=os.path.join(graphdir,'sca_timeseriesmodel.png'))

    ## Add resiudal plot here
    # Excellent: 
    # pearson - 2015: 0.94

    #print ("Pearson:", pearson(newstream.ndarray[1], np.asarray(Ilst)))


if report:
    """
    # #####################################################################################
    # #################                      Report                     ###################
    # #####################################################################################
    """

    abstracttext = 'At the Conrad Observatory in lower Austria gamma measurements are performed since five years. These measurements aim on the identification of radon variation patterns linked to environmental and geodynamic effects. Gamma determinations are performed within the seismological tunnel of the observatory, 60 m below ground and 150 m inside the mountain. The tunnel is not ventilated and well sealed against the environment. Although conditions within the tunnel are very stable, strong seasonal patterns of gamma variations are present corresponding well to variations of the temperature difference between outside and tunnel, as observed in numerous other studies around the globe. In order to explain this relationship, a physical model is introduced consisting of two building blocks: a production and a diffusion term. It is shown that such temperature depended diffusion process can explain the observed features very well. It further explains the presence of time shifted and smeared diurnal signals within the timeseries. For identification and correct interpretation of other geodynamic signals, such diffusion related environmental influence needs to be subtracted. A first-order approach for such correction is provided here.'
    keywords = 'gamma variation, radon, radiometry, diffusion'

    introductiontext = 'Temporal variation patterns of radon gas are of paramount interest basically for two reasons: Firstly, the radioactive gas radon is well known for its health risks. Radon can accumulate in buildings, mines and other underground constructions and can cause radiation sickness and lung cancer \\citep{US-EPA2003, Ubysz2017}. Secondly, radon in a geological environment can provide insigths into transport processes and routes, and thus its variations contain informations on fault systems and other deformation processes. Not least among this interest was the potential of recognizing precursory signals of earthquakes although such hypotheses are heavily debated in the scientific community \\citep{Woith2015,HwaOh2015}. \\\\ Radon ($^{222}$Rn) is a radioactive inert gas formed by disintegration from $^{226}$Ra as part of the $^{238}$U decay series. It disintegrates in several steps to stable $^{206}$Pb. $^{222}$Rn occurs at varying concentrations in geological environments. The combination of its noble gas character and its radioactive decay make it a unique ultra-trace component for tracking temporally varying natural processes. Using nuclear techniques, the measurement sensitivity for $^{222}$Rn is extremely high and can be performed with a time resolution in the order of 1 hour or less, even at very low radiation backgrounds. Measurement are commonly performed using alpha silicon diodes, gamma crystal scintillators and ionization chamber detectors of which gamma probes reach the highest sensitivity \\citep{Zafrir2011}, although they only provide an indirect measure of decay products of radon ($^{222}$Rn). \\\\ During the last decade numerous publications were devoted to clarify the possible reasons of observed variation patterns and their potential environmental and geodynamic consequence. Long term measurements of radon concentration in underground laboratories, mines and boreholes, are typically used to investigate variation patterns \\citep[see][and many more]{Garavaglia1998, Steinitz2007, Chambers2009, Steinitz2010, Barbosa2010, Choubey2011, Mentes2015, HwaOh2015}. Our measurements within the seismological tunnel of the Conrad Observatory will contribute to this collection. In all studies large temporal variations of radioactive count rates are reported, related to $^{222}$Rn variation, and seasonal variations are observed. Furthermore, in basically all cases a significant correlation between variational patterns and outside temperature variation has been observed. As an underlying physical process for the coupling mechanism between outside and inside conditions, very often density driven ventilation is considered and tested by first order transport models \\citep{Finkelstein2006,Kowalczk2010}. Ventilation requires pressure differences which are not always observed \\citep{Steinitz2007}. Concentration related diffusion has been mentioned by some authors in combination with production of radon \\citep{Finkelstein2006}. It has, however, not been considered as a dominant transport process so far. \\\\ Here we present a long-term single channel analyzer (SCA) monitoring of gamma variation within the seismological tunnel of the Conrad Observatory. The here presented timeseries shows the already well known seasonal signals and a clear correlation with temperature differences to the outside temperature. In order to clarify the coupling mechanism we will introduce a simple physical model which basically consists of two term: a production term describing the formation of new radiogenic isotopes and a diffusion term, describing concentration related diffusion, who s effectivity is modulated by temperature differences. It will be shown that this model almost perfectly allows to reconstruct observed gamma variations in a non-ventialted underground structure. Subtracting such environmental signal is essential for interpreting and identifying other dynamic signals within the radon timeseries.'

    instrumenttext = 'Scintillation instruments (NaI) are used to determine gamma contributions of the disintegration products of Rn. For the SCA measurements a gamma detector (3x3 inch, NaI, Scionix scintillation detector 76B76/3M-HV-E3-X2 with build amplifier and SCA) is used to measure the variation of gamma radiation in the air of the seismic tunnel at the Conrad Observatory (Figure \\ref{fig:sitemap}). Data acquisition is done by a Campbell 800 Data Logger. The primary resolution is 1 minute. The single channel analyzer measures the cumulative count rate between channels xxx and xxx, corresponding to an energy range from xxx KeV to xxx KeV. The energy range is adjusted so that one decay product of $^{222}$Rn ($^{214}$Bi) contributes to the minute counts. Furthermore, this isotope is the dominant source of count variation as it is directly related to mobile RN. The SCA experiment is complemented by SSNTD short- and long-term exposimeter probes, which are set up close to the gamma sensor with 10 cm, directly on the concrete of the measurement pier. SSNTD exposimeter measurements near the SCA allow for relations between gamma counts and radon concentrations.  SSNTD probes short- and long-term exposimeter probes from two different companies (KIT and Radon) have been obtained and their results results are summarized in table \\ref{}. \\\\ The SCA sensor is placed on a concrete block at 131 meters of the 145 m long tunnel (Figure \\ref{fig:sitemap}a). Towards the single entrance, three usually closed doors prevent any significant air movement. As shown later the temperature is absolutely constant with variation below 0.04$^\circ$C throughout the year. The tunnel wall consists of at least 20 cm of reinforced concrete with additional sealing and shotcrete, effectively shielding the tunnel interior (Figure \\ref{fig:sitemap}c). Thus, all gamma contributions and Rn within the tunnel originates from the concrete of pier, floor and walls within the tunnel.'

    try:
        startdate = str(timelist[0])
    except:
        startdate = '2015-10-xx'

    resultssca1 = 'Figure \\ref{fig:sca_timeseries} provides an overview of all SCA measurements since beginning of the timeseries in November 2012. The most obvious feature in this almost 5 year long timeseries is the clearly visible seasonal variation with maximum values in summer and minimum values in winter, as similarly observed in numerous other studies \\citep[e.g.]{Barbosa2010,Mentes2015,HwaOh2015}. The minimum values show a clear limit, never falling below $C_{BG}\\approx$14000 counts/min, hereinafter referred to as the background level. Large short term variations are predominantly visible in summer reaching peak heights with more then double the amount of counts as the background level. The ambient sensor temperature is almost constant at 6.80 $\pm$ 0.05 $^{\\circ}$C (Figure \\ref{fig:sca_timeseries}) . The average temperature of the tunnel (9$^{\\circ}$C) is 2.2$^{\\circ}$C larger. As already noted above, gamma measurements are performed in a tunnel segment which is separated from the host rock by concrete, shielding gamma contribution from outside. Therefore, detected gamma sources predominantly originate from the concrete itself. {\\bf Description of the observed signal, spectra, signal content - Susana.} The variational part of observed gamma count timeseries needs to be related either to a non-equilibrium decay process or a radioactive source varying in concentration. However, the observed variations cannot be explained by simple non-equilibrium decay as some count rate reductions are much faster as a theoretical decay would allow for (Figure \\ref{fig:sca_decaydecrease}). Therefore, additional processes need to act in order to explain the data and mobile Rn and its gamma emitting decay products are the obvious (and only) candidate for the observed concentration variations. The only possible transport route is along the non-ventilated tunnel and, therefore, a concentration depended diffusion seems to be the most likely candidate. Diffusion happens from high concentration regions towards low concentration and is also affected by atmospheric gradients. Pressure measurements do not indicate any different variation patterns between external (outside the observatory) and inside conditions. Temperature differences are, however significant. Plotting the temperature difference between outside and average tunnel temperature, clearly shows emphasizes the similarities between these observed signals (Figure \\ref{fig:sca_param}a). As can be seen from this plot, maxima in count rate are directly related to maxima in the temperature gradient between tunnel and outside ($\delta$T$_{SCA}$), as long as the temperature difference is positive. As shown in Figure \\ref{fig:sca_param}b, there is a quasi-linear relationship between the amplitudes of count maxima and their preceding maxima in $\delta$T$_{SCA}$, indicating that the effectiveness of diffusion between tunnel interior and outside is directly (and, in first order, linearly) related to the outside temperature. The maxima in $\delta$T$_{SCA}$ are preceding count rate maxima in time in a fairly constant way with an average delay of almost 2 days (Figure \\ref{fig:sca_param}c). Based on these observations it is possible to develop a physical model, connecting count variations and $\delta$T$_{SCA}$.'

    resultssca2 = 'A physical model for explaining the dependency of radon concentration, which is directly related to the variations in count rate, to external temperature variations basically involves two processes: 1.) A secular equilibrium process between a long-lived parent (Ra) and short-lived daughter (Rn) isotopes. Production of new Radon occurs within the concrete of the tunnel, followed by decay with half live of 3.568 days by disintigration towards 214Pb/214Bi and finally 206Pb. 2.) Diffusion, which explains the net flux of radon from a region of higher concentration to one of lower concentration, modulated by the temperature difference between these regions. \\\\ From inspecting the observational timeseries (Figure \\ref{fig:sca_timeseries}), we see that if $\delta$T$_{SCA}$ is negative, i.e. the outside temperature is below the average tunnel temperature, then the diffusion process is dominating, indicating that all newly formed Rn is effectively removed from the tunnel. If $\delta$T$_{SCA}$ is positive, diffusion is getting less effective, and the count rate is gradually building up towards a maximum equilibrium level directly related to the temperature difference as confirmed by Figure \\ref{fig:sca_param}. If the $\delta$T$_{SCA}$ increases further, the equilibrium concentration and thus the net count rate is raising linearly in first order. These observations can be expressed mathematically using the following scheme: \\subsubsection{production term} The secular equilibrium process, summarizing production and decay of Rn, can be expresses as follows, using the justified simplification that the halflive of the parent nuclide is infinitely larger than that of the daughter nuclide: \\begin{equation} C_{t} = C_{BG} + C_{max} (1 - \exp^{-\lambda{}t}) \\end{equation} $C_{max}$ denotes the maximum count level too be reached if only this process is acting. $C_{BG}$ corresponds to the background level related to stationary gamma sources. $\\lambda$ is related to the half live of $^{222}$Rn by $\\ln{}2/T_{1/2}$. \\subsubsection{diffusion term} The one-dimensional diffusion equation or Ficks second law is perfectly suited to describe concentration diffusion in tubes or tube like structures, like the tunnel of the Conrad Observatory. \\begin{equation} \\frac{\\delta{}C}{\\delta{}t} = D \\frac{\\delta{}^2C}{\\delta{}x^2} \\end{equation} $D$ corresponds to the diffusion coefficient. A useful elementary solution of the diffusion equation is the solution to an instantaneous, localized release in an infinite domain initially free of the substance, except for the localized release $M = C_x$ at $x=0$. This approximations assumes a perfectly clean tunnel as an initial start condition with only background radiation, which is well justified. Localized source and infinite domain are necessary simplifications, justified by the fact that we only analyze the diffusion equation at the sensor position. Along this line (see \\citet{diffusion} for mathematical details), concentration related diffusion in a non-ventilated tunnel can be expressed as: \\begin{equation} C_{x,t} = \\frac{M}{\\sqrt{4\\pi{}D t}} \exp^{-\\frac{x^2}{4 D t}} \\end{equation} For our analysis we are only interested in concentration variations at the source near the sensor position ($x$=0). Thus the diffusion equation simplifies to \\begin{equation} C_{t} = \\frac{M}{\\sqrt{4\\pi{}D t}} \\end{equation} $M$ describes the total mass released at the source per unit-cross section. It can be determined experimentally from the linear relationship in Figure \\ref{fig:sca_param}b by solving equation 4 for equilibrium count rates $C_{equ}$ at which production and diffusion equalize in dependency of the temperature difference $\\delta{}T$. Thus, $M$ is also a function of temperature difference. The diffusion coefficient $D$ is not constant, but obviously related to the outside temperature. This can be described in a most general way using the Arrhenius relation \\begin{equation} D = D_{0} \exp^{-\\frac{E_A}{R T}} \\end{equation} Here, $E_A$ is the activation energy, $R$ is the universal gas constant and $T$ the absolute temperature. For evaluation of the model we assume that $D_0$ is well represented by its open air value of $1.1 10^{-5}$m$^2/$s.'

    #is related to the count rate and can be determined from \\begin{equation} M = \\frac{\\delta{}C_{equ}}{(\\frac{1}{\\sqrt{4 \\pi D t}} - \\frac{1}{\\sqrt{4 \\pi D (t+\\delta{}t)}})} \\end{equation}  $\\delta{}C_{equ}$ corresponds to the equilibrium countrate at which production and diffusion equalize in dependency of the temperature difference $\\delta{}T$ (figure). For this relation we assume a linear dependecy a outlined in figure.
    # It can also be expressed as an emperical model as outlined below. Here, gamma counts at time $t+1$ ($C_{t+1}$) are related to the previous state $C_t$ by: \\begin{equation} C_{t+1} = C_t + f(C_{t}, C_{max}) \\end{equation} $C_{max}$ denotes the maximum count level too be reached which according to Figure \\ref{fig:sca_param} is directly related to $\delta{}T_{t}$ and can be estimated by \\begin{equation} C_{max} = m ^. \delta{}T_{t-d} + C_{BG} \\end{equation}  The parameter $d$ is related to the delay between $\delta{}T$ change and effective consequence on $C_t$. The most simple definition of $f(C_{t}, C_{max})$, which is however in agreement with all experimental findings above is: \\begin{equation} f(C_{t}, C_{max}) =  R (C_{max} - C_{t}) \\end{equation} R describes the transport rate i.e. the reduction of counts per time interval. This means that if $C_{max}$ is larger then the current count rate, then the measured count rate is increasing. If $C_{max}$ is smaller because of dropping $\delta$T, then the measured count rate is decreasing. For an initial test we assume $R$ to be linear and identical for build up and removal. An average $R$ is also experimentally derived from the slope rates and corresponds to 25 counts/min. 

    resultssca3 = 'In summary, assuming above simplifications to be reasonable, we can calculate the count rate variation based solely on an initial background level ($C_{BG}$) and temperature differences between outside and inside the tunnel $\delta$T. \\begin{equation} C_{t+1,dT} =  C_{t,dT} + ( \\text{Production}(C_{t,dT},C_{max}) - \\text{Diffusion}(M,D) ) \\end{equation} Starting form the background level we calculate Production and Diffusion contributions for each successive time step. From all required parameters, the maximum possible count rate $C_{max}$ and the activation energy $E_A$ remain elusive. For $C_{max}$ we know that its value need to be above the highest measured values. A reasonable asumption are 60000 counts/min, which is about 50\\% larger then the highest measured value. $E_A$ for radon diffusion is assumed to be constant. Several studies on different media provided similar values for $E_A$ and here we use a value of 58kJ/mol \\citep{Somasundara2006}. Plotting observed counts and modelled counts (Figure \\ref{fig:sca_timeseriesmodel}) shows, that the model is perfectly suited to simulate observed count variations. The Pearson coefficient, describing the similarity between the observed and modelled curves, results in 0.9 (1 would be obtained by identical curves) underlining the visual impression. Therefore, we conclude that a simple temperature governed diffusion process dominantly explains the variational gamma count pattern and its underlying variability in radon concentration.'

    #discsca1 = 'Hypothesis: If $T_{ext} < T_{in}$ then transport between tunnel and exterior takes place, so that newly generated Rn within the tunnel is gradually removed. If the $T_{ext}$ is larger then the $T_{in}$ then this transport process is blocked. The blocking, thus the maximum equilibrium level is related to the temperature difference. If $T_{ext}$ drops below $T_{in}$ then the removal is initiated, which means that Rn concentration needs to reach an extremum and then reduce. The reduction rate should be a combination of decay and transport and should provide information on removal speed. The maximum value reached if $T_{ext} > T_{in}$ should eventually be linked to $T_{diff}$. Test: 1. Do gamma concentration start to increase when $T_{ext}$ passes $T_{in}$? 2. Does gamma concentration bend down if $T_{in}$ passes $T_{ext}$? If 1 and 2 are true then a) determine the delay ratio until background level is reached again. b) Determine a maximum level, temperature-diff at maximum relationship. If this observation is correct then: define two states (a) accumulation state and (b) background state. Consequences: 1.) Accumulation is dependent on T and therefore (with some phase shift) outside T leads to concentration variation inside. Therefore diurnal variation (sometimes referred to as solar tides) will show up. 2.) The concentration variation of Rn at Cobs provides only information on T diif and has no other geophysical meaning. Possible values: Phase shift 2-4 hours ??'

    discsca1 = 'The main aims of this study comprise the identification of variation patterns in long term gamma observations, its physical background, consequences for the interpretation of such signals and, finally, to verify whether such signals can eventually be corrected for. \\\\'

    discsca2 = 'A seasonal variation pattern is observed within the gamma timeseries at the Conrad Observatory. Similar variations have been found in many other localities as summarized in the introduction. When looking at the signal in detail, we observe seasonal trends and superimposed onto this signal we also find diurnal variations during gamma maxima in summer. The conditions at the measurement position are, however, extremely stable and we can exclude any ventilation or heat transfer. \\\\'

    discsca3 = 'As shown above, a simple transport model, which is solely based on temperature gradients between varying outside conditions and constant tunnel temperature, is highly efficient to simulate gamma variations as observed in the seismological tunnel of the Conrad Observatory. This model does not require ventilation or heat transport, but explains the relationship to temperature differences by concentration diffusion. The diffusion process further leads to inherent smoothing, strongly reducing diurnal signatures. When looking at the residuals of model and data it is obvious, that we have no perfect match yet. The residuals show, that the model underestimates count rate changes in winter and overestimates them in summer. The model makes use of simplifications like treating source and diffusion as being localized at a single spot in an infinite domain. Only temperature variations are considered to modulate the diffusion process although there are many conditions within the transport path (doors, heaters, ventilation, human presence, even cooling fans of computers) which in our experimental setup cannot be exactly traced and included into the process. Other constraints come from the selection and determination of model parameters, in particular the assumption of a linear relationship between maximum temperatures and count rate. Although such linear relationship is well sustained by the experimental findings, both values cannot be linearly related. First of all, because of delay, maximum count values are not necessary reached following a sharp temperature peak. Thus, values shown in Figure \\ref{fig:sca_param}b will tend to be below the maximum possible value. So the best estimate of the relation function should be the upper envelop of all data. {\\bf Open points: test another function, discuss time shifts (radon and its decay products), parameter Cmax , Analyze the residuum... Are there any region of difference and are the related to snow cover, heavy rain, etc?}'

    discsca4 = 'When looking at short time intervals of observed and modelled data, where the count rates are changing strongly, diurnal signatures are visible both in data and model (Figure \\ref{fig:sca_zoom}). The model shows such daily variations more clearly as compared to data. Because of smoothing and time delays because of the diffusion process, the diurnal signatures are shifted in time and also smeared in terms of their maximum frequency. {\\bf Check frequencies.... I suspect that it will be hard to distinguish between lunar and solar periodicities}. Usually variation data are analyzed by some sort of timeseries analysis for identification of dominant periodicities and their underlying relationship to tides and other geophysical processes. Very often, a diurnal signal is detected hereby, although its relationship to temperature variations is often questioned due to the fact, that the sensor does not see any temperature variation, that such signals are not continuously present and finally there is no direct relation between temperature signals and count variation. During summer, with large temperature variations and large gradients, diurnal signals get superimposed onto the virtual count rate signal, although this signals are smoothed and, particularly shifted in time. In winter, however, or at times of negative temperature differences, such diurnal signals are not transferred. Thus we are confident, that the observation of diurnal (and eventually lunar) patterns are actually triggered by temperature related diffusion.'


    #resultssca2 = 'The Hilbert-Huang transform (HHT) is a way to decompose a signal into so-called intrinsic mode functions (IMF) along with a trend, and obtain instantaneous frequency data. It is designed to work well for data that is non-stationary and non-linear. The fundamental part of the HHT is the empirical mode decomposition (EMD) method. Breaking down signals into various components, EMD can be compared with other analysis methods such as Fourier transform and Wavelet transform. Using the EMD method, any complicated data set can be decomposed into a finite and often small number of components. These components form a complete and nearly orthogonal basis for the original signal. In addition, they can be described as intrinsic mode functions (IMF). (cite) Without leaving the time domain, EMD is adaptive and highly efficient (citation needed). Since the decomposition is based on the local characteristic time scale of the data, it can be applied to non-linear and non-stationary processes (citation needed). An IMF is defined as a function that satisfies the following requirements: In the whole data set, the number of extrema and the number of zero-crossings must either be equal or differ at most by one. At any point, the mean value of the envelope defined by the local maxima and the envelope defined by the local minima is zero. It represents a generally simple oscillatory mode as a counterpart to the simple harmonic function. By definition, an IMF is any function with the same number of extrema and zero crossings, whose envelopes are symmetric with respect to zero (citation needed). This definition guarantees a well-behaved Hilbert transform of the IMF.  (Benifits and drawbacks .... non-destructive, summing up all components restores the original curve with no information loss) \\\\ The background gamma radiation, due probably mainly to sources in the concrete is in the order to $1.5x10^4$ counts (per minute). A long term variation of radon is reflected as an annual radon signal with large amplitude  of $1.5x10^4$ counts and a maximum in summer. The driving mechanism of this annual pattern is subject to further analysis. Small to large ($1.0x10^4$ counts) non periodic multi-day signals lasting from two to several tens of days are superimposed. Daily periodic signals of much lower amplitude are observed in summer, with amplitudes generally up to $2x10^3$ counts.'

    conc = 'Gamma variations at the seismological tunnel of the Conrad Observatory in Austria exhibit a strong seasonal pattern and clear relationship with temperature differences between outside and tunnel. The tunnel is not ventilated and well shielded against the environment. Variation of gamma counts are related to the radon disintegration products originating from the concrete. The observed variation patterns can be very well explained by a diffusion model making use use of a 1D diffusion term, of which the diffusion coefficient is dependent on temperature variation, and a secular equilibrium production term, describing the formation of radon from its parent isotope Ra. A model based solely on variations of temperature differences and experimentally determined parameters simulates the observed gamma variation with a Pearson coefficient of 0.9. As temperature differences inherently contain a diurnal periodicity, this transfer to the count rate with gradient dependent amplitudes and phase shifts obviously dependent on the specific layout of location/building/tunnel. This diffusion process masks effectively all other eventually possible signals. Therefore, for a proper analysis of gamma variation and identification of other environmental and geodynamic signatures requires the reduction of such temperature controlled  diffusion terms in particular in non-ventilated environments. The simplified model presented here provides a further step in this direction.'

    texoutput = True
    if texoutput:
        texdir = "/home/leon/CronScripts/MagPyAnalysis/RadonAnalysis/Report/SCA"
        ## Assemble Text and create README and TeX
        with open(os.path.join(texdir,'scareport.tex'), 'wb') as texfile:
            texfile.write("\\documentclass{elsart}\n")
            texfile.write("\\renewcommand{\\baselinestretch}{1}\n")
            texfile.write("\\usepackage[utf8]{inputenc}\n")
            texfile.write("\\usepackage{textcomp}\n")
            texfile.write("\\usepackage{deluxetable}\n")
            texfile.write("\\usepackage{natbib}\n")
            texfile.write("\\usepackage{graphicx}\n")
            texfile.write("\\usepackage{amsmath}\n")
            texfile.write("\\usepackage{amssymb}\n")
            texfile.write("\\usepackage{epsfig}\n")
            texfile.write("\\usepackage{subcaption}\n")
            texfile.write("\\usepackage{setspace}\n")
            texfile.write("\\bibpunct[,]{[}{]}{;}{a}{,}{,}\n")
            texfile.write("\\bibliographystyle{jgrstyle}\n")
            texfile.write("\\newcommand{\sus}{susceptibility}\n")
            texfile.write("\\hyphenation{paleo-intensity paleo-intensities}\n")
            texfile.write("\\sloppy\n")
            texfile.write("%-----------------------------------------------------------------\n")
            texfile.write("\\begin{document}\n")
            texfile.write("% ----------------------------------------------------------------\n")
            texfile.write("\\begin{frontmatter}\n")
            texfile.write("\\title{Diffusion controlled underground gamma variation at the Conrad Observatory in Austria}\n")
            texfile.write("\\author{R. Leonhardt, }\n")
            texfile.write("\\author{G. Steinitz, }\n")
            texfile.write("\\author{W. Hasenburger, }\n")
            texfile.write("\\author{S. Barbosa}\n")
            texfile.write('\\address{Zentralanstalt f{\"u}r Meteorologie und Geodynamik, Hohe Warte 38, 1190 Wien, Austria}\n')
            texfile.write("\\address{Geological Survey of Israel}\n")
            texfile.write('\\address{Montanuniversit{\"a}t Leoben, Austria}\n')
            texfile.write("\\begin{abstract}\n")
            texfile.write("{}\n".format(abstracttext))
            texfile.write("\\end{abstract}\n")
            texfile.write("\\begin{keyword}\n")
            texfile.write("{}\n".format(keywords))
            texfile.write("\\end{keyword}\n")
            texfile.write("\\end{frontmatter}\n")
            texfile.write("% ----------------------------------------------------------------\n")
            texfile.write("% ----------------------------------------------------------------\n")
            texfile.write("\\section{Introduction}\\label{intro}\n")
            texfile.write("{}\n".format(introductiontext))
            texfile.write("% ----------------------------------------------------------------\n")
            texfile.write("\\section{Instrumentation and Methods}\\label{inst}\n")
            texfile.write("{}\n".format(instrumenttext))
            texfile.write("\\begin{figure}[htb]\n")
            texfile.write("\\centering\n")
            texfile.write("\\includegraphics[width=\\textwidth]{../graphs/SGO_sitemap.png}\n")
            texfile.write("\\caption{a) map of the seismological tunnel at the Conrad Observatory. The blue point marks the sensor position of the SCA. b) The Conrad Observatory is located in Austria about 50 km south-west of Vienna. c) cross section of the seismological tunnel.}\n")
            texfile.write("\\label{fig:sitemap}\n")
            texfile.write("\\end{figure}\n")
            texfile.write("%\\input{../Tables/insttable}\n")
            texfile.write("% ----------------------------------------------------------------\n")
            # double figure
            texfile.write("% ----------------------------------------------------------------\n")
            texfile.write("\\section{Results}\\label{results}\n")
            texfile.write("% ----------------------------------------------------------------\n")
            texfile.write("\\subsection{Timeseries}\\label{conf}\n")
            texfile.write("% ----------------------------------------------------------------\n")
            texfile.write("\\begin{figure}[htb]\n")
            texfile.write("\\centering\n")
            texfile.write("\\includegraphics[width=\\textwidth]{../graphs/sca_timeseries.png}\n")
            texfile.write("\\caption{Timeseries of the SCA measurements within the seismological tunnel.}\n")
            texfile.write("\\label{fig:sca_timeseries}\n")
            texfile.write("\\end{figure}\n")
            texfile.write("{}\n".format(resultssca1))
            texfile.write("\\begin{figure}[htb]\n")
            texfile.write("\\centering\n")
            texfile.write("\\includegraphics[width=0.5\\textwidth]{../graphs/sca_decaydecrease.png}\n")
            texfile.write("\\caption{Reduction of countrate (green) in comparison to a pure decay. As the countrate is reduced faster, an additional reduction process needs to act.}\n")
            texfile.write("\\label{fig:sca_decaydecrease}\n")
            texfile.write("\\end{figure}\n")
            texfile.write("\\begin{figure}\n")
            texfile.write("\\centering\n")
            texfile.write("\\begin{subfigure}{\\textwidth}\n")
            texfile.write("\\centering\n")
            texfile.write("\\includegraphics[width=\\linewidth]{../graphs/sca_countdeltat.png}\n")
            texfile.write("\\end{subfigure}\n")
            texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
            texfile.write("\\centering\n")
            texfile.write("\\includegraphics[width=\\linewidth]{../graphs/sca_param_lin.png}\n")
            texfile.write("\\end{subfigure}%\n")
            texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
            texfile.write("\\centering\n")
            texfile.write("\\includegraphics[width=\\linewidth]{../graphs/sca_param_2.png}\n")
            texfile.write("\\end{subfigure}\n")
            texfile.write("\\caption{(a) Dependency between count rate maxima and the amplitudes of $\delta$T values. For better visibility only data for 2015 is shown here. (b) Data from the full timeseries is used to show the linear tendency between count rate maxima and $\delta$T values. (c) The relative time shift between count maxima and preceding maxima in $\delta$T in minutes. The average delay corresponds to $d$=2700 minutes. All values are determined automatically by extrema analysis on the smoothed curves, obtained by a gaussian low-pass filter with cut-off frequency $6^.10^{-6} Hz$ to filter diurnal temperature patterns. Please note that only for this analysis filtered data sets are used. All previous and further analysis will be based on the original raw data.}.\n")
            texfile.write("\\label{fig:sca_param}\n")
            texfile.write("\\end{figure}\n")
            texfile.write("% ----------------------------------------------------------------\n")
            texfile.write("\\subsubsection{Diffusion model}\\label{sec:sca_transport}\n")
            texfile.write("% ----------------------------------------------------------------\n")
            #texfile.write("\\begin{figure}[htb]\n")
            #texfile.write("\\centering\n")
            #texfile.write("\\includegraphics[width=0.8\\textwidth]{../graphs/sca_model.png}\n")
            #texfile.write("\\caption{Model of slowly acting atmospheric transport processes in the seismological tunnel (gray). The sketch illustrated the tunnel between 50 and 145m, including the three doors. The red box marks the position of the SCA instrument. The blue arrow indicates diffusion (diffusion of radon through the tunnel towards the outside of the observatory). Red arrows indicate a temperature dependent blocking process which reduces Rn diffusion). The temperature dependence come from differences between tunnel temperaure (T$_i$) and external temperature (T$_o$).}\n")
            #texfile.write("\\label{fig:sca_model}\n")
            #texfile.write("\\end{figure}\n")
            texfile.write("{}\n".format(resultssca2))
            texfile.write("{}\n".format(resultssca3))
            texfile.write("\\begin{figure}[htb]\n")
            texfile.write("\\centering\n")
            texfile.write("\\includegraphics[width=\\textwidth]{../graphs/sca_timeseriesmodel.png}\n")
            texfile.write("\\caption{Timeseries of measured counts and modelled counts within the seismological tunnel. Also show is $\delta$T, which is the basis of the modelled counts.}\n")
            texfile.write("\\label{fig:sca_timeseriesmodel}\n")
            texfile.write("\\end{figure}\n")
            texfile.write("% ----------------------------------------------------------------\n")
            texfile.write("\\section{Discussion}\\label{disc}\n")
            texfile.write("% ----------------------------------------------------------------\n")
            #texfile.write("\\subsection{SCA gamma variation explained by a simple Rn transport model}\\label{sec:sca_transportmodel}\n")
            texfile.write("{}\n".format(discsca1))
            texfile.write("{}\n".format(discsca2))
            texfile.write("{}\n".format(discsca3))
            texfile.write("\\begin{figure}[htb]\n")
            texfile.write("\\centering\n")
            texfile.write("\\includegraphics[width=0.8\\textwidth]{../graphs/sca_zoom.png}\n")
            texfile.write("\\caption{Comparison of observed and modeled counts for a small time window.}\n")
            texfile.write("\\label{fig:sca_zoom}\n")
            texfile.write("\\end{figure}\n")
            texfile.write("{}\n".format(discsca4))
            # Eventually add a power spectrum of fig:peakshift or temperature analysis
            texfile.write("% ----------------------------------------------------------------\n")
            texfile.write("\\section{Conclusion}\\label{conc}\n")
            texfile.write("%-----------------------------------------------------------------\n")
            texfile.write("{}\n".format(conc))
            texfile.write("%GATHER{radon.bib}\n")
            texfile.write("\\bibliography{radon}{}\n")
            texfile.write("%-----------------------------------------------------------------\n")
            texfile.write("\\end{document}\n")

        print ("")
        print ("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print ("Don't forget:")
        print (" a) copy deluxtable.sty to TeX directory")
        print (" b) copy geomag.bib to TeX directory")
        print (" c) copy jgrstyle.bst to TeX directory")
        print (" e) edit README file in IAF directory")
        print (" f) copy pngs tp graphs directory")


    """
    Exposimeter table

    Detektor	Firma	Ort	Expositionsstart	Expositionsende	Exposimetertyp	Tage	Konzentration (Bq/cubicm)	Messunsicherheit (Bq/cubicm)	EG (Bq/cubicm)	NWG (Bq/cubicm)
    1441	KIT	SGO Stollen	16.07.13	21.11.13	LZ	128	ueberexponiert			
    1442	KIT	GMO AS-O-15m	17.07.13	21.11.13	LZ	127	1071	79	4	9
    1443	KIT	ZAMG Tiefkeller	30.07.13	22.11.13	LZ	115	92	9	3	7
    1444	KIT	GMO VS-O-150m	17.07.13	21.11.13	LZ	127	1579	116	5	10
    1429	KIT	SGO BL 45m	16.07.13	25.07.13	KZ	9	66	30	33	76
    1430	KIT	SGO Stollen	16.07.13	25.07.13	KZ	9	10866	809	45	103
    1431	KIT	ZAMG Tiefkeller	30.07.13	06.08.13	KZ	7	235	50	44	100
    1432	KIT	SGO BL 5m	16.07.13	25.07.13	KZ	9	1551	139	35	80
    428890	Radon Analytics	SGO Stollen	16.07.13	21.11.13	LZ		4400			
    573139	Radon Analytics	SGO BL 45m	16.07.13	21.11.13	LZ		<15			
    580506	Radon Analytics	SGO BL 5m	16.07.13	21.11.13	LZ		2900			
    502059	Radon Analytics	ZAMG Tiefkeller	30.07.13	22.11.13	LZ		70			
    """


