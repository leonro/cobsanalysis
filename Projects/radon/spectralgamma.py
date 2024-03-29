from radonanalysismethods import *
import os
import glob

"""
Next steps:
1.) Get environment data and radon sca data and add to filtered timeseries (continuously)
2.) Send results to Gidi and Susana
3.) Close the valve
4.) After 4 weeks - check backgroundlevel
5.) eventually empty radon canister
6.) remove calibration standards
7.) after two weeks open valve again

inbetween rewrite job on windows so that manual downliad is not necessary any more

"""


# INSIDE: SHT75_RASHT001, BMP085_10085001
# OUTSIDE: SHT75_RASHT002, BMP085_10085002

## TEST AREA:
# Get environment data from Box

#Preamble
liste1 = []
liste2 = []
liste3 = []
liste4 = []
liste5 = []
liste6 = []
listemca = []
listesca = []
listeshift = []
timelist = []
namelist = []
savedarrays = []
result = {}

graphdir = "/srv/projects/radon/Report/graphs"
texoutput = True
bmppath_box = '/srv/projects/radon/tables/env-box-inside-bmp-1min*'
shtpath_box = '/srv/projects/radon/tables/env-box-inside-sht*'

spectraldatapath = "/srv/projects/radon/confinedExp/spectral/AllSpectralData"

startnum = 401179
backgrdend=401507
accumulationend=401929


newest = max(glob.iglob(os.path.join(spectraldatapath,'*.Chn')), key=os.path.basename)

try:
    endlongterm = int(os.path.basename(newest).replace("Spectral_","").replace(".Chn",""))
except:
    print ("Could not identify latest file") 
    endlongterm = 416000 # Get this number directly from filelist

# Further steps
valveclosed = 413877
csremoved = 413700
baremoved = 413800
valveopened = 413877

filerange = range(startnum,endlongterm)
#filerange = range(401279,402371)
#filerange = range(401279,401289)
#filerange = range(407000,412617)
plotselection = [401279,402370,407000,410000]


# Halbwertszeiten:
#Ba-133: 10.51 a
#Cs-137: 30.17 a


"""
# #####################################################################################
# #################             Useful analysis methods             ###################
# #####################################################################################
"""

def singlespecanalysis(data,roi=[],plot=False,name='example',background=None,energycalib=True,plotname='Spectra'):
    """
    Takes data of a single spectrum and calculates compton background, corrected curve.
    Identifies maxima in +-10 channels of given roi and defines rois.
    If energy levels are provided, a enenrgy calibration curve is determined.
    Returns a dictionary containing Roi: [Center, width, peakcount, roicount], ResiudalCompt:[], CalibrationFit:, 

    check interpolation for Spectral_401188.Chn

    """ 

    if not roi:
        print ("please provide a list of roi s like [63, 114, 231, 291, 363, [197,398]]")

    searchrange = 10
    channellist = []
    xs, ys = 0., 0.

    result[name] = data

    if not background:
        interp, maxx, x,y = comptoncorr(data)
        datacorr = data[0:maxx]-interp
    else:
        # background is a list with two arrays: [mean,stddev]
        #print ("Using backgroud subtraction")
        #print (len(data), len(background[0]))
        datacorr = data-background[0]
        maxx = len(datacorr)
    for elem in roi:
        if isinstance(elem, int):
            peak = max(datacorr[elem-searchrange:elem+searchrange+1])
            # Fit peak
            xw,yw = getdatawin(datacorr, peak)
            # store xw and yw for rectangle in datacorr plot
            if elem == 291:
                xs = xw
                ys = yw
            max_x, max_y, I, Iuncert, wi = fitpeak(xw,yw,n=4,plot=plot)
            width = 5 # the practical measure of resolution is the width of the photopeak at  half  its  amplitude  known  as  the  Full  Width  at  Half  Ma
            count = sum(datacorr[elem-width:elem+width])
            #result[str(elem)] = [list(datacorr).index(peak), width, peak, count, 0]
            result[str(elem)] = [max_x, wi, max_y, I, Iuncert, datacorr[elem], 293-max_x]
            channellist.append(max_x)
        else:
            try:
                if len(elem) == 2:
                    peak = max(datacorr[elem[0]:elem[1]])
                    width = int((elem[1]-elem[0])/2.)
                    count = sum(datacorr[elem[0]:elem[1]])
                    result[str(elem[0])+'-'+str(elem[1])] = [list(datacorr).index(peak), width, peak, count, 0, datacorr[elem[0]],0]
            except:
                print ("Failure")

    #print ("result", result)
    if energycalib:
        #energylist = [356, 667, 1460, 1764, 2600]
        #energycalibration(range(0,maxx), ch=channellist, e=energylist, n=1, use=2, plot=plot)
        data_new, coefs = energycalibration(range(0,1025), data, ch=channellist, e=energylist, n=2, use=4, plot=plot,addzero=True, plotmax = maxx)
        newtime = mdates.date2num(datetime.datetime.utcfromtimestamp(int(name)*3600.)) # - datetime.timedelta(days=1)
        result[newtime] = data_new
        result[str(newtime)+'_'+str(coefs)] = coefs

    if plot:
        # Plot spectra
        if not plotname:
            plotname = 'Spectra'
        fig = plt.figure(2)
        ax = fig.add_subplot(1,1,1)
        ax.set_yscale('log')
        #plt.title("Spectra comparison")
        plt.ylabel("counts per hour [1/h]")
        if not background:
            plt.xlabel("channel [number]")
            plt.plot(x,y,color='green',linewidth=0.6)
        else:
            plt.xlabel("energy [KeV]")
            ax.fill_between(range(0,len(data)), data, background[0], alpha=.1, color='brown')
            ax.fill_between(range(0,len(data)), background[0]+background[1], background[0]-background[1], alpha=.25, color='green')
            ax.plot(range(0,len(data)),background[0], '-', color='green')
        plt.plot(data[:maxx],color='orange',linewidth=0.6)
        if not background:
            plt.legend(('Interpolated/linear function','Spectrum'),loc=3)
        else:
            plt.legend(('Background determination','Spectrum'),loc=3)
        x1,x2,y1,y2 = plt.axis()
        # Add ROIs
        #patchlst = [item for item in roi]
        patchlst = [[result[str(item)][0]-result[str(item)][1],result[str(item)][1]] for item in roi if isinstance(item, int)]
        #colorlst = ['red','blue','yellow','green'] #,'brown']
        #isotoplst = ['Ba','Cs','Ka','Bi'] #,'Tl']
        for idx, p in enumerate([patches.Rectangle((pa[0], y1), 2*pa[1], y2-y1,facecolor=colorlst[i],alpha=0.1,) for i,pa in enumerate(patchlst)]):
            ax.add_patch(p)
            ax.text(patchlst[idx][0], y2-0.8*(y2-y1), '(${}$)'.format(isotopelst[idx]), horizontalalignment='left', color=colorlst[idx], verticalalignment='bottom')
        #plt.plot(x,y,np.arange(0,maxx), interp, color='green',linewidth=0.6)
        plt.grid()
        pylab.savefig(os.path.join(graphdir,'{}_{}.png'.format(plotname,name))) # Speichert den spectra-plot als png-Datei

        fig = plt.figure(3)
        ax = fig.add_subplot(1,1,1)
        plt.ylabel("counts per hour [1/h]")
        ax.set_yscale('log')
        difffunc = datacorr
        if not background:
            plt.xlabel("channel [number]")
            ax.set_xlim([30,500])
            #difffunc = data[:maxx]-interp
        else:
            plt.xlabel("energy [KeV]")
            #difffunc = datacorr
        plt.plot(difffunc, color='blue',linewidth=0.6) # Plottet die Differenz aus spec1 und der interpolierten Funktion
        x1,x2,y1,y2 = plt.axis()
        # Add ROIs
        #patchlst = [item for item in roi]
        patchlst = [[result[str(item)][0]-result[str(item)][1],result[str(item)][1]] for item in roi if isinstance(item, int)]
        for idx, p in enumerate([patches.Rectangle((pa[0], y1), 2*pa[1], y2-y1,facecolor=colorlst[i],alpha=0.1,) for i,pa in enumerate(patchlst)]):
            ax.add_patch(p)
            ax.text(patchlst[idx][0], y2-0.8*(y2-y1), '(${}$)'.format(isotopelst[idx]), horizontalalignment='left', color=colorlst[idx], verticalalignment='bottom')
        #plt.legend(('Difference'),loc=6)
        plt.grid()
        pylab.savefig(os.path.join(graphdir,'{}_corrected_{}.png'.format(plotname,name))) # Speichert den spectra-plot als pdf-Datei
        #plt.show()
        plt.close()

    #return result

def getAverage(result, filerange, plot=True):
    """
    calculates the mean of the provided filerange
    returns mean array [[mean],[sd]]
    """
    allarrays = []
    for i in filerange:
        newtime = mdates.date2num(datetime.datetime.utcfromtimestamp(int(i)*3600.))
        ar = result.get(newtime,[])
        if len(ar) > 0:
            allarrays.append(ar)

    me = np.mean(allarrays, axis=0)
    st = np.std(allarrays, axis=0)

    if plot:
        # Determine the following channel energy relation from each analysis
        fig, ax = plt.subplots(1, 1)
        ax.set_yscale('log')
        x = range(0,len(me),1)
        ax.set_xlim([0,3000])
        ax.fill_between(x, me+st, me-st, alpha=.25)
        ax.plot(x,me, '-')
        plt.xlabel("energy [KeV]")
        plt.ylabel("counts per hour [1/h]")
        plt.grid()
        #plt.show()

    return [me,st]


# Detect local minima
def comptoncorr(spectrum):
    minlist = []

    intervallist = [0,1,2,3,6,12,20,51,67,76,89,119,141,167,191,212,235,267,326,380,400,500] # Von  Hand eingebene Werte (channels) bzw. Punkte, von denne weg interpoliert werden soll

    for idx,el in enumerate(intervallist):
        if idx > 0:
            spectemp = spectrum[lastel:el]
            minspectemp = min(spectemp)
            #minindex = list(spectrum).index(minspectemp)
            minindex = lastel+list(spectemp).index(minspectemp)
            #print len(spectemp), minspectemp, minindex
            minlist.append([minindex, minspectemp])
        lastel = el
        #print el
    x = [elem[0] for elem in minlist]
    y = [elem[1] for elem in minlist]
    #print max(x)
    interfunc = interp1d(x, y, kind='linear')
    xnew = np.arange(0,max(x))
    ynew = interfunc(xnew)
    return interfunc(range(0,max(x))), max(x), x, y

    #diffunc = (spec1[0:max(x)] - interfunc(range(0,max(x))))

def fitpeak(x,y,n=4,plot=False):
    """
    Fitting a polynomial function to peaks
    """
    I, Iuncert = 0,0
    coefs, C_p = np.polyfit(x, y, n, cov=True)
    x_new = np.linspace(x[0], x[-1], num=len(x)*10)
    TT = np.vstack([x_new**(n-i) for i in range(n+1)]).T
    yi = np.dot(TT, coefs)  # matrix multiplication calculates the polynomial values
    C_yi = np.dot(TT, np.dot(C_p, TT.T)) # C_y = TT*C_z*TT.T
    sig_yi = np.sqrt(np.diag(C_yi))  # Standard deviations are sqrt of diagonal
    max_x = x_new[list(yi).index(max(yi))]
    max_y = max(yi)
    area = True
    if area:
        # get x range
        #halfrange = 3 # given in channels
        halfrange = 2 + int(max_x/50.)
        x_win = [el for el in x_new if el > max_x-halfrange and el <= max_x+halfrange]
        y_win = yi[list(x_new).index(x_win[0]):list(x_new).index(x_win[-1])+1]
        t_y = yi+sig_yi
        y_winmax = t_y[list(x_new).index(x_win[0]):list(x_new).index(x_win[-1])+1]
        t_y = yi-sig_yi
        y_winmin = t_y[list(x_new).index(x_win[0]):list(x_new).index(x_win[-1])+1]
        if not len(x_win) == len(y_win):
            print "------------------------------------------- Check it!!!!!!!!!!!!!!!!!!!"
        I = np.trapz(y_win, x_win)
        Imax = np.trapz(y_winmax, x_win)
        Imin = np.trapz(y_winmin, x_win)
        Iuncert = (Imax-Imin)/2.
    if plot:
        fg, ax = plt.subplots(1, 1)
        #ax.set_title("Fit for Polynomial (degree {}) with $\pm1\sigma$-interval".format(n))
        plt.xlabel("channel [number]")
        plt.ylabel("counts per hour [1/h]")
        ax.fill_between(x_win, 0, y_win, alpha=.25, facecolor='green')
        ax.fill_between(x_new, yi+sig_yi, yi-sig_yi, alpha=.25)
        #ax.text(x_win, ,'I',horizontalalignment='center',color='black',verticalalignment='bottom')
        ax.plot(x_new, yi,'-')
        ax.plot(x, y, 'ro')
        ax.axis('tight')
        fg.canvas.draw()
        if x_win[0] < 290 and 290 < x_win[-1]:
            pylab.savefig(os.path.join(graphdir,'fitpeak.png')) # Speichert den fitpeak als pdf-Datei
        #plt.show()
        plt.close()
    return max_x, max_y, I, Iuncert, halfrange


def getdatawin(data, peak, width=None):
    """
    Extract data within a specfic range
    """
    xmid = list(data).index(peak)
    if not width:
        w = 2 + int(xmid/25.)
    else:
        w = width
    y = data[xmid-w:xmid+w+1]
    x = range(xmid-w,xmid+w+1)
    return x,y


def energycalibration(x, count, ch=[], e=[], n=1,  use= 2, plot=False, addzero=False, plotmax=None):
    """
    Do a energy calibration uing the provided channel/energy relation
    Default uses a linear fit (n=1)
    Use defines which elemenmt of channellist should be used for calibration (2 = first two elements)
         All other data is shown in the plot
    # use
    # returns x column converted to energy
    """

    if not plotmax:
        plotmax = x[-1]
    # use zero value for fit as well
    if addzero:
        x = np.asarray(x)
        zero = [0]
        zero.extend(ch)
        ch = zero
        zero = [0]
        zero.extend(e)
        e = zero
        use = use+1

    coefs, C_p = np.polyfit(ch[:use], e[:use], n, cov=True)
    #x_new = np.linspace(0, 500, num=len(x)*10)
    TT = np.vstack([x**(n-i) for i in range(n+1)]).T
    yi = np.dot(TT, coefs)  # matrix multiplication calculates the polynomial values
    C_yi = np.dot(TT, np.dot(C_p, TT.T)) # C_y = TT*C_z*TT.T
    sig_yi = np.sqrt(np.diag(C_yi))  # Standard deviations are sqrt of diagonal

    # allow for linear calibration and more complex
    if plot:
        # Determine the following channel energy relation from each analysis
        fig, ax = plt.subplots(1, 1)
        #fig = plt.figure(4)
        ax.set_xlim([0,plotmax])
        ax.set_ylim([0,3000])
        ax.plot(x, yi, '-',color='black')
        plt.xlabel("channel [number]")
        plt.ylabel("energy [KeV]")
        ax.fill_between(x, yi+sig_yi, yi-sig_yi, alpha=.25)
        #plt.plot(yi+sig_yi, x, '-',color='blue')
        if addzero:
            e = e[1:]
            ch = ch[1:]
            use = use-1
        ax.plot(ch[:use],e[:use],'o')
        try:
            ax.plot(ch[use:],e[use:],'o',color='red')
        except:
            pass
        #isotopelst = ['^{133}Ba','^{137}Cs','^{40}Ka','^{214}Bi']
        for idx, xy in enumerate(zip(ch[:use], e[:use])):
            ax.annotate('(${}$)'.format(isotopelst[idx]), xy=xy, xytext=(5, -7), textcoords='offset points')
        plt.grid()
        pylab.savefig(os.path.join(graphdir,'calibration.png')) # Speichert den spectra-plot als pdf-Datei
        #plt.show()

    # Interpolate data (using cubic splines) and resample at new index values
    x_new = range(0,3000,1)
    data = count
    func = interp1d(yi, data, kind='linear')
    if plot:
        # Determine the following channel energy relation from each analysis
        fig, ax = plt.subplots(1, 1)
        ax.set_yscale('log')
        ax.set_xlim([0,3000])
        ax.plot(yi, data, '-')
        #ax.plot(x_new, func(x_new),'--', color='red')
        plt.xlabel("energy [KeV]")
        plt.ylabel("counts per hour [1/h]")
        plt.grid()
        #plt.show()

    return func(x_new), coefs


"""
# #####################################################################################
# #################             Confined experiment                 ###################
# #####################################################################################
"""

analyzemca = True
if analyzemca:
    # Filter environment data
    # #######################

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # IMPORTANT: check times !!!!
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


    path = spectraldatapath
    namedummy = "Spectral_"
    # Cycle through channels
    validfilerange = []


    for i in filerange:
        # Define file name
        name = (namedummy+"%06i.Chn") % (i)
        print ("Analyzing {}".format(name))
        filename = os.path.join(path, name)
        #test=True
        #if test:
        try:
            temp = open(filename, 'rb')
            line1 = temp.read(32)
            data1 = struct.unpack("<bb3H2L8c4c2H", line1)
            nchn = data1[-1]
            line2 = temp.read(nchn*4)
            data2 = struct.unpack("<%sL" % nchn, line2)
            # put zero values in front so that channel number correspond to indicies
            data = [0] * 7
            data = data + list(data2[6:])
            data2 = np.asarray(data)

            #roi = [63, 114, 231, 291, [197,398]]

            if i in plotselection:
                #result[str(i)] = singlespecanalysis(data2,roi=roi,plot=True,name=str(i))
                singlespecanalysis(data2,roi=roi,plot=True,name=str(i))
            else:
                #result[str(i)] = singlespecanalysis(data2,roi=roi,plot=False,name=str(i))
                singlespecanalysis(data2,roi=roi,plot=False,name=str(i))
            #print result[str(i)]
            liste1.append(result[str(roi[0])][3])
            liste2.append(result[str(roi[1])][3])
            liste3.append(result[str(roi[2])][3])
            liste4.append(result[str(roi[3])][3])
            #liste5.append(result[str(roi[4])][3])
            liste6.append(result['197-398'][3])
            listemca.append(result[str(roi[3])][2])
            listesca.append(result[str(roi[3])][5])
            listeshift.append(result[str(roi[3])][6])
            datestr = int(name.strip(namedummy).strip(".Chn")) * 3600        
            newtime = datetime.datetime.utcfromtimestamp(datestr)# - datetime.timedelta(days=1)
            if newtime >= datetime.datetime(2015,10,21,11) and newtime < datetime.datetime(2015,10,21,12):
                backgrdend = i
            if newtime >= datetime.datetime(2015,11,8,1) and newtime < datetime.datetime(2015,11,8,2):
                accumulationend = i
            timelist.append(newtime)
            namelist.append(name)
            validfilerange.append(i)
            #if i in plotselection:
            #    dic = result.get(str(i))
            #    print (name, len(dic[str(i)]))
            #    savedarrays.append(dic[str(i)])
        except:
            print "----------------------------"
            print (filename)
    max1 = max(liste1)
    max2 = max(liste2)
    max3 = max(liste3)
    max4 = max(liste4)
    #max5 = max(liste5)
    max6 = max(liste6)
    liste1 = np.asarray(liste1)  # Ba
    liste2 = np.asarray(liste2)  # Cs
    liste3 = np.asarray(liste3)  # Bi1120
    liste4 = np.asarray(liste4)  # Bi1764
    #liste5 = np.asarray(liste5)
    liste6 = np.asarray(liste6)  #  all

    savedarrays = []
    for i in plotselection:
        #newtime = mdates.date2num(datetime.datetime.utcfromtimestamp(int(i)*3600.))
        ar = result.get(str(i),[])
        if len(ar) > 0:
            savedarrays.append(ar)

    print ("Background experiments ends at filenumber {}".format(backgrdend))
    print ("Accumulation experiments ends at filenumber {}".format(accumulationend))


    """
    Evolution
    """

    validdates = [datetime.datetime.utcfromtimestamp(int(elem)*3600) for elem in validfilerange]

    print ("----------------------------------------------")
    print ("VALID", validdates)
    print ("----------------------------------------------")

    # Plot single elements
    fig = plt.figure(1)
    ax = fig.add_subplot(1,1,1)
    #plt.title("Single standardised elements")
    plt.xlabel("date")
    #plt.ylabel(r'$\Sigma$'"element""/""max"r'$\Sigma$'"element"" ""[1]")
    plt.ylabel("I / max(I)")
    """
    Add the radioactive decay lines from Cs and Ba to the plot A = A0 e^( -(.693 t)/T1_2) 
    """
    x1,x2,y1,y2 = plt.axis()
    # Background
    start = mdates.date2num(validdates[0])
    end = mdates.date2num(datetime.datetime(2015,10,21,11))
    width = end - start
    rect = patches.Rectangle((start, 0), width, 1, facecolor='lightgray', alpha=0.4)
    ax.add_patch(rect)   
    ax.text(start+width/2, y2-0.65*(y2-y1),'(BG)',horizontalalignment='center',color='black',verticalalignment='bottom')
    # Radon accumulation
    start = mdates.date2num(datetime.datetime(2015,10,21,11))
    end = mdates.date2num(datetime.datetime(2015,11,8,0))
    width = end - start
    rect = patches.Rectangle((start, 0), width, 1, facecolor='darkgray', alpha=0.4)
    ax.add_patch(rect)   
    ax.text(start+width/2, y2-0.75*(y2-y1),'(IN)',horizontalalignment='center',color='black',verticalalignment='bottom')
    # LongTerm experiment
    start = mdates.date2num(datetime.datetime(2015,11,8,1))
    end = mdates.date2num(validdates[-1])
    width = end - start
    rect = patches.Rectangle((start, 0), width, 1, facecolor='lightgray', alpha=0.4)
    ax.add_patch(rect)   
    ax.text(start+width/2, y2-0.65*(y2-y1),'(LT)',horizontalalignment='center',color='black',verticalalignment='bottom')

    plt.plot_date(validdates,liste4/float(max4),'-',color='orange',linewidth=0.6)
    plt.plot_date(validdates,liste1/float(max1),'-',color='red',linewidth=0.6)
    plt.plot_date(validdates,liste2/float(max2),'-',color='blue',linewidth=0.6)
    #plt.plot(validfilerange,liste3/float(max3),color='green',linewidth=0.6)
    #plt.plot(validfilerange,liste5/float(max5),color='brown',linewidth=0.6)
    #plt.plot(validfilerange,liste6/float(max6),color='black',linewidth=0.6)
    #plt.legend(('Barium', 'Cesium', 'Bismut', 'Potassium', 'Thallium'),loc=4)

    ## Add decay rate of Ba-133 t plot
    daydiff = int((filerange[-1]-accumulationend)/24.)
    t = np.asarray(range(0,daydiff,1))
    datelst = [datetime.datetime(2015,11,8) + datetime.timedelta(days=num) for num in t]
    thalf = int(365*10.51) ## Ba-133
    plt.plot_date(datelst,decay(t,0.985,thalf), '--', color='darkred')

    plt.legend(('Bi-214 (1764 KeV)','Ba-133 (356 KeV)', 'Cs-137 (662 KeV)'),loc=4)
    plt.grid()
    pylab.savefig(os.path.join(graphdir,'timeseries-channels.png')) # Speichert den single-elements plot als pdf-Datei
    #plt.show()

    colorlist = ['green','blue','red','brown'] # used for marking the spectra

    fig = plt.figure(10)
    ax = fig.add_subplot(1,1,1)
    #plt.title("MCA versus SCA")
    plt.xlabel("date")
    #plt.ylabel(r'$\Sigma$'"element""/""max"r'$\Sigma$'"element"" ""[1]")
    plt.ylabel(r'counts')
    plt.plot_date(validdates,listesca,'-',color='blue',linewidth=0.6)
    plt.plot_date(validdates,listemca,'-',color='orange',linewidth=0.6)
    for idx, elem in enumerate(plotselection):
        if idx > 0:
            num = int(elem)*3600        
            time = datetime.datetime.utcfromtimestamp(num)# - datetime.timedelta(days=1)
            plt.axvline(time,color=colorlist[idx])
    plt.legend(('single channel', 'peak maximum'),loc=4)
    plt.grid()
    pylab.savefig(os.path.join(graphdir,'bichannel_vs_time.png')) # Speichert den single-elements plot als pdf-Datei
    #plt.show()


    fig = plt.figure(11)
    ax = fig.add_subplot(1,1,1)
    ax.set_yscale('log')
    plt.xlabel("channel [number]")
    plt.ylabel("counts per hour [1/h]")
    legendlist = []
    for idx,elem in enumerate(savedarrays):
        if idx > 0:
            plt.plot(elem[:500],color=colorlist[idx],linewidth=0.6)
            num = int(plotselection[idx])*3600        
            newtime = datetime.datetime.utcfromtimestamp(num)# - datetime.timedelta(days=1)
            legendlist.append(str(newtime))
    plt.legend(legendlist,loc=1)
    plt.grid()
    pylab.savefig(os.path.join(graphdir,'spectra.png')) # Speichert den single-elements plot als pdf-Datei
    #plt.show()


    """
    Relatvie shifts towards a fixed channel
    """
    print ("Reading Environment data from SHT inside the box...")
    sht = read(shtpath_box)
    #print sht.length()
    #mp.plot(sht)

    fig = plt.figure(12)
    ax = fig.add_subplot(1,1,1)
    #plt.title("MCA versus SCA")
    plt.xlabel("date")
    #plt.ylabel(r'$\Sigma$'"element""/""max"r'$\Sigma$'"element"" ""[1]")
    ax.set_ylabel(r'channel 291 - Bi peak', color='darkblue')
    ax.plot_date(validdates,listeshift,'-',color='darkblue',linewidth=0.6)
    ## Add temperature from sht sensor inside the box
    ax2 = ax.twinx()
    shttime = sht.ndarray[0]-1/24.
    shttemp = sht.ndarray[5]
    #plt.plot_date(shttime,shttemp,'-',color='red',linewidth=0.6)
    ax2.plot_date(shttime,shttemp,'-',color='red',linewidth=0.6)
    ax2.set_ylabel('T [deg C]', color='r')
    plt.grid()
    pylab.savefig(os.path.join(graphdir,'Bi_shift.png')) # Speichert den single-elements plot als pdf-Datei
    #plt.show()

    """
    # zoomed plot....
    fig = plt.figure(12)
    ax = fig.add_subplot(1,1,1)
    #plt.title("MCA versus SCA")
    plt.xlabel("date")
    #plt.ylabel(r'$\Sigma$'"element""/""max"r'$\Sigma$'"element"" ""[1]")
    ax.set_ylabel(r'channel 291 - Bi peak', color='darkblue')
    #ax.plot_date(validdates,listeshift,'-',color='brown',linewidth=0.6)

    ax2 = ax.twinx()
    shttime = sht.ndarray[0]
    shttemp = sht.ndarray[1]
    #plt.plot_date(shttime,shttemp,'-',color='red',linewidth=0.6)
    ax2.plot_date(shttime,shttemp,'-',color='red',linewidth=0.6)
    ax2.set_ylabel('T', color='r')
    #ax2.tick_params('y', colors='b')
    plt.grid()
    #pylab.savefig(os.path.join(graphdir,'Bi_shift.png')) # Speichert den single-elements plot als pdf-Datei
    #plt.show()
    """


    """
    Background analysis
    """

    # Background determintaion
    mean = getAverage(result, range(401279,401507),plot=False)

    """
    Reanalyse all spectra with background subtraction
    """
    bi1120, bi1764, bi609 = [],[],[]
    pb295, pb352 = [],[]
    # Analyse all spectra with new data - redefine isotopelst, energylist, colorlist

    energylist = e_energylist
    isotopelst = e_isotopelst
    colorlst = e_colorlst
    roi = e_roi

    validdates = []
    # eventually add all other gamma emitting decay products of Rn222 (Pb-214, )
    for i in range(accumulationend, filerange[-1]):
        newtime = mdates.date2num(datetime.datetime.utcfromtimestamp(int(i)*3600.))
        energydata = result.get(newtime,[])
        if len(energydata) > 0:
            validdates.append(newtime)
            print ("Energy analysis - dealing with {}".format(i))
            if i in plotselection:
                # This would rewrite tzhe existing plots....
                singlespecanalysis(energydata,roi=roi,plot=True,name=str(i),background=mean, energycalib=False,plotname='Energy')
            else:
                singlespecanalysis(energydata,roi=roi,plot=False,name=str(i),background=mean, energycalib=False)

            bi1120.append(result[str(roi[0])][3])
            bi1764.append(result[str(roi[1])][3])
            bi609.append(result[str(roi[2])][3])
            pb295.append(result[str(roi[3])][3])
            pb352.append(result[str(roi[4])][3])
        # please note: result dictionary is now overwritten....

    # Get maxima and convert to array
    bi1120 = np.asarray(bi1120)
    maxbi1120 = np.max(bi1120)
    bi1764 = np.asarray(bi1764)
    maxbi1764 = np.max(bi1764)
    bi609 = np.asarray(bi609)
    maxbi609 = np.max(bi609)
    pb295 = np.asarray(pb295)
    maxpb295 = np.max(pb295)
    pb352 = np.asarray(pb352)
    maxpb352 = np.max(pb352)


    # Plot single elements
    fig = plt.figure(1)
    ax = fig.add_subplot(1,1,1)
    #plt.title("Single standardised elements")
    plt.xlabel("date")
    #plt.ylabel(r'$\Sigma$'"element""/""max"r'$\Sigma$'"element"" ""[1]")
    plt.ylabel("I / max(I)")
    plt.plot_date(validdates,bi609/maxbi609,'-',color='darkred',linewidth=0.6)
    plt.plot_date(validdates,bi1120/maxbi1120,'-',color='orange',linewidth=0.6)
    plt.plot_date(validdates,bi1764/maxbi1764,'-',color='red',linewidth=0.6)
    plt.plot_date(validdates,pb295/maxpb295,'-',color='green',linewidth=0.6)
    plt.plot_date(validdates,pb352/maxpb352,'-',color='darkgreen',linewidth=0.6)
    plt.legend(('Bi-214 (609 KeV)','Bi-214 (1120 KeV)','Bi-214 (1764 KeV)','Pb-214 (295 KeV)', 'Pb-214 (352 KeV)'),loc=3)
    plt.grid()
    pylab.savefig(os.path.join(graphdir,'timeseries-energies.png')) # Speichert den single-elements plot als pdf-Datei
    #plt.show()


    # Get creation and UTC time

    #print len(namelist), len(liste5)
    # Save creation and UTC time in csv file
    f = open('channel_data.csv', 'w')
    f.write("%s\n" % (" # MagPy ASCII"))
    f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % ("Time", '"File"', "CountsBa[]", "CountsCs[]", "CountsBi1120[]", "CountsBi1764[]", "CountsAll[]", "CountsMCA(Bi1764)[]", "CountsSCA(Ch291)[]", "ChannelShift(Max_near_291)[]"))
    for k in range(0,len(namelist)):
        f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (timelist[k], namelist[k], liste1[k], liste2[k], liste3[k], liste4[k], liste6[k], listemca[k], listesca[k], listeshift[k]))
    f.close()

    f = open('energy_data.csv', 'w')
    f.write("%s\n" % (" # MagPy ASCII"))
    f.write("%s,%s,%s,%s,%s,%s,%s\n" % ("Time", '"File"', "Bi-214_609[KeV]", "Bi-214_1120[KeV]", "Bi-214_1764[KeV]", "Pb-214_295[KeV]", "Pb-214_352[KeV]"))
    for k in range(0,len(validdates)):
        f.write("%s,%s,%s,%s,%s,%s,%s\n" % (timelist[k], namelist[k], bi609[k],bi1120[k],bi1764[k],pb295[k],pb352[k]))
    f.close()
else:
    pass

"""
# #####################################################################################
# #################               SCA experiment                    ###################
# #####################################################################################
"""

analyzesca = False
if analyzesca:
    pass
#import scamethods

"""
scapath = '/srv/projects/radon/tables/sca-tunnel-1min*'
stream = read(scapath,starttime='2012-11-22')
stream.header['unit-col-x'] = ''
mp.plot(stream,variables=['x','t1'],outfile=os.path.join(graphdir,'sca_timeseries.png'))

max_modes=20
#col = stream._get_column(column)
#unit = stream.header['unit-col-'+column]
#timecol = stream._get_column('time')
#res = emd.emd(stream.ndarray[1],max_modes=max_modes)
mp.plotEMD(stream,'x',verbose=False,max_modes=max_modes,sratio=0.4,outfile=os.path.join(graphdir,'emd_timeseries.png'),stackvals=[1,6,14])
#sys.exit()


# Plot SCA timeseries
starttime = '2012-11-22'
starttime = '2015-01-01'
endtime = '2016-01-01'

stream = read(scapath,starttime=starttime, endtime=endtime)
stream.header['unit-col-x'] = ''
metpath = '/srv/prodcts/data/meteo/meteo*'
metstream = read(metpath,starttime=starttime, endtime=endtime)
metstream.header['col-f'] = 'T'
metstream.header['unit-col-f'] = 'deg C'
metstream.header['col-var5'] = 'P'
metstream.header['unit-col-var5'] = 'hPa'
metstream.header['col-z'] = 'snow cover'
metstream.header['unit-col-z'] = 'cm'
#res = metstream.steadyrise('dx', datetime.timedelta(minutes=60),sensitivitylevel=0.002)
#metstream = metstream._put_column(res, 't2', columnname='rain',columnunit='mm/1h')
fllist1 = metstream.flag_range(keys=['f'],below=-20)
fllist2 = metstream.flag_range(keys=['var5'],below=800)
fllist3 = metstream.flag_range(keys=['z'],above=100)
metstream = metstream.flag(fllist1)
metstream = metstream.flag(fllist2)
metstream = metstream.flag(fllist3)
metstream = metstream.remove_flagged()
metstream = metstream.flag_outlier(keys=['f','t1','var5','z'],timerange=datetime.timedelta(days=5))
metstream = metstream.remove_flagged()

newstream = mergeStreams(stream,metstream, keys=['f','z','var5'])
diff = newstream.ndarray[4] - newstream.ndarray[5] + 4.
newstream = newstream._put_column(diff,'y')
#newstream = newstream.smooth(['y'],window_length=1440)
#diff = newstream._get_column('y')


minlst = np.asarray(range(0,len(diff),1))
dtlist = diff
# ###########################
# Here I can add a time delay
# ###########################
#delaylst = [0.0]*1000
#delaylst.extend(list(dtlist))
#dtlist = delaylst[:len(minlst)]
#print dtlist
Ilst = []
lastI = 15000
for t in minlst:
    # delay defines how fast equlibrium state is obtained for certain mx
    delay = 12880.0
    delay = 1440.0
    dT = dtlist[t]
    if t == 0:
        Ist = 15000
    else:
        Ist = Ilst[t-1]
        
    I = accumulation(t, Ist, dT, delay=delay)
    if not np.isnan(I):
        lastI = I
    else:
        I = lastI
    Ilst.append(I)

newstream = newstream._put_column(np.asarray(Ilst),'var2')
#mp.plot(newstream,variables=['x','var2','y','f','t1','z','var5'],fill=['y','z'])
newstream.header['col-var2'] = 'model'
newstream.header['col-y'] = 'T_{diff}'
mp.plot(newstream,variables=['x','var2','y'],fill=['y'],outfile=os.path.join(graphdir,'sca_transportmodel.png'))
"""


"""
# #####################################################################################
# #################                      Report                     ###################
# #####################################################################################
"""

abstracttext = 'My Abstract'
keywords = 'Gamma-Spectroscopy, Radon, Radiometry'

introductiontext = 'Temporal variation patterns of radon gas in the geological environment have been observed and investigated since the second half of the last century. In particular a possible mechanical origin in its variations and its relation to fault systems and other deformation processes were of paramount interest. Not least among this interest was the potential of recognizing precursory signals of earthquakes although such hypotheses are heavily debated in the scientific community (e.g. Hartmann and Levy, 2006). During the last decade numerous publications were devoted to clarify the possible reasons of observed variation patterns and their potential geodynamic consequence. So far, however, a unique theory and physically sound explanation of the observed variation patterns remains elusive. \\\\ Radon ($^{222}$Rn) is a radioactive inert gas formed by disintegration from $^{226}$Ra as part of the $^{238}$U decay series. It disintegrates itself in several steps to stable $^{206}$Pb (Figure \\ref{fig:decayproducts}). $^{222}$Rn occurs at varying concentrations in geological environments. The combination of its noble gas character and its radioactive decay make it a unique ultra-trace component for tracking temporally varying natural processes. Using nuclear techniques, the measurement sensitivity for $^{222}$Rn is extremely high and can be performed with a time resolution in the order of 1 hour or less, even at very low radiation backgrounds. For investigating variations of $^{222}$Rn basically two measurement techniques are used: (1) direct measurement of its alpha radiation or (2) indirect measurement of gamma radiation from direct decay products $^{214}$Bi and $^{214}$Pb, which form within T$_{1/2}\\approx{}$30min. \\\\ Large temporal variations of radon ($^{222}$Rn) are often encountered in air in the geologic environment, at time scales from diurnal to annual \\citep{mentes:15}. Interpretations of the nature of these variations often invoke either above surface atmospheric variations, or the influence of subtle active geodynamic processes. Environmental influences, particularly atmospheric pressure and temperature, have been proposed for the origin of periodic signals observed in $^{222}$Rn time series (Shapiro et al., 1985; Ball et al, 1991; Pinault and Baubron, 1997; Finkelstein et al, 2006). However, other studies indicate that a consistent meteorological influence cannot be identified and suggest other influences. Among those are deformations related to hydrological processes, like loading and unloading of local water reservoirs (Trique et al., 1999). Gravitational tides have also been suggested as a dominating factor on $^{222}$Rn variability (Aumento 2002; Groves-Kirby et al 2006; Crockett et al 2006, Weinlich et al. 2006). An experimental simulation of radon signals in confined volumes of air recently added to the ongoing discussion (Steinitz et al., 2011). Here prominent periodic signals are observed which are interpreted towards forcing by a component of solar tide (Sturrock et al., 2012). So far, however, the eventual geophysical drivers of the variation of $^{222}$Rn as well as its specific qualities enabling these temporal variations remain unknown. \\\\ Understanding the radon system could open a wide field of geophysical applications. The application of stress to rocks is thought to enhance the exhalation of $^{222}$Rn from the solid mineral phase, rendering $^{222}$Rn a potential sensitive tracer of geodynamic processes in the upper crust. Transport of $^{222}$Rn in soil and water has been investigated as a tool for monitoring volcanic activity (e.g. Cigolini et al., 2001; Cigolini et al., 2009; Burton et al, 2004; Alparone et al, 2005; Imme et al, 2006). The proposition that $^{222}$Rn may serve as a useful proxy for seismic activity has been repeatedly raised (e.g. Monnin and Seidel, 1992; Segovia et al., 1995; Toutain and Baubron, 1999; Hartmann and Levi, 2006 and references therein). \\\\ In summary, many different environmental, geodynamical and physical processes can influence the radon variability. Despite the efforts of the scientific community, the nature of the physical processes driving the temporal patterns observed in $^{222}$Rn time series remains elusive and interpretation of the observed phenomena on a physical basis is not straightforward. \\\\ The main reason for the ambiguous interpretations of such processes and their effects on radon concentration is mainly related to the following reasons: (1) Variability patters are observed on many different time scales and are connected to a combination of different environmental and physical control factors with varying impact dependent on the observation location and set up . (2) The variation pattern is non-linear and non-normal distributed. (3) The pattern is non-stationary, as the relative effect of each control mechanism can change with time. Thus a simple decomposition of the radon concentration variation and a source analysis of each component is not easily possible. \\\\ Addressing the task of understanding the above- and sub-surface geophysical processes driving the radon signals and the verification of the different hypotheses requires long-term observation yielding high quality data. The Conrad Observatory provides the unique opportunity to monitor all physical surface and subsurface variations which eventually might influence radon variability at observatory time scales. The underground installation of the Conrad Observatory ascertains constant ambient temperature, and other environmental conditions like pressure variations and humidity are continuously monitored. Besides, the instrumentation at the Conrad Observatory provides a complete picture of meteorological parameters, reservoir characteristics, ambient hydrological conditions, gravitational tides and any related consequences, in particular ground deformations. Complementary, the seismological and geomagnetic facilities allow for contemporary monitoring of geodynamic activity. Well controlled conditions can be established in the laboratory, tunnel and boreholes. \\\\ Here we present two long term experiments on gamma variation performed at the Conrad Observatory in Austria. The first experiment comprises a long-term single channel analyzer (SCA) monitoring of the natural gamma variation within the seismological tunnel of the Conrad Observatory. Relations to Rn concentrations are established by SSNTD exposimeter measurements near the SCA. In the following we will present this timeseries and discuss the observed variability. (Some words on observations and results). A second experiment based on multi-channel analyzer (MCA) equipment is dedicated to detailed spectral measurements in an completely controlled environment to test sensitivity, significance and eventual biasing mechanisms of gamma measurements with the purpose of Rn monitoring. Here a lead shielded and sealed box containing a controllable radon source, calibration pads as well as many environmental sensors is used to demonstrate the importance of temperature control and careful analytical methods.' 


instrumenttext = 'Scintillation instruments (NaI) are used to determine gamma contributions of the disintegration products of Rn. Basically two different types are used here: A single-channel analyser (SCA) counts the number of pulses falling within an adjustable range. A multi-channel analyser (MCA) counts the number in each spectral window to give a spectral energy distribution. For the SCA measurements a gamma detector (3x3 inch, NaI, Scionix scintillation detector 76B76/3M-HV-E3-X2 with build amplifier and SCA)  is used to measure the variation of gamma radiation in the air of the seismic tunnel at the Conrad Observatory. Data acquisition is done by a Campbell 800 Data Logger. The primary resolution is 1 minute. The single channel analyzer measures the cumulative count rate between channels xxx and xxx, corresponding to an energy range from xxx KeV to xxx KeV. Many radiogenic isotopes are thus contributing to this counts rate, which is measured on a one minute basis. The only \\\\ To measure the alpha radiation from Rn directly, SSNTD measurements measurements were conducted. SSNTD probes short- and long-term exposimeter probes from two different companies (KIT and ...) have been obtained and used to measure Rn concentration directly at different locations. For MCA experiments, a gamma detector (3$x$3 inch, NaI, MCA, add type and company) is deployed. Description of MCA and recording software. Duration of the individual experiments and time steps.'

scatext1 = 'The SCA sensor is placed on a concrete block at 135 meters of the 145 m long tunnel. Towards the single entrance 3 usually closed doors prevent any significant air movement. As shown later the temperature is absolutely constant with variation below 0.04$^\circ$C throughout the year. The tunnel wall consists of 30 cm of reinforced concrete with additional water protection effectively shielding the tunnel interior.  ... Eventually add a small diagram showing tunnel and cross section. The single channel analyzer is running in a constant set up since November 2012 and determines cumulative counts on a minute basis. The energy range is adjusted so that one decay product of $^{222}$Rn ($^{214}$Bi) contributes to the minute counts. Furthermore, this isotope is the dominant source of count variation as it is directly related to mobile Rn. The SCA experiment is complemented by SSNTD short- and long-term exposimeter probes, which are set up close to the gamma sensor with 10 cm, directly on the concrete of the measurement pier. This data will finally be used for a gamma count/ Rn concentration relation. \\\\ Rn within the tunnel originates from the concrete lining of the floor and walls of the tunnel.'

try:
    startdate = str(timelist[0])
except:
    startdate = '2015-10-xx'
mcatext1 = 'The experiments outlined in this section are based on analyzing the spectral content of gamma radiation. For spectral analysis of the radioactive progeny we are using a 3$x$3 inch Scionix scintillation detector (76BQ76/3M-X) in combination with a ORTEC digiBase MCA Analyzer. Primary aim of this study is to monitor eventual temporal variations within data signals of individual elements. Such variation, which are often observed in timeseries of signal channel analysis, are often interpreted as being caused by variations of radon concentration and its gamma emitting decay products. In order to identify such variations and their eventual causes, we use an experimental set up where we can control and monitor many effects which could cause such variation, hereinafter denoted confined experiment. The confined experiment, which makes use of a sealed and lead shielded box including a radiogenic source, two different calibration probes and a complete monitoring of environmental parameters in- and outside is running since {a} at the Conrad Observatory. As radioactive decay occurs randomly in time, the number of events detected in a given time period is never exact. In order to obtain reasonable accuracy of such measurements it is therefore necessary to acquire data over long time periods. For our measurements we have chosen time periods of one hour, which provide reasonable accurate recordings for the elements in question.'.format(a=startdate)

mcatext2 = 'An example of a single gamma spectrum and the here performed analysis steps is outlined in Figure \\ref{fig:spectra}. In principle our analysis follows generally applied techniques \\cite[e.g.]{blum:97} although, because of the monitoring character and te primary interest in relative variations within photopeaks, we had to put some extra effort on a very robust automatic peak identification and energy calibration. Gamma counts are recorded in individual channels, which represent a certain energy level of gamma radiation. For this study we are particularly interested in isotopes, linked to the differently colored peaks in Figure \\ref{fig:spectra}a. $^{133}$Ba and $^{137}$Cs are used as calibration standards, as their well known energy levels provide an initial energy/channel calibration and allow us to identify regions-of-interest in which gamma peaks of e.g. $^{214}$Bi can be found. We are mainly interested in $^{214}$Bi, as this isotope is one decay product of $^{222}$Rn and is characterized by unique identification peaks. Usually it is assumed that channel energy relation is constant throughout a experiment. It  will be shown later that this is unfortunately not true for long term monitoring and therefore slight shifts within the energy/channel calibration have to be regarded for in analysis. As we are primarily interested in relative variations of $^{222}$Rn related isotopes like $^{214}$Bi over time, it is important to use reproducible and robust estimates of their gamma contribution with a meaningful uncertainty estimate. As we recorded spectra on a hourly rate for almost two years, such analysis also needs to be fully automatic. In principle we perform 4 analysis steps for our analysis: At first, based on a linear extrapolation of initial energy/channel calibration relation from Ba and Cs, we identify  the channels recording the gamma peacks of $^{214}$Bi ate 1120 KeV and 1764 KeV. Using these channels or channel windows as being fixed throughout all analysis would correspond to a SCA interpretation. As demonstrated later (Figure \\ref{fig:shifts}b) this is not true. \\\\ Therefore, in a second step, we assume that this channels are not fixed. It is necessary to identify the maxima within dynamic search ranges for each individual spectrum. These search ranches are defined by a window of 10 channels width. This search range is suitable to uniquely find all maxima. Then we identify nearby minima and by connecting these minima, determine a linear background function, which is subtracted from the curve (Figure \\ref{fig:spectra}b). Again maxima are dynamically searched and peaks are then fitted by a fourth order polynomial function (Figure \\ref{fig:spectra}b). Integration ranges to determine relative contributions are obtained by a window defined as \\\\ \\begin{equation}  w = 2 + p/25. \\end{equation} \\\\ with p as the peak position on the x-axis. This differs slightly from most commercial analysis routines which use the full peak area above the connecting minima. This cannot be done here because of overlapping photopeaks like $^{137}$Cs at 662 KeV and $^{214}$Bi at 609 KeV. Having now identified the exact channels we can now perform an energy calibration for each individual spectrum based on the second order polynomial calibration curve (Figure \\ref{fig:spectra}d). \\\\ For the third step we can now work with energy calibrated curve and do not have to worry about non-linear channel shifts any more. Now it is possible to average all background measurement at the beginning of the monitoring experiment and at the end of this experiment (too be done). This average background function is now subtracted from each individual raw spectrum from the LT phase (Figure \\ref{fig:energy}). Benefit of this subtraction is a removal of the Ba and Cs calibration peaks and thus a possibility to identify the masked photopeaks of $^{214}$Pb and 609KeV from $^{214}$Bi. Subtracting only the initial background function, however, would lead to a gradual overcorrection with time, as both $^{137}$Cs and $^{133}$Ba decay away with half-lives of 30.17a and 10.53a respectively. As the two peaks of $^{214}$Pb are recorded at low energy levels, which are not well resolved with the current experimental set up, these data will not be discussed any further. \\\\ Finally, in a fourth step we repeat the linear background subtraction, now based on a energy scale, as already described for step 2. For the peak identification window we also use equation 1. In summary, this automatic approach allows for an extremely robust analysis of the relative peak variation, which regards for eventual energy/channel shifts and provides reasonable error estimates (not discussed) of all steps. Too be done: The final background experiment is essential as a correct analysis can only be obtained when we close the valves again and perform a final background experiment. Between the two background experiments at the beginning and end we can then linearly interpolate in time for a reasonable consideration of Ba and Cs decay.'

resultssca1 = 'Figure \\ref{fig:sca_timeseries} provides an overview of all SCA measurements since beginning of the timeseries in November 2012. The most obvious feature in this almost 5 year long timeseries is the clearly visible yearly variation with maximum values in summer and minimum values in winter. The minimum values show a clear limit, never falling below $C_{BG} = \\approx$14000 counts/min, hereinafter referred to as the background level. Large short term variations are predominantly visible in summer reaching peak heights with more then double the amount of counts as the background level. The ambient sensor temperature is almost constant (Figure \\ref{fig:sca_timeseries}). Small temperature peaks, which are related to installation work within the tunnel section, never exceed 0.4 degrees. Overall the temperature of the sensor is extremely stable, 6.80 $\pm$ 0.05 degree on average. The average temperature of the tunnel (9$^{\\circ}$C) is 2.2$^{\\circ}$C larger (because of a gradient along the 145 m axis). As already noted above, gamma measurements are performed in a tunnel segment which is separated from the host rock by concrete, shielding gamma contribution from outside. Therefore, all detected gamma sources originate from the concrete itself. The variational part of observed gamma count timeseries needs to be related either to a non-equilibrium decay process or a radioactive source varying in concentration. Mobile Rn and its gamma emitting decay products is the obvious (and only) candidate for concentration variations. Considering the SCA energy window, observed gamma variations are solely related to the decay product $^{214}$Bi. Measurements of Rn directly using alpha probes in similar underground conditions show almost identical variation patterns \\citep{mentes:15}. The observed variations cannot be explained by simple non-equilibrium decay as some count rate reductions are much faster as a theoretical decay would allow for (Figure \\ref{fig:sca_decay}). Therefore, additional processes need to act in order to explain the data. The only possible transport route is along the tunnel and a concentration depended diffusion seems to be the most likely candidate. Diffusion happens from high concentration regions towards low concentration and is also affetced by atmospheric gradients. Pressure measurements do not indicate any differences between external (outside the observatory) and inside conditions. Temperature differences are, however significant. Plotting the temperature difference between tunnel and outside, already shows distinct similarities between the observed signals (Figure \\ref{fig:sca_countdeltat}). As can be seen from this plot, maxima in count rate are directly related to maxima in the temperature gradient between tunnel and outside ($\delta$T$_{SCA}$), as long as the temperature difference is positive. Furthermore, the maxima in $\delta$T$_{SCA}$ are preceding count rate maxima in time. As shown in Figure \\ref{fig:sca_param}, there is a quasi-linear relationship between the amplitudes of count maxima and there preceding maxima in $\delta$T$_{SCA}$, indicating that the effectiveness of diffusion between tunnel interior and outside is directly (and linearly) related to the outside temperature. Based on these observations it is possible to develop both a simple emperical model and a physical model, connecting count variations and $\delta$T$_{SCA}$.'


#discsca1 = 'Hypothesis: If $T_{ext} < T_{in}$ then transport between tunnel and exterior takes place, so that newly generated Rn within the tunnel is gradually removed. If the $T_{ext}$ is larger then the $T_{in}$ then this transport process is blocked. The blocking, thus the maximum equilibrium level is related to the temperature difference. If $T_{ext}$ drops below $T_{in}$ then the removal is initiated, which means that Rn concentration needs to reach an extremum and then reduce. The reduction rate should be a combination of decay and transport and should provide information on removal speed. The maximum value reached if $T_{ext} > T_{in}$ should eventually be linked to $T_{diff}$. Test: 1. Do gamma concentration start to increase when $T_{ext}$ passes $T_{in}$? 2. Does gamma concentration bend down if $T_{in}$ passes $T_{ext}$? If 1 and 2 are true then a) determine the delay ratio until background level is reached again. b) Determine a maximum level, temperature-diff at maximum relationship. If this observation is correct then: define two states (a) accumulation state and (b) background state. Consequences: 1.) Accumulation is dependent on T and therefore (with some phase shift) outside T leads to concentration variation inside. Therefore diurnal variation (sometimes referred to as solar tides) will show up. 2.) The concentration variation of Rn at Cobs provides only information on T diif and has no other geophysical meaning. Possible values: Phase shift 2-4 hours ??'


discsca1 = 'As shown above, a simple transport model, which is solely based on temperature gradients, is highly efficient to simulate gamma variations as observed in the seismological tunnel of the Conrad Observatory. Discussion 1: Validity of the model: The linear relationship between Cmax and deltaT is definitely a reasonable but rough approximation. First of all, because of the delay, maximum count values are not necessary reached following a sharp temperature peak. In order to account for this, we smoothed the temperature difference by a running average window of 4 days. As still more daily variations are present in the model as compared to data (Figure \\ref{fig:sca_zoom}), this smoothing is not absolutely sufficient for periods of high variability. In particular for low count rates, larger deviation about the assumed linear trend are observed. Another rough assumption is the constant transport rate R, which in reality cannot be constant, because it will depend on accessibility, and thus on many conditions within the transport path (doors, heaters, ventilation, human presence, air flow, even cooling fans of computers) which in our experimental setup cannot be exactly traced and included into the process. Therefore, the simple constant assumption is a reasonable guess describing the overall process but does not allow to specifically track and correct short term transportation variations. This also renders/limits residual analysis to identify signals not related to atmospheric transport. 2. Other observations and explanations based on the model: Similar variation have been found in many other localities \\cite{mentes:15} , others .... These variations are observed both for gamma counts and direct alpha concentration analysis. Mostly, variation data are analyzed by some sort of timeseries analysis for identification of dominant periodicities and their underlying relationship to tides and other geophysical processes. Very often, a diurnal signal is detected hereby, although its relationship to temperature variations is often questioned due to the fact, that the sensor does not see any temperature variation, measurement system and radiation should not depend on such variations at all (which cannot be confirmed here in this study, see section \\ref{disc:shifts}), such signals are not continuously present and finally there is no direct relation between temperature signals and count variation. As shown above, a slowly acting atmospheric transport process explains our results, which because of its slowliness does not require any measurable temperature signal close to the sensor, which ideally is running in constant conditions. During summer, with large temperature variations and large gradients, diurnal signals get superimposed onto the virtual count rate signal, although this signals are smoothed and, particularly shifted in time. In winter, however, or at times of weag gradients, such diurnal signals are not transferred. Thus we are confident, that the observation of varying diurnal pattern (solar tide) are actually triggered by temperature related atmospheric transport. 3. Are there any significant signals within the residual'


resultsmca2 = 'Figure \\ref{fig:mca_timeseries} depicts temporal results from the timeseries of the confined MCA experiment. The full timeseries can be split in four phases which are summarized if Figure \\ref{fig:mca_timeseries}a: background measurement (BG), gradual increase due to radon enhancement (IN), temperature influence test (at 2015-10-21, not highlighted), artifical radon source enhanced record (LT). Shown in this subfigure are results of integration areas (I) of analysis step 2 (Figure \\ref{fig:spectra}c) normalized to the maximum value for calibration elements Ba and Cs, as well as Bi at 1764 KeV. The count rates of Ba and Cs are solely related to two calibration pads within the experimental setup and are subject to radioactive decay according to \\begin{equation} I = I0 e^{-ln2 t / T_{1/2}} \\end{equation}. For Ba with its short half life of 10.51a this trend is highlighted in Figure \\ref{fig:mca_timeseries}a by a dashed line, perfectly matching the measurement. The Bi content is directly related to the Rn source, as radon disintegrates to Bi (over other products) within minutes (Figure \\ref{fig:decayproducts}). The decay of Ba and Cs obviously affect the background value of the full spectrum as a quasi-linearly decreasing trend is also visible in the Bi timeseries. This effect can be accounted for by dynamic background subtraction as described in analysis step 3 (and step 4). This (is not yet but will be as soon as the final average BG experiment is performed) is shown in \\ref{fig:mca_timeseries}a. Here only results of the LT phase of the experiment are shown for decay products of Rn after full energy calibrated analysis. To be verified: A very minor long term variation (yearly trend with maximum in summer) is visible although this trend is not significant. No further signal is present.'

discmca1 = 'The timeseries of Rn decay product Bi obtained from fully analyzed spectra, demonstrates that gamma variations obtained from these products are suitable to track relative Rn concentration variations. The results further demonstrate that this relationship is simple straightforward and is not modulated by any other physical process except simple decay.'

discmca2 = 'Non-linear variations of photopeak channel positions are observed and thus the energy to channel relationship is not constant throughout the monitoring experiment. This shifting trend is shown for three spectra from different times throughout the LT phase in Figure \\ref{fig:peakshifts}a. The shift of the photopeak position of Bi relative to its initial channel is shown in Figure \\ref{fig:peakshifts}b. Also plotted here is the temperature within the sealed box demonstrating that these peak shifts are definitely related to ambient temperature variation. Just looking at the forced temperature experiment clearly demonstrates this fact (Figure \\ref{fig:shiftzoom}). With our experimental setup in which scintillator, amplifier and MCA unit are combined the actual temperature dependent unit cannot be uniquely identified. Nevertheless, this observation is particularly important for the interpretation of single channel analysis timeseries, as they typically use a similar unit combination, and a fixed channel or channel window is used. If this window is small, the count rate strongly non-linearly depends on temperature. This effect is emphasized in Figure \\ref{fig:shift}. Here the blue curve depicts timeseries results which would be obtained by a SCA analyzer initially centered on the Bi photopeak (1764 KeV) at channel 291. The orange curve shows the count rate for the same peak if a dynamic variation is considered (analysis step 2). Colored vertical lines correspond to the spectra shown in Figure \\ref{peakshifts}a. As can be seen the variations in count rate are not matching the temperature variations any more. This is a consequence of the completely non-linear relationship of channel shifts and obtained count rate as sketched in ... . If the selected channel covers a gamma peak, slight variations of the channel will only minimally affect the count rate. If the channel covers the left peak flank a temperature increase will lead to a reducing count rate, if it is centered on the right flank increasing temperature will lead to a count increase. Thus such channel variation will be superimposed and add additional variation to the count rates independent from concentration variations of the investigated isotopes. Especially for long-term monitoring it is obviously necessary to use large energy windows for the SCA and keep temperature as constant as possible. The SCA experiments in the tunnel of the Conrad Observatory fulfill these conditions, particularly because temperature variations near the sensor are negligible.'

conc = '1. MCA: Gamma spectrometry is very useful to monitor variations of disintegration products from Rn. 2. MCA: Our experiments clearly show a significant dependency of the energy/channel calibration from temperature of the sensor/data acquisition system. If only a channel dependent analysis is performed, this dependency transfer to a completely non-linear and indifferent connection of count rate with temperature, which might result in positive, negative or negligible temperature/count rate dependency depending on the channel integral used by SCA analyzers. Although we are aware that this observation might only be related to our brand of analyzer/recorder, there are frequent examples of SCA analysis in the literature mentioning diurnal variation patterns in count rate with indifferent/phase shifted connections to temperature. We therefore want to stress that is really worthwhile to keep above findings in mind and carefully review the measurement system. 3. MCA: It is shown that by a full energy dependent analysis in confined conditions, variations of Rn can be traced very well. This is however also true for the experimental step 2, which most closely represents experimental conditions when investigating natural variations, where the actual background is not known. Therefore MCA experiments are perfectly suited. 4. SCA: SCA gamma variation which are directly linked to disintegration products of Rn, provided that temperature is stable (see above) and a reasonable channel range has been chosen, covering Rn decay products like 214Bi. 5. SCA: Variation patterns in gamma counts are predominantly related to transport processes of Rn within the building which are directly linked to temperature differences between (non-sealed) laboratory conditions and external atmosphere. As temperature differences inherently contain a diurnal periodicity, this transfer to the count rate with gradient dependent amplitudes and phase shifts obviously dependent on the specific layout of location/building/tunnel. 6. SCA: Transport processes mask effectively all other eventually possible signals. Therefore, for a proper analysis of gamma variation and its linkage to natural Rn signals, atmospheric transport must (a) either be completely prohibited or (b) independently be assessed and subtracted from the variation curve. The simplified model presented here might give some indications for latter, yet transport is definitely much more complex and strongly depends on the specific laboratory conditions. 7. SCA: Analyze the residuum... Are there any region of difference and are the related to snow cover, heavy rain, etc?'


if texoutput:
    texdir = "/srv/projects/radon/Report"
    ## Assemble Text and create README and TeX
    with open(os.path.join(texdir,'spectralreport.tex'), 'wb') as texfile:
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
        texfile.write("\\title{Analyzing natural and synthetic gamma radiation at the Conrad Observatory in Austria}\n")
        texfile.write("\\author{R. Leonhardt, }\n")
        texfile.write("\\author{G. Steinitz, }\n")
        texfile.write("\\author{W. Hasenburger, }\n")
        texfile.write("\\author{M. Haas, }\n")
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
        texfile.write("\\begin{figure}[htb]\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=0.8\\textwidth]{graphs/Rn_productsofdecay.png}\n")
        texfile.write("\\caption{Decay products of $^{222}$Rn. Important gamma emitting products with short time differences are $^{214}$Pb and $^{214}$Bi.}\n")
        texfile.write("\\label{fig:decayproducts}\n")
        texfile.write("\\end{figure}\n")
        texfile.write("% ----------------------------------------------------------------\n")
        texfile.write("\\section{Instrumentation and Methods}\\label{inst}\n")
        texfile.write("{}\n".format(instrumenttext))
        texfile.write("%\\input{../Tables/insttable}\n")
        texfile.write("% ----------------------------------------------------------------\n")
        texfile.write("\\subsection{Single channel analysis}\\label{SCA}\n")
        texfile.write("{}\n".format(scatext1))
        texfile.write("\\subsection{Multi channel analysis}\\label{MCA}\n")
        texfile.write("{}\n".format(mcatext1))
        # double figure
        texfile.write("\\begin{figure}\n")
        texfile.write("\\centering\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/Spectra_410000.png}\n")
        texfile.write("%\\caption{A subfigure}\n")
        texfile.write("%\\label{fig:sub1}\n")
        texfile.write("\\end{subfigure}%\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/Spectra_corrected_410000.png}\n")
        texfile.write("%\\caption{A subfigure}\n")
        texfile.write("%\\label{fig:sub2}\n")
        texfile.write("\\end{subfigure}\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/fitpeak.png}\n")
        texfile.write("%\\caption{A subfigure}\n")
        texfile.write("%\\label{fig:sub1}\n")
        texfile.write("\\end{subfigure}%\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/calibration.png}\n")
        texfile.write("%\\caption{A subfigure}\n")
        texfile.write("%\\label{fig:sub2}\n")
        texfile.write("\\end{subfigure}\n")
        texfile.write("\\caption{Analysis of spectrograms: (a) shows a typical spectrogram. Regions of interest (ROI) are highlighted and the linear function which is subtracted to remove the Compton background is shown as well. (b) denotes the spectrogram after removing the linear background. (c) shows how a polynomial function is fitted to each ROI. Also shown is the one-sigma uncertainty range of the fit  (blue shades) and the integration interval (green), which by an identically window width relative to the maximum or the peaks for all measurements. (d) shows the energy calibration for which peak position (from c) are connected with known energy levels.}\n")
        texfile.write("\\label{fig:spectra}\n")
        texfile.write("\\end{figure}\n")
        # add here one figure with energy as x axis
        texfile.write("\\begin{figure}\n")
        texfile.write("\\centering\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/Energy_410000.png}\n")
        texfile.write("%\\caption{A subfigure}\n")
        texfile.write("%\\label{fig:sub1}\n")
        texfile.write("\\end{subfigure}%\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/Energy_corrected_410000.png}\n")
        texfile.write("%\\caption{A subfigure}\n")
        texfile.write("%\\label{fig:sub2}\n")
        texfile.write("\\end{subfigure}\n")
        texfile.write("\\caption{(a) Diagram showing energy calibrated spectra base on the results of figure . The blue curve is the average hourly background data, analyzed from 500 hourly measurements. The blue shaded region marks a 1 sigma uncertainty. The red curve is a single energy calibrated spectrum of the enhanced state. The difference of the two curves is also shaded. (b) show the difference spectrum of the red curve and the average background. The three gamma peaks at 609 KeV (masked by the CS-137 peak before), 1120 KeV and 1764 KeV are now clearly visible.}\n")
        texfile.write("\\label{fig:enerygy}\n")
        texfile.write("\\end{figure}\n")
        texfile.write("% ----------------------------------------------------------------\n")
        texfile.write("\\section{Results}\\label{results}\n")
        texfile.write("\\subsection{Natural gamma variation (SCA experiment)}\\label{conf}\n")
        texfile.write("\\subsubsection{Timeseries}\\label{conf}\n")
        texfile.write("\\begin{figure}[htb]\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=0.8\\textwidth]{graphs/sca_timeseries.png}\n")
        texfile.write("\\caption{Timeseries of the SCA measurements within the seismological tunnel.}\n")
        texfile.write("\\label{fig:sca_timeseries}\n")
        texfile.write("\\end{figure}\n")
        texfile.write("{}\n".format(resultssca1))
        texfile.write("\\begin{figure}\n")
        texfile.write("\\centering\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/sca_param_1.png}\n")
        texfile.write("%\\caption{A subfigure}\n")
        texfile.write("%\\label{fig:sub1}\n")
        texfile.write("\\end{subfigure}%\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/sca_param_2.png}\n")
        texfile.write("%\\caption{A subfigure}\n")
        texfile.write("%\\label{fig:sub2}\n")
        texfile.write("\\end{subfigure}\n")
        texfile.write("\\caption{(a) Dependency between count rate maxima and the amplitudes of $\delta$T values. The linear tendency is highlighted by the best fitting line with $m = $ and $C_{BG} = $. (b) The relative time shift between count maxima and preceding maxima in $\delta$T in minutes. The average delay corresponds to $d= $. All values are determined automatically by extrema analysis on the smoothed curves of Figure \\ref{fig:sca_countdeltat}}.\n")
        texfile.write("\\label{fig:sca_param}\n")
        texfile.write("\\end{figure}\n")
        texfile.write("\\subsubsection{Transport model}\\label{sec:sca_transport}\n")
        texfile.write("\\begin{figure}[htb]\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=0.8\\textwidth]{graphs/sca_model.png}\n")
        texfile.write("\\caption{Model of slowly acting atmospheric transport processes in the seismological tunnel (gray). The sketch illustrated the tunnel between 50 and 145m, including the three doors. The red box marks the position of the SCA instrument. The blue arrow indicates diffusion (diffusion of radon through the tunnel towards the outside of the observatory). Red arrows indicate a temperature dependent blocking process which reduces Rn diffusion). The temperature dependence come from differences between tunnel temperaure (T$_i$) and external temperature (T$_o$).}\n")
        texfile.write("\\label{fig:sca_model}\n")
        texfile.write("\\end{figure}\n")
        texfile.write("\\begin{figure}[htb]\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\textwidth]{graphs/sca_countdeltat.png}\n")
        texfile.write("\\caption{Timeseries of the SCA counts and the temperature difference $\delta$T between exernal temperature T$_o$ outside the observatory and tunnel temperature T$_i$. T$_i$ exceeds T$_o$ in the blue shaded region, which shows least variation in count rate and values about the background level. T$_o$ exceeds T$_i$ in the red shaded region, with large synchronous variations of counts and $\\delta$T}\n")
        texfile.write("\\label{fig:sca_countdeltat}\n")
        texfile.write("\\end{figure}\n")
        texfile.write("\\begin{figure}[htb]\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=0.8\\textwidth]{graphs/sca_timeseriesmodel.png}\n")
        texfile.write("\\caption{Timeseries of measured counts and modeled counts within the seismological tunnel. Also show is $\delta$T, which is the basis of the modeled counts.}\n")
        texfile.write("\\label{fig:sca_timeseriesmodel}\n")
        texfile.write("\\end{figure}\n")

        texfile.write("\\subsection{Confined setup (MCA experiment)}\\label{conf}\n")
        texfile.write("\\begin{figure}\n")
        texfile.write("\\centering\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/timeseries-channels.png}\n")
        texfile.write("%\\caption{A subfigure}\n")
        texfile.write("%\\label{fig:sub1}\n")
        texfile.write("\\end{subfigure}%\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/timeseries-energies.png}\n")
        texfile.write("%\\caption{A subfigure}\n")
        texfile.write("%\\label{fig:sub2}\n")
        texfile.write("\\end{subfigure}\n")
        texfile.write("\\caption{(a) Evolution of counts in selected ROI throughout the time period of all experiments. The left region (BG) covers all background measurements. After opening  the valve, the Radon container is gradually filling (IN) until equilibrium is reached. The majority of the experiment is dominated by the 'enhanced' (LT) state. The gradual decrease of relative counts in the Cs and Ba timeseries is directly linked to their decay rate. This is emphasized by the dashed dark-red line which denoted the theoretical decay rate of Ba-133, which matches the observed rate. (b) Evolution of energies of decay products of Rn-222 during the LT phase of the experiment. All three Bi-214 isotopes show identical variations with time, which correspond almost identically to the observed variations in of (a). Pb-214 isotopes show much stronger variations. The main reason for this observation is the subtraction of a constant background signal which does not regard for the decay of calibration isotopes Ba and Cs. Therefore, with increasing time we subtract an initially high level, which in reality gets smaller and smaller. This is leading to the observed relative decrease in figure xxxb and particluarly effects the low energy Pb isotopes close to the Ba energy range and below the Compton edge of Cs-137. }\n")
        texfile.write("\\label{fig:mca_timeseries}\n")
        texfile.write("\\end{figure}\n")
        texfile.write("{}\n".format(resultsmca2))
        texfile.write("% ----------------------------------------------------------------\n")
        texfile.write("\\section{Discussion}\\label{disc}\n")
        texfile.write("\\subsection{Confined MCA timeseries}\\label{disc:mca}\n")
        texfile.write("{}\n".format(discmca1))
        texfile.write("\\subsection{Channel shiftings in MCA analysis}\\label{disc:shifts}\n")
        texfile.write("{}\n".format(discmca2))
        texfile.write("\\begin{figure}\n")
        texfile.write("\\centering\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/spectra.png}\n")
        texfile.write("\\end{subfigure}%\n")
        texfile.write("\\begin{subfigure}{.5\\textwidth}\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=\\linewidth]{graphs/Bi_shift.png}\n")
        texfile.write("\\end{subfigure}\n")
        texfile.write("\\caption{(a) Diagram showing three spectra at different times relative to the channels. The shift of the peaks relative to their channels is easily to be seen. (b) Channel shifts over time i.e. difference of a fixed Bi-214 channel analysis to the maximum representative for Bi-214 at 1764 KeV as obtained by polynomial fitting. Also shown is the temperature within the confined box. Obviously temperature variations lead to shifts of the channels recording individual gamma peaks, which has strong implications on SCA analysis.}\n")
        texfile.write("\\label{fig:peakshift}\n")

        texfile.write("\\end{figure}\n")
        texfile.write("\\begin{figure}[htb]\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=0.8\\textwidth]{graphs/Bi_shift_zoom.png}\n")
        texfile.write("\\caption{Same figure as in figure xx. Zoomed on the heating experiment.}\n")
        texfile.write("\\label{fig:shiftzoom}\n")
        texfile.write("\\end{figure}\n")

        texfile.write("\\begin{figure}[htb]\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=0.8\\textwidth]{graphs/bichannel_vs_time.png}\n")
        texfile.write("\\caption{Timeseries plot of the hourly count rate at a single channel (Bi maximum in red and channel 291 in blue). The red curve makes use of the analysis with maximum identification as described in section xx. The blue curve would be obtained if only a single fixed channel is analyzed associated with the Bi peak.}\n")
        texfile.write("\\label{fig:shift}\n")
        texfile.write("\\end{figure}\n")

        texfile.write("\\subsection{SCA gamma variation explained by a simple Rn transport model}\\label{sec:sca_transportmodel}\n")
        texfile.write("{}\n".format(discsca1))
        texfile.write("\\begin{figure}[htb]\n")
        texfile.write("\\centering\n")
        texfile.write("\\includegraphics[width=0.8\\textwidth]{graphs/sca_zoom.png}\n")
        texfile.write("\\caption{Comparison of observed and modeled counts for a small time window.}\n")
        texfile.write("\\label{fig:sca_zoom}\n")
        texfile.write("\\end{figure}\n")

        # Eventually add a power spectrum of fig:peakshift or temperature analysis
        texfile.write("% ----------------------------------------------------------------\n")
        texfile.write("\\section{Conclusion}\\label{conc}\n")
        texfile.write("%-----------------------------------------------------------------\n")
        texfile.write("{}\n".format(conc))
        """
        texfile.write("%GATHER{radon.bib}\n")
        texfile.write("\\bibliography{geomag}{}\n")
        """
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


