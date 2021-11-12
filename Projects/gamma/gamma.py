from radonanalysismethods import *
from magpy.stream import KEYLIST, DataStream
from matplotlib.dates import date2num
from datetime import datetime, timedelta
from magpy.acquisition import acquisitionsupport as acs

from martas import martaslog as ml
logpath = '/var/log/magpy/mm-pro-gamma.log'
sn = 'POWEHI' # servername
statusmsg = {}


import re
import getopt


"""

BACKGROUND of this APP

Analyse spectral data in a given time range, extract count rates for given ROI's
and write this data to a specified repository

This job runs in parallel with 
a) analysis of T,P,RH - CO Monitor
b) ionometer

"""


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

# get all filename
def NumList(source, namedummy):
        matches = []
        dirs = []
        for root, dirname, filenames in os.walk(source):
            for filename in filenames:
                if filename.startswith(namedummy):
                    num = filename.replace(namedummy,'').split('.')[0]
                    matches.append(int(num))
                    dirs.append(root)
        return matches,dirs


## Get all basic data from a configuration file
def readconf(path):
    """
    sensorname  :  xx
    sensorid  :  xx
    voltage  :  720  # -> datatype for eventually new datarevision
    graphdir  :  /path
    plots  :  random  # selects 2 random time steps
    plots  :  427760  # list of filenumbers
    notes  :  20181118T000000,20181122T235900,Testlauf
    notes  :  20181123T000000,current,Active
    notes  :  20181118T000000,20181122T235900,background
    roi   : 
    energies  : 
    isotops  :    
    element  :  
    """
    confdict = {}
    return confdict

#graphdir = confdict.get('graphdir')
#texoutput = confdict.get('debug')
#confdict.get('notes')

graphdir = "/tmp/graphs"

if not os.path.exists(graphdir):
    os.makedirs(graphdir)

setupnotes = {}


#filerange = range(startnum,endlongterm)
#filerange = range(401279,402371)
#filerange = range(401279,401289)
#filerange = range(407000,412617)
#plotselection = [401279,402370,407000,410000]
plotselection = [427760]


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


    print ("##################################################")
    print ("# SINGLESPECANALYSIS")
    print ("# #########################################################################")
    print ("# 1. Definitions")
    print ("# #########################################################################")

    if not roi:
        """
        ROI (Region-of-interest) is defined in radonanalysismethods 
        """
        print ("please provide a list of roi s like [63, 114, 231, 291, 363, [197,398]]")
    print (roi)
    #print (channellist)
    print (energylist)

    searchrange = 10
    channellist = []
    xs, ys = 0., 0.

    result[name] = data

    print ("# #########################################################################")
    print ("# 2. Compton/Background treatment")
    print ("# #########################################################################")

    if not background:
        """
        background is a list with two arrays: [mean,stddev]
        """
        print (" - BACKGROUND not defined -> using comptoncorr method")
        interp, maxx, x,y = comptoncorr(data)
        datacorr = data[0:maxx]-interp
    else:
        print (" - subtracting BACKGROUND")
        datacorr = data-background[0]
        maxx = len(datacorr)

    print ("# #########################################################################")
    print ("# 3. Identification of peak position (channel) in each ROI")
    print ("# #########################################################################")

    for elem in roi:
        if isinstance(elem, int):
            peak = max(datacorr[elem-searchrange:elem+searchrange+1])
            # Fit peak
            xw,yw = getdatawin(datacorr, peak)
            # store xw and yw for rectangle in datacorr plot
            if elem == roi[-2]:
                xs = xw
                ys = yw
            max_x, max_y, I, Iuncert, wi = fitpeak(xw,yw,n=4,plot=plot)
            width = 5 # the practical measure of resolution is the width of the photopeak at  half  its  amplitude  known  as  the  Full  Width  at  Half  Ma
            count = sum(datacorr[elem-width:elem+width])
            #result[str(elem)] = [list(datacorr).index(peak), width, peak, count, 0]
            result[str(elem)] = [max_x, wi, max_y, I, Iuncert, datacorr[elem], 293-max_x]
            channellist.append(max_x)
            print ("  - ROI: {} -> Peak identified at {}".format(elem, max_x))
        else:
            try:
                if len(elem) == 2:
                    peak = max(datacorr[elem[0]:elem[1]])
                    width = int((elem[1]-elem[0])/2.)
                    count = sum(datacorr[elem[0]:elem[1]])
                    result[str(elem[0])+'-'+str(elem[1])] = [list(datacorr).index(peak), width, peak, count, 0, datacorr[elem[0]],0]
            except:
                print ("  - Failure for ROI {}".format(elem))

    print ("# #########################################################################")
    print ("# 4. Energy calibration")
    print ("# #########################################################################")
    #energycalib = False
    #print ("result", result)
    if energycalib:
        #energylist = [356, 667, 1460, 1764, 2600]
        #energycalibration(range(0,maxx), ch=channellist, e=energylist, n=1, use=2, plot=plot)
        data_new, coefs = energycalibration(range(0,1025), data, ch=channellist, e=energylist, n=2, use=4, plot=plot,addzero=True, plotmax = maxx)
        newtime = mdates.date2num(datetime.utcfromtimestamp(int(name)*3600.)) # - datetime.timedelta(days=1)
        result[newtime] = data_new
        result[str(newtime)+'_'+str(coefs)] = coefs
    else:
        print (" - skipped")

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
        plt.show()
        plt.close()

    #return result
    print (" - done")

def getAverage(result, filerange, plot=True):
    """
    calculates the mean of the provided filerange
    returns mean array [[mean],[sd]]
    """
    allarrays = []
    for i in filerange:
        newtime = mdates.date2num(datetime.utcfromtimestamp(int(i)*3600.))
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
        plt.show()

    return [me,st]


# Detect local minima
def comptoncorr(spectrum):
    minlist = []

    #intervallist = [0,1,2,3,6,12,20,51,67,76,89,119,141,167,191,212,235,267,326,380,400,500] # Von  Hand eingebene Werte (channels) bzw. Punkte, von denne weg interpoliert werden soll

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
            print ("------------------------------------------- Check it!!!!!!!!!!!!!!!!!!!")
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
        plt.show()
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

    print (" - Running energycalib method")
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

    print (" - energycalib: determination of fits")

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
        plt.show()

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
        ax.plot(x_new, func(x_new),'--', color='red')
        plt.xlabel("energy [KeV]")
        plt.ylabel("counts per hour [1/h]")
        plt.grid()
        plt.show()

    print (" - energycalib: done")

    #####
    ## Will break if func(x_new) is not providing valid data
    ## Might happen if peaks are not identified
    #####
    return func(x_new), coefs


"""
# #####################################################################################
# #################             Confined experiment                 ###################
# #####################################################################################
"""

def analyse_mca(path, startdate=None, enddate=None, config={}):
    """
    DESCRIPTION:
    Anlyze MCA reads spectral data files with time stamp in filename (e.g. hour)

    RETURN:
    Datastream

    config contains:
    filename="Spectral_",
    filedate="", 
    roi
    sensorname  :  xx
    sensorid  :  xx
    voltage  :  720  # -> datatype for eventually new datarevision
    graphdir  :  /path
    plots  :  random  # selects 2 random time steps
    plots  :  427760  # list of filenumbers
    notes  :  20181118T000000,20181122T235900,Testlauf
    notes  :  20181123T000000,current,Active
    notes  :  20181118T000000,20181122T235900,background
    roi   : 
    energies  : 
    isotops  :    
    element  :  
    
    """
    if not startdate:
        startdate = datetime.utcnow()-timedelta(days=1)
    if not enddate:
        enddate = datetime.utcnow()

    #analyzemca = False
    #if analyzemca:
    # Filter environment data
    # #######################
    filterenv = False
    backgrdend=0
    accumulationend=0

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # IMPORTANT: check times !!!!
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    #path = r"/home/leon/CronScripts/MagPyAnalysis/RadonAnalysis/ScriptsThemisto"
    path = path
    #namedummy = "Spectral_"
    namedummy = config.get('filename')
    resolution = config.get('filedate')
    

    numlist, dirs = NumList(path,namedummy)
    #print (dirs)

    print ("limit filerange by starttime and endtime")
    # min and max number within given timerange
    if resolution in ['Day','day']:
        div = 86400.
    elif resolution in ['Minute','minute','minutes']:
        div = 60.
    else:  # hour as default
        div = 3600.

    mintime = float(startdate.strftime("%s"))/div
    maxtime = float(enddate.strftime("%s"))/div
    validrange = [int(mintime),int(maxtime)]
    filerange = [[el,i] for i,el in enumerate(numlist) if el in range(min(validrange),max(validrange))]

    if not len(filerange) > 0:
        print ("Did not find any spectrum files within the given time range")
        return DataStream()

    # get config data
    if config.get('plots') in ['Random','random']:
        # select one diagram randomly
        import random
        plotselection = [random.choice(filerange)]
    if not config.get('plots') in ['Random','random']:
        pl = config.get('plots').split(',')
        try:
            plotselection = [int(el) for el in pl]
        except:
            plotselection = []
    roi = config.get('roi')

    dataarray = [[] for i in range(len(roi)+1)]
    # Cycle through channels
    validfilerange = []

    print ("# -------------------------------------------------------------------------")
    print ("# A. Main prog")
    print ("# -------------------------------------------------------------------------")
    print ("# Cycling through all spectrograms")
    print (filerange)


    for eleme in filerange:
        # Define file name
        i = eleme[0]
        idx = eleme[1]
        name = (namedummy+"%06i.Chn") % (i)
        print ("# -------------------------------------------------------------------------")
        print (" - Analyzing {}".format(name))
        print ("# -------------------------------------------------------------------------")
        filename = os.path.join(dirs[idx], name)
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
            testplot = False
            if testplot:
                print (" - Raw data plot")
                # Determine the following channel energy relation from each analysis
                fig, ax = plt.subplots(1, 1)
                ax.set_yscale('log')
                ax.set_xlim([0,1050])
                ax.plot(range(0,len(data2)), data2, '-')
                plt.xlabel("channel []")
                plt.ylabel("counts per hour [1/h]")
                plt.grid()
                plt.show()

            print ("# -------------------------------------------------------------------------")
            print (" - Analysis of spectrum")

            try:
                if i in plotselection:
                    print ("  --> Plotting")
                    #result[str(i)] = singlespecanalysis(data2,roi=roi,plot=True,name=str(i))
                    singlespecanalysis(data2,roi=roi,plot=True,name=str(i))
                    plt.show()
                else:
                    #result[str(i)] = singlespecanalysis(data2,roi=roi,plot=False,name=str(i))
                    singlespecanalysis(data2,roi=roi,plot=False,name=str(i))
            except:
                singlespecanalysis(data2,roi=roi,plot=False,name=str(i))


            print ("# -------------------------------------------------------------------------")
            print (" - Extracting energy levels")

            dataarray[0].append(date2num(datetime.utcfromtimestamp(int(i)*3600.)))
            for idx,elem in enumerate(roi):
                exec("roiname = str(roi[{}])".format(idx))
                if isinstance(elem, (list,)):
                    roiname = roiname.replace("[","").replace("]","").replace(", ","-")
                #print (idx,roiname)
                dataarray[idx+1].append(result[roiname][3])
                #exec("liste{}.append(result[roiname][3])".format(idx+1))

            listemca.append(result[str(roi[3])][2])
            listesca.append(result[str(roi[3])][5])
            listeshift.append(result[str(roi[3])][6])

            # Add flags to the datastream
            # Notes should be converted to flags
        except:
            print ("----------------------------")
            print ("Failed analysis for {}!".format(filename))

    print ("# -------------------------------------------------------------------------")
    print ("# Creating summary and MagPy file for storage")

    starray = [np.asarray([]) for el in KEYLIST]
    for idx,elem in enumerate(dataarray):
        starray[idx] = np.asarray(elem)
    #dataarray = np.asarray([np.asarray(elem) for elem in dataarray])
    #print (dataarray)

    print (KEYLIST[1:len(roi)+1])

    stream = DataStream()
    stream.ndarray = np.asarray(starray)
    #stream.header = header
    stream = stream.sorting()
    stream.header['SensorElements'] = ",".join(["Counts{}".format(elem) for elem in energylist])
    stream.header['SensorKeys'] = KEYLIST[1:len(roi)+1]

    return stream

    """
    import magpy.mpplot as mp
    mp.plot(stream)
    print ("DoNE")
    sys.exit()
    header['SensorElements'] = "CountsBa,CountsCs,CountsBi,CountsBi,CountsAll"

    max1 = max(liste1)
    max2 = max(liste2)
    max3 = max(liste3)
    max4 = max(liste4)
    max5 = max(liste5)
    max6 = max(liste6)
    liste1 = np.asarray(liste1)  # Ba
    liste2 = np.asarray(liste2)  # Cs
    liste3 = np.asarray(liste3)  # Bi1120
    liste4 = np.asarray(liste4)  # Bi1764
    liste5 = np.asarray(liste5)
    liste6 = np.asarray(liste6)  #  all

    print ("# Content:")
    print (liste2)



    # Get creation and UTC time

    #print len(namelist), len(liste5)
    # Save creation and UTC time in csv file
    f = open('channel_data.csv', 'w')
    f.write("%s\n" % (" # MagPy ASCII"))
    f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % ("Time", '"File"', "CountsBa[]", "CountsCs[]", "CountsBi1120[]", "CountsBi1764[]", "CountsAll[]", "CountsMCA(Bi1764)[]", "CountsSCA(Ch291)[]", "ChannelShift(Max_near_291)[]"))
    for k in range(0,len(namelist)):
        f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (timelist[k], namelist[k], liste1[k], liste2[k], liste3[k], liste4[k], liste6[k], listemca[k], listesca[k], listeshift[k]))
    f.close()


    savedarrays = []
    for i in plotselection:
        #newtime = mdates.date2num(datetime.utcfromtimestamp(int(i)*3600.))
        ar = result.get(str(i),[])
        if len(ar) > 0:
            savedarrays.append(ar)

    """

def main(argv):
    path = ''
    startdate = ''
    enddate = ''
    conf = ''
    dummy = DataStream()
    destination = ''
    global energylist
    global isotopelst
    global colorlst
    global intervallist
    name = "{}-Projects-gamma".format(sn)    
    #global roi
    try:
        opts, args = getopt.getopt(argv,"hp:b:e:m:d:",["path=","begin=","end=","config=","destination=",])
    except getopt.GetoptError:
        print ('gamma.py -p <path> -b <begin> -e <end> -m <config> -d <destination>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('-------------------------------------')
            print ('Description:')
            print ('-- gamma.py analyses gamma data  --')
            print ('-------------------------------------')
            print ('Usage:')
            print ('gamma.py -p <path> -b <begin> -e <end> -m <config> -d <destination>')
            print ('-------------------------------------')
            print ('Options:')
            print ('-p            : path to gamma spectral data like "/home/max/myarchive"')
            print ('-b            : begin: default yesterday')
            print ('-e            : end: default = datetime.utcnow()')
            print ('-d            : path to store data')
            print ('-m            : config file')
            print ('-------------------------------------')
            print ('Example:')
            print ('python gamma.py /media/Samsung/Observatory/data/ -b "2012-06-01" -m myconf.cfg')
            sys.exit()
        elif opt in ("-p", "--path"):
            path = arg
        elif opt in ("-b", "--begin"):
            startdate = arg
        elif opt in ("-e", "--end"):
            enddate = arg
        elif opt in ("-m", "--config"):
            conf = arg
        elif opt in ("-d", "--destination"):
            destination = arg

    if path == '':
        path = r"/home/leon/CronScripts/MagPyAnalysis/RadonAnalysis/ScriptsThemisto"

        #print ('Specify a path to gamma data:')
        #print ('-- check gamma.py -h for more options and requirements')
        #sys.exit()

    if conf == '':
        print ('Specify a configuration file:')
        print ('-- check gamma.py -h for more options and requirements')
        sys.exit()

    # Test time
    if not startdate == '':
        starttime = dummy._testtime(startdate)
        print (starttime)
    else:
        starttime = datetime.utcnow()-timedelta(days=2)
    if not enddate == '':
        endtime = dummy._testtime(enddate)
    else:
        endtime = datetime.utcnow()

    # Read Config data
    confdict = acs.GetConf(conf)
    #print (confdict)

    debug = False
    if confdict.get('debug') in ['True','true']:
        debug = True

    if not confdict.get('plots') in ['Random','random']:
        pl = confdict.get('plots').split(',')
        try:
            plotselection = [int(el) for el in pl]
        except:
            plotselection = []
    else:
        plotselection = 'random'
    print ("SELECTED for plotting:", plotselection)
    graphdir = confdict.get('graphdir')
    if not os.path.exists(graphdir):
        os.makedirs(graphdir)

    # Get analysis data
    ok = True
    if ok:
        energylist = [int(el) for el in confdict.get('energies').split(',')]
        if debug:
            print ("E:", energylist)
        isotopelst = confdict.get('isotops').split(',')
        if debug:
            print ("Isotops:", isotopelst)
        colorlst = confdict.get('colors').split(',')
        if debug:
            print ("Colors:", colorlst)
        intervallist = [int(el) for el in confdict.get('intervals').split(',')]
        if debug:
            print ("Intervals:", intervallist)
        roi = []

        roistrtmp = confdict.get('roi')
        roistr = re.sub("\[[^]]*\]", lambda x:x.group(0).replace(',',';'), roistrtmp)

        for el in roistr.split(','):
            el = el.strip()
            if not el.startswith('['):
                roi.append(int(el))
            else:
                lelt = el.replace('[','').replace(']','')
                lel = [int(ele) for ele in lelt.split(';')]
                roi.append(lel)
        if debug:
            print ("Rois:", roi)
        confdict['roi'] = roi

        try:
            stream = analyse_mca(path, startdate=starttime, enddate=endtime, config=confdict)

            stream.header['SensorSerialNum'] = confdict.get('sensorserial','')
            stream.header['SensorDataLogger'] = confdict.get('sensordatalogger','')
            stream.header['SensorDataLoggerSerialNum'] =  confdict.get('sensordataloggerserialnum','')
            stream.header['SensorName'] =  confdict.get('sensorname','')
            stream.header['SensorID'] = "{}_{}_0001".format(confdict.get('sensorname','None'),confdict.get('sensorserial','12345')) 
            stream.header['DataID'] = "{}_{}_0001_0001".format(confdict.get('sensorname','None'),confdict.get('sensorserial','12345')) 
            stream.header['StationID'] =  confdict.get('StationID','WIC')
            print (stream.length())

            print ("writing data")
            if destination:
                stream.write(destination,coverage='year', mode="replace", format_type='PYSTR', filenamebegins='{}_'.format(stream.header['SensorID']), dateformat='%Y')
            #stream.write(destination, format_type='PYASCII', filenamebegins='{}_'.format(stream.header['SensorID']))

            statusmsg[name] = 'gamma analysis successfully finished'
        except:
            statusmsg[name] = 'gamma analysis failed'
        martaslog = ml(logfile=logpath,receiver='telegram')
        martaslog.telegram['config'] = '/home/cobs/SCRIPTS/telegram_notify.conf'
        martaslog.msg(statusmsg)


if __name__ == "__main__":
   main(sys.argv[1:])



