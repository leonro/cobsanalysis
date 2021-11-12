# modules
import struct
import os, sys
import numpy as np
import scipy.ndimage.filters as filters
import scipy.ndimage.morphology as morphologyimport
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.dates as mdates
import matplotlib.image as mpimg

import os
import datetime
import time
import csv
#from scipy.signal import argrelextrema
from scipy.interpolate import interp1d
from scipy import interpolate
from scipy.optimize import curve_fit, minimize
from scipy.special import factorial
import pylab
import numpy.polynomial.polynomial as poly
#from scipy.optimize import curve_fit

local = True
if local:
    import sys
    sys.path.insert(1,'/home/leon/Software/magpy-git/')

from magpy.stream import read, mergeStreams, nan_helper
import magpy.mpplot as mp
import magpy.opt.emd as emd

graphdir = "/home/leon/CronScripts/MagPyAnalysis/RadonAnalysis/Report/graphs"

thalfRn = 3.8235

# When using Voltage of 650 V
# Analysis data
energylist = [356, 662, 1120, 1764, 609]
isotopelst = ['^{133}Ba','^{137}Cs','^{214}Bi','^{214}Bi','^{214}Bi']
colorlst = ['red','blue','orange','orange','green','brown']
roi = [63, 114, 192, 291, [197,398]]

# Energiepeaks of Bi-124: 609, 1120, 1764
e_energylist = [609, 1120, 1764, 295, 352]
e_isotopelst = ['^{214}Bi','^{214}Bi','^{214}Bi','^{214}Pb','^{214}Pb']
e_colorlst = ['orange','orange','orange','green','green','green']
e_roi = e_energylist


# When using Voltage of 720 V
# roi ele: 
energylist = [356, 662, 1120, 1460, 1764, 609]
isotopelst = ['^{133}Ba','^{137}Cs','^{214}Bi','^{40}Ka','^{214}Bi','^{214}Bi']
colorlst = ['red','blue','orange','green','orange','brown']
roi = [100, 183, 305, 393, 473, [305,598]]
# For compton-background determination
intervallist = [0,1,2,3,7,15,49,77,90,109,156,204,280,362,420,449,518,548,628,666,730] # Von  Hand eingebene Werte (channels) bzw. Punkte, von denne weg interpoliert werden soll

"""
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++++++++++++++++++++++++++++ Natural gamma specific +++++++++++++++++++++++++++++++
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
"""

### Following my model: Diffusion could discribe the concentration dependend (and temperature dependend) influx from outside into the tunnel (and not removal of radon) and thus, a blocking of removal of mass...


# two possibilities of treating diffusion -> 1) concentration depedend (high Randon conc inside - diffuses outward to low conc. 2) effecitivity is dependend on temperature diff.

## A simple numerical example: 
## 1.) the relative decay per minute is (0.0125 of 100)
## 2.) lets assume the production to be 0.5
## 3.) diffusion needs to be 0.4875 to equalize production
## 4.) if diff < 0.48 then an increase of concentration results
## 5.) at dT < 0 diff must be >= 0.48
## 6.) at dT > 0 diff must be < 0.48
## 7.) at dT > 0 diff must drop in dependency of concentration-> if conc = conc(max) diff must be 0.48 again
## -> lesson learned: diffusion is a simple decay like process: a c = c0 * exp(-D*t)
## D needs to have a unit like m2/s
## c = M/np.sqrt(4*3.14*D*t) *np.exp(-x*x/(4*D*t)) at x = 0 -> c = M/np.sqrt(4*Pi*D*t)
## M = needs to have a length unit (1/m) if M == 100 then D should be 3454 for diff == 0.48
## if D increases then diff get smalles -> D increases with T -> Arrehnius-> diff get smaller
## 8.)assume: M changes with conc -> physically plausible
##  e.g. at dT = 10 -> M = 115 (M is related to cmax 
## at M 100 and D = 1081 (dT = 10) -> diff = 0.42
## at M 115 and D = 1081 -> diff = 0.48
## -> optimal M is obtained at diff = 0.48 for each D (D is dependent of dT)
## lets get all Ms for which diff=0.48 in dependency of dT
## t = 1

# 0. Formula: arrhenius relation, and basic
def diffusioncoefficient(D0, dT, E): # 3454.):
    # Arrenhius relation
    R = 8.314 # J/K/mol
    # dT Temperature diff
    if np.nanmean(dT) > 200:
        TK = dT
    else:
        TK = dT+273.25
    # E activation energy
    """
    if dT == 0:
        return D0 * np.exp(-E/float(R*1.))
    elif dT < 0:
        return D0 * np.exp(-E/float(R*1.))
    else:
        #print dT, -E/float(R*dT)
        return D0 * np.exp(-E/float(R*dT)) # with E as an activation energy,dT
    """
    return D0 * np.exp(-E/float(R*TK))


def dc2(x, a, b, c): # 3454.):
    # Arrenhius relation
    R = 8.314 # J/K/mol
    return c + a*np.exp(-b/(R*x))


def decay(t, c0, thalf):
    return c0*np.exp(-0.693*t/thalf)

# 1. Formula: production and decay, assuming constant background, production by el with thalf >>>> thalfRn
def secularequi(t,cmax,thalf,bg):
    # cmax is the maximum equilibrium count
    return bg + cmax*(1-np.exp(-0.693*t/thalf))

def getCountSE(c0, dt, cmax,thalf,bg):
    t = np.log(1 - (c0-bg)/float(cmax)) * thalf/-0.693
    cnext = bg + cmax*(1-np.exp(-0.693*(t+dt)/thalf))
    return cnext-c0

# 2. Formula: diffusion rate ind dependency of dT and c0

def diffusion(t,M,D,bg=0.):
    return bg + M/np.sqrt(4*3.1415*(D)*t) # *np.exp(-x*x/(4*D*t))  as x = 0

def getCountDIF(M, dt, t, D):
    #M is a function of cmax, dT
    if D == 0 or t == 0:
        cr = 0
    else:
        cr = M/np.sqrt(4*np.pi*D*t) - M/np.sqrt(4*np.pi*D*(t+dt))
    # cr = 9, t and dt = 1 -> 9 = M/np.sqrt(4*np.pi*D) - M/np.sqrt(4*np.pi*D*2) =  M/np.sqrt(4*np.pi*D) * (1-1/np.sqrt(2))-> 9/0.293 * np.sqrt(4*np.pi*D) = M = 30.171 * 3.545 *np.sqrt(D) = 108.891 *np.sqrt(D)
    return cr

def M_func(E, D0, cmax, bg, m, a, c, tm, tbg, mode='linear'):
    cntrange = np.asarray(range(int(bg-1000),int(cmax+50000),1))
    Mlst = []
    thalf = 3.8235*1440
    t = 1
    dt = 1
    R = 8.314
    dT1lst,dT2lst= [],[]
    for cnt in cntrange:
        c0 = cnt
        if mode=='linear':
            dT = (c0-bg)/float(m)  # (c0-bg)/m   ###### Using a linear approximation -> change that !!
        elif mode=='nonlinear':
            # c = 14000. + a*np.exp(-b/(R*dT))
            #np.log((c - bg)/ a) = -b/(R*T)
            dT = -E / ( R * np.log((c0 - c)/ a))
        else:
            dT = (c0-tbg)/float(tm)
        """
        dT1 = (c0-bg)/float(m)  # (c0-bg)/m   ###### Using a linear approximation -> change that !!
        dT2 = -E / ( R * np.log((c0 - bg)/ a))
        dT1lst.append(dT1+273.25)
        dT2lst.append(dT2)
        """
        equirate = getCountSE(c0, 1, cmax,thalf,bg)
        D = diffusioncoefficient(D0, dT, E)
        #D = D0
        #print dT, equirate, D
        if D == 0 or t == 0:
            M = 0.
        else:
            M = equirate / (1/np.sqrt(4*np.pi*D*t) - 1/np.sqrt(4*np.pi*D*(t+dt)))
        Mlst.append(M)
    """
    fig = plt.figure(1)
    plt.title("counts vs dt")
    ax = fig.add_subplot(1,1,1)
    plt.plot(dT1lst,cntrange,'-')
    plt.plot(dT2lst,cntrange,'-')
    plt.show()
    """
    # Now interpolate this and obtain M = func(c0) and return func
    func = interp1d(cntrange, Mlst, kind='linear')
    return func

# Empirical (mode = 3), Physical (mode = 5)
def accumulation(Istart, dT, bg=15000, rate=20., m=819.0, mode=3, E = 200., D0 = 1000., cmax = 65000., c0=1000000000., Mfunc = None):
    # dT constrains the maximum possible value
    # lets first assume a simple 4 hour function, 
    # which means that the maximum value is obtained after 4 hours, independent of start condition
    # Istart is the previous concentration value
    # delay gives a time constraint dependend on the sampling rate - try 4 hours 
    #m = 768.0
    #m = 819.0
    # dT can should be the mean

    #mode = 3 # simple linear reduction

    if dT > 0:
        Imax = dT*m + bg # m is obtained by a simple linear max_count rate versus T_diff ratio 
    else:
        Imax = bg # m is obtained by a simple linear max_count rate versus T_diff ratio
    if mode == 1:
        # Linear redcution and accumulation rate (only dependent on difference between (cmax-Ibg)/delay
        # cmax differs from Imax: Imax limited to bg, cmax not
        # cmax = np.abs(dT)*m + bg  -> if dT = 0 -> no transport id dT = 0 -> wrong
        # -> Example: Imax=15000, Ibg=15000, Istart=20000
        Inew = Istart + (cmax - Iprev)/(rate)
    elif mode == 2:
        # difference dependent reduction
        # -> Example: Imax=15000, Ibg=15000, Istart=20000 
        #             -> -5000/delay -> if delay == 1 then total diff is removed in 1min, 10-> 500 min
        #             -> with const delay: 10000 in 1000 min
        # -> delay: 1/t(min) 
        Inew = Istart + (Imax - Istart)/(rate)
    elif mode == 3:
        # simple constant reduction rate (e.g. 20counts/min)
        Inew = Istart + np.sign(Imax - Istart)*rate
    elif mode == 4:
        Inew = Istart + (Imax - Iprev)/(delay/np.abs(dT))
        # However: delay is not dependent on dT
    elif mode == 5:
        c0=Istart
        thalf = 3.8235*1440. #https://en.wikipedia.org/wiki/Radon
        cntprod = getCountSE(c0, 1, cmax,thalf,bg)
        #print ("Now Getting M for ", Istart, bg)
        if Istart <= bg:
            Istart = bg+1
        #print Istart
        M = float(Mfunc(Istart)) # will be very small (nan) close to bg
        #print M
        if np.isnan(M):
            M = 0.
        #print ("=", M)
        D = diffusioncoefficient(D0, dT, E)
        #D = D0
        cntdiff = getCountDIF(M, 1, 1, D)
        #print cntprod, cntdiff
        Inew = Istart + (cntprod - cntdiff)
        # return Inew
        # However: delay is not dependent on dT
    if Inew < bg:
        Inew = bg

    return Inew

## http://stackoverflow.com/questions/25828184/fitting-to-poisson-histogram
def poisson(k, lamb):
    """poisson pdf, parameter lamb is the fit parameter"""
    return (lamb**k/factorial(k)) * np.exp(-lamb)


def negLogLikelihood(params, data):
    """ the negative log-Likelohood-Function"""
    lnl = - np.sum(np.log(poisson(data, params[0])))
    return lnl

def pearson(a, b):
    from scipy.stats.stats import pearsonr
    if not len(a) == len(b):
        print ("Pearson R: vectors do not have equivalent length")
        return (0,0)
    else:
        return pearsonr(a,b)


def running_mean(x, N, mode='full', debug=False):

    if debug:
        print ("running_mean1",len(x))
    ## Interpolate Nan
    newar = np.array([])
    nans, y = nan_helper(x)
    x[nans]= np.interp(y(nans), y(~nans), x[~nans])
    newar = np.concatenate((newar,x))
    x = newar

    if debug:
        print ("running_mean2-after nan",len(x))

    ## start and endvalue treatment....
    if int(N) % 2 == 0:
        nhalf = int(N/2.)
        N = int(N)+1
    else:
        N = int(N)
        nhalf = int((N-1)/2.)
    if mode=='full':
        x = np.insert(x,0,[x[0]]*nhalf)
        x = np.insert(x,-1,[x[-1]]*(nhalf))
    elif mode=='end':
        x = np.insert(x,-1,[x[-1]]*(N-1)) # nhalf*2

    if debug:
        print ("running_mean2-after endvaluetreatment",len(x), nhalf, N)

    ## Calc mean (first solution is fastest but eventually does not work in python3.5
    #try:
    cumsum = np.cumsum(np.insert(x, 0, 0)) 
    return (cumsum[N:] - cumsum[:-N]) / N
    #except:
    #    print ("Running mean: python 3.5 solution")
    #    return np.convolve(x, np.ones((N,))/N)[(N-1):]


def TransportBackground(countarray, backgroundlevel=15000):
    ### Get background mean and 2 sigma of it
    ### -------------------------------------
    ### Parameter: bg (background)
    try:
        # select all data from month 12,1,2,3
        #### Better select the smallest 10 Percent
        sortcount = np.argsort(countarray)
        mincnt = int(len(sortcount)*0.20)  # 0.33
        countmin = sortcount[:mincnt]
        print (np.mean(countarray[countmin]), np.std(countarray[countmin]))
        backgrdlst = np.asarray([el for el in countarray if el < backgroundlevel+1000])
        significancethreshold = np.mean(countarray[countmin]) + 4*np.std(countarray[countmin])
        print (np.mean(backgrdlst), np.std(backgrdlst))
        bg = np.mean(countarray[countmin])
    except:
        print ("Pass")
        significancethreshold = backgroundlevel+1000
        bg = backgroundlevel

    ### Significancethrehold: Only data above this values are examined for delay investigation
    ### -------------------------------------
    print ("significancethreshold:", significancethreshold)
    print ("background:", bg)
    return (bg, significancethreshold)


def TransportBoundaryCond(countarray, deltatarray, tarray, bg=(15000,16000), searchrange=[1440,4320], toffset=2., debug=False):
    """
    # Should be run on a filtered/smoothed dataset
    """
    dtcntmax,dtmax,dtdelaymax = [],[],[]
    tcntmax,tmax,tdelaymax = [],[],[]
    lincntmax,lindtmax = [],[]
    E = 60000.
    c0 = 10000000000

    abstarray = tarray+273.25

    deltatarray = deltatarray+toffset
    if debug:
        print ("Lengths", len(countarray), len(deltatarray), len(tarray))

    test = True
    if test:
        # now find local maxima in countarray
        from scipy.signal import argrelextrema
        locmaxcnt = argrelextrema(countarray, np.greater)[0]
        locmaxcnt = np.asarray([el for el in locmaxcnt if countarray[el] > bg[1]])

        # local maxima of deltat above zero
        locmaxdt = argrelextrema(deltatarray, np.greater)[0]
        locmaxdt = np.asarray([el for el in locmaxdt if deltatarray[el] > 0])

        # local maxima in temperature outside
        locmaxt = argrelextrema(abstarray, np.greater)[0]

        # from locmaxcnt find maximum values of dt in dtarray[locmaxcnt-2880:locmaxcnt] # two days before
        # add both values to a 
        for i, locmax in enumerate(locmaxcnt):
            print ("ToDo:",len(locmaxcnt)-i)
            if locmax-searchrange[1] >= 0:
                lowrange = locmax-searchrange[1]
                uprange = locmax-searchrange[0]
            else: 
                lowrange = 0
                uprange = locmax
            windt = np.asarray([el for el in locmaxdt if el in range(lowrange,uprange,1)])
            wint = np.asarray([el for el in locmaxt if el in range(lowrange,uprange,1)])
            #print (windt, wint)
            if len(wint) > 0:
                tmaxi = np.max(abstarray[wint])
                tmin = wint[list(abstarray[wint]).index(tmaxi)]
                delay = locmax-tmin
                tcntmax.append(countarray[locmax])
                tmax.append(tmaxi)
                tdelaymax.append(delay)
            if len(windt) > 0:
                dtmaxi = np.max(deltatarray[windt])
                dtmin = windt[list(deltatarray[windt]).index(dtmaxi)]
                delay = locmax-dtmin
                dtcntmax.append(countarray[locmax])
                dtmax.append(dtmaxi)
                dtdelaymax.append(delay)
        tcntmax = np.asarray(tcntmax)
        tmax = np.asarray(tmax)
        tdelaymax = np.asarray(tdelaymax)
        lintmax = np.asarray(tmax)
        lintcntmax = np.asarray(tcntmax)
        dtcntmax = np.asarray(dtcntmax)
        dtmax = np.asarray(dtmax)
        dtdelaymax = np.asarray(dtdelaymax)
        lindtmax = np.asarray(dtmax)
        lincntmax = np.asarray(dtcntmax)

        ratio = (lincntmax-bg[0])/lindtmax
        iratio = np.argsort(ratio)
        print ("Amount of data", len(iratio))
        ctrn = int(len(iratio)*0.99)
        if ctrn > 3:
            iratiomax = iratio[-ctrn:]
        else:
            iratiomax = iratio
        xm = np.mean(lindtmax[iratiomax])
        ym = np.mean(lincntmax[iratiomax])
        m = (ym-bg[0])/xm
        print ('dT-Cound max:',m)
        def lin(x,m,t):
            return m*x+t
        lin_vals, covar = curve_fit(lin, tmax, tcntmax)
        print ('LineFit vals:',lin_vals[0],lin_vals[1])
        
        # Linear model and exponential model
        xlin = np.asarray(range(int(np.min(abstarray)),int(np.max(abstarray)),1))
        xlindt = np.asarray(range(int(np.min(deltatarray)),int(np.max(deltatarray)),1))
        #init_vals = np.asarray([1000., 60000., 14500.])     # for [amp, cen, wid]
        print ("Starting exponential fit ...", tmax, tcntmax)
        init_vals = [40482539837383.,60000.,bg[0]]
        best_vals, covar = curve_fit(dc2, tmax, tcntmax, p0=init_vals)
        print ("Best values", float(best_vals[0]), float(best_vals[1]), float(best_vals[2]))

        ### dT vs MaxCount relationship plus maximum envelope
        ### -------------------------------------
        fig = plt.figure(1)
        ax = fig.add_subplot(1,1,1)
        plt.xlabel("dT (maxima) [$^\circ$C]")
        plt.ylabel("Cnt (maxima)")
        plt.xlim(-2,25)
        plt.ylim(10000,40000)
        plt.plot(dtmax,dtcntmax,'.',color='darkred')
        plt.plot(dtmax[iratiomax],dtcntmax[iratiomax],'.',color='darkred')
        plt.plot(xlindt, m*xlindt+bg[0],'--',color='blue')
        pylab.savefig(os.path.join(graphdir,'sca_param_lin.png')) 
        plt.show()
        fig = plt.figure(1)
        ax = fig.add_subplot(1,1,1)
        plt.xlabel(u"T (maxima) [K]")
        plt.ylabel("Cnt (maxima)")
        plt.plot(tmax,tcntmax,'.',color='darkred')
        #plt.plot(tmax[iratiomax],tcntmax[iratiomax],'o',color='darkred')
        plt.plot(xlin, dc2(xlin, best_vals[0], best_vals[1], best_vals[2]), '--',color='magenta')
        plt.plot(xlin, lin_vals[0]*xlin+lin_vals[1], '--',color='green')
        pylab.savefig(os.path.join(graphdir,'sca_param_exp.png')) 
        plt.show()
        E = best_vals[1]
        c0 = best_vals[0]
        c = best_vals[2]
        tbg = lin_vals[1]
        tm = lin_vals[0]

    ### Average time delay (min) between dT max and following Count max
    ### -------------------------------------
    print ("Mean delay", np.mean(dtdelaymax), len(dtdelaymax))
    fig = plt.figure(1)
    plt.xlabel("Delay [minutes]")
    plt.ylabel("N")
    n, bins, patches = plt.hist(dtdelaymax, 5, facecolor='darkred', alpha=0.5 ) #, normed=1)
    # add a 'best fit' line
    # calculate binmiddles
    bin_middles = 0.5*(bins[1:] + bins[:-1])
    parameters, cov_matrix = curve_fit(poisson, bin_middles, n) 
    """
    result = minimize(negLogLikelihood,  # function to minimize
                  x0=np.ones(1),     # start value
                  args=(dtdelaymax,),      # additional arguments for function
                  method='Powell',   # minimization method, see docs
                  )
    # result is a scipy optimize result object, the fit parameters 
    # are stored in result.x
    print(result)
    """
    x_plot = np.asarray(range(0, max(dtdelaymax), 10))
    plt.plot(x_plot, poisson(x_plot, *parameters), 'r-', lw=2)
    #y = mlab.normpdf( bins, mu, sigma)
    #l = plt.plot(bins, y, 'r--', linewidth=1)
    pylab.savefig(os.path.join(graphdir,'sca_param_2.png')) 
    plt.show()
    delay = np.mean(dtdelaymax)

    ### Delay independent of dT and MaxCount -> reason for using simple reduction/accumulation rates
    ### -------------------------------------
    """
    fig = plt.figure(1)
    ax = fig.add_subplot(1,1,1)
    plt.xlabel("Delay (maxima)")
    plt.ylabel("dT (maxima)")
    plt.plot(delaymax,dtmax,'.',color='darkred')
    plt.show()
    fig = plt.figure(1)
    ax = fig.add_subplot(1,1,1)
    plt.xlabel("Delay (maxima)")
    plt.ylabel("Cnt (maxima)")
    plt.plot(delaymax,cntmax,'.',color='darkred')
    pylab.savefig(os.path.join(graphdir,'sca_param_3.png')) 
    plt.show()
    """

    return (m, delay, E, c0, c, tm, tbg)



"""
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
++++++++++++++++++++++++++++ Spectral analysis +++++++++++++++++++++++++++++++
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
"""

