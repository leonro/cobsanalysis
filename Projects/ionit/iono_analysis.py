#!/usr/bin/env python

from magpy.stream import *   
import magpy.mpplot as mp


### READING IM806 Data

impath = '/srv/projects/ionit/data_bohrlochraum'
envpath = '/srv/projects/radon/tables'
radonpath = '/srv/projects/radon/tables'
meteopath = '/srv/products/data/meteo'

#imfile = imfiles[-1]
tfile = 'env-box-outside-sht-1min_20*'
pfile = 'env-box-outside-bmp-1min_20*'
rfile = 'sca-tunnel-1min_20*'
mfile = 'meteo*'

# Define some important times:
startofborehole = '2016-02-18'

print ("Running ionit analysis ...")

rdata = read(os.path.join(radonpath,rfile),starttime='2016-12-01')
#print (rdata.ndarray[0])
#mp.plot(rdata)
mdata = read('/srv/products/data/meteo/meteo*', starttime='2016-12-01')
#mp.plot(mdata)

#sys.exit()


# Run the analysis and plots
imdata = read(os.path.join(impath,'*'))
print (imdata.length())
#mp.plot(imdata)

st,et = imdata._find_t_limits()
st = startofborehole

imdata = imdata.trim(starttime=st,endtime=et)
tdata = read(os.path.join(envpath,tfile),starttime=st,endtime=et)
pdata = read(os.path.join(envpath,pfile),starttime=st,endtime=et)
rdata = read(os.path.join(radonpath,rfile),starttime=st,endtime=et)
mdata = read(os.path.join(meteopath,mfile),starttime=st,endtime=et)
#mp.plot(envdata)
mp.plotStreams([imdata,tdata,pdata,rdata,mdata],[['y','t1'],['t1','var1'],['x'],['x'],['f']],noshow=True)
pltsavepath = "/srv/projects/ionit/graphs/ionit-timeseries.png"
plt.savefig(pltsavepath)

