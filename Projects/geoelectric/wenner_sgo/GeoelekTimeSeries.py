# Manual: http://www.l-gm.de/documents/4pointlight10w_v4_58en.pdf
from magpy.stream import *
import magpy.mpplot as mp

def readLMEL(filename):

    fh = open(filename, 'rt')
    newdatablock = False
    headerblock = True
    foundfirstsingleinput = False
    datet = ''
    val1 = np.nan
    val2 = np.nan
    val3 = np.nan
    val4 = np.nan
    val5 = np.nan
    val6 = np.nan
    val7 = np.nan
    array = [[] for el in KEYLIST]
    posx = KEYLIST.index('x')
    posy = KEYLIST.index('y')
    posz = KEYLIST.index('z')
    posf = KEYLIST.index('f')
    post1 = KEYLIST.index('t1')
    post2 = KEYLIST.index('t2')
    posvar1 = KEYLIST.index('var1')
    posvar2 = KEYLIST.index('var2')
    posvar3 = KEYLIST.index('var3')
    stream = DataStream()

    for line in fh:
            cont = line.split()
            if line.isspace():
                # blank line
                continue
            elif line == 'E':
                # blank line
                print ("Found End")
                break
            elif len(cont) == 2:
                if not headerblock:
                    # Test if first element is a date:
                    try:
                        datet = datetime.strptime(cont[0]+'_'+cont[1],"%d.%m.%Y_%H:%M:%S")
                        #print ("Found new data block", datet, line)
                        newdatablock = True
                    except:
                        pass
                else:
                    print ("Header Line", line)
            elif len(cont) == 1:
                if newdatablock == True and foundfirstsingleinput == False and not cont[0] == 'E':
                    foundfirstsingleinput = True
                    val1 = float(cont[0])
                elif newdatablock == True and foundfirstsingleinput == True and not cont[0] == 'E':
                    val2 = float(cont[0]) # voltage
                    foundfirstsingleinput = False
                else:
                    print ("Eventually found header element", cont[0])
            elif len(cont) == 6:
                if newdatablock == True:
                    # found data line
                    vallst = []
                    for el in cont:
                        vallst.append(float(el))
                    if vallst[0] == 0.0 and vallst[1] == 0.0:
                        for el in cont:
                            vallst.append(np.nan)
                    else:
                        if not vallst[2] in [0,np.nan]:
                            array[0].append(date2num(datet))
                            array[posx].append((vallst[0]/vallst[2]))  # U[0][mV]/I[mA] = R resistivity
                            array[posy].append((vallst[1]/vallst[2]))  # U[90][mV]/I[mA] = R Imaginary
                            array[post1].append(val2)  # Supply-Voltage
                            array[posvar1].append(vallst[3])  # DeltaU[0]   -> check unit %?
                            array[posvar2].append(vallst[4])  # DeltaU[90]
                            array[posvar3].append(vallst[5])  # ??
                            array[posz].append(vallst[2])  # I [mA]
                            array[posf].append(val1)
                    newdatablock = False
            elif len(cont) == 4:
                print ("Still within header. Found", line)
                headerblock = False
                stream.header['SensorID'] = 'LP4_'+serial.strip('.')+'_0001'
                stream.header['SensorDate'] = constructiondate
            else:
                serial = cont[1]
                constructiondate = datetime.strptime(cont[2].strip('\n'),"%d.%m.%Y")
                print ("Header...:", line)

    fh.close()
    stream.ndarray = np.asarray(array)
    return stream

print ("Running wenner analysis ... (GeoelekTimeSeries.py)")

mydir = "/srv/projects/geoelectrics/Daten-Geoelektrik/"
data = DataStream()
for fi in os.listdir(mydir):
    if fi.startswith("geoelektrik_mon"):
        print(os.path.join(mydir, fi))
        filename = os.path.join(mydir, fi)
        cdata = readLMEL(filename)
        data.extend(cdata.container,cdata.header,cdata.ndarray)

data = data.sorting()
data = data.get_gaps()
data = data.offset({'time':timedelta(hours=-2)})

"""
data = DataStream()
data1 = readLMEL('/home/leon/CronScripts/Geoelektrik/geoelektrik_monitoring-2017-04-01.txt')
data.extend(data1.container,data1.header,data1.ndarray)
data2 = readLMEL('/home/leon/CronScripts/Geoelektrik/geoelektrik_monitoring-2017-05-04.txt')
data.extend(data2.container,data2.header,data2.ndarray)
"""
#mp.plot(data)

meteo = read('/srv/products/data/meteo/meteo-1min*')
print (meteo._get_key_headers())
#mp.plot(meteo)


# Create a small report and add the following plot -> send out per mail ???
mp.plotStreams([data,meteo],variables=[['x'],['y','z']],noshow=True)
pltsavepath = "/srv/projects/geoelectrics/graphs/wenner_timeseries.png"
plt.savefig(pltsavepath)
#print ("Result", data)
