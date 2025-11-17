import sys,os
import zipfile
from magpy.stream import *




def ggp_prodc(ym, datype, outf):
    y,m=ym.split('-')[0], ym.split('-')[1]
    fpath='/srv/archive/SGO/IGRAV_050_0001/raw/'+y
    #write IGETS file
    if True:
        stream=read( fpath+'/Data_iGrav050_'+m+'*.tsf', channels='1,2')#, debug=True)
        print('data read')
        if False:
            from magpy.core import plot as mp
            mp.tsplot(stream)
            from matplotlib import pyplot as plt
            plt.show()
        #filename
        instrument='IGRAV'
        sensor='co050'
        code='00'
        #fbegin='IGETS-'+instrument+'-'+datype+'-'+sensor+'-'+year+month+code #added lines 6375-76 in stream.py for correct ggp name...
        fext='.ggp'
        #header info
        stream.header['SensorID']='GWR IGRAV050'
        stream.header['StationName']='Conrad Observatory'
        stream.header['StationCountry']='Austria'
        stream.header['StationEmail']='P. Arneitz (patrick.arneitz@geosphere.at)'
        stream.header['StationLatitude']=  '  47.9283     .0001          measured'
        stream.header['StationLongitude']= '  15.8598     .0001          measured'
        stream.header['StationElevation']= '1044.1200     .0100          measured'



        if datype=='MIN':
            print('preparing minute data')
            #replace gaps before filtering
            stream = stream.get_gaps()#debug=True)
            stream=stream.filter(keys=['x','y'], filter_type = 'gaussian', resample_period=60, filter_width = timedelta(minutes=2), noresample=False)
            fbegin='IGETS-'+instrument+'-'+'MIN'+'-'+sensor+'-'+y+m+code
            stream.write(outf, filenamebegins=fbegin, filenameends=fext, format_type='GGP', coverage='month', debug=True)
        else:
            print('preparing second data')
            #no filter for sec data
            fbegin='IGETS-'+instrument+'-'+'SEC'+'-'+sensor+'-'+y+m+code
            stream.write(outf, filenamebegins=fbegin, filenameends=fext, format_type='GGP', coverage='month', debug=True)
            fsname=fbegin+fext #for zipping
            if datype=='SECMIN':
                #replace gaps before filtering
                stream = stream.get_gaps()#debug=True)
                stream=stream.filter(keys=['x','y'], filter_type = 'gaussian', resample_period=60, filter_width = timedelta(minutes=2), noresample=False)
                fbegin='IGETS-'+instrument+'-'+'MIN'+'-'+sensor+'-'+y+m+code
                stream.write(outf, filenamebegins=fbegin, filenameends=fext, format_type='GGP', coverage='month', debug=True)
            #zipping second file
            file_path = os.path.join(outf, fsname)
            zip_name = fsname.replace('ggp','zip')
            zip_path = os.path.join(outf, zip_name)
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                # arcname controls how the file appears inside the zip
                zipf.write(file_path, arcname=os.path.basename(file_path))
            os.remove(file_path)
        print('finish')
                


if __name__ == '__main__':
    ym=sys.argv[1]
    datype=sys.argv[2]
    outf=sys.argv[3]
    if True:
        print('Generating ggp product for %s' % ym)
    ggp_prodc(ym, datype, outf)