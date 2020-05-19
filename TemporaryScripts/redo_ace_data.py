from magpy.stream import *
import magpy.mpplot as mp
from datetime import datetime, timedelta

def merge_ACE(streama, streamb, keys):
    # Merge streamb into streama without interpolation

    for key in keys:
        a = streamb._get_column(key)
        streama._put_column(a, key)
        streama.header['col-'+key] = streamb.header['col-'+key]
        streama.header['unit-col-'+key] = streamb.header['unit-col-'+key]

    return streama


path = "/srv/archive/external/esa-nasa/ace/raw/"

start = datetime.strptime("2016-07-27", "%Y-%m-%d")
end = datetime.strptime("2016-08-01", "%Y-%m-%d")
#end = datetime.strptime("2015-12-15", "%Y-%m-%d")

while start <= end:

    date = datetime.strftime(start, "%Y%m%d")
    swe_file = date+"_ace_swepam_1m.txt"
    mag_file = date+"_ace_mag_1m.txt"
    ace_swe = read(os.path.join(path, swe_file))
    ace_mag = read(os.path.join(path, mag_file))

    ace_1min = merge_ACE(ace_swe, ace_mag, ['x','y','z','f','t1','t2'])
    ace_1min.write("files", filenamebegins="ace_1m_", format_type="PYCDF")

    print("Reading %s..." % date)
    epa_file = date+"_ace_epam_5m.txt"
    sis_file = date+"_ace_sis_5m.txt"
    ace_epa = read(os.path.join(path, epa_file))
    ace_sis = read(os.path.join(path, sis_file))

    ace_5min = merge_ACE(ace_epa, ace_sis, ['x','y'])
    ace_5min._print_key_headers()
    #mp.plot(ace_5min)
    ace_5min.write("files", filenamebegins="ace_5m_", format_type="PYCDF", skipcompression=True)

    start = start + timedelta(days=1)
