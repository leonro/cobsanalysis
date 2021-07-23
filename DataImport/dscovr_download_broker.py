from magpy.stream import read
import magpy.mpplot as mp
from datetime import datetime, timedelta

# Define ranges:
today = datetime.utcnow()
yesterday = today - timedelta(days=1)
starttime = datetime.strftime(yesterday, "%Y-%m-%d") + "T00:00:00"
endtime =   datetime.strftime(today, "%Y-%m-%d") + "T00:00:00"

# Read data and trim to ranges (for yesterday):
dscovr_plasma = read("http://services.swpc.noaa.gov/products/solar-wind/plasma-3-day.json")
dscovr_plasma = dscovr_plasma.trim(starttime=starttime, endtime=endtime)
dscovr_mag = read("http://services.swpc.noaa.gov/products/solar-wind/mag-3-day.json")
dscovr_mag = dscovr_mag.trim(starttime=starttime, endtime=endtime)

# Fill out header information:
dscovr_plasma.header['unit-col-var1'] = "cm^-3"
dscovr_plasma.header['unit-col-var2'] = "km/s"
dscovr_plasma.header['unit-col-var3'] = "K"
dscovr_mag.header['unit-col-x'] = "nT"
dscovr_mag.header['unit-col-y'] = "nT"
dscovr_mag.header['unit-col-z'] = "nT"
dscovr_mag.header['unit-col-f'] = "nT"
dscovr_mag.header['unit-col-var1'] = "deg"
dscovr_mag.header['unit-col-var2'] = "deg"

# Write data to archive:
dscovrpath = "/home/cobs/SPACE/incoming/DSCOVR/collected/"
dscovr_plasma.write(dscovrpath, format_type="PYCDF", filenamebegins="DSCOVR_plasma_")
dscovr_mag.write(dscovrpath, format_type="PYCDF", filenamebegins="DSCOVR_mag_")

print ("DSCOVR data written successfully.")
print ("-----------------------------")
print ("SUCCESS")

"""
downloaddict = {"dscovr_plasma" :  {"source"="http://services.swpc.noaa.gov/products/solar-wind/plasma-3-day.json", 
                                    "savename"="DSCOVR_plasma_", 
                                    "format"="PYCDF", 
                                    "header"= {"unit-col-var1"="cm^-3", "unit-col-var2"="km/s", "unit-col-var3"="K"} }
                "dscovr_mag" :  {"source"="http://services.swpc.noaa.gov/products/solar-wind/plasma-3-day.json", 
                                    "savename"="DSCOVR_plasma_", 
                                    "format"="PYCDF", 
                                    "header"= {"unit-col-var1"="cm^-3", "unit-col-var2"="km/s", "unit-col-var3"="K"} } 
               }
"""
