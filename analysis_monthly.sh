#!/bin/sh

ANALYSIS='/home/cobs/ANALYSIS'
# make a permanent vaiable in environment
TLOG="$ANALYSIS/Logs/monthly.log"


### Uploading files to the databank
### ###############################


###  Project applications
python /home/cobs/ANALYSIS/Projects/geoelectric/wenner_sgo/GeoelekTimeSeries.py > $TLOG 2>&1
python /home/cobs/ANALYSIS/Projects/ionit/iono_analysis.py > $TLOG 2>&1
# Still missing - spectral...
