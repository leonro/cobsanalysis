#!/bin/sh

ANALYSIS='/home/cobs/ANALYSIS'
# make a permanent vaiable in environment
TLOG="$ANALYSIS/Logs/5min.log"


### Uploading files to the databank
### ###############################

### Get data from a password protected ftp site 
###  add credentials and address first by addcred.py
#python /home/cobs/ANALYSIS/DataProducts/getprimary.py > $TLOG 2>&1
#python /home/cobs/ANALYSIS/DataProducts/magnetism_products.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/DataProducts/lemi_data.py > $TLOG 2>&1
#python /home/cobs/ANALYSIS/PeriodicGraphs/magnetism_graph.py >> $TLOG 2>&1

