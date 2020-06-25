#!/bin/bash

ANALYSIS='/home/cobs/ANALYSIS'
# make a permanent vaiable in environment
TLOG="$ANALYSIS/Logs/daily.log"
DATE="/bin/date"
ECHO="/bin/echo"
PYTHONPATH="/home/cobs/anaconda2/bin/python"
PATH=/home/cobs/anaconda2/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games

#source /home/cobs/anaconda2/bin/activate /home/cobs/anaconda2/envs/magpyenv

### Uploading files to the databank
### ###############################

### Get data from a password protected ftp site 
###  add credentials and address first by addcred.py
$DATE > $TLOG
$ECHO "1. Gamma" >> $TLOG
## Scripts expects data sets in /srv/projects/gamma
## -> these data sets are updated permanently by the following jobs:
## -> 1) MCA data: Themisto Scripts GAMMA if missing check mount of /media/win
## -> 2) Arduino data: Themisto MARTAS
## -> 3) iono: Themisto
## -> 4) from themisto to projects: on vega -> /SCRIPTS/collect.sh

$PYTHONPATH /home/cobs/ANALYSIS/Projects/gamma/gamma.py -p /srv/projects/gamma/ -m /home/cobs/ANALYSIS/Projects/gamma/mygamma.cfg -d /srv/projects/gamma/ >> $TLOG 2>&1

$DATE >> $TLOG

$PYTHONPATH /home/cobs/ANALYSIS/DataProducts/magnetism_checkadj.py

$DATE >> $TLOG

$PYTHONPATH /home/cobs/ANALYSIS/FileUploads/file_upload_qd.py >> $TLOG
