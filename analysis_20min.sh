#!/bin/sh

ANALYSIS='/home/cobs/ANALYSIS'
# make a permanent vaiable in environment
TLOG="$ANALYSIS/Logs/20min.log"
DATE="/bin/date"
ECHO="/bin/echo"
PYTHONPATH="/home/cobs/anaconda2/bin/python"
PATH=/home/cobs/anaconda2/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games

### Uploading files to the databank
### ###############################

### Get data from a password protected ftp site 
###  add credentials and address first by addcred.py
$DATE > $TLOG
$ECHO "1. Get earthquake information" >> $TLOG
$PYTHONPATH /home/cobs/ANALYSIS/FileDownloads/neic_download.py >> $TLOG 2>&1
$DATE >> $TLOG
$ECHO "2. Send Telegram Quake Info" >> $TLOG
$PYTHONPATH /home/cobs/ANALYSIS/Info/tg_quake.py >> $TLOG 2>&1
$DATE >> $TLOG


