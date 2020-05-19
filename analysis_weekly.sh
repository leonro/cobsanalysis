#!/bin/bash

ANALYSIS='/home/cobs/ANALYSIS'
# make a permanent vaiable in environment
TLOG="$ANALYSIS/Logs/weekly.log"
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

# Upload Telegram messages
$PYTHONPATH /home/cobs/ANALYSIS/Info/tg_base.py >> $TLOG 2>&1

