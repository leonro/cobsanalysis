#!/bin/sh

ANALYSIS='/home/cobs/ANALYSIS'
# make a permanent vaiable in environment
TLOG="$ANALYSIS/Logs/5min.log"
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
$ECHO "1. Identify primary instrument" >> $TLOG
$PYTHONPATH /home/cobs/ANALYSIS/DataProducts/getprimary.py -c /home/cobs/CONF/wic.cfg >> $TLOG 2>&1
$DATE >> $TLOG
$ECHO "2. Automatic flags" >> $TLOG
$PYTHONPATH /home/cobs/ANALYSIS/DataProducts/flagging.py -c /home/cobs/CONF/wic.cfg -j flag >> $TLOG 2>&1
$DATE >> $TLOG
$ECHO "3. Running magnetic analysis" >> $TLOG
$PYTHONPATH /home/cobs/ANALYSIS/DataProducts/magnetism_products.py -c /home/cobs/CONF/wic.cfg >> $TLOG 2>&1
$DATE >> $TLOG
$ECHO "4. Activity information " >> $TLOG
$PYTHONPATH /home/cobs/ANALYSIS/Info/tg_kval.py >> $TLOG 2>&1
$DATE >> $TLOG

