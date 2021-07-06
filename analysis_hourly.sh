#!/bin/sh

ANALYSIS='/home/cobs/ANALYSIS'
# make a permanent vaiable in environment
TLOG="$ANALYSIS/Logs/hourly.log"
DATE="/bin/date"
ECHO="/bin/echo"

### Uploading files to the databank
### ###############################

### Get data from a password protected ftp site 
###  add credentials and address first by addcred.py
$DATE > $TLOG
$ECHO "1. Title graphs" >> $TLOG
$ECHO "moved to 117 (sagittarius)" >> $TLOG
#python /home/cobs/ANALYSIS/TitleGraphs/mag_graph.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/TitleGraphs/weather_graph.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/TitleGraphs/gravity_graph.py >> $TLOG 2>&1

### Periodic plots
$ECHO "2. Periodic plots" >> $TLOG
$ECHO "moved to 117 (sagittarius)" >> $TLOG
#python /home/cobs/ANALYSIS/PeriodicGraphs/tilt_graph.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/PeriodicGraphs/magactivity_graph.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/PeriodicGraphs/supergrad_graph.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/PeriodicGraphs/gamma_graph.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/PeriodicGraphs/spaceweather_graph.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/PeriodicGraphs/gravity_graph.py >> $TLOG 2>&1

$DATE >> $TLOG

### DataProducts
$ECHO "3. DataProducts" >> $TLOG
$ECHO "moved to 117 (sagittarius)" >> $TLOG
#python /home/cobs/ANALYSIS/DataProducts/current_weather.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/DataProducts/current_weather_new.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/DataProducts/magnetism_products_hourly.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/DataProducts/rcsdata_upload.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/DataProducts/gin_upload.py >> $TLOG 2>&1
#bash /home/cobs/ANALYSIS/DataProducts/GIC/cleanup_tempfiles.sh
#python /home/cobs/ANALYSIS/DataProducts/GIC/transfer_gic_anim.py
#python /home/cobs/ANALYSIS/DataProducts/StormDetection/StormDetector_graph.py
#python /home/cobs/ANALYSIS/DataProducts/meteo_upload.py >> $TLOG 2>&1 -> moved to SCRIPTS daily
#python /home/cobs/ANALYSIS/DataProducts/radon_project.py >> $TLOG 2>&1

$DATE >> $TLOG
