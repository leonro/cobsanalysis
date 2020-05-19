#!/bin/bash

ANALYSIS='/home/cobs/ANALYSIS'
# make a permanent vaiable in environment
TLOG="$ANALYSIS/Logs/hourly.log"
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
$ECHO "1. Title graphs" >> $TLOG
$ECHO "------------------------------------------" >> $TLOG
$PYTHONPATH /home/cobs/ANALYSIS/TitleGraphs/mag_graph.py >> $TLOG 2>&1
$PYTHONPATH /home/cobs/ANALYSIS/TitleGraphs/weather_graph.py >> $TLOG 2>&1
# -> gravity temporarly deactivated Jan 2019 
#$PYTHONPATH /home/cobs/ANALYSIS/TitleGraphs/gravity_graph.py >> $TLOG 2>&1

### Periodic plots
$ECHO "2. Periodic plots" >> $TLOG
$ECHO "------------------------------------------" >> $TLOG
$PYTHONPATH /home/cobs/ANALYSIS/PeriodicGraphs/tilt_graph.py >> $TLOG 2>&1
$PYTHONPATH /home/cobs/ANALYSIS/PeriodicGraphs/supergrad_graph.py >> $TLOG 2>&1
$PYTHONPATH /home/cobs/ANALYSIS/PeriodicGraphs/gamma_graph.py >> $TLOG 2>&1
$PYTHONPATH /home/cobs/ANALYSIS/PeriodicGraphs/spaceweather_graph.py >> $TLOG 2>&1
$PYTHONPATH /home/cobs/ANALYSIS/PeriodicGraphs/current_weatherchanges.py >> $TLOG 2>&1
# -> gravity temporarly deactivated Jan 2019 
#$PYTHONPATH /home/cobs/ANALYSIS/PeriodicGraphs/gravity_graph.py >> $TLOG 2>&1

# needs approximately 6 min (8min on vega with all other jobs active)
$DATE >> $TLOG

# Upload Telegram messages
$ECHO "3. Info channels" >> $TLOG
$ECHO "------------------------------------------" >> $TLOG
#$PYTHONPATH /home/cobs/ANALYSIS/Info/tg_quake.py >> $TLOG 2>&1 # moved to 20min
$PYTHONPATH /home/cobs/ANALYSIS/Info/tg_pha.py >> $TLOG 2>&1

### DataProducts
$ECHO "4. Data Products" >> $TLOG
$ECHO "------------------------------------------" >> $TLOG
$PYTHONPATH /home/cobs/ANALYSIS/DataProducts/current_weather.py >> $TLOG 2>&1
$PYTHONPATH /home/cobs/ANALYSIS/DataProducts/radon_project.py >> $TLOG 2>&1
# -> autodif check temporarly deactivated Jan 2019 8was magnetism_products_hourly before
#$PYTHONPATH /home/cobs/ANALYSIS/DataProducts/autodif.py >> $TLOG 2>&1
$PYTHONPATH /home/cobs/ANALYSIS/DataProducts/rcsdata_upload.py >> $TLOG 2>&1
##python /home/cobs/ANALYSIS/DataProducts/gin_upload.py >> $TLOG 2>&1
bash /home/cobs/ANALYSIS/DataProducts/GIC/cleanup_tempfiles.sh
python /home/cobs/ANALYSIS/DataProducts/GIC/transfer_gic_anim.py
python /home/cobs/ANALYSIS/DataProducts/StormDetection/StormDetector_graph.py
##python /home/cobs/ANALYSIS/DataProducts/meteo_upload.py >> $TLOG 2>&1 ### still at vega/SCRIPTS
source activate helioenv
# This replaces old version solarwind_to_gic.py
python /home/cobs/ANALYSIS/DataProducts/gicpred/predict_GICs.py
source deactivate
source activate updatedenv
#python /home/cobs/ANALYSIS/DataProducts/GIC/src/solarwind_to_gic.py >> $TLOG 2>&1
#python /home/cobs/ANALYSIS/DataProducts/GIC/src/solarwind_to_gic.py --predstorm >> $TLOG 2>&1
LASTGICFILE=$(ls -t /home/cobs/ANALYSIS/DataProducts/GIC/tempfiles/gicfiles/ | head -1)
(cd /home/cobs/ANALYSIS/DataProducts/GIC && python src/gic_dailyplot.py -f $LASTGICFILE -m db -t station >> $TLOG 2>&1)
source deactivate

$DATE >> $TLOG
