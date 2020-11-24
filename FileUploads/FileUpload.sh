#!/bin/bash
# shell script to call MARTAS/app/file_upload
# configuration data in ~/ANALYSIS/conf

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# At the moment there is no connection between sag and broker
# job running at server 195 (and prepared on 205)
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

cd /home/cobs/MARTAS/app
/home/cobs/anaconda2/bin/python file_upload.py -j /home/cobs/ANALYSIS/conf/uploadjobs.json -m /home/cobs/ANALYSIS/conf/jobmemory.json
