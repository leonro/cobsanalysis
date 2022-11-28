#!/usr/bin/env python
#********************************************************************
# Regularly download GFZ kp data.
#
# Cronjob activated 2016-02-26 on Vega.
#	$ crontab -e
#	24 2,5,8,11,14,17,20,23 * * * python /home/cobs/CronScripts/kpdownload/gfzkp_download.py
#	(Job runs every three hours.)
#
#********************************************************************

import sys
from magpy.stream import *
from magpy.transfer import *
import urllib.request
from datetime import datetime

kppath = '/home/cobs/SPACE/incoming/GFZ/kp'
kpplotpath = '/home/cobs/SPACE/graphs/kp_latest.png'

# READ GFZ KP DATA:
# -----------------
kp = read(path_or_url='http://www-app3.gfz-potsdam.de/kp_index/qlyymm.tab')

# Append that data to local list:
kp.write(kppath,filenamebegins='gfzkp',format_type='PYCDF',dateformat='%Y%m',coverage='month')

# DOWNLOAD CURRENT KP PLOT:
# -------------------------
today = datetime.utcnow().strftime("%Y-%m-%d")
url_plot = "https://kp.gfz-potsdam.de/fileadmin/kp-archiv/png/{}.png".format(today)
urllib.request.urlretrieve(url_plot, kpplotpath)

print ("Kp File written successfully.")
print ("-----------------------------")
print ("SUCCESS")
