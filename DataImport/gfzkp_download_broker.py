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

kppath = '/home/cobs/SPACE/incoming/gfz/kp'

# READ GFZ KP DATA:
# -----------------
kp = read(path_or_url='http://www-app3.gfz-potsdam.de/kp_index/qlyymm.tab')

# Append that data to local list:
kp.write(kppath,filenamebegins='gfzkp',format_type='PYCDF',dateformat='%Y%m',coverage='month')

print ("Kp File written successfully.")
print ("-----------------------------")
print ("SUCCESS")
