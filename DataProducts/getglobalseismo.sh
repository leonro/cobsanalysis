#!/bin/sh

# Settings for Cron
#export GMTHOME=/usr/local/GMT4.5.8
export PATH=/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:$PATH
#export NETCDFHOME=/usr/local/netcdf-3.6.3

# takes about 1 min to finish

BASEPATH=/home/cobs/ANALYSIS/Seismo
GMTPATH=/etc/GMT

# ---------------------------------------------------------------
# Extract earthquake data from USGS catalog
# ---------------------------------------------------------------
QUAKEFILE=$BASEPATH/neic_quakes.d

#wget http://earthquake.usgs.gov/earthquakes/feed/csv/4.5/week -q -O $QUAKEFILE
wget http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.csv -q -O $QUAKEFILE
