# ANALYSIS Scripts at the Conrad Observatory

## 1. Introduction

The following README contains an overview about all analysis scripts used at the Conrad Observatory. A short description of each script is accompanied by dependencies and some typical schedules of the respective application. A detailed description of how the analysis scripts interact with MARTAS and MARCOS machines at the Conrad Observatory, and how different script are used for consecutive automatic data analysis is to be found in the README.md document of the MarcosScripts repository. While this "ANALYSIS" repository contains detailed descriptions and application examples of individual scripts, the "Marcos" repository README will focus on application examples for testing and production based an optimal usage of various different scripts. 


## 2. Overview


| Script        | Location | Schedule         | Config        | Version | Dependencies | Tested          |
|---------------|----------|------------------|---------------|---------|--------------|-----------------|
| adjusted.py   | products | 5min             | baseline.cfg  | 2.0.0   | martas       | martas-analysis |
| weather.py    | products | hourly           | weather.cfg   | 2.0.0   | martas       | app_tester      |
| flag.py       | products | 5min             | flagdict.json | 2.0.0   | martas       | martas-analysis |



| Script                      | Location        | Schedule         | Config          | Analysis2.0     | Dependencies | Comments          |
|-----------------------------| --------------- |------------------|-----------------|-----------------|--------------|-------------------|
| magnetism\_products.py      | DataProducts    | 5min             | wic.cfg         | adjusted        |              | QD missing        |
| weather\_products.py        | DataProducts    | hourly           | wic.cfg         | weather.py      |              |                   |
| getprimary.py               | DataProducts    | 5min             | wic.cfg         | --              | martas.core  | done              |
| ggp\_products.py            | DataProducts    |                  |                 |                 |              | DEVELOP           |
| gravity\_products.py        | DataProducts    | hourly           |                 |                 |              |                   |
| obsstatus\_products.py      | DataProducts    |                  |                 |                 |              |                   |
| tilt\_products.py           | DataProducts    |                  |                 |                 |              |                   |
| flagdata.py                 | DataProducts    |                  |                 | --              |              | DEFUNC            |
| flagging.py                 | DataProducts    | 5min             | wic.cfg         | flag.py         |              | done              |
| gamma\_products.py          | DataProducts    | hourly           | wic.cfg         |                 |              |                   |
| magnetism\_checkadj.py      | DataProducts    | daily            | wic.cfg         |                 |              |                   |
| baseline\_generator.py      | DataProducts    | to daily         | gam.cfg/swz.cfg |                 |              |                   |
| convert\_data.py            | DataProducts    | hourly           | wic.cfg         | weather.py?     |              |                   |
| create\_meteo\_files.py     | DataProducts    | daily            |                 | weather.py?     |              |                   |
| tg\_pha.py                  | Info            | analysis_20min   |                 | MARTAS telegram |              |                   |
| tg\_kval.py                 | Info            | 5min             |                 | MARTAS telegram |              |                   |
| tg\_quake.py                | Info            | analysis_20min   |                 | MARTAS telegram |              |                   |
| tg\_base.py                 | Info            | weekly           | wic.cfg         | MARTAS telegram |              | py3               |
| logfiledate.py              | Info            | weekly           | wic.cfg         | MARTAS telegram |              | py2 ... py3       |
| quakes\_import.py           | DataImport      |                  | wic.cfg         | MARTAS telegram |              | py3               |
| dscovr\_download\_broker.py | DataImport      |                  | -               | Logfile monitor |              | running on broker |
| gfzkp\_download\_broker.py  | DataImport      |                  | -               | Logfile monitor |              | running on broker |
| ace\_conversion.py          | DataImport      |                  |                 | MARTAS telegram |              |                   |
| wbvimport.py                | DataImport      |                  |                 | MARTAS telegram | pandas, xlrd | py3               |
| mag\_graph.py               | TitleGraphs     | analysis_20min   |                 | MARTAS telegram |              |                   |
| weather\_graph.py           | TitleGraphs     | analysis_20min   |                 | MARTAS telegram |              |                   |
| general\_graph.py           | PeriodicGraphs  | analysis_hourly  |                 | MARTAS telegram |              | py2 and py3       |
| tilt\_graph.py              | PeriodicGraphs  | analysis_hourly  |                 | MARTAS telegram |              |                   |
| supergrad\_graph.py         | PeriodicGraphs  | analysis_hourly  |                 | MARTAS telegram |              |                   |
| gamma\_graph.py             | PeriodicGraphs  | analysis_hourly  |                 | MARTAS telegram |              |                   |
| spaceweather\_graph.p       | PeriodicGraphs  | analysis_hourly  |                 | MARTAS telegram |              |                   |
| current\_weatherchanges.py  | PeriodicGraphs  | analysis_hourly  |                 | MARTAS telegram |              |                   |
| gamma.py                    | Projects/gamma/ | analysis_daily   |                 | MARTAS telegram |              |                   |
| iono\_analysis.py           | Projects/ionit  | analysis_monthly |                 | MARTAS telegram |              |                   |
| GeoelekTimeSeries.py        | Projects/geoelectric/wenner\_sgo/ | analysis_monthly |                 | MARTAS telegram |              |                   |


>bash /home/cobs/ANALYSIS/DataProducts/GIC/cleanup_tempfiles.sh
>python /home/cobs/ANALYSIS/DataProducts/GIC/transfer_gic_anim.py
>python /home/cobs/ANALYSIS/DataProducts/StormDetection/StormDetector_graph.py
>python /home/cobs/ANALYSIS/DataProducts/gicpred/predict_GICs.py
>LASTGICFILE=$(ls -t /home/cobs/ANALYSIS/DataProducts/GIC/tempfiles/gicfiles/ | head -1)
>(cd /home/cobs/ANALYSIS/DataProducts/GIC && python src/gic_dailyplot.py -f $LASTGICFILE -m db -t station >> $TLOG 2>&1)


## 3. Descriptions of DataProducts scripts

The folder DataProducts contains scripts which aim on the production of data sets to be published and distributed. Basically the scripts read raw data contents from sensors. Then they apply flagging and filtering routines, combine data sets from different sensors, add meta information from the database and finally export the data sets into specific formats. 

### 3.1 getprimary.py

DESCRIPTION
   Checks all variometers and scalar sensors as defined in a configuration file.
   The first instrument in the list, which fulfills criteria on data availablility
   will be selected as primary instrument. Both primary instruments will be stored
   within a current.data json structure.

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)

OUTPUT
    writes to current data into the data file 'current data' as defined in 
    the configuration file

APPLICATION
    PERMANENTLY with cron:
        python getprimary.py -c /etc/marcos/analysis.cfg


### 3.2 flagging.py

DESCRIPTION
   This method can be used to flag data regularly, to clean up the
   existing falgging database, to upload new flags from files and
   to archive "old" flags into json file structures.

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

   The upload method also works for pkl (pickle) files. However,
   a successful upload requires that the upload is performed with
   the same major python version as used for pkl creation. 


PARAMETERS
Option | Name | Type |  Description
------ | ---- | ---- | -------------
   | flagdict |  dict  |  currently hardcoded into the method. Looks like { SensorNamePart : [timerange, keys, threshold, window, markall, lowlimit, highlimit]}
-c | configurationfile   |   file    |  too be read from GetConf2 (martas)
-j | joblist     |   list    |  jobs to be performed - default "flag" (flag, clean, uploud, archive)
-e | endtime     |   date    |  date until analysis is performed - default "datetime.utcnow()"
-p | path        |   string  |  upload - path to upload directory
-s | sensor      |   string  |  delete - sensor of which data is deleted
-o | comment     |   string  |  delete - flag comment for data sets to be deleted

OUTPUT
    write flags - to database etc


APPLICATION
    PERMANENT with cron:
        python flagging.py -c /etc/marcos/analysis.cfg
    YEARLY with cron:
        python flagging.py -c /etc/marcos/analysis.cfg -j archive
    DAILY with cron:
        python flagging.py -c /etc/marcos/analysis.cfg -j upload,clean -p /srv/archive/flags/uploads/
    REDO:
        python flagging.py -c /etc/marcos/analysis.cfg -e 2020-11-22


### 3.3 magnetism\_products.py

DESCRIPTION
   Creates magnetism products and graphs. The new version is making use of a configuration file.
   magneism_products.py creates adjusted data files and if a certain time condition as defined
   in the configuration file is met, then quasidefinitive data is produced as well, provided
   actual flagging information is available.
   Date files are stored in the archive directory and uploaded to all connected databases.
   Besides adjusted and quasidefinitive data, k-values and a general two-day variation graph is
   created.

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -j joblist             :   string  :  comma separated list like adjusted,quasidefinitive,addon
    -l loggername          :   string  :  name for the loggerfile
    -e endtime             :   date    :  date until analysis is performed, default "datetime.utcnow()"

OUTPUT


APPLICATION
    PERMANENTLY with cron:
        python magnetism_products.py -c /etc/marcos/analysis.cfg
    REDO analysis for a time range:
    (startime is defined by endtime - daystodeal as given in the config file 
        python magnetism_products.py -c /etc/marcos/analysis.cfg -e 2020-11-22

### 3.4 weather\_products.py

DESCRIPTION
   Analyses environmental data from various different sensors providing weather information.
   Sources are RCS raw data, RCS analyzed meteo data, ultrasonic, LNM and BM35 pressure.
   Creates a general METEO table called METEOSGO_adjusted, by applying various flagging
   methods and synop codes (LNM). Additionally, a short term current condition table is created (30min mean),
   two day and one year plots are generated. Beside, different measurements are compared for similarity (e.g.
   rain from bucket and lnm)

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"

APPLICATION
    PERMANENTLY with cron:
        python weather_products.py -c /etc/marcos/analysis.cfg
    REDO analysis for a time range:
        (startime is defined by endtime - daystodeal as given in the config file 
        python weather_products.py -c /etc/marcos/analysis.cfg -e 2020-11-22

### 3.5 gamma\_products.py

DESCRIPTION
   Analyses gamma measurements.
   Sources are SCA gamma and METEO data. Created standard project tables and a Webservice
   table version.

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"

APPLICATION
    PERMANENTLY with cron:
        python gamma_products.py -c /etc/marcos/analysis.cfg
    REDO analysis for a time range:
        (startime is defined by endtime - daystodeal as given in the config file 
        python gamma_products.py -c /etc/marcos/analysis.cfg -e 2020-11-22

### 3.6 magnetism\_checkadj.py

DESCRIPTION
   Compares adjusted data sets from all sensors for similarity. If not similar then a
   waring is send out. The same comparison is done for all F sensors using the 
   offsets contained in the database.

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -j joblist             :   list    :  vario,scalar
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"

APPLICATION
    PERMANENTLY with cron:
        python magnetism_checkadj.py -c /etc/marcos/analysis.cfg
    REDO analysis for a time range:
        (startime is defined by endtime - daystodeal as given in the config file 
        python magnetism_checkadj.py -c /etc/marcos/analysis.cfg -e 2020-11-22


### 3.7 baseline\_generator.py


DESCRIPTION
   Create a BLV file from expected field values D,I,F. If no field values are provided
   then IGRF values will be obtained for the StationLocation from the obscode provided in
   the confguration file. Make sure that no current.data path is defined for stations.
   The application will calculate basevalues for the primary variometer as definied in
   the configuration and create a BLV outfile within the defined dipath (conf).

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -v vector              :   int     :  only provided for testing, obtaining IGRF value otherwise
    -t time                :   string  :  default is 1. of month, at least after the fifth day (so that data should be present) 

APPLICATION
    Runtime:
        python3 baseline\_generator.py -c ../conf/gam.cfg
    Testing:
        python3 baseline\_generator.py -c ../conf/gam.cfg -t 2018-08-08T07:41:00 -v 64.33397725500629,4.302646668706179,48621.993688723036 -D

### 3.8 convert\_data.py

DESCRIPTION
   Converts data from a database to any specified data format. Unlike mpconvert
   it automatically chooses the best match containing the given sensor name fragment

PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)

APPLICATION
    PERMANENTLY with cron:
        python3 convert\_data.py -c ~/CONF/wic.cfg -s BM35 -o /srv/products/data/meteo/pressure/


## 4. Descriptions of TitleGraphs scripts

The folder TitleGraphs contains scripts to generate and assemble graphs for the homepage, particualry graphs with mixtures from fotos and real time data. 

### 4.1 mag\_graph.py

### 4.2 weather\_graph.py

### 4.3 gravity\_graph.py


## 5. Descriptions of Info scripts

### 5.1 tg\_pha.py

### 5.2 tg\_kval.py

### 5.3 tg\_quake.py

### 5.4 tg\_base.py

### 5.5 logfiledate.py

DESCRIPTION
   Script to check the creation date of the latest file matching a certain structure
   in a defined directory


PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods

PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -p path                :   string  :  directory path to be investigated 
    -s structure           :   string  :  match of filename
                                          structure like "*.json" or "*A16.txt". Default is "*" 
    -a age                 :   number  :  tolerated age (integer). Default is 1
    -i iterate             :   string  :  increment of age (day, hour, minute, second). Default is day 
    -l logger              :   string  :  name of the logger

APPLICATION
    Check whether most recent upload happend in the last 24 hours:
        python3 logfiledates.py -c /etc/marcos/analysis.cfg -p /srv/archive/flags/uploads/ -a 1 -i day')
    Checking for most recent succsessful AutoDIF analysis:
        python3 logfiledates.py -c /etc/marcos/analysis.cfg -p /srv/archive/DI/raw/ -a 1 -i day')



## 6. Descriptions of PeriodicGraphs scripts

### 6.1 general\_graph.py

DESCRIPTION
   Creates plots for a specific sensor.
PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods
PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -r range               :   int     :  default  2 (days)
    -s sensor              :   string  :  sensor or dataid
    -k keys                :   string  :  comma separated list of keys to be plotted
    -f flags               :   string  :  flags from other lists e.g. quakes, coil, etc
    -y style               :   string  :  plot style
    -l loggername          :   string  :  loggername e.g. mm-pp-tilt.log
    -e endtime             :   string  :  endtime (plots from endtime-range to endtime)

APPLICATION
    PERMANENTLY with cron:
        python webpage_graph.py -c /etc/marcos/analysis.cfg
    SensorID:
        python3 general_graph.py -c ../conf/wic.cfg -e 2019-01-15 -s GP20S3NSS2_012201_0001 -D
    DataID:
        python3 general_graph.py -c ../conf/wic.cfg -e 2019-01-15 -s GP20S3NSS2_012201_0001_0001 -D


### 6.1 tilt\_graph.py

### 6.2 supergrad\_graph.py

### 6.3 gamma\_graph.py

### 6.4 spaceweather\_graph.py

### 6.5 current\_weatherchanges.py


## 7. Descriptions of DataImport scripts

### 7.1 gfzkp\_download\_broker.py

Reads Kp activity indicies from Geoforschungszentrum Potsdam (GFZ) using MagPy and stores this data as CDF files.

IMPORTANT:
This script is running on the data broker. CDF files are then transferred to primary and secondary servers using "file_download" routines running on the specific server.

MONITORING:
This output of the script should be written to a log file. Using "monitor.py" from MARTAS, the log file can be tested for SUCCESS messages.

### 7.2 dscovr\_download\_broker.py

IMPORTANT:
This script is running on the data broker. CDF files are then transferred to primary and secondary servers using "file_download" routines running on the specific server.

MONITORING:
This output of the script should be written to a log file. Using "monitor.py" from MARTAS, the log file can be tested for SUCCESS messages.


### 7.3 quakes\_import.py

Please note: This script needs to access the internet (and not the BROKER)

DESCRIPTION
   Downloads Earthquake data from two online sources:
      AT: austrian seismological service
      NEIC: National earthquake information center (US)
PREREQUISITES
   The following packegas are required:
      geomagpy >= 0.9.8
      martas.martaslog
      martas.acquisitionsupport
      analysismethods
PARAMETERS
    -c configurationfile   :   file    :  too be read from GetConf2 (martas)
    -p path                :   string  :  path where to temporarly save the neic raw data

APPLICATION
    PERMANENTLY with cron:
        python3 python quakes_import.py -c /home/user/CONF/wic.cfg -p /srv/archive/external/neic/neic_quakes.d

### 7.4 wbvimport.py

Please note: This script makes use of a configuration file in ~/CONF
It can be run however also in standalone mode by providing all necessary options.
If options are provided together with cfg data, options will override configuration
data.

DESCRIPTION
    Import raw data from csv and excel files provided for warmbad villach measurements.
    Imported data will be written to provided databases and/or as files within the selected
    directory. A subdirectory consitisting of DataID/data.cdf will be established. If file
    output is selected a MagPy PYCDF structure will be established. 
    
PREREQUISITES
   The following packages are required:
      pandas
      xlrd
      geomagpy >= 1.1.6
      martas.martaslog
      martas.acquisitionsupport
      analysismethods
APPLICATION
      python3 wbvimport.py -h
      python3 wbvimport.py -c /home/cobs/CONF/wbv.cfg -b 2023-04-01 -D
COMMENTS
   The script joins excel and csv data (exl is only used if no csv data is present)
   Corrections i.e. in 2017 are hardcoded and need to be cited in the database


## 8. CRONTAB on ANALYSIS

'''
# prevent cron mails and overflow
MAILTO=""
# other commands
PYTHON=/usr/bin/python3
HOME=/home/cobs
LOGPATH=/var/log/magpy

# m h  dom mon dow   command
# -----------------------------
# MARTAS Jobs
# -----------------------------
# Periodically testing threshold every 10 min
#1,11,21,31,41,51  *  *  *  *    /home/cobs/MARTAS/app/threshold.py -m /etc/martas/threshold.cfg
# Periodical process monitoring
#30  *  *  *  *    /home/cobs/MARTAS/app/monitor.py -c /etc/martas/monitor.cfg

# ------------------------------
# FAST jobs - 5 to 10 min or faster
# ------------------------------
9,19,29,39,49,59  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/DataProducts/getprimary.py -c /home/cobs/CONF/wic.cfg > $LOGPATH/cron-dp-getprimary 2>&1
8,18,28,38,48,58  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/DataProducts/flagging.py -c /home/cobs/CONF/wic.cfg -j flag > $LOGPATH/cron-dp-flagging 2>&1
*/10  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/DataProducts/magnetism_products.py -c /home/cobs/CONF/wic.cfg > $LOGPATH/cron-dp-magprod 2>&1
4,14,24,34,44,54  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/Info/tg_kval.py > $LOGPATH/cron-info-kval  2>&1
5,15,25,35,45,55  *  *  *  *  rsync -avz -e ssh `find /srv/products/data/magnetism/variation/sec/ -type f -mtime -1` cobs@138.22.30.117:/home/cobs/SPACE/data/WICsec_v/
5,15,25,35,45,55  *  *  *  *  rsync -avz -e ssh `find /srv/products/data/magnetism/variation/min/ -type f -mtime -1` cobs@138.22.30.117:/home/cobs/SPACE/data/WICmin_v/
5,15,25,35,45,55  *  *  *  *  rsync -avz -e ssh `find /srv/products/data/magnetism/quasidefinitive/min/ -type f -mtime -5` cobs@138.22.30.117:/home/cobs/SPACE/data/WICmin_qd/
5,15,25,35,45,55  *  *  *  *  rsync -avz -e ssh `find /srv/products/data/magnetism/quasidefinitive/sec/ -type f -mtime -5` cobs@138.22.30.117:/home/cobs/SPACE/data/WICsec_qd/



# ------------------------------
# 20 min JOBS
# ------------------------------
1,21,41  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/DataImport/quakes_import.py -c /home/cobs/CONF/wic.cfg -p /srv/archive/external/neic/neic_quakes.d
2,22,42  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/Info/tg_quake.py -c /home/cobs/CONF/wic.cfg


# ------------------------------
# HOURLY JOBS
# ------------------------------
#24  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/TitleGraphs/mag_graph.py >> $TLOG 2>&1
#24  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/TitleGraphs/weather_graph.py >> $TLOG 2>&1
#25  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/PeriodicGraphs/tilt_graph.py >> $TLOG 2>&1
#25  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/PeriodicGraphs/supergrad_graph.py >> $TLOG 2>&1
#26  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/PeriodicGraphs/gamma_graph.py >> $TLOG 2>&1
#26  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/PeriodicGraphs/spaceweather_graph.py >> $TLOG 2>&1
#27  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/PeriodicGraphs/current_weatherchanges.py >> $TLOG 2>&1
27  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/Info/tg_pha.py -c /home/cobs/CONF/wic.cfg > $LOGPATH/cron-info-pha 2>&1
28  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/DataProducts/weather_products.py -c /home/cobs/CONF/wic.cfg > $LOGPATH/cron-dp-weathprod  2>&1
29  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/DataProducts/gamma_products.py -c /home/cobs/CONF/wic.cfg > $LOGPATH/cron-dp-gamprod  2>&1
34  *  *  *  *  $PYTHON /home/cobs/ANALYSIS/DataProducts/convert_data.py -c /home/cobs/CONF/wic.cfg -s BM35 -o /srv/products/data/meteo/pressure/  > $LOGPATH/cron-dp-convertbm35  2>&1
# uploads to broker
# -----------------
# RCS
34  *  *  *  *  rsync -avz -e ssh `find /srv/archive/SGO/RCST7_20160114_0001/raw/ -type f -mtime -5` cobs@138.22.30.117:/home/cobs/SPACE/data/rcst7/
34  *  *  *  *  rsync -avz -e ssh `find /srv/archive/SGO/RCSG0_20160114_0001/raw/ -type f -mtime -5` cobs@138.22.30.117:/home/cobs/SPACE/data/rcsg0/
34  *  *  *  *  rsync -avz -e ssh `find /srv/archive/SGO/GAMMA_SFB867_0001/raw/ -type f -mtime -5` cobs@138.22.30.117:/home/cobs/SPACE/data/gamma/
# Tilt data
35  *  *  *  *  rsync -avz -e ssh `find /srv/archive/SGO/LM_TILT01_0001/raw/ -type f -mtime -5` cobs@138.22.30.117:/home/cobs/SPACE/data/tilt/
# LNM
36  *  *  *  *  rsync -avz -e ssh `find /srv/archive/SGO/LNM_0351_0001/raw/ -type f -mtime -5` cobs@138.22.30.117:/home/cobs/SPACE/data/lnm/
37  *  *  *  *  rsync -avz -e ssh `find /srv/products/data/meteo/pressure/ -type f -mtime -5` cobs@138.22.30.117:/home/cobs/SPACE/data/bm35/

# ------------------------------
# DAILY ANALYSIS JOBS
# ------------------------------
# contains LEMI036_2 -> wait until End of September 2021 to activate for BLV
#1  2  *  *  *    $PYTHON /home/cobs/ANALYSIS/DataProducts/magnetism_checkadj.py -c /home/cobs/CONF/wic.cfg
#$PYTHONPATH /home/cobs/ANALYSIS/Projects/gamma/gamma.py -p /srv/projects/gamma/ -m /home/cobs/ANALYSIS/Projects/gamma/mygamma.cfg -d /srv/projects/gamma/ >> $TLOG 2>&1
#15  2  *  *  *   $PYTHON /home/cobs/ANALYSIS/DataProducts/flagging.py -c /home/cobs/CONF/wic.cfg -j upload,clean -p /srv/archive/flags/uploads/  > $LOGPATH/cron-dp-flagdaily 2>&1

# logfiledate has been replaced by monitor.py jobs on ALDEBARAN and SOL
#$PYTHONPATH /home/cobs/ANALYSIS/DataProducts/logfiledates.py -c /home/cobs/CONF/wic.cfg -p /srv/archive/WIC/DI/raw/ -s "*A16_WIC.txt" -a 2 -i day -l autodif >> $TLOG
# uploads to broker
# -----------------
15  2  *  *  *  rsync -avz -e ssh `find /srv/archive/SGO/BGSINDCOIL_1_0001/raw/ -type f -mtime -3` cobs@138.22.30.117:/home/cobs/SPACE/data/induction/


# ------------------------------
# WEEKLY ANALYSIS JOBS
# ------------------------------
# INFO
15  12  *  * 2    $PYTHON /home/cobs/ANALYSIS/Info/tg_base.py -c /home/cobs/CONF/wic.cfg -t /etc/martas/telegram.cfg

# ------------------------------
# MONTHLY ANALYSIS JOBS
# ------------------------------
# baseline generator schwaz: offsets determined as average of 2020-08-04 and 2020-08-06 - valid from 08-2019 until 18-07-2021
#0  3  7  *  *     $PYTHON $HOME/ANALYSIS/DataProducts/baseline_generator.py -c $HOME/CONF/swz.cfg -t `date -d "$D" "+\%Y"`-`date -d "$D" "+\%m"`-01T03:00:00 -o I:-0.00778,D:0.04308 -l mm-basegen-swz.log > /var/log/magpy/cron-baseline-swz.log 2>&1
0  3  7  *  *     $PYTHON $HOME/ANALYSIS/DataProducts/baseline_generator.py -c $HOME/CONF/swz.cfg -t `date -d "$D" "+\%Y"`-`date -d "$D" "+\%m"`-01T03:00:00 -l mm-basegen-swz.log > /var/log/magpy/cron-baseline-swz.log 2>&1

# ------------------------------
# YEARLY ANALYSIS JOBS
# ------------------------------
#30  2  1  2  *    $PYTHON /home/cobs/ANALYSIS/DataProducts/flagging.py -c /home/cobs/CONF/wic.cfg -j archive > $LOGPATH/cron-db-flagarchive 2>&1
'''


## 8. The database structure - groups, types and location details

SENSORS contains the fields SensorGroup and SensorType. 

### 8.1 SensorGroup

SensorGroup describes the main purpose of a specific sensor. A geomagnetic variometer is designed to measure magnetic 
field variations, but usually also contains temperature probes and voltage acquisition.
The SensorGroup would be magnetism. SensorGroup is used by flagging tools. Thus, a magnetic 
anomaly like a geomagnetic storm will affect all magnetism sensors at one location and thus a group
flag is useful.

At the Conrad Observatory we would record the following sensor groups (examples):

| SensorGroup   | instruments belonging to this group |
|---------------|-------------------------------------| 
| magnetism     | FGE, LEMI, GSM, GP20S3, CS,         |
| meteorology   | LNM, BM35, METEO,                   |
| environment   | ENV05, DS, MQ135,                   |
| gravity       | iGRAV, tilt, LM                     |
| radiometry    | RADON, GAMMA,                       |
| remotecontrol | RCS,                                |
| space         | DSCOVR, GOES,                       |


### 8.2 SensorType

SensorType contains the primary sensor mechanism or measurement principle

| SensorType       | instruments belonging to this group                       |
|------------------|-----------------------------------------------------------| 
| fluxgate         | FGE, LEMI,                                                |
| overhauser       | GSM,                                                      |
| optically pumped | GP20S3, CS,                                               |
| gravity          | iGRAV                                                     |
| spectrometer     | RADON, GAMMA,                                             |
| disdrometer      | LNM,                                                      |
| gas              | MQ135,                                                    |
| temperature      | Env, DS,                                                  |
| pressure         | BM35,                                                     |
| multiple         | DSCOVR, GOES, METEO, (only if a primary is not specified) |
