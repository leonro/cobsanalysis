# ANALYSIS Scripts at the Conrad Observatory

## 1. Introduction

The following README contains an overview about all analysis scripts used at the Conrad Observatory. A short description of each script is accompanied by dependencies and some typical schedules of the respective application.

## 2. Overview


Script                        | Location        |  Schedule         |  Config        |   Monitor         |  Dependencies  | Comments
----------------------------- | --------------- | ----------------- | -------------- | ----------------- | -------------- | --------
magnetism\_products.py        | DataProducts    | analysis_5min     | CONF/wic.cfg   |  MARTAS telegram  |                | py2 and py3
weather\_products.py          | DataProducts    | analysis_hourly   | CONF/wic.cfg   |  MARTAS telegram  |                | py2 and py3
getprimary.py                 | DataProducts    | analysis_5min     | CONF/wic.cfg   |  MARTAS telegram  |                | py2 and py3
flagging.py                   | DataProducts    | analysis_5min     | CONF/wic.cfg   |  MARTAS telegram  |                | py2 and py3
gamma\_products.py            | DataProducts    | analysis_hourly   | CONF/wic.cfg   |  MARTAS telegram  |                | py2 and py3
magnetism\_checkadj.py        | DataProducts    | analysis_daily    | CONF/wic.cfg   |  MARTAS telegram  |                | py2 and py3
baseline\_generator.py        | DataProducts    | to daily          | gam.cfg/swz.cfg |  MARTAS telegram  |                | py2 and py3
tg\_pha.py                    | Info            | analysis_20min    |                |  MARTAS telegram  |                | 
tg\_kval.py                   | Info            | analysis_5min     |                |  MARTAS telegram  |                | 
tg\_quake.py                  | Info            | analysis_20min    |                |  MARTAS telegram  |                | 
tg\_base.py                   | Info            | analysis_weekly   |                |  MARTAS telegram  |                | 
logfiledate.py                | Info            | analysis_weekly   | CONF/wic.cfg   |  MARTAS telegram  |                | py2 ... py3
neic\_download.py             | FileDownloads   |                   |                |  MARTAS telegram  |                | py2 ... py3
dscovr\_download.py           | FileDownloads   |                   | -              |  MARTAS telegram  |                | py2 ... py3
gfzkp\_download.py            | FileDownloads   |                   | -              |  MARTAS telegram  |                | py2 ... py3
merkur\_download.py           | FileDownloads   |                   |                |  MARTAS telegram  |                | replace with rsync
ace\_download_min.py          | FileDownloads   |                   |                |  MARTAS telegram  |                | 
ace\_download_daily.py        | FileDownloads   |                   |                |  MARTAS telegram  |                | 
mag\_graph.py                 | TitleGraphs     | analysis_20min    |                |  MARTAS telegram  |                | 
weather\_graph.py             | TitleGraphs     | analysis_20min    |                |  MARTAS telegram  |                | 
general\_graph.py             | PeriodicGraphs  | analysis_hourly   |                |  MARTAS telegram  |                | py2 and py3
tilt\_graph.py                | PeriodicGraphs  | analysis_hourly   |                |  MARTAS telegram  |                | 
supergrad\_graph.py           | PeriodicGraphs  | analysis_hourly   |                |  MARTAS telegram  |                | 
gamma\_graph.py               | PeriodicGraphs  | analysis_hourly   |                |  MARTAS telegram  |                | 
spaceweather\_graph.p         | PeriodicGraphs  | analysis_hourly   |                |  MARTAS telegram  |                | 
current\_weatherchanges.py    | PeriodicGraphs  | analysis_hourly   |                |  MARTAS telegram  |                | 
gamma.py                      | Projects/gamma/ | analysis_daily    |                |  MARTAS telegram  |                | 
iono\_analysis.py             | Projects/ionit  | analysis_monthly  |                |  MARTAS telegram  |                | 
GeoelekTimeSeries.py          | Projects/geoelectric/wenner\_sgo/ | analysis_monthly  |                |  MARTAS telegram  |                |


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
    -e endtime             :   date    :  date until analysis is performed
                                          default "datetime.utcnow()"

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
        python3 baseline_generator.py -c ../conf/gam.cfg
    Testing:
        python3 baseline_generator.py -c ../conf/gam.cfg -t 2018-08-08T07:41:00 -v 64.33397725500629,4.302646668706179,48621.993688723036 -D

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


## 7. Descriptions of FileDownloads scripts

### 7.1 neic\_download.py


