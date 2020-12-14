# ANALYSIS Scripts at the Conrad Observatory

## 1. Introduction

The following README contains an overview about all analysis scripts used at the Conrad Observatory. A short description of each script is accompanied by dependencies and some typical schedules of the respective application.

## 2. Overview


Script                        | Location        |  Schedule         |  Config        |   Monitor         |  Dependencies  | Comments
----------------------------- | --------------- | ----------------- | -------------- | ----------------- | -------------- | --------
magnetism\_products\_new.py   | DataProducts    | analysis_5min     | CONF/wic.cfg   |  MARTAS telegram  |                | py2 and py3
current\_weather\_new.py      | DataProducts    | analysis_hourly   | CONF/wic.cfg   |  MARTAS telegram  |                | py2 and py3
getprimary.py                 | DataProducts    | analysis_5min     | CONF/wic.cfg   |  MARTAS telegram  |                | py2 and py3
flagging.py                   | DataProducts    | analysis_5min     | CONF/wic.cfg   |  MARTAS telegram  |                | py2 and py3
radon\_project.py             | DataProducts    | analysis_hourly   |                |  MARTAS telegram  |                | 
rcsdata\_upload.py            | DataProducts    | analysis_hourly   |                |  MARTAS telegram  |                | 
magnetism\_checkadj.py        | DataProducts    | analysis_daily    |                |  MARTAS telegram  |                | 
tg\_pha.py                    | Info            | analysis_20min    |                |  MARTAS telegram  |                | 
tg\_kval.py                   | Info            | analysis_5min     |                |  MARTAS telegram  |                | 
tg\_quake.py                  | Info            | analysis_20min    |                |  MARTAS telegram  |                | 
tg\_base.py                   | Info            | analysis_weekly   |                |  MARTAS telegram  |                | 
file\_upload\_qd.py           | FileUploads     | analysis_daily    |                |  MARTAS telegram  |                | 
neic\_download.py             | FileDownloads   | analysis_20min    |                |  MARTAS telegram  |                | 
mag\_graph.py                 | TitleGraphs     | analysis_20min    |                |  MARTAS telegram  |                | 
weather\_graph.py             | TitleGraphs     | analysis_20min    |                |  MARTAS telegram  |                | 
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

### 3.2 flagging.py

### 3.3 magnetism\_products\_new.py

### 3.4 current\_weather\_new.py

### 3.5 rcsdata\_upload.py

### 3.6 radon\_project.py


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


## 6. Descriptions of PeriodicGraphs scripts

### 6.1 tilt\_graph.py

### 6.2 supergrad\_graph.py

### 6.3 gamma\_graph.py

### 6.4 spaceweather\_graph.py

### 6.5 current\_weatherchanges.py


## 7. Descriptions of FileDownloads scripts

### 7.1 neic\_download.py


