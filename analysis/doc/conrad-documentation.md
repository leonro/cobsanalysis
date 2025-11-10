# Data analysis at the Conrad Observatory

written by R. Leonhardt, 2025

## 1. Introduction

## 2. Analysis and interpretation of weather data

### 2.1 Data sources

Meteorological data is acquired with several different sensors, which are connected to various acquisition units. Table X.1 summarizes the individual sensors, their acquisition units and the DataIDs under which data stored in database and archive. 

| Sensor            |  Acquisition Unit (SensorID in DB) |  Components  |  Resolution   |
| ----------------- | ---------------------------------- | ------------ | ------ |
| Thiess Anemometer |  ULTRASONIC_xxxx_0001              | T, v, dir    |  1-min      |
| BM35 Pressure     |  BM35_xx...                        | P            |  0.5-sec      |
| Thiess LNM        |  LNM_                              | T, droplet, visibility, synop, etc |  1-min      |
| JC Ultrasonic     |  RCST7_20...                       | snowheight   |  2-sec*      |
| 430_T             |  RCST7_20...                       | T            |  2-sec*      |
| SK                |  RCST7_20...                       | serviceswitch  |  2-sec*      |
| AP23mA Rain bucket |  RCST7_20...                       | rain            |  2-sec*      |
| 430_F             |  RCST7_20...                       | rh           |  2-sec*      |
| METEO - RCS**     | METEO_T7_                          | all RCS data |  1-min      

* the sampling rate is irregularly spaced in time
** METEO RCS is a anaysis program which internally analyses the RCS T7 data and calculates 1-min means.

Add a figure with the sensors on the tower.

#### 2.1.1 Ultrasonic wind data

Data from the Thiess 2D Ultrasonic wind sensors contains three columns: 

| key  | content       | column name |
| ---- | ------------- | ----------- |
| t1   | temperature   | Tv |
| var1 | windspeed     | V  |
| var2 | winddirection | Dir |

Data comes in 1-min resolution using a MARTAS acquisition system and no flagging is applied. The Ultrasonic sensors is maintained every 2 years. For this purpose the sensor is removed and replaced by an identical sensor with different serial number. The removed sensor is then handed over to the meteorogical technical team for service. 
The temperature reading has a slightly non-linear offset in comparison to the other outside temperature probes (see X.2). As an approximation, a single offset value has been determined and this value is applied to correct Tv. Please note, that Tv is our secondary temperature probe in cases when 430_T data is not available. 

#### 2.1.2 BM035 pressure data

Data from the BM35 pressure sensor contains one column:

| key  | content       | column name |
| ---- | ------------- | ----------- |
| var3 | pressure      | p1 |

Data is obtained in 2Hz resolution by a MARTAS acquisition system. Data is filtered to 1-sec using a Gaussian filter of 3.33 sec windows width. Data exceeding the validity range of 800 to 1000 mBar is flagged as non-reliable.

flagging: (check amount of flags in old database for 2025)

#### 2.1.3 Thiess LNM LaserNiederschlagsMonitor

The Thiess LNM system is also connected to a MARTAS system. Two files are generated in realtime, *.asc with spectral data and *.bin with basic timeseries. 

The basic time series contains 1-min data of 

| key  | content       | column name |
| ---- | ------------- | ----------- |
| x    |               | rainfall |
| y    |               | visibility |
| z    |               | reflictivity  |
| f    |               | P_tot |
| t1   |               | T |
| t2   |               | T_el  |
| var1 |               | I_tot |
| var2 |               | I_fluid  |
| var3 |               | I_solid  |
| dx   |               | P_slow  |
| dy   |               | P_fast  |
| dz   |               | P_small |
| str1 |               | Synop code |

1-min resolution (no flagging)

#### 2.1.4 RCS T7 

A number of sensors is connected to the RCS (remote control system) for data acquisition. This sensors comprise a JC ultrasonic snowheight sensor, a 430_T temperature and 430_F relative humidity sensor, and a rain bucket of type AP23mA. A problem with RCS data is that time spacing is not equidistant and the data set contains numerous gaps. If gaps are filled with NaN, then cumulative rain is wrongly determined because of those NaN values.

| key  | content       | column name |
| ---- | ------------- | ----------- |
| x    | snowheight    | JC     |
| y    | temperature   | 430A_T |
| z    | Service Schalter | (SK | 
| t1   | rain (up to 10mm) | AP23mA |
| t2   | rel. humidity | 430A_F |
| var1 | former windspeed | CH00 |
| var2 | former winddirection | CH01 |

Service Schalter - flag data when SK == 1 (rain and snow height, only affects rain, but person nearby snow height)
CH00 and CH01 are not used any more and can be dropped

The basic time step leads to a 2 sec resolution, which is however not homogenuous. The pre-2025 flagging algorythm produced way too many flags?

IMPORTANT NOTE: time coverage does not fit to file names

#### 2.1.5 METEO RCS

The METEO RCS data file is an internal program which analyses and averages RCS T7 data. The underlying routines are neither described nor  available. Unlike the RCS T7 data which currently is only available every 6 hours, METEO data is available near realtime. Therefore we use the METEO datafiles for our anaylsis and replace this data by reproducibly described means and filtered data as soon as RCS T7 data is available.

| key  | content       | column name |
| ---- | ------------- | ----------- |
| x    |               | SK  | 
| y    |               | Ap23mm  |  
| z    |               | snowheigth |
| f    | temperature   |  T |
| t1    | rel. humidity |  rh  |
| t2    | 430UEV       |  |
| dx    | AP23mA       |  |


1 min resolution

### 2.2 Independent Measurements of redundant parameters

#### 2.2.1 Outside temperature

The following systems record the outside temperature at a very sinmilar position outside the SGO. METEO and RCST7 should be identical.  The Thiess Ultrasonic anometer records temperature approcimately x m apart (and above). The Thies LNM system also records temperature within 1m distance of the RCS 430_T sensor. As shown in Fig. Y the temperature recordings are different for all these systems. LNM temperature shows a different response when temperature changes. Ultra shows similar variations as 430_T but largest offset with ~0.9°C.
We choose 430_T as the primary temperature channel. The Ultrasonic Windmeter is used as a secondary temperature source.

