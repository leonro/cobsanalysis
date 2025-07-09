# Data files

## ipc-noise

Contains three data files of the same measurements system, the LEMI025_22_0004 sensor. 
Orientation and setup is identical. Th sensor is always connected with the same 
FT232 serial converter.

| File       | connected to               | other active sensors/ipcs in same cabinet |
|------------|----------------------------|-------------------------------------------|
| 2025-05-30 | Rasp. Pi 4 Model B Rev 1.5 | none, GSM90_14245                         |
| 2025-06-09 | Beaglebone black C3?       | none, GSM90 switched on                   |
| 2025-07-04 | Beaglebone black C3?       | Rasp4 with active GSM90_14245             |

How to find out the IPC tyoe:
Raspberry: cat /sys/firmware/devicetree/base/model
Beaglebone: no command existing 