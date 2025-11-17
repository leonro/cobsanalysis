#!/usr/bin/env python
# coding=utf-8

"""
Testing module for methods from apps
"""

import unittest

import sys
sys.path.insert(1,'/home/leon/Software/MARTAS/') # should be magpy2
sys.path.insert(1,'/home/leon/Software/magpy/') # should be magpy2
sys.path.insert(1,'/home/leon/Software/cobsanalysis/') # should be magpy2

import os
from analysis.products import weather

#import shutil
#import numpy as np
#from magpy.stream import DataStream, read
#from magpy.core import database
#from magpy.opt import cred as mpcred
#from datetime import datetime, timedelta, timezone


class TestWeather(unittest.TestCase):
    """
    Test environment for weather.py methods
      combine_weather
      pressure_to_sea_level
      snow_or_nosnow
      transform_ultra
      transform_pressure
      transform_lnm
      transform_meteo
      transform_rcs
    """

    def test_transform_anemometer(self):
        basepath = "examplefiles/weather"
        ultraoffsets = {"ULTRASONICDSP_0001106088_0001" : {'t2':'-0.87'}}
        ultram = weather.transfrom_ultra(os.path.join(basepath, "ULTRA*"), offsets=ultraoffsets, debug=True)
        self.assertEqual(len(ultram), 1439)

    def test_transform_pressure(self):
        basepath = "examplefiles/weather"
        bm35m = weather.transfrom_pressure(os.path.join(basepath, "BM35*"), debug=True)
        self.assertEqual(len(bm35m), 1440)

    def test_transform_laser(self):
        basepath = "examplefiles/weather"
        lnmm = weather.transfrom_lnm(os.path.join(basepath, "LNM_0351_0001_0001_*"), debug=True)
        self.assertEqual(len(lnmm), 1439)

    def test_transform_rcs(self):
        basepath = "examplefiles/weather"
        rcst7m, fl1 = weather.transfrom_rcs(os.path.join(basepath, "RCS*"), debug=True)
        self.assertEqual(len(rcst7m), 1439)

    def test_transform_meteo(self):
        basepath = "examplefiles/weather"
        meteom, fl2 = weather.transfrom_meteo(os.path.join(basepath, "METEO*"), debug=True)
        self.assertEqual(len(meteom), 1440)

    def test_transform_combine(self):
        basepath = "examplefiles/weather"
        ultraoffsets = {"ULTRASONICDSP_0001106088_0001" : {'t2':'-0.87'}}
        ultram = weather.transfrom_ultra(os.path.join(basepath, "ULTRA*"), offsets=ultraoffsets, debug=False)
        bm35m = weather.transfrom_pressure(os.path.join(basepath, "BM35*"), debug=False)
        lnmm = weather.transfrom_lnm(os.path.join(basepath, "LNM_0351_0001_0001_*"), debug=False)
        rcst7m, fl1 = weather.transfrom_rcs(os.path.join(basepath, "RCS*"), debug=False)
        meteom, fl2 = weather.transfrom_meteo(os.path.join(basepath, "METEO*"), debug=False)
        fl = fl1.join(fl2)
        self.assertEqual(len(fl), 60)
        result = weather.combine_weather(ultram=ultram, bm35m=bm35m, lnmm=lnmm, rcst7m=rcst7m, meteom=meteom)

        #self.assertEqual(len(dl), 10)


if __name__ == "__main__":
    unittest.main(verbosity=2)