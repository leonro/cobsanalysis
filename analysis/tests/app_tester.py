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
import shutil
import numpy as np
from magpy.stream import DataStream, read
from magpy.core import database
from magpy.opt import cred as mpcred

from datetime import datetime, timedelta, timezone
from analysis.products import weather



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

    def test_transform_datelist(self):
        ultram = weather.transfrom_ultra(os.path.join(basepath, "ULTRA*", starttime=None, endtime=None, offsets=ultraoffsets),
                                 debug=True)
        bm35m = weather.transfrom_pressure(os.path.join(basepath, "BM35*"), starttime=None, endtime=None, debug=True)
        lnmm = weather.transfrom_lnm(os.path.join(basepath, "LNM_0351_0001_0001_*"), starttime=None, endtime=None, debug=True)
        rcst7m, fl1 = weather.transfrom_rcs(os.path.join(basepath, "RCS*"), starttime=None, endtime=None, debug=True)
        meteom, fl2 = weather.transfrom_meteo(os.path.join(basepath, "METEO*"), starttime=None, endtime=None, debug=True)

        # return flags and store them as well !!
        fl = fl1.join(fl2)
        result = combine_weather(ultram=ultram, bm35m=bm35m, lnmm=lnmm, rcst7m=rcst7m, meteom=meteom)

        self.assertEqual(len(dl), 10)

    #def test_create_data_selectionlist(self):
    #    dt = archive.create_data_selectionlist(blacklist=None, debug=False)
    #    self.assertEqual(ar[1],11)

    #def test_get_data_dictionary(self):
    #    cfg = archive.get_data_dictionary(db,sql,debug=False)
    #    self.assertEqual(cfg.get("station"),"myhome")

    #def test_get_parameter(self):
    #    sens = archive.get_parameter(plist, debug=False)
    #    self.assertEqual(sens,[])

    #def test_validtimerange(self):
    #    archive.validtimerange(timetuple, mintime, maxtime, debug=False)
