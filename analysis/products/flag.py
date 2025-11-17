#!/usr/bin/env python
# coding=utf-8

from martas.core import methods as mm
from martas.core import analysis as maan

config = mm.get_conf('/home/cobs/.martas/conf/basevalue_blvcomp_main.cfg')
flagdict = mm.get_json('/home/cobs/SCRIPTS/flagdict.json')

config = mm.check_conf(config)
mf = maan.MartasAnalysis(config=config, flagdict=flagdict)
fl = mf.periodically(debug=True)
#print (fl)
suc = mf.update_flags_db(fl, debug=True)
