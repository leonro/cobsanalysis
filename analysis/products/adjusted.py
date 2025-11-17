#!/usr/bin/env python
# coding=utf-8

from martas.core import methods as mm
from martas.core import analysis as maan

config = mm.get_conf('/home/cobs/.martas/conf/basevalue_blvcomp_main.cfg')
config = mm.check_conf(config)
mf = maan.MartasAnalysis(config=config)
primary_vario = mf.get_primary(config.get("vainstlist",[]))
primary_scalar = mf.get_primary(config.get("scinstlist",[]))

merged = mf.magnetism_data_product('adjusted', primary_vario, primary_scalar, debug=True)
results = merged.get('merge')
for sr in results:
    data = results.get(sr)
    mf.db.write(data, tablename=data.header.get("DataID"))
    print ("Done for", sr)

