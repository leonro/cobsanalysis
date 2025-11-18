#!/usr/bin/env python
# coding=utf-8

from martas.core import methods as mm
from martas.core import analysis as maan
import json

statusmsg = {}
statusdict = {}
config = mm.get_conf('/home/cobs/.martas/conf/basevalue_blvcomp_main.cfg')
config = mm.check_conf(config)
mf = maan.MartasAnalysis(config=config)
primary_vario = mf.get_primary(config.get("vainstlist",[]))
primary_scalar = mf.get_primary(config.get("scinstlist",[]))
statusdict["primary_variometer"] = primary_vario
statusdict["primary_scalar"] = primary_scalar

merged = mf.magnetism_data_product('adjusted', primary_vario, primary_scalar, debug=True)
results = merged.get('merge')
for sr in results:
    statusdict["Sampling rate {}".format(sr)] = "Fail"
    data = results.get(sr)
    if len(data) > 0:
        statusdict["Sampling rate {}".format(sr)] = "Success"
    mf.db.write(data, tablename=data.header.get("DataID"))
    print ("Done for", sr)

statusmsg["adjusted"] = json.dumps(statusdict)
# log it
