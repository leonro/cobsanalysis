#!/bin/env/python
from magpy.stream import read
import magpy.mpplot as mp
from magpy.database import *
import magpy.opt.cred as mpcred

dbpasswd = mpcred.lc('cobsdb','passwd')
db = mysql.connect(host="138.22.188.195",user="cobs",passwd=dbpasswd,db="cobsdb")

vario1 = read('/srv/archive/WIC/LEMI036_1_0002/raw/*2016-02-28.bin')
vario2 = read('/srv/archive/WIC/LEMI025_22_0002/raw/*2016-02-28.bin')

print vario1.length(), vario2.length()

mp.plotStreams([vario1,vario2],[['z'],['z']])

