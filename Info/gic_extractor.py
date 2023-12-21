#!/usr/bin/env python3
# coding=utf-8

"""
Programm to extract GIC values for all stations covering a certain time range.
Analyzes availability, variability, extrema and current values.
Creates
"""
def read_gicnow_data(db,source='GICAUT',maxsensor=10, minutes=5, maxvals=5, debug=False):
    knewsql = ''
    gicdata = []
    status = {}
    amount = 0
    addcommlist = []
    start = datetime.utcnow()-timedelta(minutes=minutes)
    trange = datetime.strftime(start, "%Y-%m-%d %H:%M:%S")
    for i in range(1,maxsensor):
        name = "{}_GIC{:02d}_0001_0001".format(source,i)
        sn = "GIC{:02d}".format(i)
        try:
            if debug:
                print ("Checking table ", name)
            gicdat = dbselect(db,'x', name,'time > "{}"'.format(trange))
            status[name] = len(gicdat)
            if len(gicdat) > 0:
                amount += 1
                addcommlist.append(sn)
            gicdata.extend(gicdat)
        except:
            pass

    def is_number(var):
        try:
            var = float(var)
            if np.isnan(var):
                return False
            return True
        except:
            return False

    # remove nans and using absolutes
    cleangicdata = [np.abs(x) for x in gicdata if is_number(x)]
    if debug:
        print ("GIC data", cleangicdata)
    if len(cleangicdata) > 5:
        # get the 5 largest values and calculate median
        sortedgic = sorted(cleangicdata)
        gicvals = sortedgic[-maxvals:]
    else:
        gicvals = cleangicdata
    if len(cleangicdata) > 2:
        active = 1
        gicval = np.median(gicvals)
    else:
        active = 0
        gicval = 0.0
    if debug:
        print (gicval, active, amount)
    comment = "median of {} largest absolut values from {} stations ({})".format(maxvals, amount, ",".join(addcommlist))

    gicnewsql = _create_gicnow_sql(gicval,start,active, comment)

    return [gicnewsql]
