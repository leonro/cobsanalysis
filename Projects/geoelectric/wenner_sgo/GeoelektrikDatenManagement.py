# Manual: http://www.l-gm.de/documents/4pointlight10w_v4_58en.pdf
import serial


def readdata(text):
    print "Reading..."
    r = ''
    try:
        while True:
            line = s.read()
            r += line
            print line
            if line == 'E': # line == '@':
                break
    except:
        print "Loop broken"
    t = r.split('\r')
    f = open('/home/leon/CronScripts/Geoelektrik/geoelektrik_%s.txt' % text,'w')
    for i in t:
        f.write(i+'\n')
    f.close()


print "Connecting..."
s = serial.Serial('/dev/ttyUSB0', baudrate=19200)
readdata('monitoring-2017-07-21')
s.close()


