#!/usr/bin/env python
# coding=utf-8


from __future__ import print_function
from __future__ import unicode_literals

# Define packges to be used (local refers to test environment) 
# ------------------------------------------------------------
import os
import glob
from datetime import datetime
import paho.mqtt.client as mqtt
import json
import socket

#import subprocess
#from subprocess import check_call
#import telepot
#from telepot.loop import MessageLoop


class martaslog(object):
    """
    Class for dealing with and sending out change notifications
    of acquisition and analysis states
    """
    def __init__(self, logfile='/var/log/magpy/martasstatus.log', receiver='mqtt'):
        self.mqtt = {'broker':'localhost','delay':60,'port':1883,'stationid':'wic', 'client':'P1','user':None,'password':None}
        self.telegram = {'config':"/home/leon/telegramtest.conf"}
        self.logfile = logfile
        self.receiver = receiver
        self.hostname = socket.gethostname()
        # requires json, socket etc

    def updatelog(self,logfile,logdict):
        changes={}
        if os.path.isfile(logfile):
            # read log if exists and exentually update changed information
            # return changes
            with open(logfile, 'r') as file:
                exlogdict = json.load(file)
            print ("Logfile {} loaded".format(logfile))
            for el in logdict:
                if not el in exlogdict:
                    # Adding new sensor and state
                    print ("Not Existing:", el)
                    changes[el] = logdict[el]
                else:
                    print ("Existing:", el)
                    # Checking state
                    if not logdict[el] == exlogdict[el]:
                        # state changed
                        changes[el] = logdict[el]
            ## check for element in exlogdict which are not in logdict
            for el in exlogdict:
                if not el in logdict:
                    # Sensor has been removed
                    print ("Removed:", el)
                    changes[el] = "removed"

            if not len(changes) == 0:
                # overwrite prexsiting logfile
                print ("-------------")
                print ("Changes found")
                print ("-------------")
                with open(logfile, 'w') as file:
                    file.write(json.dumps(logdict)) # use `json.loads` to do the reverse
        else:
            # write logdict to file
            with open(logfile, 'w') as file:
                file.write(json.dumps(logdict)) # use `json.loads` to do the reverse
            print ("Logfile {} written successfully".format(logfile))

        return changes

    def msg(self, dictionary):
        changes = self.updatelog(self.logfile,dictionary)
        if len(changes) > 0:
            self.notify(changes)

    def notify(self, dictionary):
        #if receiver == "stdout":
        print ("Changed content:", dictionary)

        if self.receiver == 'mqtt':
            stationid = self.mqtt.get('stationid')
            broker = self.mqtt.get('broker')
            mqttport = self.mqtt.get('port')
            mqttdelay = self.mqtt.get('delay')
            client = self.mqtt.get('client')
            mqttuser = self.mqtt.get('user')
            mqttpassword = self.mqtt.get('password')
            topic = "{}/{}/{}".format(stationid,"statuslog",self.hostname)
            print ("Done. Topic={},".format(topic))
            print ("Done. User={},".format(mqttuser))
            client = mqtt.Client(client)
            if not mqttuser == None:
                client.username_pw_set(username=mqttuser, password=mqttpassword)
            print (broker, mqttport, mqttdelay)
            client.connect(broker, mqttport, mqttdelay)
            client.publish(topic,json.dumps(dictionary))
            print ('Update sent to MQTT')
        elif self.receiver == 'telegram':
            #try: # import Ok
            import telegram_send
            # except: # import error
            #try: # conf file exists
            # except: # send howto
            # requires a existing configuration file for telegram_send
            # to create one use:
            # python
            # import telegram_send
            # telegram_send.configure("/path/to/my/telegram.cfg",channel=True)
            tgmsg = ''
            for elem in dictionary:
                tgmsg += "{}: {}\n".format(elem, dictionary[elem])
            telegram_send.send(messages=[tgmsg],conf=self.telegram.get('config'),parse_mode="markdown")
            print ('Update sent to telegram')
        else:
            print ("Given receiver is not yet supported")

    def receiveroptions(self,receiver,options):
        dictionary = eval('self.{}'.format(receiver))
        for elem in options:
            dictionary[elem] = options[elem]
        print ("Dictionary {} updated".format(receiver))
     

# class martaslog():
#     def init -> logfile
#              -> change notification
#     def updatelog -> see below
#     def msg(dict) -> call updatelog
#                   -> send changes to specified output
#     def receiver(protocol,configdict) -> smtp,telegram,mqtt
#     def logfile(path) -> smtp,telegram,mqtt
#
# Application:
# import martaslog
# martaslog.logfile('/var/log/magpy/statuslog.log') 
# martaslog.receiver('mqtt',{'broker':'localhost','mqttport':1883,'mqttdelay':60})
## or martaslog.receiver('telegram',{'telegramconf':'path'})
## or martaslog.receiver('smtp',{'telegramconf':'path'})
# status['xxx'] = 'yyy'
# martaslog.msg(status) -> automatically send all changes to receiver

