#!/usr/bin/env python3

# Listens for UDP broadcast events from a WeatherFlow Smart Weather Station, logs them and passes observations on via MQTT
#
# October 2017 - Steve Cliffe <steve@sjcnet.id.au>
#

import os
import socket
import datetime
import paho.mqtt.publish as mqttPub

wfPort = 50222
logFile = '/var/log/weatherflow'
mqttServer = 'mythical'
mqttTopicBase = '/sensor/weather/mqtt/'
debug = False

##
##      Functions
##

def log(msg):
    logTs = datetime.datetime.now().strftime('%d/%m/%y %H:%M:%S')
    if debug is True:
        print(logTs + ' ' + msg)
    else:
        lf = open(logFile, 'a')
        lf.write(logTs + ' ' + msg + '\n')
        lf.close()


def processWfEvent(data):
    try:
        event = eval(data)
    except SyntaxError:
        log('Bogus packet received: ' + data)
        return
    if debug:
        log(data)
    if 'type' in event:
        type = event['type']
    else:
        log('Unable to determine event type in: ' + data)
        return

    if type == 'evt_precip':
        log('Rain started')
    if type == 'evt_strike':
        evt = event['evt']
        log('Lightning strike detected %d km away' % evt[1])
    if type == 'rapid_wind':
        ob = event['ob']
        log('Rapid wind speed %f mps' % ob[1])
    if type == 'station_status':
        log('Station Status: uptime %d, voltage %.2f, sensor_status %d, rssi %s' % (event['uptime'], event['voltage'], event['sensor_status'], event['rssi']))
    if type == 'hub-status':
        log('Hub Status: uptime %d, firmware %s, rssi %s' % (event['uptime'], event['firmware_version'], event['rssi']))
    if type == 'obs_air':
        obs = event['obs'][0]
        mqttPub.single(mqttTopicBase + 'pressure', 'N:%.1f' % obs[1], qos=0, hostname=mqttServer)
        mqttPub.single(mqttTopicBase + 'temperature', 'N:%.1f' % obs[2], qos=0, hostname=mqttServer)
        mqttPub.single(mqttTopicBase + 'humidity', 'N:%.1f' % obs[3], qos=0, hostname=mqttServer)
        mqttPub.single(mqttTopicBase + 'lightning-count', 'N:%.1f' % obs[4], qos=0, hostname=mqttServer)
        mqttPub.single(mqttTopicBase + 'lightning-distance', 'N:%.1f' % obs[5], qos=0, hostname=mqttServer)
        mqttPub.single(mqttTopicBase + 'battery-air', 'N:%.1f' % obs[6], qos=0, hostname=mqttServer)

##
##      Main
##

if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.bind(('', wfPort))
    except socket.error:
        log('Unable to bind socket')
        sys.exit(1)

    while True:
        data, addr = s.recvfrom(1024)
        if data:
            processWfEvent(data.decode('utf-8').rstrip())
