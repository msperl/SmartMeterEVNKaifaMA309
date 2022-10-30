#! /usr/bin/env python3

from gurux_dlms.GXByteBuffer import GXByteBuffer
import serial
import time
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from binascii import unhexlify
import sys
import string
import paho.mqtt.client as mqtt
from gurux_dlms.GXDLMSTranslator import GXDLMSTranslator
from gurux_dlms.GXDLMSTranslatorMessage import GXDLMSTranslatorMessage
from bs4 import BeautifulSoup
import os
import traceback
import json

#workarround for missing distutils.util
def str_to_bool(value: any) -> bool:
    if not value:
        return False
    return str(value).lower() in ("y", "yes", "t", "true", "on", "1")

class mbus_parser():

    mbus_message_length=282
    mbus_start_pattern=b'\x68\xfa\xfa\x68\x53\xff\x00\x01\x67\xdb\x08'
    mbus_end_pattern=b'\x16'

    def __init__(self, dev, key):
        self.serial_dev = dev
        self.key = key
        self.homeassistant_mqtt_publish = 0
        self.printRaw = True
        self.printXml = True

        self.tr = GXDLMSTranslator()
        self.tr.blockCipherKey = GXByteBuffer(self.key)
        self.tr.comments = True

        self.ser = serial.Serial(port=self.serial_dev,
                                 baudrate=2400,
                                 bytesize=serial.EIGHTBITS,
                                 parity=serial.PARITY_NONE
                                )
    
    def readMbusMessage(self):
        # wait for the start sequence
        self.ser.read_until(self.mbus_start_pattern)
    
        # nw read the complete block 
        self.data = (self.mbus_start_pattern + self.ser.read(size=self.mbus_message_length - len(self.mbus_start_pattern))).hex()

        if self.printRaw:
            print("Data received: %s" % self.data, flush=True)
        
        if not self.data.endswith(self.mbus_end_pattern.hex()):
            print("%s: unexpected data trailer - need to restart - %s" % (sys.argv[0], self.data), file=sys.stderr)
            self.data = None
    
        # there is another marker we can check as well - we can also try to match that one...
        # complete start pattern is: 68FAFA6853FF000167DB08................81F820

        # and there is in principle a CRC checksum, but how that is computed is not documented...

    def parseData(self):
        self.xml = ""
        if not self.data:
            self.xml = None
            return
        msg = GXDLMSTranslatorMessage()
        msg.message = GXByteBuffer(self.data)
        pdu = GXByteBuffer()
        self.tr.completePdu = True
        while self.tr.findNextFrame(msg, pdu):
            pdu.clear()
            self.xml += self.tr.messageToXml(msg)

        if self.printXml:
            print("xml received: %s" % self.xml, flush=True)

        # parse
        soup = BeautifulSoup(self.xml, 'lxml')
        results_32 = soup.find_all('uint32')
        results_16 = soup.find_all('uint16')

        #ActiveEnergy A+ in KiloWattstunden
        self.kWhP = int(str(results_32)[16:16+8],16)/1000

        #ActiveEnergy A- in KiloWattstunden
        self.kWhN = int(str(results_32)[52:52+8],16)/1000


        #CurrentElectricPower P+ in Watt
        self.WattP = int(str(results_32)[88:88+8],16)

        #CurrentElectricPower P- in Watt
        self.WattN = int(str(results_32)[124:124+8],16)

        #Voltage L1 in Volt
        self.VoltageL1 = int(str(results_16)[16:20],16)/10

        #Voltage L2 in Volt
        self.VoltageL2 = int(str(results_16)[48:52],16)/10

        #Voltage L3 in Volt
        self.VoltageL3 = int(str(results_16)[80:84],16)/10

        #Current L1 in Ampere
        self.CurrentL1 = int(str(results_16)[112:116],16)/100

        #Current L2 in Ampere
        self.CurrentL2 = int(str(results_16)[144:148],16)/100

        #Current L3 in Ampere
        self.CurrentL3 = int(str(results_16)[176:180],16)/100

        #PowerFactor
        self.PowerFactor = int(str(results_16)[208:212],16)/1000
            
    def printValues(self):
        if not self.xml:
            return
        print('Voltage L1:  ' + str(self.VoltageL1))
        print('Voltage L2:  ' + str(self.VoltageL2))
        print('Voltage L3:  ' + str(self.VoltageL3))
        print('Current L1:  ' + str(self.CurrentL1))
        print('Current L2:  ' + str(self.CurrentL2))
        print('Current L3:  ' + str(self.CurrentL3))
        print('WattP+:      ' + str(self.WattP))
        print('WattP-:      ' + str(self.WattN))
        print('Watt:        ' + str(self.WattP - self.WattN))
        print('PowerFactor: ' + str(self.PowerFactor))
        print('kWh+:        ' + str(self.kWhP))
        print('kWh-:        ' + str(self.kWhN))
        print('kWh:         ' + str(self.kWhP - self.kWhN))
        print('==========================================================', flush=True)

    def publishValues(self, client):
        if not self.xml:
            return
        client.publish(mqttTopicPrefix + "/VoltageL1",   self.VoltageL1)
        client.publish(mqttTopicPrefix + "/VoltageL2",   self.VoltageL2)
        client.publish(mqttTopicPrefix + "/VoltageL3",   self.VoltageL3)
        client.publish(mqttTopicPrefix + "/CurrentL1",   self.CurrentL1)
        client.publish(mqttTopicPrefix + "/CurrentL2",   self.CurrentL2)
        client.publish(mqttTopicPrefix + "/CurrentL3",   self.CurrentL3)
        client.publish(mqttTopicPrefix + "/WattP",       self.WattP)
        client.publish(mqttTopicPrefix + "/WattN",       self.WattN)
        client.publish(mqttTopicPrefix + "/Watt",        self.WattP - self.WattN)
        client.publish(mqttTopicPrefix + "/kWhP",        self.kWhP)
        client.publish(mqttTopicPrefix + "/kWhN",        self.kWhN)
        client.publish(mqttTopicPrefix + "/kWh",         self.kWhP - self.kWhN)
        client.publish(mqttTopicPrefix + "/PowerFactor", self.PowerFactor)

    def publishHomeAssistant(self, client):
        state_topic = "homeassistant/sensor/" + mqttTopicPrefix + "/state"
        configs = {
              "L1_Voltage":    { "device_class": "voltage",      "unit_of_measurement": "V",   "value": self.VoltageL1 },
              "L2_Voltage":    { "device_class": "voltage",      "unit_of_measurement": "V",   "value": self.VoltageL2 },
              "L3_Voltage":    { "device_class": "voltage",      "unit_of_measurement": "V",   "value": self.VoltageL3 },
              "L1_Current":    { "device_class": "current",      "unit_of_measurement": "A",   "value": self.CurrentL1 },
              "L2_Current":    { "device_class": "current",      "unit_of_measurement": "A",   "value": self.CurrentL2 },
              "L3_Current":    { "device_class": "current",      "unit_of_measurement": "A",   "value": self.CurrentL3 },
              "Watt_consumed": { "device_class": "power",        "unit_of_measurement": "W",   "value": self.WattP },
              "Watt_produced": { "device_class": "power",        "unit_of_measurement": "W",   "value": self.WattN },
              "Watt":          { "device_class": "power",        "unit_of_measurement": "W",   "value": self.WattP - self.WattN },
              "kWh_consumed":  { "device_class": "energy",       "unit_of_measurement": "kWh", "value": self.kWhP },
              "kWh_produced":  { "device_class": "energy",       "unit_of_measurement": "kWh", "value": self.kWhN },
              "kWh":           { "device_class": "energy",       "unit_of_measurement": "kWh", "value": self.kWhP - self.kWhN },
              "PowerFactor":   { "device_class": "power_factor", "unit_of_measurement": "%",   "value": 100 * self.PowerFactor },
        }
        # produce the data structure as well as enrich the config settings
        state = {}
        for k,v in configs.items():
            # assign value
            state[k] = v["value"]
            # enrich config
            if not "name" in v:
                v["name"] = k
            if not "value_template" in v:
                v["value_template"] = "{{ value_json." + k + " }}"
            if not "state_topic" in v:
                v["state_topic"] = state_topic
        # and now send the config
        print ("state_topic: " + state_topic)
        print ("state:       " + json.dumps(state, indent=4))
        # publish config every 100 state publishes
        if self.homeassistant_mqtt_publish % 100 == 0:
            print ("config:      " + json.dumps(configs, indent=4))
        self.homeassistant_mqtt_publish += 1  
        
# the encryption_key
encryption_key = os.environ.get("EVN_KEY")
if not encryption_key:
    print("%s: environment variable EVN_KEY is not set" % sys.argv[0], file=sys.stderr)
    sys.exit(1)
comport = os.environ.get("SERIAL_PORT")
if not comport:
    print("%s: environment variable SERIAL_PORT is not set" % sys.argv[0], file=sys.stderr)
    sys.exit(1)

try:
    print("opening Serial", flush=True)
    mbus = mbus_parser(comport, encryption_key)
except Exception as e:
    print("%s: could not connect to serial - %s" % (sys.argv[0], str(e)), file=sys.stderr)
    sys.exit(2)

# handle print args
mbus.printRaw = str_to_bool(os.environ.get("PRINT_RAW","False"))
mbus.printXml = str_to_bool(os.environ.get("PRINT_XML","False"))
printValues = str_to_bool(os.environ.get("PRINT_DATA","False"))
mqttHomeAssistant = str_to_bool(os.environ.get("MQTT_HOME_ASSISTANT", "False"))
    
#MQTT Broker 
mqttBroker = os.environ.get("MQTT_HOST")
if mqttBroker:
    try:
        mqttBrokerPort = int(os.environ.get("MQTT_PORT", "1883"))
        mqttUser = os.environ.get("MQTT_USER", "")
        mqttPasswort = os.environ.get("MQTT_PASS", "")
        mqttClientName = os.environ.get("MQTT_CLIENT_NAME", "SmartMeter")
        mqttTopicPrefix = os.environ.get("MQTT_TOPIC_PREFIX", "Smartmeter")
        
        client = mqtt.Client(mqttClientName)
        client.username_pw_set(mqttUser, mqttPasswort)
        client.connect(mqttBroker, port=mqttBrokerPort, keepalive=60)
        client.loop_start()
    except Exception as e:
        print("%s: the broker IP/port/user/password is wrong - %s" % (sys.argv[0], str(e)), file=sys.stderr)
        sys.exit(2)
else:
    # force printing if no broker is configured
    printValues = True

print("Starting main loop", flush=True)
while 1:
    try:
        mbus.readMbusMessage()
        mbus.parseData()
        if printValues:
            mbus.printValues()
        if mqttBroker:
            if mqttHomeAssistant:
                mbus.publishHomeAssistant(client)
            else:
                mbus.publishValues(client)
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("%s: Error handling the main loop: %s" % (sys.argv[0], format(e)), file=sys.stderr)
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stderr)

