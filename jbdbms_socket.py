#!/usr/bin/env python3

	# using python 3.9
	
	#DW - input parameters - -b A4:C1:38:6B:5C:29 -i 30 -m jbdbms
	
from bluepy.btle import Peripheral, DefaultDelegate, BTLEException
import struct
import argparse
import json
import time
import binascii
import atexit
import paho.mqtt.client as paho


 	# Command line arguments
parser = argparse.ArgumentParser(description='Fetches and outputs JBD bms data')
parser.add_argument("-b", "--BLEaddress", help="Device BLE Address", required=True)
parser.add_argument("-i", "--interval", type=int, help="Data fetch interval", required=True)
parser.add_argument("-m", "--meter", help="meter name", required=True)
args = parser.parse_args()
z = args.interval
meter = args.meter	

mqtt_topic ="data/bms"
#gauge ="data/bms/gauge"
mqtt_broker="localhost"
mqtt_port=1883
#mqtt_port=8086

def on_log(client, userdata, level, buf):  #DW
    print("log: ",buf)                     #DW

def disconnect():
    mqtt.disconnect()
    print("mqtt_broker disconnected")

def cellinfo1(data):			# process pack info
    infodata = data
    i = 4                       # Unpack into variables, skipping header bytes 0-3
    volts, amps, remain, capacity, cycles, mdate, balance1, balance2 = struct.unpack_from('>HhHHHHHH', infodata, i)
    volts=volts/100
    amps = amps/100
    capacity = capacity/100
    remain = remain/100
    watts = round(volts*amps, 2)  							# adding watts field for dbase
    message1 = {
        "meter": "bms",								# not sending mdate (manufacture date)
        "volts": volts,
        "amps": amps,
        "watts": watts,
        "remain": remain #DW,
#DW        "capacity": capacity #DW,
#DW        "cycles": cycles
    }
#DW    ret = mqtt.publish(gauge, payload=json.dumps(message1), qos=0, retain=False)
    ret = mqtt.publish(mqtt_topic, payload=json.dumps(message1), qos=0, retain=False)   #DW
    bal1 = (format(balance1, "b").zfill(16))		
    message2 = {
#DW        "meter": "bms",							# using balance1 bits for 16 cells
#DW        "c16" : int(bal1[0:1]),
#DW        "c15" : int(bal1[1:2]),                 # balance2 is for next 17-32 cells - not using
#DW        "c14" : int(bal1[2:3]), 							
#DW        "c13" : int(bal1[3:4]),
#DW        "c12" : int(bal1[4:5]), 				# bit shows (0,1) charging on-off			
#DW        "c11" : int(bal1[5:6]),
#DW        "c10" : int(bal1[6:7]),
#DW        "c09" : int(bal1[7:8]),
#DW        "c08" : int(bal1[8:9]),
#DW        "c07" : int(bal1[9:10]),
#DW        "c06" : int(bal1[10:11]), 		
#DW        "c05" : int(bal1[11:12]),
        "c04" : int(bal1[12:13]) ,
        "c03" : int(bal1[13:14]),
        "c02" : int(bal1[14:15]),
        "c01" : int(bal1[15:16])
    }
    ret = mqtt.publish(mqtt_topic, payload=json.dumps(message2), qos=0, retain=False)

def cellinfo2(data):
    infodata = data
    i = 0                          # unpack into variables, ignore end of message byte '77'
    protect,vers,percent,fet,cells,sensors,temp1,temp2,b77 = struct.unpack_from('>HBBBBBHHB', infodata, i)
    temp1 = round(((temp1-2731)/10*1.8) + 32, 2)  #DW
    temp2 = round(((temp2-2731)/10*1.8) + 32, 2)  #DW			# fet 0011 = 3 both on ; 0010 = 2 disch on ; 0001 = 1 chrg on ; 0000 = 0 both off
    prt = (format(protect, "b").zfill(16))		# protect trigger (0,1)(off,on)
    message3 = {
        "meter": "bms",
        "ovp" : int(prt[0:1]), 			# overvoltage
        "uvp" : int(prt[1:2]), 			# undervoltage
        "bov" : int(prt[2:3]), 		# pack overvoltage
        "buv" : int(prt[3:4]),			# pack undervoltage
        "cot" : int(prt[4:5]),		# current over temp
        "cut" : int(prt[5:6]),			# current under temp
        "dot" : int(prt[6:7]),			# discharge over temp
        "dut" : int(prt[7:8]),			# discharge under temp
        "coc" : int(prt[8:9]),		# charge over current
        "duc" : int(prt[9:10]),		# discharge under current
        "sc" : int(prt[10:11]),		# short circuit
        "ic" : int(prt[11:12]),        # ic failure
        "cnf" : int(prt[12:13])	    # config problem
    }
  #DW  ret = mqtt.publish(mqtt_topic, payload=json.dumps(message3), qos=0, retain=False)
    message4 = {
        "meter": "bms",
 #DW       "protect": protect,
        "percent": percent,
 #DW       "fet": fet,
 #DW       "cells": cells,
        "temp1": temp1,
        "temp2": temp2
    }
    ret = mqtt.publish(mqtt_topic, payload=json.dumps(message4), qos=0, retain=False)    # not sending version number or number of temp sensors

def cellvolts1(data):			# process cell voltages
    global cells1
    celldata = data             # Unpack into variables, skipping header bytes 0-3
    i = 4
    cell1, cell2, cell3, cell4 = struct.unpack_from('>HHHH', celldata, i)
    cells1 = [cell1, cell2, cell3, cell4] 	# needed for max, min, delta calculations
    message5 = {
        "meter" : "bms",
        "cell1": cell1,
        "cell2": cell2,
        "cell3": cell3,
        "cell4": cell4
    }
#DW    ret = mqtt.publish(gauge, payload=json.dumps(message5), qos=0, retain=False)
    ret = mqtt.publish(mqtt_topic, payload=json.dumps(message5), qos=0, retain=False) #DW

    cellsmin = min(cells1)          # min, max, delta
    cellsmax = max(cells1)
    delta = cellsmax-cellsmin
    mincell = (cells1.index(min(cells1))+1)
    maxcell = (cells1.index(max(cells1))+1)
    message6 = {
        "meter": meter,
        "mincell": mincell,
        "cellsmin": cellsmin,
        "maxcell": maxcell,
        "cellsmax": cellsmax,
        "delta": delta
    }
#DW    ret = mqtt.publish(gauge, payload=json.dumps(message6), qos=0, retain=False)
#DW    ret = mqtt.publish(mqtt_topic, payload=json.dumps(message6), qos=0, retain=False)  #DW

class MyDelegate(DefaultDelegate):		    # notification responses
	def __init__(self):
		DefaultDelegate.__init__(self)
	def handleNotification(self, cHandle, data):
		hex_data = binascii.hexlify(data) 		# Given raw bytes, get an ASCII string representing the hex values
		text_string = hex_data.decode('utf-8')  # check incoming data for routing to decoding routines
		if text_string.find('dd04') != -1:	                             # x04 (1-8 cells)	
			cellvolts1(data)
		elif text_string.find('dd03') != -1:                             # x03
			cellinfo1(data)
		elif text_string.find('77') != -1 and len(text_string) == 28 or len(text_string) == 36:	 # x03
			cellinfo2(data)		
try:
    print('attempting to connect')		
    bms = Peripheral(args.BLEaddress,addrType="public")
except BTLEException as ex:
    time.sleep(10)
    print('2nd try connect')
    bms = Peripheral(args.BLEaddress,addrType="public")
except BTLEException as ex:
    print('cannot connect')
    exit()
else:
    print('connected ',args.BLEaddress)

atexit.register(disconnect)
mqtt = paho.Client("control3")      #create and connect mqtt client
mqtt.username_pw_set("mqttuser",password="SuperSecretPassword") #DW
mqtt.connect(mqtt_broker,mqtt_port)
#mqtt.tls_set() #DW
bms.setDelegate(MyDelegate())		# setup bt delegate for notifications

mqtt.on_log=on_log #DW

	# write empty data to 0x15 for notification request   --  address x03 handle for info & x04 handle for cell voltage
	# using waitForNotifications(5) as less than 5 seconds has caused some missed notification
while True: #Indent the 5 lines below
    result = bms.writeCharacteristic(0x15,b'\xdd\xa5\x03\x00\xff\xfd\x77',False)		# write x03 w/o response cell info
    bms.waitForNotifications(5)
    result = bms.writeCharacteristic(0x15,b'\xdd\xa5\x04\x00\xff\xfc\x77',False)		# write x04 w/o response cell voltages
    bms.waitForNotifications(5)
    print('Waiting for', z, "seconds")
    time.sleep(z)
    print('Completed')


   
