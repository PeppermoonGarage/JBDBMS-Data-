#!/usr/bin/python
from subprocess import Popen
import sys

filename = "/home/pi/jbdbms_socket.py"
while True:
    print("\nStarting " + filename)
    p = Popen("python3 " + filename + " -b A4:C1:38:6B:5C:29 -i 30 -m jbdbms", shell=True)
    p.wait()