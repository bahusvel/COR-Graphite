__author__ = 'denislavrov'
from cor.api import CORModule, Message
import struct
import ipaddress
import socket
import sys


class GraphiteGrapher(CORModule):

	def sensor_reading(self, message):
		ipid = self.locationID
		for src in message.source[:-1]:
			addr = ipaddress.ip_address(struct.unpack(">I", src))
			ipid += "/" + addr

		gaddr = ipid.replace('.', '_').replace('/', '.') + '.' + message.payload["type"]
		tstamp = int(float(message.payload["timestamp"]))
		message.payload.pop("timestamp")
		message.payload.pop("type")
		vals = message.payload
		lines = []
		for key in vals.keys():
			lines.append("%s %s %d" % (gaddr + '.' + key, vals[key], tstamp))
		msg = '\n'.join(lines) + '\n'
		self.sock.sendall(msg.encode("ASCII"))

	def __init__(self, locationID=None, server='127.0.0.1', port=2003, *args, **kwargs):
		super().__init__(*args, **kwargs)
		if locationID is None:
			pass  # query manager for his location
		self.locationID = locationID
		self.sock = socket.socket()
		try:
			self.sock.connect((server, port))
		except:
			print("Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % {
				'server': server,
				'port': port})
			sys.exit(1)
		self.add_topics({"SENSOR_READING": self.sensor_reading})
