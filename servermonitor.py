import psutil
import socket
import pwd, os, time
import subprocess
import MySQLdb
import logging


try:
	db = MySQLdb.connect(host="192.168.229.1", user="minhtam", passwd="123456", db="MONITOR")
except Exception, e:
	print "Can't connect to database"

cursor = db.cursor()
hostname = socket.gethostname()

def bytesToMegabytes(n):
	return n / 1024 / 1024


def getUsers():
	users = subprocess.check_output("who")
	return set([x.split()[0] for x in users.splitlines()])

def getCPU():
	numOfCPU = psutil.cpu_count()
	usageCPU = psutil.cpu_percent()
	currentUsage = usageCPU / numOfCPU
	return round(usageCPU, 3)

def getRAM():
	total, available, percent, used, free, active, inactive, 	\
	buff, cache, shared  = psutil.virtual_memory()
	return round(percent, 3), bytesToMegabytes(available), 	\
		bytesToMegabytes(cache)


def getIO():
	read_count, write_count, read_bytes, write_bytes, 	\
		read_time, write_time, read_merged_count, 	\
		write_merged_count, busy_time = 		\
			psutil.disk_io_counters()
	return read_bytes, write_bytes

def getNET():
	bytes_sent, bytes_recv, packets_sent, packets_recv, 	\
		errin, errout, dropin, dropout = 	\
		psutil.net_io_counters()
	return round(bytes_sent, 9), round(bytes_recv, 9)

def updateServer():
	cpu = getCPU()
	read_count, write_count = getIO()
	ram, available, cache = getRAM()
	
	try:
		server = 0
		print hostname
		cursor.execute("SELECT NAME FROM SERVER WHERE NAME = %s", (hostname, ))
		for row in cursor:
			server = row[0]
	except Exception, e:
		print "SELECT NAME ERROR!"
	
	if server == 0:
		try:
			cursor.execute("INSERT INTO SERVER (NAME) VALUES (%s)", (hostname, ))
		except Exception, e:
			print "INSERT INTO SERVER ERROR!"
			print repr(e)
	print read_count, write_count
	try:
		cursor.execute("INSERT INTO sSAMPLE (NAME, CPU, RAM, RAM_AVAILABLE, RAM_CACHED, DISK_IN, DISK_OUT) VALUES (%s, %s, %s, %s, %s, %s, %s)", (hostname, cpu, ram, available, cache, read_count, write_count))
	except Exception, e:
		print "INSERT INTO sSAMPLE ERROR!"
		print repr(e)
	db.commit()


logging.basicConfig(level=logging.DEBUG, filename="server.log")

if __name__ == '__main__':
	while(True):
		try:
			updateServer()
		except:
			logging.exception("Error: ")
		time.sleep(60)
