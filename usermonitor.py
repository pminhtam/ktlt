import os, datetime, psutil, time, subprocess
import socket
import MySQLdb
import logging

# Connect to database
try:
	global db
	db = MySQLdb.connect(host="localhost", user="root",  \
			passwd="123456", \
			db="MONITOR")
except Exception, e:
	print 1, repr(2)
	print "Khong the ket noi DB"


# Global variables
excludeUser = ['root', 'avahi', 'dbus', 'polkitd', 'rpc',  \
			'rpcuser', 'postfix']
excludeProcess = ['sshd', 'bash', '(sd-pam)']
hostname = socket.gethostname()
cursor = db.cursor()
numOfCPU = psutil.cpu_count()
storeTime = {}

# Convert Bytes to MiB
def bytesToMegabytes(n):
	return n / 1024 / 1024


# Recursive algorithm to find all children of a process
# @profile
def getAllChildren(childList, parentPid):
	completeList = []
	for item in childList:
		ppid = int(item.split(' ')[0])
		pid = int(item.split(' ')[1])
		if ppid == parentPid:
			completeList.append(pid)
			completeList += 	\
				getAllChildren(childList, pid)
	return completeList


# Add pid to psutil process class
def addPID(pid):
	try:
		psutil.Process(pid)
	except psutil.NoSuchProcess:
		pass
	

# Get all processes run by user
# @profile
def processList2():
	processList = []
	for proc in psutil.process_iter():
		try:
			real, effective, saved = proc.uids()
		except psutil.NoSuchProcess:
			pass
		if real >= 1000:
			processList.append(proc)
	return processList

def getProcessList():
	processList = []

	pids = [pid for pid in os.listdir('/proc') 	\
			if pid.isdigit()]

	for pid in pids:
		try:
			filename = '/proc/' + str(pid)
			if os.stat(filename).st_uid >= 1000:
				try:
					processList.append(psutil.Process(int(pid)))
				except psutil.NoSuchProcess:
					pass
		except OSError:
			pass
	return processList


# Generates lists of pid, ppid and a combined list
# @profile
def getPidLists(processList):
	pidList, ppidList, allChildren = [], [], []

	for proc in processList:
		try:
			pinfo = proc.as_dict(attrs=['pid', 'name', 'ppid'])
		except psutil.NoSuchProcess:
			pass
		else:
			if pinfo['name'] not in excludeProcess:
				pid = pinfo['pid']
				ppid = pinfo['ppid']
				pidList.append(pid)
				ppidList.append(ppid)
				myStr = str(ppid) + ' ' + str(pid)
				allChildren.append(myStr)
	
	return pidList, ppidList, allChildren
			


# Creates a dictionary of key[parent pid] and all its value as children pids
# @profile
def processDict(pidList, ppidList, allChildren):
	parent, childOfParent = [], []
	localDict = {}

	for item in allChildren:
		if int(item.split(' ')[0]) not in pidList:
			parent.append(int(item.split(' ')[1]))
		if int(item.split(' ')[1]) not in ppidList:
			childOfParent.append(item)
	
	for pid in parent:
		localDict[pid] = getAllChildren(allChildren, pid)
	return localDict


# Returns of dictionary of parent pid and all its children as a process class
# @profile
def getCompleteDict(processDict, processList):
	finalDict = {}
	for key, value in processDict.iteritems():
		myList = []
		myProc = ''
		for proc in processList:
			try:
				pinfo = proc.as_dict(attrs=['ppid', 'pid', 'name'])
			except psutil.NoSuchProcess:
				print 'Fail: ', info
				pass
			else:
				if key == pinfo['ppid']:
					myProc = proc
				if pinfo['pid'] in value:
					myList.append(proc)
		if myProc != '':
			finalDict[myProc] = myList

	return finalDict


# Calculates process system resource usage
# @profile
def calculateResources(processDict):
	# For each main process and all its children
	for proc, children in processDict.iteritems():
		try:
			childrenList = children

			# Get user data and resource data for proc and resource data for children
			timestamp, real, username, name, pid, cmd = getProcData(proc)
			procCPU, procRAM, procrss, procvms, procReadIO, procWriteIO = getProcResources(proc, pid)

			childCPU, childRAM, childrss, childvms, childReadIO, childWriteIO = getChildData(childrenList)

			# Sum proc and children
			sumCPU = round((procCPU + childCPU) / numOfCPU, 3)
			sumRAM = round((procRAM + childRAM), 3)
			rss = bytesToMegabytes(procrss + childrss)
			vms = bytesToMegabytes(procvms + childvms)
			ReadIO = procReadIO + childReadIO
			WriteIO = procWriteIO + childWriteIO

			# Add the user, job and sample to database if not exists
			addUser(real, username)
			addJob(pid, real, timestamp, name, cmd)
			addSample(pid, real, timestamp, sumCPU, sumRAM, rss, vms, ReadIO, WriteIO)

			db.commit()
		except psutil.NoSuchProcess:
			pass
	cleanupStoreTime(processDict)
	

# @profile
def getProcData(proc):
	try:
		timestamp = datetime.datetime.fromtimestamp(proc.create_time()).strftime("%Y-%m-%d %H:%M:%S")
		real, effective, saved = proc.uids()
		username = proc.username()
		name = proc.name()
		pid = proc.pid
		cmd = ' '.join(proc.cmdline())
	except psutil.NoSuchProcess:
		pass
	return timestamp, real, username, name, pid, cmd

# @profile
def getProcResources(proc, pid):
	try:
		rIO, wIO, rB, wB, read_chars, write_chars 	\
			= proc.io_counters()
		rss, vms, shared, text, lib, data, dirty = proc.memory_info()
		print "CPU percent:", proc.cpu_percent(), "%"
		sumCPU = proc.cpu_percent()
		sumRAM = proc.memory_percent()
		sumRAMrss = rss
		sumRAMvms = vms
		sumReadIO = rIO
		sumWriteIO = wIO
	except Exception, e:
		print "getProcessResouces"
		print repr(e)
	return sumCPU, sumRAM, sumRAMrss, sumRAMvms, sumReadIO, sumWriteIO


# @profile
def getChildData(childrenList):
	sumCPU = 0.0
	sumRAM = 0.0
	sumRAMrss = 0
	sumRAMvms = 0
	sumReadIO = 0
	sumWriteIO = 0
	
	for child in childrenList:
		if child.is_running():
			try:
				pid = child.pid
				addPID(pid)
				rIO, wIO, rB, wB, read_chars,	\
				write_chars  = child.io_counters()

				rss, vms, shared, text, lib, data, dirty = child.memory_info()
				cpu = child.cpu_percent()
				sumCPU += cpu
				sumRAM += child.memory_percent()
				sumRAMrss += rss
				sumRAMvms += vms
				sumReadIO += rIO
				sumWriteIO += wIO
			except Exception, e:
				print "getChildData"
				print 12, repr(e)

	return sumCPU, sumRAM, sumRAMrss, sumRAMvms, sumReadIO, sumWriteIO


# @profile
def cleanupStoreTime(pDict):
	allKeys = []
	for key, value in pDict.iteritems():
		allKeys.append(key)
	
	for key, value in list(storeTime.items()):
		if key not in allKeys:
			del storeTime[key]

# @profile
def addServer(hostname):
	host = [hostname]
	try:
		server = 0
		cursor.execute("SELECT `NAME` FROM `SERVER` WHERE NAME = %s", (host, ))
		for row in cursor:
			server = row[0]
	except Exception, e:
		print "SELECT FROM `SERVER` ERROR!"
		print e

	if server == 0:
		try:
			cursor.execute("INSERT INTO `SERVER` (NAME) VALUES (%s)", (host, ))
		except Exception, e:
			print "INSERT INTO `SERVER` ERROR!"
			print e
		
# @profile
def addUser(real, username):
	try:
		user = 0
		cursor.execute("SELECT `UID` FROM `USER` WHERE `UID` = %s AND `NAME` = %s", (real, username))
		for row in cursor:
			user = row[0]
	except Exception, e:
		print "SELECT `UID` ERROR!"
		print e
	
	if user == 0:
		try:
			cursor.execute("INSERT INTO USER (UID, NAME, SERVER) VALUES (%s, %s, %s)", (real, username, hostname))
		except Exception, e:
			print "INSERT INTO USER ERROR!"
	else:
		try:
			cursor.execute("UPDATE USER SET SERVER = %s WHERE UID = %s AND NAME = %s", (hostname, real, username))
		except Exception, e:
			print "UPDATE USER ERROR!"


# @profile 
def addJob(pid, real, timestamp, name, cmd):
	try:
		isRunning = 0
		cursor.execute("SELECT PID FROM JOB WHERE UID = %s AND PID = %s AND START_TIME = %s", (real, pid, timestamp))
		for row in cursor:
			isRunning = row[0]
	except Exception, e:
		print "SELECT PID ERROR!"
	
	if isRunning == 0:
		try:
			cursor.execute("INSERT INTO JOB(PID, UID, START_TIME, CMD_NAME, COMMAND, SERVER) VALUES (%s, %s, %s, %s, %s, %s)", (pid, real, timestamp, name, cmd, hostname))
		except Exception, e:
			print "INSERT INTO JOB ERROR!"

# @profile
def addSample(pid, real, timestamp, CPU, RAM, rss, vms, ReadIO, WriteIO):
	try:
		cursor.execute("INSERT INTO jSAMPLE (PID, UID, START_TIME, CPU, RAM, RAM_RSS, RAM_VMS, DISK_IN, DISK_OUT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (pid, real, timestamp, CPU, RAM, rss, vms, ReadIO, WriteIO))
	except Exception, e:
		print "INSERT INTO jSAMPLE ERROR!"


logging.basicConfig(level=logging.DEBUG, filename='process.log')
if __name__ == '__main__':
	while(True):
		try:
			addServer(hostname)
			
			myDict = []
			processList = processList2()
			pidList, ppidList, childrenList = getPidLists(processList)
			myDict = processDict(pidList, ppidList, childrenList)
			completeDict = getCompleteDict(myDict, processList)

			calculateResources(completeDict)
		except:
			logging.exception("Error: ")
		time.sleep(1)

