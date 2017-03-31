import MySQLdb
import sys
import datetime

# connect to database
try:
	db = MySQLdb.connect(host="localhost", user="root", passwd="123456", db="MONITOR")
except Exception, e:
	print "Can't connect to database"

def bytesToMegabytes(n):
	return n / 1024 / 1024

def existingUser(userID):
	cursor = db.cursor()
	try:
		userExists = 0
		cursor.execute("SELECT USER_NAME FROM PREDICTION WHERE UID = %s", (userID, ))
		for row in cursor:
			userExists = row[0]
	except Exception, e:
		print "existingUser"
		print repr(e)
	cursor.close()

	if userExists == 0:
		return False
	else:
		return True

# Find last server and log in time to decide if RAM cache is importtant 
def lastUsedServer(userID):
	cursor = db.cursor()
	try:
		cursor.execute("SELECT LAST_USED_SERVER, LAST_LOGIN FROM PREDICTION WHERE UID = %s", (userID, ))
		for row in cursor:
			lastServer, lastLogin = row
	except Exception, e:
		print "lastUsedServer"
		print repr(e)
		pass
	cursor.close()

	return lastServer, lastLogin

# Find the RAM load on the server since last login
def serverRAMLoad(server, timestamp):
	cursor = db.cursor()
	try:
		cursor.execute("SELECT MIN(DISK_IN), MAX(DISK_IN) FROM sSAMPLE WHERE NAME = %s AND TIMESTAMP > %s", (server, timestamp))
		minDiskIn, maxDiskIn = 0, 0
		for row in cursor:
			minDiskIn, maxDiskIn = row
		if (minDiskIn, maxDiskIn) == (None, None):
			minDiskIn, maxDiskIn = 0, 0

	except Exception, e:
		print "serverRamLoad"
		print repr(e)

	# Get the amount of cached RAM on the server
	try:
		cursor.execute("SELECT RAM_CACHED FROM sSAMPLE WHERE NAME = %s AND DISK_IN > %s", (server, maxDiskIn))
		cachedRAM = 0
		for row in cursor:
			cachedRAM = row[0]
	except Exception, e:
		print "serverRamLoad 2"
		print repr(e)
	cursor.close()

	# Calculate the amount of data tha has been read since last login
	diskDifference = maxDiskIn - minDiskIn
	return diskDifference, cachedRAM

# Get the RAM usage for the user
def getUserAvgRAM(userID):
	cursor = db.cursor()
	try: 
		cursor.execute("SELECT AVG_RAM FROM PREDICTION WHERE UID = %s", (userID, ))
		avgRAM = 0
		for row in cursor:
			avgRAM = row[0]
	except Exception, e:
		print "getUserAvgRAM"
		print repr(e)
	return avgRAM


# Calculate to see if the server might have the user data left in cache
def calculateCache(serverLoad, userLoad, cache):
	serverLoad = bytesToMegabytes(serverLoad)
	cacheNotReplace = cache - serverLoad

	if cacheNotReplace > userLoad:
		return True
	else:
		return False


# Find the active load on the server and the predicted load 
def getServerCPU(server):
	cursor = db.cursor()
	try:
		serverCPU, timestamp = 0, 0
		cursor.execute("SELECT CPU, TIMESTAMP FROM sSAMPLE WHERE NAME = %s ORDER BY TIMESTAMP DESC", (server, ))
		for row in cursor:
			serverCPU, timestamp = row
	except Exception, e:
		print "getServerCPU"
		print repr(e)
	
	cursor.close()

	lastHour = timestamp - datetime.timedelta(hours=1)

	# Get the predicted load on the server from the users currently active 
	cursor = db.cursor()
	try:
		userCPU = []
		cursor.execute("SELECT AVG_CPU FROM PREDICTION WHERE LAST_USED_SERVER = %s AND LAST_LOGIN > %s", (server, lastHour))
		for row in cursor:
			userCPU.append(row[0])
	except Exception, e:
		print "getServerCPU 2"
		print repr(e)
	cursor.close()
	
	# Since active users reprents the current load on the system we compare which is the largest of them (predicted vs active) and choose the largest to challenge the user
	if serverCPU > sum(userCPU):
		return serverCPU
	else:
		return sum(userCPU)


# Get the predicted CPU load for the user
def getUserCPU(userID):
	cursor = db.cursor()
	try:
		cursor.execute("SELECT AVG_CPU FROM PREDICTION WHERE UID = %s", (userID, ))
	except Exception, e:
		print "getUserCPU"
		print repr(e)
	
	for item in cursor:
		userCPU = item[0]
	
	cursor.close()
	return userCPU

# Find the server with the most available CPU
def leastLoadServer():
	cursor = db.cursor()
	try:
		serverList = []
		cursor.execute("SELECT NAME FROM SERVER")
		for row in cursor:
			serverList.append(row[0])
	except Exception, e:
		print "leastLoadServer"
		print repr(e)

	serverList = []
	for name in cursor:
		serverList.append(name[0])

	serverDict = {}
	for server in serverList:
		activeServerLoad, timestamp = 0, 0
		cursor.execute("SELECT CPU, TIMESTAMP FROM sSAMPLE WHERE NAME = %s ORDER BY TIMESTAMP DESC", (server, ))
		for item in cursor:
			activeServerLoad, timestamp = item

		lastHour = timestamp - datetime.timedelta(hours=1)

		userLoad = []
		cursor.execute("SELECT AVG_CPU FROM PREDICTION WHERE LAST_USED_SERVER = %s AND LAST_LOGIN > %s", (server, lastHour))

		for item in cursor:
			userLoad.append(item[0])

		totalServerLoad = activeServerLoad + sum(userLoad)
		serverDict[server] = totalServerLoad
	
	cursor.close()

	# Print (erturn the server with the lowest load
	print min(serverDict, key=serverDict.get)

if __name__ == '__main__':
	userID = sys.argv[1]
	userExists = existingUser(userID)

	# If the server is already in the prediction table we want to check if RAM cache is available
	if userExists:
		lastServer, lastLogin = lastUsedServer(userID)
		loadSinceLast, cachedRAM = serverRAMLoad(lastServer, lastLogin)

		# Get the avg RAM usage for the user from the prediction table
		avgRAMUsage = getUserAvgRAM(userID)
		# See if cache is still available
		cacheAvailable = calculateCache(loadSinceLast, avgRAMUsage, cachedRAM)

		# if there is still cache available, we check if the server has enough memory CPU to support the user
		if cacheAvailable:
			serverCPU = getServerCPU(lastServer)
			userCPU = getUserCPU(userID)
			totalCPU = serverCPU + userCPU
			if totalCPU < 100:
				print lastServer
			else:
				leastLoadServer()
		else:
			leastLoadServer()
	else:
		leastLoadServer()
