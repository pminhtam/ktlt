import time
import MySQLdb

# Connect to database
try:
	global db
	db = MySQLdb.connect(host="localhost", user="root",passwd="123456",db="MONITOR")
except Exception, e:
	print 1, repr(2)
	print "Khong the ket noi DB"


def predictUserLoad(userID):
	cursor = db.cursor()
	# A test to see if the user already exits in the Prediction table
	userExists = 0
	cursor.execute("SELECT UID FROM PREDICTION WHERE UID = %s", (userID, ))
	for row in cursor:
		userExist = row[0]
	
	
	# Get the monitored data for the user from the user monitor table
	sqlSelect = ("SELECT MAX(CPU), MAX(RAM), AVG(CPU), AVG(RAM), MAX(RUN_TIME) FROM jSAMPLE WHERE UID = %s", (userID, ))
	maxCPU, maxRAM, avgCPU, avgRAM, runTime = 0, 0, 0, 0, 0
	cursor.execute(sqlSelect[0], sqlSelect[1])
	for row in cursor:
		maxCPU, maxRAM, avgCPU, avgRAM, runTime = row

	# Get the user name and which server the user was last logged in to
	sqlSelect =  ("SELECT NAME, SERVER FROM USER WHERE UID = %s", (userID, ))
	name, server = 0, 0
	cursor.execute(sqlSelect[0], sqlSelect[1])
	for row in cursor:
		name, server = row

	# if the user doesn't exist we insert the new user which the data from the user monitor table
	if userExists == 0:
		sqlInsert = ("INSERT INTO PREDICTION (UID, USER_NAME, LAST_USED_SERVER, LAST_LOGIN, AVG_CPU, MAX_CPU, AVG_RAM, MAX_RAM) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", (userID, name, server, runTime, avgCPU, maxCPU, avgRAM, maxRAM))
		cursor.execute(sqlInsert[0], sqlInsert[1])

	# if the user already exists in the precdiction table we combine the exist data with the new monitored data
	else:
		sqlSelect = ("SELECT AVG_CPU, MAX_CPU, AVG_RAM, MAX_RAM FROM PREDICITON WHERE UID = %s", (userID, ))
		preAvgCPU, preMaxCPU, preAvgRAM, preMaxRAM = 0, 0, 0, 0
		cursor.execute(sqlSelect)
		for row in cursor:
			preAvgCPU, preMaxCPU, preAvgRAM, preMaxRAM = row

		# We want to adjust the value of average CPU for the user in the prediction table by using an algorithm tha can adjust the value based on the difference
		difference = avgCPU - preAvgCPU
		newAvgCPU = changeAlgorithm(avgCPU, preAvgCPU, difference)

		# We want to adjust the value of average RAM for the user in the prediction table by using an algorithm that can adjust the value based on the difference
		difference = avgRAM - preAvgRAM
		newAvgRAM = changeAlgorithm(avgRAM, preAvgRAM, difference)

		# We check if the user have heavier CPU jobs running in the system
		if maxCPU > preMaxCPU:
			newMaxCPU = maxCPU
		else:
			newMaxCPU = preMaxCPU

		# Check if the user have heavier RAM jobs running in the system
		if maxRAM > preMaxRAM:
			newMaxRAM = maxRAM
		else:
			newMaxRAM = preMaxRAM
		# Update the prediction table with the new calculated data
		sqlInsert = ("UPDATE PREDICTION SET LAST_USED_SERVER = %s, LAST_LOGIN = %s, AVG_CPU = %s, MAX_CPU = %s, AVG_RAM = %s, MAX_RAM = %s WHERE UID = %s", (server, runTime, newAvgCPU, newMaxCPU, newAvgRAM, newMaxRAM, userID))
		cursor.execute(sqlInsert[0], sqlInsert[1])

		# Clean up the user monitor table
		sqlDelete = ("DELETE FROM jSAMPLE WHERE UID = %s AND RUN_TIME < %s", (userID, runTime))
		cursor.execute(sqlDelete)
	cursor.close()
	db.commit()

while (True):
	userIDs = []
	cursor = db.cursor()
	cursor.execute("SELECT UID FROM USER")
	for row in cursor:
		userIDs.append(row[0])

	for userID in userIDs:
		print userID
		predictUserLoad(userID)
		
	cursor.close()
	time.sleep(3600)

