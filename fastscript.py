from __future__ import division
import os, datetime, psutil, time, subprocess
import socket
import MySQLdb, MySQLdb.cursors
import logging
import pprint
import math
from decimal import *
import csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from sets import Set
from itertools import groupby

# Connect to database
try:
	db = MySQLdb.connect(host="localhost", user="root", passwd="123456", db="MONITOR") #, cursorclass=MySQLdb.cursors.SSCursor)
except Exception, e:
	print "Can't connect to database"

userID = 'root'

def getAllUsers():
	cursor = db.cursor()
	data = []
	try:
		cursor.execute("SELECT NAME FROM USER")
	except Exception, e:
		print "getAllUsers"
		print repr(e)
	
	for row in cursor:
		data.append(row)
	cursor.close()
	return data


def getUserID(userID):
	cursor = db.cursor()
	try:
		UID = cursor.execute("SELECT UID FROM USER WHERE NAME = %s", (userID, ))
	except Exception, e:
		print "getUserID"
		print repr(e)
	
	row = cursor.fetchone()
	cursor.close()
	return row[0]

def getJobs(UID):
	cursor = db.cursor()
	data = []
	try:
		cursor.execute("SELECT PID, START_TIME, CMD_NAME FROM JOB WHERE UID = %s", (UID, ))
	except Exception, e:
		print "getJobs"
		print repr(e)
	for row in cursor:
		data.append(row)
	cursor.close()
	return data

def getJobData(uid):
	cursor = db.cursor()
	data = []
	try:
		cursor.execute("SELECT CPU, RUN_TIME FROM jSAMPLE WHERE UID = %s", (uid, ))
	except Exception, e:
		print "getJobData"
		print repr(e)
	for row in cursor:
		data.append(row)
	cursor.close()
	return data


def getServer(UID):
	try:
		server = db.query("SELECT SERVER FROM USER WHERE UID = %s", (UID,))
	except Exception, e:
		print "getServer: ", repr(e)
	
	return server

def getServerData(server):
	try:
		db.query("SELECT * FROM sSAMPLE WHERE NAME = %s", server)
		server_data = db.store_result()
	except Exception, e:
		print "getServerData: ", repr(e)
	return server_data

def topFive(data):
	dict1 = dict()
	returnData = []

	for item in data:
		if item[2] in dict1:
			dict1[item[2]] = dict1[item[2]] + 1
		else:
			dict1[item[2]] = 1
	count = 0
	for w in sorted(dict1, key=dict1.get, reverse=True):
		if count <= 5:
			combine = str(w) + " " + str(dict1[w])
			returnData.append(combine)
		count += 1
	return returnData

def averageJobCPU(jobData):
	maxCPU = 0
	mySum = 0
	cpuTotal = []
	endTime = jobData[-1][1]
	startTime = jobData[0][1]
	CPUStart = jobData[0][0]

	dataPoints = len(jobData)
	timeDiff = int((endTime - startTime).total_seconds())

	if dataPoints < timeDiff:
		for row in jobData:
			if int((row[1] - startTime).total_seconds()) > 1:
				diff = int((row[1] - startTime).total_seconds()) - 1
				missingCPU = Decimal((CPUStart + row[0]) / 2 * diff) + row[0]
			else:
				missingCPU = row[0]
			cpuTotal.append(missingCPU)
			startTime = row[1]
			CPUStart = row[0]
		if row[0] > maxCPU:
			maxCPU = round(row[0], 3)
	else:
		for row in jobData:
			missingCPU = row[0]
			cpuTotal.append(missingCPU)
			if row[0] > maxCPU:
				maxCPU = round(row[0], 3)
		try:
			mySum = sum(cpuTotal) / timeDiff
		except Exception, e:
			pass
		
		if mySum == None or mySum == 0:
			return 0, timeDiff, maxCPU
		else:
			return mySum, timeDiff, maxCPU


def averageTotalCPU(avgCPU):
	return round(sum(agvCPU)/len(avgCPU), 2)


def averageTotalTime(runTime):
	return sum(runTime) / len(runTime)

def timeConverter(sec): 
	m, s = divmod(sec, 60)
	h, m = divmod(m, 60)
	return h, m, s

def generateGraphs(userID, user):
  userName = user[0]
  jobNames = []
  data = getJobs(userID)

  for item in data:
    jobNames.append(item[2])

  uniqueJobs = Set(jobNames)

  for item in uniqueJobs:
    avgCPU = []
    length = 0
    fig, ax = plt.subplots()
    itemData = []
    for row in data:
      rowData = []
      if row[2] == item:
        jobData = getJobData(userID)
        CPU, diff, maxCPU = averageJobCPU(jobData)
        avgCPU.append(CPU)
        for CPU in jobData:
          rowData.append(round(CPU[0], 3))
        if len(jobData) > length:
          length = len(jobData)
        itemData.append(rowData)
      x = [sum(e)/len(e) for e in zip(*itemData)]

      y = []
      count = 0
      while count < len(x):
        y.append(count)
        count += 1

      plt.plot(x)
      plt.axis([0, len(x), 0, 100])
      plt.xlabel('Time in Seconds')
      plt.ylabel('% CPU')
      #plt.title(userName + ' - ' + item + ' AvgCPU: ' + str(averageTotalCPU(avgCPU)))
      plt.grid(True)

  directory = 'tam/'
  if not os.path.exists(directory):
    os.makedirs(directory)
  plt.savefig(directory + item + '.png')
def keyfunc(timestamp, interval = 60):
	xt = datetime.datetime(2017, 3, 18)
	dt = datetime.datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S')
	delta_second = int((dt - xt).total_seconds())
	normalize_second = (delta_second / interval) * interval
	return xt + datetime.timedelta(seconds=normalize_second)

logging.basicConfig(level=logging.DEBUG, filename='dataPoints.log')
if __name__ == '__main__':
	d = {}
	userName = 'root'
	userID = getUserID(userName)
	data = getJobData(userID)
	generateGraphs(userID,['root'])
	results = []
	for k, g in groupby(data, key=lambda i: keyfunc(i[1])):
		n =  sum(1 for x in g)
		avg_level = sum([x[0] for x in g]) / n
		results.append((k, avg_level))
	print results
