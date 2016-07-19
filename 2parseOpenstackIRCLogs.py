# -*- coding: utf-8 -*-
## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl itself.
## Please see the Perl Artistic License 2.0.
## 
## Copyright (C) 2004-2016 Megan Squire <msquire@elon.edu>
## With code contributions from:
## Gavan Roth
## Becca Gazda
## Evan Ashwell
## 
## We're working on this at http://flossmole.org - Come help us build 
## an open and accessible repository for data and analyses for open
## source projects.
## 
## If you use this code or data for preparing an academic paper please
## provide a citation to:
## 
## Howison, J., Conklin, M., & Crowston, K. (2006). FLOSSmole: 
## A collaborative repository for FLOSS research data and analyses. 
## International Journal of Information Technology and Web Engineering, 
## 1(3), 17–26.
## 
## and
## 
## FLOSSmole (2004-2016) FLOSSmole: a project to provide academic access to data 
## and analyses of open source projects.  Available at http://flossmole.org 
################################################################
# usage: 
# > python 2parseOpenstackIRCLogs.py datasource_id filepath password
#
# example:
# > python 2parseOpenstackIRCLogs.py 63707 openstack password
#
# filepath can be one of: openstack, dev, infra, meeting, meeting-alt, meeting-3, dns
# run this seven times, once for each irc type
################################################################
import re
import codecs
import pymysql
import sys
import datetime

# takes user inputs
datasource_id = int(sys.argv[1])
filePath      = str(sys.argv[2])
pw            = str(sys.argv[3])

if filePath == 'openstack':
    forge_id = '49'
    tablename = 'openstack_irc'
elif filePath == 'dev':
    forge_id = '50'
    tablename = 'openstackdev_irc'
elif filePath == 'infra':
    forge_id = '51'
    tablename = 'openstackinfra_irc'
elif filePath == 'meeting':
    forge_id = '52'
    tablename = 'openstackmeeting_irc'
elif filePath == 'meeting-alt':
    forge_id = '53'
    tablename = 'openstackmeetingalt_irc'
elif filePath == 'meeting-3':
    forge_id = '54'
    tablename = 'openstackmeeting3_irc'
elif filePath == 'dns':
    forge_id = '55'
    tablename = 'openstackdns_irc'
else:
    print("invalid irc type, must be one of openstack, dev, infra, meeting, meeting-alt, meeting-3, or dns")
    exit
    
# Establishes connection to the databases (local and remote)
db1 = pymysql.connect(host="grid6.cs.elon.edu",
                      user="megan",
                      passwd=pw,
                      db="ossmole_merged",
                      use_unicode=True,
                      charset="utf8")
cursor1 = db1.cursor()

db2 = pymysql.connect(host="grid6.cs.elon.edu",
                      user="megan",
                      passwd=pw,
                      db="openstack_irc",
                      use_unicode=True,
                      charset="utf8")
cursor2 = db2.cursor()

db3 = pymysql.connect(host="flossdata.syr.edu",
                      user="megan",
                      passwd=pw,
                      db="openstack_irc",
                      use_unicode=True,
                      charset="utf8")
cursor3 = db3.cursor()

# get files to process
cursor1.execute('SELECT datasource_id, comments \
                 FROM datasources \
                 WHERE datasource_id >= %s \
                 AND forge_id = %s',
                (datasource_id, forge_id))
rows = cursor1.fetchall()

for row in rows:
    current_ds = row[0]
    fileLoc = row[1]
    linecounter = 0
    
    patternDateOfEntry = re.compile('^(.*)\.txt$', re.UNICODE)
    dateOfEntry = patternDateOfEntry.search(fileLoc.split('/', 1)[1]).group(1)

    print('processing ', current_ds, ' at ', fileLoc, 'for ', dateOfEntry)
    
    log = codecs.open(fileLoc, 'r', encoding='utf-8', errors='ignore')
    line=log.read()
    line= line[2:]
    line= line[:-1]
    log=line.split("\\n")
    
    for newLine in log:
        # set up db variables to be parsed out
        linecounter += 1
        lineMessage = ""
        timeOfEntry = ""
        lineType = ""
        sendUser = ""
        # ==================
        # === scenario 1 ===
        # the timestamps have a T in them
        # four examples below are: action, message (incl. bots), system, github
        #2014-01-02T19:36:27  * [gnubie] waves
        #2014-03-31T07:27:09  <JacobSanford> Hey everyone.
        #2014-03-31T15:16:26  -github- [razor-server] smcclellan opened pull
        #2015-06-22T00:28:00  *** Bendoin <Bendoin!~bendoin@62.119.162.213> has j
        
        if (newLine[10:11] == "T"):
            try:
                pattern1 = re.compile('^(.*?)T(.*?)\s\s(.*?)$', re.UNICODE)
                timeOfEntry = pattern1.search(newLine).group(2)
                restOfLine  = pattern1.search(newLine).group(3)
                
                if (restOfLine[0:2] == "* "):
                    lineType = "action"
                    lineMessage = restOfLine[2:]
                    # detect sendUser is the first word after the *
                    pattern1a = re.compile('(.*?) (.*?)', re.UNICODE)
                    sendUser = pattern1a.search(lineMessage).group(1)
                elif(restOfLine[:1] == "<"):
                    lineType = "message"
                    # detect sendUser is between the <>
                    # detect lineMessage is everything after the >
                    pattern1m = re.compile('\<(.*?)\>(.*?)$', re.UNICODE)
                    sendUser = pattern1m.search(restOfLine).group(1)
                    lineMessage = pattern1m.search(restOfLine).group(2)
                elif(restOfLine[0:1] == "-"):
                    lineType = "github"
                    lineMessage = restOfLine            
                    pattern1g = re.compile('\-(.*?)\-(.*?)$', re.UNICODE)
                    sendUser = pattern1g.search(restOfLine).group(1)
                elif(restOfLine[0:3] == "***"):
                    lineType = "system"
                    sendUser = None
                    pattern1s = re.compile('\*\*\*\s+(.*?)$', re.UNICODE)
                    lineMessage = pattern1s.search(restOfLine).group(1)
            except:
                continue
        # ==================
        # === scenario 2 ===
        # the times are in []
        # three examples below are: action, message, system
        #[2008/04/24 23:05:19] * Volcane tries
        #[2013/03/22 07:02:25] <upgrayedd> what's @options in the context
        #[2013/06/22 20:59:24] @ lavaman joined channel #puppet-razor
        
        elif (newLine[:1] == "["):
            try:
                pattern2 = re.compile('^\[(.*?) (.*?)\]\s(.*?)$', re.UNICODE)
                timeOfEntry = pattern2.search(newLine).group(2)
                restOfLine  = pattern2.search(newLine).group(3)
                
                if (restOfLine[0:2] == "* "):
                    lineType = "action"
                    lineMessage = restOfLine[2:]
                    # detect sendUser is the first word after the *
                    pattern2a = re.compile('(.*?) (.*?)', re.UNICODE)
                    sendUser = pattern2a.search(lineMessage).group(1)
                elif(restOfLine[:1] == "<"):
                    lineType = "message"
                    # detect sendUser is between the <>
                    # detect lineMessage is everything after the >
                    pattern2m = re.compile('\<(.*?)\>(.*?)$', re.UNICODE)
                    sendUser = pattern2m.search(restOfLine).group(1)
                    lineMessage = pattern2m.search(restOfLine).group(2)
                elif(restOfLine[0:1] == "-"):
                    lineType = "github"
                    pattern2g = re.compile('\-(.*?)\-(.*?)$', re.UNICODE)
                    sendUser = pattern2g.search(restOfLine).group(1)
                    lineMessage = restOfLine
                elif(restOfLine[0:2] == "@ "):
                    lineType = "system"
                    sendUser = None
                    lineMessage = restOfLine[2:]
            except:
                continue
        # ==================
        # === scenario 3 ===
        # the time and date are separated by a space
        # the examples below are system:     
        #2014-02-10 00:24:48	-->	gnarf (gnarf@unaffiliated/gnarf) has joi
        #2014-02-10 00:22:48	<--	shr3kst3r (shr3kst3r@2600:3c00::f03c:91f 
        #2014-02-10 04:08:53	--	Topic for #puppet-dev is "Discussion for
        # the example below is a message:
        # 2014-02-10 00:00:21	NivagSwerdna	which returns null {"node_count"
        # the example below is an action:
        # 2014-02-10 10:39:18	 *	Dominic wonders if there's a language or
        
        elif (newLine[10:11] == " "):
            try:            
                pattern3 = re.compile('^(.*?) (.*?)\s+(.*?)$', re.UNICODE)
                timeOfEntry = pattern3.search(newLine).group(2)
                restOfLine  = pattern3.search(newLine).group(3)
            
                if (restOfLine[0:1] == "*"):
                    lineType = "action"
                    pattern3a = re.compile('\*\s+(.*?) (.*?)$', re.UNICODE)
                    sendUser = pattern3a.search(restOfLine).group(1)
                    lineMessage = sendUser + " " + pattern3a.search(restOfLine).group(2) 
                elif(restOfLine[0:3] == "<--") or (restOfLine[0:3] == "-->") or (restOfLine[0:2] == "--"):
                    lineType = "system"
                    pattern3s = re.compile('\S+\s+(.*?)$', re.UNICODE)
                    sendUser = None
                    lineMessage = pattern3s.search(restOfLine).group(1)
                else:
                    lineType = "message"
                    pattern3m = re.compile('(.*?)\s+(.*?)$', re.UNICODE)
                    sendUser = pattern3m.search(restOfLine).group(1)
                    lineMessage = pattern3m.search(restOfLine).group(2)
            except:
                continue
        #uncomment for debugging
        #print lineType,"|",dateOfEntry,"|",timeOfEntry,"|",sendUser,"|",lineMessage         
        
        #takes care of extra blank lines etc
        else:
            continue
        # insert parsed stuff into db
            
        insertQuery = "INSERT IGNORE INTO " + tablename + "(datasource_id, \
                            line_num, \
                            line_message, \
                            date_of_entry, \
                            time_of_entry,\
                            type, \
                            send_user, \
                            last_updated) \
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        dataVariables = (current_ds,
                         linecounter,
                         lineMessage,
                         dateOfEntry,
                         timeOfEntry,
                         lineType,
                         sendUser,
                         datetime.datetime.now())
        try:
            cursor2.execute(insertQuery, dataVariables)
            db2.commit()
        except pymysql.Error as error:
            print(error)
            db2.rollback()        
        try:
            cursor3.execute(insertQuery, dataVariables)
            db3.commit()
        except pymysql.Error as error:
            print(error)
            db3.rollback()            
cursor1.close()
cursor2.close()
cursor3.close()

db1.close()
db2.close()
db3.close()
