#!/usr/bin/perl
## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl itself.
## Please see the Perl Artistic License.
## 
## Copyright (C) 2004-2016 Megan Squire <msquire@elon.edu>
## Contributions from:
## Evan Ashwell - converted from perl to python
##
## We're working on this at http://flossmole.org - Come help us build 
## an open and accessible repository for data and analyses for open
## source projects.
##
## If you use this code or data for preparing an academic paper please
## provide a citation to 
##
## Howison, J., Conklin, M., & Crowston, K. (2006). FLOSSmole: 
## A collaborative repository for FLOSS research data and analyses. 
## International Journal of Information Technology and Web Engineering, 1(3), 17–26.
##
## and
##
## FLOSSmole (2004-2016) FLOSSmole: a project to provide academic access to data 
## and analyses of open source projects.  Available at http://flossmole.org 
################################################################
# usage:
# > python3 2parseUbuntuLogs.py <datasource_id> <password>
#
# example usage:
# > python3 2parseUbuntuIRCLogs.py 62204 password
#
# purpose: 
# open each IRC log in the directory, parse out the interesting bits
# notes:
# START WITH THE FIRST DS IN THE DB YOU ARE INTERESTED IN
# Does not need to be the latest one
#
################################################################
import pymysql
import re
import sys
import codecs
import html
import datetime

datasource_id = str(sys.argv[1])
password      = str(sys.argv[2])
forge_id      = 43

if datasource_id and password:
	# connect to db (once at local grid6, and once at Syracuse)
	# dsn takes the format of "DBI:mysql:ossmole_merged:grid6.cs.elon.edu"
    try:
        db1 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='ossmole_merged',
                                  user='megan',
                                  password=password,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)
        
    try:
        db2 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='irc',
                                  user='megan',
                                  password=password,
                                  charset='utf8')
    
    except pymysql.Error as err:
        print(err)
        
    try:
         db3 = pymysql.connect(host='flossdata.syr.edu',
                                  database='irc',
                                  user='megan',
                                  password=password,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)
        
    cursor1 = db1.cursor()
    cursor2 = db2.cursor()
    cursor3 = db3.cursor()
    
    selectQuery = "SELECT datasource_id, comments \
		            FROM datasources \
            		WHERE datasource_id >= %s \
		            AND forge_id=%s"  
    cursor1.execute(selectQuery,(datasource_id,forge_id))
    rows = cursor1.fetchall()    
        
    for row in rows : 
        ds      = row[0]
        fileLoc = row[1] # the file location is kept in the 'comments' field
        print ("==================\n")

        # open the file
        print ("working on file: ", fileLoc)
        log  = codecs.open(fileLoc, 'r', encoding='utf-8', errors='ignore')
        line = log.read()
        line = line[2:]
        line = line[:-1]
        log  = line.split("\\n")
        
        # PARSE OUT DATE
        # date is in the filename, in the format:
        # 62204/20160512
        datelog = ""
        formatting = re.search('(.*?)\/(.*?)$',fileLoc)
        
        if formatting:  
            tempdate = formatting.group(2)
            correctForm = re.search('^(\d\d\d\d)(\d\d)(\d\d)',tempdate)
            if correctForm:
                datelog = correctForm.group(1) + "-" + correctForm.group(2) + "-" + correctForm.group(3)
            else:
                datelog = formatting.group(2)

        # PARSE OUT DETAILS        
        linenum = 0;
        for line in log:    
            linenum     += 1
            send_user    = ""
            timelog      = ""
            line_message = ""
            messageType  = ""
        
            # parse out rest of details & insert
            # 1. get system message vs regular message, parse
            # 2. insert
            #
            # here are the two patterns (note that the system message has no time):
            # === foo is now known as foo_
            # [00:04] <bar> your on unity desktop?
            #
            # note also that bots are treated as regular messages
            messageChecker = re.search('\[(.*?)\]\s+\<(.*?)\>(.*?$)',line)
            systemChecker  = re.search('\=\=\=\s+(.*?)$',line)
            if messageChecker: #regular message
                timelog = messageChecker.group(1)
                send_user = messageChecker.group(2)
                line_message = messageChecker.group(3)
                messageType = "message"
            elif systemChecker: # system message
                messageType = "system"
                line_message = systemChecker.group(1)
                timelog = None
                send_user = None
            
            if ((datasource_id) and (messageType != "")):
                insertQuery="INSERT IGNORE INTO ubuntu_irc (datasource_id,\
                             line_num,\
                             full_line_text,\
                             line_message,\
                             date_of_entry,\
                             time_of_entry,\
                             type,\
                             send_user,\
                             last_updated)\
                             VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                currDate=datetime.datetime.now()
                #======
                # LOCAL
                #======
                try:
                    cursor2.execute(insertQuery,
                                    (datasource_id,
                                     linenum,
                                     str(line),
                                     str(line_message),
                                     datelog,
                                     timelog,
                                     messageType,
                                     send_user,
                                     currDate))
                    db2.commit()
                except pymysql.Error as error:
                    print(error)
                    db2.rollback()
                    
                #======
                # REMOTE
                #======
                try:
                    cursor3.execute(insertQuery,
                                    (datasource_id,
                                     linenum,
                                     str(line),
                                     str(line_message),
                                     datelog,
                                     timelog,
                                     messageType,
                                     send_user,
                                     currDate))
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
else:
	print ("You need both a datasource_id and a password to start on your commandline.")
