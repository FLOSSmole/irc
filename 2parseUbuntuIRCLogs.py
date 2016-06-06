#!/usr/bin/perl
## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl itself.
## Please see the Perl Artistic License 2.0.
## 
## Copyright (C) 2004-2015 Megan Squire <msquire@elon.edu>
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
# > python3 2parseActiveMQIRCLogs.pl <datasource_id><password> 
#
# example usage:
# > python3 2parseActiveMQIRCLogs.pl 62204 password 
#
# purpose: 
# open each IRC log in the directory, parse out the interesting bits
# save to database table
# 
# each date = one datasource_id
# each line is incremented with a number for that date
################################################################
import pymysql
import sys
import re
import codecs
import datetime

datasource_id = str(sys.argv[1])
password= str(sys.argv[2])
forge_id = 36
newDS= datasource_id
if datasource_id and password:
	# connect to db (once at local grid6, and once at Syracuse)
	# dsn takes the format of "DBI:mysql:ossmole_merged:grid6.cs.elon.edu"
    try:
        db1 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='test',
                                  user='eashwell',
                                  password=password,
                                  charset='utf8')
    
    except pymysql.Error as err:
        print(err)
    try:
        db2 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='test',
                                  user='eashwell',
                                  password=password,
                                  charset='utf8')
    
    except pymysql.Error as err:
        print(err)
        
    try:
         db3 = pymysql.connect(host='flossdata.syr.edu',
                                  database='rubygems',
                                  user='megan',
                                  password=password,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)
    
    cursor1= db1.cursor()
    cursor2= db2.cursor()
    cursor3= db3.cursor()
    
    selectQuery="select datasource_id, comments \
		from test.datasources \
		where datasource_id >= %s \
		and forge_id=%s"  
    cursor1.execute(selectQuery,(datasource_id,forge_id))
    rows=cursor1.fetchall()    
    
    print(rows)
     
    for row in rows : 
        newDS= row[0]
        fileLoc= row[1]
        print ("==================\n")
        
        #date is in the filename, in the format:
        # 48039/20140306
        datelog =""
        formatting= re.search("^(.*?)\/(.*?)$",fileLoc)
        if formatting:
            tempdate = formatting.group(2)
            correctForm= re.search("^(\d\d\d\d)(\d\d)(\d\d)",tempdate)
            if correctForm:
                datelog = correctForm.group(1) + "-" + correctForm.group(2) + "-" + correctForm.group(3)
                fileLoc= formatting.group(1)+ "/" + datelog
            else:
                datelog=formatting.group(2)
        
        # open the file
        print("working on: ", fileLoc)
        log = codecs.open(fileLoc, 'r', encoding='utf-8', errors='ignore')
        line=log.read()
        line= line[2:]
        line= line[:-3]
        log=line.split("\\n")
        
        
        
        # Parse out details
        linenum = 0
        for line in log:   
            print(line)
            linenum= linenum+1
            send_user = ""
            timelog = ""
            line_message = ""
            messageType = ""
        
            # parse out rest of details & insert
            # 1. get system message vs regular message, parse
            # 2. insert
            #
            # here are the two patterns:
            # [20:05] <zmhassan> any tips would be gladly appreciated
            # [16:09] *** jbonofre has quit IRC (Excess Flood)
            regularMessage= re.search('^\[(.*?)\]\s+\<(.*?)\>\s+(.*?)$',line)
            systemMessage= re.search('^\[(.*?)\]\s+\*\*\*\s+(.*?)$',line)
            if regularMessage: #regular message
                timelog = regularMessage.group(1)
                send_user = regularMessage.group(2)
                line_message = regularMessage.group(3)
                messageType = "message";
            elif systemMessage: # system message
                messageType = "system";
                timelog = systemMessage.group(1)
                line_message = systemMessage.group(2)
            

            if ((datasource_id) and (messageType != "")):
                insertQuery= "INSERT INTO `apache_activemq_irc`(`datasource_id`,\
                              line_num,\
                              full_line_text,\
                              line_message,\
                              date_of_entry,\
                              time_of_entry\
                              ,type\
                              ,send_user\
                              ,last_updated) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                currDate= datetime.datetime.now()
                
                #======
                # LOCAL
                #======                
                try:    
                    cursor2.execute(insertQuery,
                                   (newDS,
                                    linenum,
                                    line,
                                    line_message,
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
                                   (newDS,
                                    linenum,
                                    line,
                                    line_message,
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
	exit;
