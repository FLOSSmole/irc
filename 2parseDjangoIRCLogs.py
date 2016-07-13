# -*- coding: utf-8 -*-
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
# > python3 2parseDjangoLogs.py <datasource_id> <password>
#
# example usage:
# > python3 2parseDjangoIRCLogs.py 62204 password
#
# purpose: 
# open each IRC log in the directory, parse out the interesting bits
################################################################
import pymysql
import re
import sys
import codecs
import datetime
import html

datasource_id = str(sys.argv[1])
password      = str(sys.argv[2])
forge_id      = 42

if datasource_id and password:
    try:
        db1 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='ossmole_merged',
                                  user='megan',
                                  password=password,
                                  use_unicode=True,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)
    
    try:
        db2 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='irc',
                                  user='megan',
                                  password=password,
                                  use_unicode=True,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)
       
    try:
         db3 = pymysql.connect(host='flossdata.syr.edu',
                                  database='irc',
                                  user='megan',
                                  password=password,
                                  use_unicode=True,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)
   
       
    cursor1 = db1.cursor()
    cursor2 = db2.cursor()
    cursor3 = db3.cursor()
    
    selectQuery="SELECT datasource_id, comments \
		        FROM datasources \
		        WHERE datasource_id >= %s \
		        AND forge_id=%s"  
    cursor1.execute(selectQuery,(datasource_id,forge_id))
    rows = cursor1.fetchall()
     
    for row in rows: 
        ds= row[0]
        fileLoc = row[1] # the comments column holds the file location
        print ("==================\n")
        
        # open the file
        log = codecs.open(fileLoc, 'r', encoding='utf-8', errors='ignore')
        line=log.read()
        line= line[2:]
        line= line[:-1]
        
        # PARSE OUT DATE
        # date is in the filename, in the format:
        # 51146/2015apr09
        datelog = ""
        formatting = re.search('(.*?)\/(.*?)$',fileLoc)
        if formatting:              
            tempdate = formatting.group(2)
            print("got " + tempdate + " for date")
            tempdateMatch = re.search('(\d\d\d\d)-(\w\w\w)-(\d\d)',tempdate)
            if tempdateMatch:
                yyyy = tempdateMatch.group(1)
                mon = tempdateMatch.group(2)
                dd = tempdateMatch.group(3)
                datelog = yyyy + "-" + mon + "-" + dd
            else:
                datelog = tempdate
        datelog = datetime.datetime.strptime(datelog,"%Y%b%d")
        
        # PARSE OUT line_message, line_num, send_user
        lineCollector = re.search("<ul id=\"ll\">(.*?)<\/ul>",line)
        if lineCollector:
            ul = lineCollector.group(1);
            lis = ul.split("\\n")
            for li in lis:
                send_user = ""
                line_message = ""
                messageType = ""
                # here is the pattern
                #<li class="le" rel="frege"><a href="#1462000" name="1462000">#</a> <span style="color:#481e7c;8" class="username" rel="frege">&lt;frege&gt;</span> I have a south question </li>
                lineSep= re.search("<li class=\"le\" rel=\"(.*?)\">(.*?)name=\"(.*?)\">(.*?)</span>(.*?)<\/li>$",li)
        
                if lineSep:
                    send_user    = lineSep.group(1)
                    line_num     = lineSep.group(3)
                    line_message = lineSep.group(5)
            
                    # clean up html
                    clean_line_message = html.unescape(line_message)
                    print(datelog)
                    print ("inserting row for ", line_num)

                    #======
                    # LOCAL
                    #======
                    try:
                        insertQuery="INSERT INTO `django_irc`(`datasource_id`,\
                        `line_num`,\
                        `full_line_text`,\
                        `line_message`, \
                        `date_of_entry`, \
                        `time_of_entry`, \
                        `type`, \
                        `send_user`, \
                        `last_updated`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        cursor2.execute(insertQuery,(datasource_id,
                                                    line_num,
                                                    li,
                                                    clean_line_message,
                                                    datelog,
                                                    None,
                                                    'message',
                                                    send_user,
                                                    datetime.datetime.now()))
                        db2.commit()
                    except pymysql.Error as err:
                        print(err)

                    #======
                    # REMOTE
                    #======

                    try:
                        cursor3.execute(insertQuery,(datasource_id,
                                                    line_num,
                                                    li,
                                                    clean_line_message,
                                                    datelog,
                                                    None,
                                                    'message',
                                                    send_user,
                                                    datetime.datetime.now()))
                        db3.commit()
                    except pymysql.Error as err:
                        print(err)
    
    cursor1.close()
    cursor2.close()
    cursor3.close()
    db1.close()
    db2.close()
    db3.close()
else:
	print ("You need a datasource_id, dateToStart, and password on your commandline.")
 
