#!/usr/bin/perl
## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl itself.
## Please see the Perl Artistic License.
## 
## Copyright (C) 2004-2015 Megan Squire <msquire@elon.edu>
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
## FLOSSmole (2004-2015) FLOSSmole: a project to provide academic access to data 
## and analyses of open source projects.  Available at http://flossmole.org 
#
import pymysql
import re
import sys
import codecs
import html
import datetime
# --------------------------------------------------
# subroutine: parseFile
# takes: two database connections (local and remote) and a datasource_id
# purpose:
# --get each file on disk
# --pull out the lines
# --parse out the pieces of the lines
# --write each line to the irc table in both local and remote db
# --------------------------------------------------
def parseFile(dbh2, dbh3, ds, fileLoc):
    p_dbh2  = dbh2
    p_dbh3  = dbh3
    p_ds= ds
    p_fileLoc = fileLoc
    cursor2=p_dbh2.cursor()
    #date is in the filename, in the format:
    # 51146/2015apr09
    datelog =""
    formatting= re.search('(.*?)\/(.*?)$',p_fileLoc)
    if formatting:              
        tempdate = formatting.group(2)
        print("got " + tempdate + " for date")
        tempdateMatch= re.search('(\d\d\d\d)-(\w\w\w)-(\d\d)',tempdate)
        if tempdateMatch:
        	#convert three letter abbr to 2-digit month
            mm= tempdateMatch.group(2)
            datelog = tempdateMatch.group(1) + "-" + mm + "-" + tempdateMatch.group(3)
        else:
            datelog= tempdate
    datelog=datetime.datetime.strptime(datelog,"%Y-%b-%d")
    # open the file
    try:
        log = codecs.open(fileLoc, 'r', encoding='utf-8', errors='ignore')
        line=log.read()
        line= line[2:]
        line= line[:-1]
    except pymysql.Error as err:
        print(err)
        
    lineCollector= re.search("<ul id=\"ll\">(.*?)<\/ul>",line)
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
                send_user= lineSep.group(1)
                junk2= lineSep.group(2)
                line_num= lineSep.group(3)
                junk4= lineSep.group(4)
                line_message= lineSep.group(5)
                
                # clean up html
                clean_line_message = html.unescape(line_message)
                print(datelog)
                print ("inserting row for ", line_num)

                # insert row into table 
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
                    cursor2.execute(insertQuery,(p_ds,line_num,li,clean_line_message,datelog,None,'message',send_user,datetime.datetime.now()))
                    dbh3.commit()
                except pymysql.Error as err:
                    print(err)
"""            
                #======
                # REMOTE
                #======

                    my $insert3 = $p_dbh3->prepare(qq{
									INSERT IGNORE INTO django_irc
										(datasource_id, 
										line_num,
										full_line_text,
										line_message,
										date_of_entry,
										type,
										send_user,
										last_updated) 
									VALUES (?,?,?,?,?,'message',?,NOW())
									});
				$insert3->execute($p_ds, $line_num, $li, $clean_line_message, $datelog, $send_user)
					or die "Couldn't execute statement on REMOTE: " . $insert3->errstr;
				$insert3->finish();
"""
################################################################
# usage:
# > perl 2parseDjangoLogs.pl <datasource_id> 
#
# example usage:
# > perl 2parseDjangoIRCLogs.pl 51146 
#
# purpose: 
# open each IRC log in the directory, parse out the interesting bits
# notes:
# START WITH THE FIRST DS IN THE DB YOU ARE INTERESTED IN
# Does not need to be the latest one
#
################################################################

datasource_id = str(sys.argv[1])
password= str(sys.argv[2])
forge_id = 42

if datasource_id:
    try:
        dbh2 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='test',
                                  user='eashwell',
                                  password=password,
                                  charset='utf8')
    
    except pymysql.Error as err:
        print(err)
    try:
        dbh3 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='test',
                                  user='eashwell',
                                  password=password,
                                  charset='utf8')
    
    except pymysql.Error as err:
        print(err)
        """
    try:
         dbh3 = pymysql.connect(host='flossdata.syr.edu',
                                  database='rubygems',
                                  user='megan',
                                  password=password,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)
        dbh2= "remote"
        """
    cursor= dbh2.cursor()
    
    selectQuery="select datasource_id, comments \
		from test.datasources \
		where datasource_id >= %s \
		and forge_id=%s \
           Limit 1"  
    cursor.execute(selectQuery,(datasource_id,forge_id))
    rows=cursor.fetchall()
    
    print(rows)
    print("returned")
     
    for row in rows : 
        ds= row[0]
        fileLoc= row[1]
        print ("==================\n")
        parseFile(dbh2, dbh3, ds, fileLoc)
    cursor.close()
    dbh2.close()
    dbh3.close()
    print("done")
else:
	print ("You need both a datasource_id and a date to start on your commandline.")
	exit;

