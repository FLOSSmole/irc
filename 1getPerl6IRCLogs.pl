# -*- coding: utf-8 -*-
## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl itself.
## Please see the Perl Artistic License 2.0.
## 
## Copyright (C) 2004-2016 Megan Squire <msquire@elon.edu>
## Other contributors: Greg Batchelor - converted perl to python
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
#
################################################################
# usage:
# > python 1getPerl6IRCLogs.py <new_datasource_id> <date-to-start> <password>
#
# THIS DATASOURCE IS THE NEXT ONE AVAIL IN THE DB - AND IT WILL GET INCREMENTED
# DATE TO START is the oldest un-collected date; 
# the script will go from there through yesterday in order
#
# example usage:
# > python 1getPerl6IRCLogs.py 62938 20150603 password
#
# purpose: 
# grab all the IRC logs from http://irclog.perlgeek.de/perl6/
# parse these files looking for facts to populate the perl6 irc table
################################################################
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2
import pymysql
import datetime
from datetime import date, timedelta
import sys
import codecs
import os

datasource_id = str(sys.argv[1])
date_to_start = str(sys.argv[2])
password      = str(sys.argv[3])
urlstem       = "http://irclog.perlgeek.de/perl6/"
forge_id      = 65
newDS         = datasource_id

if datasource_id and date_to_start and password:
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
         db2 = pymysql.connect(host='flossdata.syr.edu',
                                  database='ossmole_merged',
                                  user='megan',
                                  password=password,
                                  use_unicode=True,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)
    
    cursor1 = db1.cursor()
    cursor2 = db2.cursor()
    
    os.mkdir (datasource_id)
    
    # get yesterday's date - we will stop collecting on that date
    yesterday = datetime.datetime.now()-timedelta(days= 1)
    dateS = datetime.datetime(int(p_date_to_start[0:4]),int(p_date_to_start[4:-2]),int(p_date_to_start[6:]))
    while(dateS <= yesterday):
        print ("working on ...")
        print (date)
        
        # get yyyy, mm, dd to put into URL
        yyyy = dateS.year
        mm   = dateS.month
        dd   = dates.day
        # put leading zeroes on dd & mm
        if (dd < 10):
            dd = str("0" + str(dd))
        if (mm < 10):
            mm = str("0"+str(mm))
                
        # get file
        # Log URLs are in this format:
        # https://irclog.perlgeek.de/perl6/2016-05-20
        newURL   = urlstem + str(yyyy) + "-" + str(mm) + "-" + str(dd)
        print ("getting URL... ", newURL)

        saveLoc = datasource_id + "/" + str(yyyy) + str(mm) + str(dd)
        print ("saving as...", saveLoc)
        
        try:
            html = urllib2.urlopen(newURL).read()
        except urllib2.HTTPError as error:
            print(error)
        else:
            outfile = codecs.open(saveLoc,'w')
            outfile.write(str(html))
            outfile.close()
        
        insertQuery = "INSERT INTO datasources(datasource_id, \
                    forge_id, \
                    friendly_name, \
                    date_donated, \
                    contact_person, \
                    comments, \
                    start_date, \
                    end_date)  \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        #======
        # LOCAL
        #======
            try:
                cursor1.execute(insertQuery, 
                    (str(newDS), 
                     forge_id,
                     'Perl 6 IRC ' + str(yyyy) +str(mm) + str(dd),
                     datetime.datetime.now(),
                     'msquire@elon.edu', 
                     saveLoc, 
                     datetime.datetime.now(), 
                     datetime.datetime.now()))
                db1.commit() 
            except pymysql.Error as error:
                print(error)
                db1.rollback()
        #======
        # REMOTE
        #======               
            try:
                cursor2.execute(insertQuery, 
                    (str(newDS), 
                     forge_id,
                     'Perl 6 IRC ' + str(yyyy) +str(mm) + str(dd),
                     datetime.datetime.now(),
                     'msquire@elon.edu', 
                     saveLoc, 
                     datetime.datetime.now(), 
                     datetime.datetime.now()))
                db2.commit() 
            except pymysql.Error as error:
                print(error)
                db2.rollback()
        #increment date by one
        dateS = dateS + timedelta(days=1)
        newDS += 1
    cursor1.close()
    cursor2.close()
    
    db1.close()
    db2.close()

else:
	print ("You need both a datasource_id and a date to start on your commandline.")
