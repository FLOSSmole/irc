#!/usr/bin/perl
## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl itself.
## Please see the Perl Artistic License 2.0.
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
################################################################
# usage:
# > perl 1getPerl6IRCLogs.pl <new_datasource_id> <date-to-start> 
#
# THIS DATASOURCE IS THE NEXT ONE AVAIL IN THE DB - AND IT WILL GET INCREMENTED
# DATE TO START is the oldest un-collected date; 
# the script will go from there through yesterday in order
# example usage:
# > perl 1getPerl6IRCLogs.pl 51255 20150406
#
# purpose: 
# grab all the IRC logs from http://irclog.perlgeek.de/perl6/
# parse these files looking for facts to populate the django irc table
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


# --------------------------------------------------
# get the files from the web site
# starting with the date given on the command line
# store each URL as a local file
# --------------------------------------------------
def grabFiles(dbh1, dbh2, datasource_id, urlstem, date_to_start):
    cursor= dbh1.cursor()
    """
    cursor2= dbh2.cursor()
    """       
    p_datasource_id = datasource_id
    p_urlstem = urlstem
    p_date_to_start = date_to_start
    
    newds = p_datasource_id
        
    #get yesterday's date
    yesterday= datetime.datetime.now()-timedelta(days= 1)
    print ("yesterday's date is:",yesterday)
    dates= datetime.datetime(int(p_date_to_start[0:4]),int(p_date_to_start[4:-2]),int(p_date_to_start[6:]))
    while(dates <= yesterday):
        print ("working on ...")
        print (date)
        
        # get yyyy, mm, dd and put into URL
        yyyy = dates.year
        mm   = dates.month
        dd   = dates.day
        # put leading zeroes on dd & mm
        if (dd < 10):
            dd = str("0" + str(dd))
        if (mm < 10):
            mm = str("0"+str(mm))
        fileName= "/"+str(yyyy)+str(mm)+str(dd)
        
        # get file
        # Log URLs are in this format:
        # http://irclogs.ubuntu.com/2015/03/09/%23ubuntu.txt
        filestem = str(yyyy) + "-" +str(mm) + "-" +str(dd)
        newURL = p_urlstem + filestem
        print ("getting URL " + newURL)

        saveLoc = p_datasource_id +fileName
        print ("...saving as:",  saveLoc)
        
        try:
            html = urllib2.urlopen(newURL).read()
        except urllib2.HTTPError as error:
            print(error)
        else:
            fileloc = datasource_id + fileName
            outfile = codecs.open(fileloc,'w')
            outfile.write(str(html))
            outfile.close()
        #======
        # LOCAL
        #======
            try:
                cursor.execute(u"INSERT INTO datasources1(datasource_id, \
                    forge_id, \
                    friendly_name, \
                    date_donated, \
                    contact_person, \
                    comments, \
                    start_date, \
                    end_date)  \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                    (str(newds), 
                     forge_id,
                     'Perl 6 IRC ' + str(yyyy) +str(mm) + str(dd),
                     datetime.datetime.now(),
                    'msquire@elon.edu', 
                    saveLoc, 
                    datetime.datetime.now(), 
                    datetime.datetime.now()))
                dbh1.commit() 
            except pymysql.Error as error:
                print(error)
                dbh1.rollback()
        #increment date by one
        dates= dates + timedelta(days=1)
        newds= int(newds) +1
    cursor.close()
    
        #=====
        # REMOTE
        #=====
"""
        try:
            cursor2.execute(u"INSERT INTO datasources(datasource_id, \
                forge_id, \
                friendly_name, \
                date_donated, \
                contact_person, \
                comments, \
                start_date, \
                end_date)  \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                (str(newds), 
                forge_id,
                'Ubunto IRC',
                str(datetime.now()),
                'msquire@elon.edu', 
                fileloc, 
                str(datetime.now()), 
                str(datetime.now())))
            dbh2.commit() 
        except pymysql.Error as error:
            print(error)
            dbh2.rollback()
       
        
        #increment date by one
        date= date + timedelta(day = 1)
        newds= newds +1
"""

datasource_id = str(sys.argv[1])
date_to_start = str(sys.argv[2])
password= str(sys.argv[3])
urlstem = "http://irclog.perlgeek.de/perl6/"
forge_id = 65

if datasource_id and date_to_start:
    try:
        dbh1 = pymysql.connect(host='grid6.cs.elon.edu',
                                  database='test',
                                  user='gbatchelor',
                                  password=password,
                                  charset='utf8')
    
    except pymysql.Error as err:
        print(err)
    try:
         dbh2 = pymysql.connect(host='flossdata.syr.edu',
                                  database='rubygems',
                                  user='megan',
                                  password=password,
                                  charset='utf8')
    except pymysql.Error as err:
        print(err)
        dbh2= "remote"
    
    os.mkdir (datasource_id)
    grabFiles(dbh1, dbh2, datasource_id, urlstem, date_to_start)
    dbh1.close()
else:
	print ("You need both a datasource_id and a date to start on your commandline.")
	exit;
