## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl itself.
## Please see the Perl Artistic License 2.0.
## 
## Copyright (C) 2004-2016 Megan Squire <msquire@elon.edu>
## Major Contributions from Evan Ashwell (converted from perl to python)
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
# > python 1getDjangoIRCLogs.py <new_datasource_id> <date-to-start> 
#
# THIS DATASOURCE IS THE NEXT ONE AVAIL IN THE DB - AND IT WILL GET INCREMENTED
# (one per day)
# DATE TO START is the oldest un-collected date; 
# the script will go from there through yesterday, in order
# example usage:
# > python 1getDjangoIRCLogs.py 61838 20160603
#
# purpose: 
# grab all the IRC logs from "http://django-irc-logs.com/"
# parse these files looking for facts to populate the django irc table
################################################################

import sys
import pymysql
import datetime
from datetime import date, timedelta
import os
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2
import codecs
 
datasource_id = str(sys.argv[1])
dateToStart   = str(sys.argv[2])
password      = str(sys.argv[3])
urlStem       = 'http://django-irc-logs.com/'
forgeID       = 42
newDS         = int(datasource_id)

if datasource_id and dateToStart and password:
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
   
    # make directory and save file
    os.mkdir(datasource_id)
    
    # get yesterday's date
    yesterday = datetime.datetime.now() - timedelta(days = 1)
    print ("yesterday's date is:",yesterday)
    dateS = datetime.datetime(int(dateToStart[0:4]),int(dateToStart[4:-2]),int(dateToStart[6:]))
    
    while(dateS <= yesterday):
        print("working on ...")
        print(dateS)
        
        # get yyyy, mon, dd to put into URL
        yyyy = dateS.year
        # convert month to three letter abbr
        mon  = dateS.strftime("%b").lower()
        dd   = dateS.day
        
        # put leading zeroes on dd 
        if (dd < 10):
            dd = str("0" + str(dd))
            
        # get file
        # Log URLs are in this format:
        # http://django-irc-logs.com/2014/sep/13/
        urlFile = str(yyyy) + "/" + str(mon) + "/" + str(dd) 
        fullURL = urlStem + urlFile
        print ("getting URL:", fullURL)       
        
        try:
            html = urllib2.urlopen(fullURL).read()
        except urllib2.HTTPError as error:
            print(error)
        else:
            saveLoc = str(datasource_id) + "/" + str(yyyy) + str(mon) + str(dd)
            print ("...saving as:", saveLoc) 
            outfile = codecs.open(saveLoc,'w')
            outfile.write(str(html))
            outfile.close()
        #======
        # LOCAL
        #======   
            try:
                cursor1.execute(u"INSERT INTO datasources(datasource_id, \
                    forge_id, \
                    friendly_name, \
                    date_donated, \
                    contact_person, \
                    comments, \
                    start_date, \
                    end_date)  \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                    (str(newDS), 
                     forgeID,
                     'Django IRC' + fullURL,
                     datetime.datetime.now(),
                     'msquire@elon.edu', 
                     saveLoc, 
                     datetime.datetime.now(), 
                     datetime.datetime.now()))
                db1.commit() 
            except pymysql.Error as error:
                print(error)
                db1.rollback()
                
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
                    (str(newDS), 
                     forgeID,
                     'Django IRC' + fullURL,
                     datetime.datetime.now(),
                     'msquire@elon.edu', 
                     saveLoc, 
                     datetime.datetime.now(), 
                     datetime.datetime.now()))
                db2.commit() 
            except pymysql.Error as error:
                print(error)
                db2.rollback()
        #increment date by one & datasource_id by one
        dateS = dateS + timedelta(days=1)
        newDS += 1     
    cursor1.close()
    cursor2.close()
    db1.close()
    db2.close()
else:
	print ("You need both a datasource_id and a date to start on your commandline.")
