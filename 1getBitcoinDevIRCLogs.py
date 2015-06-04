## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl.
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
# 1getBitcoinDevIRCLogs.py 51313 20100922
# 51313 in that example is hte next available datasource id in the database
# 20100922 is the date to start with
# purpose: 
# grab all the IRC logs from http://bitcoinstats.com/irc/bitcoin-dev/logs/
################################################################

import datetime
import urllib2
import codecs
import sys
import os
import MySQLdb

# constants
urlstem = 'http://bitcoinstats.com/irc/bitcoin-dev/logs/'

# grab commandline args
datasource_id = str(sys.argv[1])
mystartdate = str(sys.argv[2])

# make a date object from my startdate
startdate = datetime.datetime.strptime(mystartdate, '%Y%m%d').date()

# calculate yesterday's date
today = datetime.date.today()
oneday = datetime.timedelta(days=1)
yesterday = today - oneday

# initialize counters
currentdate = startdate
newds = int(datasource_id)

# Open local database connection
db1 = MySQLdb.connect(host="local.host",\
    user="user", \
    passwd="pass", \
    db="ossmole_merged", \
    use_unicode=True, \
    charset="utf8")
cursor1 = db1.cursor()
cursor1.execute('SET NAMES utf8mb4')
cursor1.execute('SET CHARACTER SET utf8mb4')
cursor1.execute('SET character_set_connection=utf8mb4')

# Open remote database connection
db2 = MySQLdb.connect(host="remote.host",\
    user="user", \
    passwd="pass", \
    db="ossmole_merged", \
    use_unicode=True, \
    charset="utf8")
cursor2 = db2.cursor()
cursor2.execute('SET NAMES utf8mb4')
cursor2.execute('SET CHARACTER SET utf8mb4')
cursor2.execute('SET character_set_connection=utf8mb4')

# for each date between the startdate and yesterday, 
# go grab the corresponding log file.
# bitcoindev log file URLs are in the format:
# http://bitcoinstats.com/irc/bitcoin-dev/logs/2010/09/22
os.makedirs(datasource_id)
while (currentdate <= yesterday):
    year = str(currentdate.year)
    month = str(currentdate.month).zfill(2)
    day = str(currentdate.day).zfill(2)
    print "processing ",currentdate
    url = urlstem + year + "/" + month + "/" + day
    print "grabbing", url
    
    # to stop 403 Forbidden errors
    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}

    req = urllib2.Request(url, headers=hdr)
    
    # grab the file and write it out
    html = urllib2.urlopen(req).read()
    fileloc = datasource_id + '/' + str(currentdate)
    outfile = codecs.open(fileloc,'w')
    outfile.write(html)
    outfile.close()
    
    # put new datasource_id in databases
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
            (str(newds), 
            '66',
            'Bitcoin-Dev IRC Logs',
            str(today),
            'msquire@elon.edu', 
            fileloc, 
            str(today), 
            str(today)))
        db1.commit() 
    except MySQLdb.Error as error:
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
            (str(newds), 
            '66',
            'Bitcoin-Dev IRC Logs',
            str(today),
            'msquire@elon.edu', 
            fileloc, 
            str(today), 
            str(today)))
        db2.commit() 
    except MySQLdb.Error as error:
        print(error)
        db2.rollback()
        
        
    currentdate = currentdate + oneday
    newds += 1
db1.close()
db2.close()
cursor1.close()
cursor2.close()
