# -*- coding: utf-8 -*-
# This program is free software; you can redistribute it
# and/or modify it under the GPLv2
#
# We're working on this at http://flossmole.org - Come help us build
# an open and accessible repository for data and analyses for open
# source projects.
#
# If you use this code or data for preparing an academic paper please
# provide a citation to
#
# Howison, J., Conklin, M., & Crowston, K. (2006). FLOSSmole:
# A collaborative repository for FLOSS research data and analyses.
# International Journal of Information Technology and
# Web Engineering, 1(3), 17–26.
#
# and
#
# FLOSSmole: a project to provide academic access to data
# and analyses of open source projects. Available at http://flossmole.org
#
################################################################
# usage:
# > python parseChatzillaLogFile.py <datasource_id> <file> <debug mode> <password>
#
# example usage:
# > python parseChatzillaLogFile.py 8441 log.txt PROD password
#
# purpose:
# open a chatzilla log file with results of /list in it, and parse out the
# topics, channels, and number of users; put these in table in db
################################################################
import pymysql
import re
import sys
import datetime
import codecs

datasource_id = str(sys.argv[1])
file = str(sys.argv[2])
DEBUG = str(sys.argv[3])
password = str(sys.argv[4])

if (DEBUG and datasource_id and file):
    if DEBUG == 'PROD':
        try:
            dbh1 = pymysql.connect(host='grid6.cs.elon.edu',
                                   database='irc',
                                   user='megan',
                                   password=password,
                                   charset='utf8')
        except pymysql.Error as err:
            print(err)
        cursor1 = dbh1.cursor()
        try:
            dbh2 = pymysql.connect(host='flossdata.syr.edu',
                                   database='irc',
                                   user='megan',
                                   password=password,
                                   charset='utf8')
        except pymysql.Error as err:
            print(err)
        cursor2 = dbh2.cursor()

    # read in file
    print("opening file:" + file)
    try:
        log = codecs.open(file, 'r', encoding='utf-8', errors='ignore')
    except pymysql.Error as error:
        print(error)

    # undef $/;
    # here is a typical line:
    # [2014-03-25 09:42:12] === #softuni   3
    # https://softuni.bg/ - Software University Bulgaria

    for line in log:
        # parse out channel name
        line = re.search('^(.*?)===\s#(.*?)\s+(.*?)\s+(.*?)$', str(line))
        if line:
            junk = line.group(1)
            channel_name = line.group(2)
            num_users = line.group(3)
            topic = line.group(4)
        else:
            junk = ""
            channel_name = ""
            num_users = ""
            topic = ""
        """
        print("channel name: " + channel_name)
        print("num users: " + num_users)
        print("topic: ", topic)
        print("===")
        """
        # insert into database
        if ((DEBUG == "PROD")):
            print("ran")
            try:
                insertQuery = "INSERT INTO fn_irc_channels (channel_name,\
                                                         num_users,\
                                                         topic,\
                                                         datasource_id,\
                                                         last_updated)\
                                                         VALUES (%s,%s,%s,%s,\
                                                         %s)"
                cursor1.execute(insertQuery, (channel_name, num_users,
                                              topic, datasource_id,
                                              datetime.datetime.now()))
                dbh1.commit()
            except pymysql.Error as error:
                print(error)
                dbh1.rollback()

    cursor1.close()
    dbh1.close()
    cursor2.close()
    dbh2.close()

else:
    print("You need a datasource_id, a file,\
          and a debug mode on your commandline.")
    exit
    
