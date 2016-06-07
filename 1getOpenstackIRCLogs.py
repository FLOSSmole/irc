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
# > python 1getOpenstackIRCLogs.py datasource_id startdate irctype password
#
# example:
# > python 1getOpenstackIRCLogs.py 63707 2014-06-10 openstack password
#
# startdate is the next date you want to collect from in the format 2015-08-31
# irctype can be one of: openstack, dev, infra, meeting, meeting-alt, meeting-3, dns
# run this seven times, once for each irc type
################################################################

try:
    import urllib.request as urllib2
except ImportError:
    import urllib2
import codecs
import datetime
import sys
import os
import pymysql
    
# takes user inputs
# ds stand for the next available data source
#the start date is the day after the last known date data was collected
# irctype is openstack, dev, infra, meeting, meeting-alt, meeting-3, or dns
ds      = sys.argv[1]
start   = sys.argv[2]
irctype = sys.argv[3]
pw      = sys.argv[4]
newds   = int(ds)

if irctype == 'openstack':
    directory = 'openstack'
    forge_id = '49'
    urlstem = "http://eavesdrop.openstack.org/irclogs/%23openstack/%23openstack."
    if not os.path.exists(directory):
        os.makedirs(directory)
elif irctype == 'dev':
    forge_id = '50'
    directory = 'dev'
    urlstem = "http://eavesdrop.openstack.org/irclogs/%23openstack-dev/%23openstack-dev."
    if not os.path.exists(directory):
        os.makedirs(directory)
elif irctype == 'infra':
    forge_id = '51'
    directory = 'infra'
    urlstem = "http://eavesdrop.openstack.org/irclogs/%23openstack-infra/%23openstack-infra."
    if not os.path.exists(directory):
        os.makedirs(directory)
elif irctype == 'meeting':
    forge_id = '52'
    directory = 'meeting'
    urlstem = "http://eavesdrop.openstack.org/irclogs/%23openstack-meeting/%23openstack-meeting."
    if not os.path.exists(directory):
        os.makedirs(directory)
elif irctype == 'meeting-alt':
    forge_id = '53'
    directory = 'meeting-alt'
    urlstem = "http://eavesdrop.openstack.org/irclogs/%23openstack-meeting-alt/%23openstack-meeting-alt."
    if not os.path.exists(directory):
        os.makedirs(directory)
elif irctype == 'meeting-3':
    forge_id = '54'
    directory = 'meeting-3'
    urlstem = "http://eavesdrop.openstack.org/irclogs/%23openstack-meeting-3/%23openstack-meeting-3."
    if not os.path.exists(directory):
        os.makedirs(directory)
elif irctype == 'dns':
    forge_id = '55'
    directory = 'dns'
    urlstem = "http://eavesdrop.openstack.org/irclogs/%23openstack-dns/%23openstack-dns."
    if not os.path.exists(directory):
        os.makedirs(directory)
else:
    print("invalid irc type, must be one of openstack, dev, infra, meeting, meeting-alt, meeting-3, or dns")
    
    
if irctype == 'openstack':
    friendly= 'openstack IRC Logs'
else:
    friendly = 'openstack-' + directory + ' IRC Logs '

# dates
startDate   = datetime.datetime.strptime(start, '%Y-%m-%d').date()
endDate     = datetime.date.today()
dateDonated = datetime.datetime.now()

# databases
db1 = pymysql.connect(host="grid6.cs.elon.edu", 
                      user="megan", 
                      passwd=pw, 
                      db="ossmole_merged", 
                      use_unicode=True, 
                      charset = "utf8")
cursor1 = db1.cursor()
db2 = pymysql.connect(host="flossdata.syr.edu",
                      user="megan", 
                      passwd=pw,
                      db="ossmole_merged",
                      use_unicode=True,
                      charset = "utf8")
cursor2 = db2.cursor()

while(startDate != endDate):
    print(startDate)
    date = datetime.date.isoformat(startDate)
    url = urlstem + date + ".log"
    print("Working on...", url)
    
    try:
        html = urllib2.urlopen(url).read()
        fileloc = directory + '/' + date + '.txt'
        out = codecs.open(fileloc, "w")
        out.write(str(html))
        out.close
        
        insertQuery = "INSERT INTO datasources(datasource_id, \
                forge_id, \
                friendly_name, \
                date_donated, \
                contact_person, \
                comments, \
                start_date, \
                end_date)  \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        dataValues = (str(newds), 
                forge_id,
                friendly+date,
                str(dateDonated),
                'msquire@elon.edu', 
                fileloc, 
                str(endDate), 
                str(endDate))
                
        # put new datasource_id in databases (local and remote)
        try:
            cursor1.execute(insertQuery, dataValues)
            db1.commit() 
        except pymysql.Error as error:
            print(error)
            db1.rollback()

        try:
            cursor2.execute(insertQuery, dataValues)
            db2.commit() 
        except pymysql.Error as error:
            print(error)
            db2.rollback()

        # increment for the next file
        startDate = startDate + datetime.timedelta(days=1)
        newds += 1
        
    except urllib2.HTTPError:
        # increment for the next file
        startDate = startDate + datetime.timedelta(days=1)
        pass

db1.close()
db2.close()
cursor1.close()
cursor2.close()
