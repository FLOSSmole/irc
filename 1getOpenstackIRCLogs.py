# -*- coding: utf-8 -*-
# usage: python openstack_irc_fetcher.py 53029 startdate irctype password
# startdate is the next date you want to collect from in the format 2015-08-31
# irctype can be one of: openstack, dev, infra, meeting, meeting-alt, meeting-3, dns
# run this seven times, once for each irc type
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
ds = int(sys.argv[1])
newds = int(ds)
mystartdate = str(sys.argv[2])
irctype = str(sys.argv[3])
pw = str(sys.argv[4])

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
    exit
if irctype == 'openstack':
    friendly= 'openstack' + ' IRC Logs '
else:
    friendly = 'openstack-' + directory + ' IRC Logs '

# dates
startdate = datetime.datetime.strptime(mystartdate, '%Y-%m-%d').date()
enddate = datetime.date.today()
dateDonated= datetime.datetime.now()

# databases
db1 = pymysql.connect(host="grid6.cs.elon.edu", 
                      user="eashwell", 
                      passwd=pw, 
                      db="test", 
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

while(startdate != enddate):
    print(startdate)
    date = datetime.date.isoformat(startdate)
    url = urlstem + date + ".log"
    print(url)
    try:
        html = urllib2.urlopen(url).read()
        fileloc = directory + '/' + date + '.txt'
        out = codecs.open(fileloc, "w")
        out.write(str(html))
        out.close
        
        # put new datasource_id in databases (local and remote)
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
                forge_id,
                friendly+date,
                str(dateDonated),
                'msquire@elon.edu', 
                fileloc, 
                str(enddate), 
                str(enddate)))
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
                (str(newds), 
                forge_id,
                friendly+date,
                str(dateDonated),
                'msquire@elon.edu', 
                fileloc, 
                str(enddate), 
                str(enddate)))
            db2.commit() 
        except pymysql.Error as error:
            print(error)
            db2.rollback()

        # increment for the next file
        startdate = startdate + datetime.timedelta(days=1)
        newds += 1
        
    except urllib2.HTTPError:
        # increment for the next file
        startdate = startdate + datetime.timedelta(days=1)
        pass

db1.close()
db2.close()
cursor1.close()
cursor2.close()
