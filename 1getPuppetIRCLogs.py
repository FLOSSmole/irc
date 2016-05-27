# -*- coding: utf-8 -*-
# usage: python puppet_irc_fetcher.py 53029 startdate irctype password
# startdate is the next date you want to collect from
# irctype can be one of: gen, dev, razor
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
# irctype is dev and razor
ds = int(sys.argv[1])
newds = int(ds)
mystartdate = str(sys.argv[2])
irctype = str(sys.argv[3])
pw = str(sys.argv[4])

if irctype == 'razor':
    directory = 'razor'
    forge_id = '69'
    urlstem = "http://www.puppetlogs.com/puppetrazor/%23puppet-razor-"
    if not os.path.exists(directory):
        os.makedirs(directory)
elif irctype == 'dev':
    forge_id = '70'
    directory = 'dev'
    urlstem = "http://www.puppetlogs.com/puppetdev/%23puppet-dev-"
    if not os.path.exists(directory):
        os.makedirs(directory)
elif irctype == 'gen':
    forge_id = '68'
    directory = 'gen'
    urlstem = "http://www.puppetlogs.com/puppet/%23puppet-"
    if not os.path.exists("gen"):
        os.makedirs("gen")
else:
    print("invalid irc type, must be one of gen, dev, or razor")
    exit

friendly = 'Puppet ' + directory + ' IRC Logs '

# dates
startdate = datetime.datetime.strptime(mystartdate, '%Y-%m-%d').date()
enddate = datetime.date.today()

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
                str(enddate),
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
                str(enddate),
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
