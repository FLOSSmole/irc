# -*- coding: utf-8 -*-
## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl itself.
## Please see the Perl Artistic License 2.0.
## 
## Copyright (C) 2004-2016 Megan Squire <msquire@elon.edu>
## With code contributions from:
## Greg Batchelor
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
# > perl 2parsePerl6IRCLogs.py <datasource_id> 
#
# example usage:
# > perl 2parsePerl6IRCLogs.py 62938 
#
# purpose: 
# open each IRC log in the directory, parse out the interesting bits
# notes:
# START WITH THE FIRST DS IN THE DB YOU ARE INTERESTED IN
# Does not need to be the latest one
#
################################################################
import re
import codecs
import pymysql
import sys
import datetime
import html

datasource_id = int(sys.argv[1])
pw            = sys.argv[2]
forge_id      = 65

if datasource_id and pw:
    # connect to db (once at local , and once at remote)
    db1 = pymysql.connect(host="grid6.cs.elon.edu",
                      user="megan",
                      passwd=pw,
                      db="ossmole_merged",
                      use_unicode=True,
                      charset="utf8")
                      
    cursor1 = db1.cursor()


    db2 = pymysql.connect(host="grid6.cs.elon.edu",
                      user="megan",
                      passwd=pw,
                      db="irc",
                      use_unicode=True,
                      charset="utf8")
                      
    cursor2 = db2.cursor()
    
    db3 = pymysql.connect(host="flossdata.syr.edu",
                      user="megan",
                      passwd=pw,
                      db="irc",
                      use_unicode=True,
                      charset="utf8")
    cursor3 = db3.cursor()

    # get the file list from the 'comments' field in the datasources table    
    cursor1.execute('SELECT datasource_id, comments \
                 FROM datasources \
                 WHERE datasource_id >= %s \
                 AND forge_id = %s',
                (datasource_id, forge_id))
                
    rows = cursor1.fetchall()

    for row in rows : 
        ds = row[0]
        fileLoc = row[1]
        print ("==================\n")
        # date is in the filename, in the format:
        # 51255/20150406
        datelog    = ""
        formatting = re.search("^(.*?)\/(.*?)$",fileLoc)
        
        if formatting:
            tempdate = formatting.group(2);
            print("got ", tempdate, " for date")
            
        date = re.search("^(\d\d\d\d)(\d\d)(\d\d)$",tempdate)  
        
        if (date):
            datelog = date.group(1)+ "-" + date.group(2) + "-" + date.group(3)
    
        # open the file
        print("opening file: " + fileLoc)
    
        log  = codecs.open(fileLoc, 'r', encoding='utf-8', errors='ignore')
        line = log.read()
        line = line[2:]
        line = line[:-1]
        table  = line
    
        # the perl6 data is in an html table
        # (there's a plaintext version but it only has mention & action, not system messages) 
        regularLOG = re.search('<table id=\"log\"(.*?)<\/table>',table)
        
        
        if (regularLOG):
            table = regularLOG.group(1)
            trs   = table.split("</tr>")
            
            line_num = 0
            for tr in trs:
                send_user    = ""
                timelog      = ""
                line_message = ""
                messageType  = ""
                line_num     += 1
                
                # here is the pattern for a system message:
                #<tr id="id_l2" class="new special dark">
                #<td class="time" id="i_-799999"><a href="/perl6/2005-02-26#i_-799999">13:45</a></td>
                #<td style="color: 0" class="nick"></td>
                #<td class="msg &#39;&#39;">ilogger starts logging <a href="/perl6/today">#perl6</a> at Sat Feb 26 13:45:34 2005</td>
                #</tr>
                
                # here is the pattern for a regular message:
                #<tr id="id_l4" class="new nick nick_feb">
                #<td class="time" id="i_-799997"><a href="/perl6/2005-02-26#i_-799997">13:46</a></td>
                #<td style="color: #04000e" class="nick">feb</td>
                #<td class="msg &#39;&#39;">autrijus: you're welcome</td>
                #</tr>
    
                # here is the pattern for an action message:
                #<tr id="id_l15" class="new nick nick_Odin- dark">
                #<td class="time" id="i_-799986"><a href="/perl6/2005-02-26#i_-799986">13:55</a></td>
                #<td style="color: #010002" class="nick">* Odin-</td>
                #<td class="msg act &#39;&#39;">places a sane-o-meter on the channel, wondering if it'll score above zero.</td>
                #</tr>
                
                systemMessage  = re.search("class\=\"nick\"\>\<\/td\>",tr)
                regMessage     = re.search("\<td class\=\"msg \&",tr)
                actionMessage  = re.search("\<td class\=\"msg act",tr)
                regUsername    = re.search("class=\"nick\">(.*?)<\/td>",tr)
                regTimelog     = re.search('td class=\"time\"(.*?)\>\<(.*?)\>(.*?)\<\/a\>',tr)
                regLineMessage = re.search('td class=\"msg(.*?)\>(.*?)<\/td\>',tr)
                
                # first case: system message (blank nick td)
                if (systemMessage):
                    send_user   = None
                    messageType = "system"
                
                # second case: regular message
                elif(regMessage):
                    messageType = "message"
                    if (regUsername):
                        send_user=regUsername.group(1)
    
                # third case: action message
                elif(actionMessage):
                    messageType = "action"
                    if (regUsername):
                        send_user=regUsername.group(1)[9:]
    
                # grab timelog: 
                # <td class="time" id="i_-799986"><a href="/perl6/2005-02-26#i_-799986">13:55</a></td>
                if (regTimelog):
                    timelog = regTimelog.group(3)
                
                # grab message
                # <td class="msg act &#39;&#39;">places a sane-o-meter on the channel, wondering if it'll score above zero.</td>
                if (regLineMessage):
                    line_message = regLineMessage.group(2)
                    # clean up html
                    line_message = html.unescape(line_message)
                    
                insertQuery="INSERT IGNORE INTO perl6_irc \
                                        (datasource_id,line_num,\
                                        line_message,\
                                        date_of_entry,\
                                        time_of_entry,\
                                        type,\
                                        send_user,\
                                        last_updated)\
                                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
                                        
                dataValues=(ds,line_num,line_message,datelog,timelog,messageType,send_user,datetime.datetime.now())                    
                
                if messageType != "":
                    try:
                        cursor2.execute(insertQuery,dataValues)
                        db2.commit()
                    except pymysql.Error as error:
                        print(error)
                        db2.rollback()
                    try:
                        cursor3.execute(insertQuery,dataValues)
                    except pymysql.Error as error:
                        print(error)
                        db3.rollback() 
             
        cursor1.close()    
        cursor2.close()
        cursor3.close()
        db1.close()
        db2.close()
        db3.close()
        print("done")

else:
	print ("You need both a datasource_id and a date to start on your commandline.")
