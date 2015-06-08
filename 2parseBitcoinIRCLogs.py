#usage 2parseBitcoinDevIRCLogs.py 51313 

from bs4 import BeautifulSoup
import codecs
import sys
import MySQLdb

datasource_id = str(sys.argv[1])
forge_id = 66
fileLoc = ''

# Open local database connection 1
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

# Open local database connection 2
db2 = MySQLdb.connect(host="local.host",\
    user="user", \
    passwd="pass", \
    db="irc", \
    use_unicode=True, \
    charset="utf8")
cursor2 = db2.cursor()
cursor2.execute('SET NAMES utf8mb4')
cursor2.execute('SET CHARACTER SET utf8mb4')
cursor2.execute('SET character_set_connection=utf8mb4')

# Open remote database connection 3
db3 = MySQLdb.connect(host="remote.host",\
    user="user", \
    passwd="pass", \
    db="irc", \
    use_unicode=True, \
    charset="utf8")
cursor3 = db3.cursor()
cursor3.execute('SET NAMES utf8mb4')
cursor3.execute('SET CHARACTER SET utf8mb4')
cursor3.execute('SET character_set_connection=utf8mb4')

# get the list of all files to parse
cursor1.execute('SELECT datasource_id, comments FROM datasources WHERE datasource_id >= %s AND forge_id= %s', (datasource_id, forge_id))
    
rows = cursor1.fetchall()
for row in rows:
    current_ds = row[0]
    fileLoc = row[1]
    linecounter = 0;
    
    date_of_entry = fileLoc.split('/',1)[1]
    time_of_entry = ''
    unix_time = ''
    send_user = ''
    line_message = ''
    cleaned_message = ''
    linetype = 'message'
    
    print 'processing ', current_ds, ' at ', fileLoc
    log = codecs.open(fileLoc, 'r', encoding='utf-8', errors='ignore')
    soup = BeautifulSoup(log)

# here is the pattern we are matching    
#<tr>
#<td class="datetime">
#<a name="l1285148000.0">
#<a href="#l1285148000.0">09:33</a>
#</a>
#</td>
#<td class="nickname">cdecker</td>
#<td>Well it&#39;s a project with 1&#39;500&#39;000 sloc
#</td>
#</tr>

    for trow in soup.find_all('tr'):
        linecounter += 1
        unix_time = trow.td.a['name']
        time_of_entry = trow.td.a.next_sibling.get_text()
        send_user = trow.td.next_sibling.next_sibling.get_text()
        line_message = trow.td.next_sibling.next_sibling.next_sibling.next_sibling.get_text().rstrip()
        print date_of_entry,'|',linecounter,'|',unix_time,'|',time_of_entry,'|',send_user,'|',line_message,'|',linetype

        #insert parsed data into databases
        try:
            cursor2.execute(u"INSERT INTO bitcoindev_irc(datasource_id, \
                line_num, \
                date_of_entry, \
                time_of_entry, \
                unix_time, \
                send_user, \
                line_message, \
                type)  \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                (current_ds, 
                linecounter,
                date_of_entry,
                time_of_entry,
                unix_time, 
                send_user, 
                line_message, 
                linetype
                ))
            db2.commit() 
        except MySQLdb.Error as error:
            print(error)
            db2.rollback()
                
        try:
            cursor3.execute(u"INSERT INTO bitcoindev_irc(datasource_id, \
                line_num, \
                date_of_entry, \
                time_of_entry, \
                unix_time, \
                send_user, \
                line_message, \
                type)  \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                (current_ds, 
                linecounter,
                date_of_entry,
                time_of_entry,
                unix_time, 
                send_user, 
                line_message, 
                linetype
                ))
            db3.commit() 
        except MySQLdb.Error as error:
            print(error)
            db3.rollback()

db1.close()
db2.close()
db3.close()  
cursor1.close()
cursor2.close() 
cursor3.close()       
