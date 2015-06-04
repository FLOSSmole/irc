#!/usr/bin/perl
## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl itself.
## Please see the Perl Artistic License.
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
# > perl 2parsePerl6IRCLogs.pl <datasource_id> 
#
# example usage:
# > perl 2parsePerl6IRCLogs.pl 51255 
#
# purpose: 
# open each IRC log in the directory, parse out the interesting bits
# notes:
# START WITH THE FIRST DS IN THE DB YOU ARE INTERESTED IN
# Does not need to be the latest one
#
################################################################
use strict;
use DBI;
use HTML::Entities;

my $datasource_id = shift @ARGV;
my $forge_id = 65;

if ($datasource_id)
{
    # connect to db (once at local , and once at remote)
    # dsn takes the format of "DBI:mysql:ossmole_merged:local.host"
    my $dsn1 = "DBI:mysql:ossmole_merged:local.host";
    my $dbh1 = DBI->connect($dsn1, "user", "pass", {RaiseError=>1});
    
    my $dsn2 = "DBI:mysql:irc:local.host";
    my $dbh2 = DBI->connect($dsn2, "user", "pass", {RaiseError=>1});
    
    my $dsn3 = "DBI:mysql:irc:remote.host";
    my $dbh3 = DBI->connect($dsn3, "user", "pass", {RaiseError=>1});
    
    # get the file list from the 'comments' field in the datasources table
    my $sth1 = $dbh1->prepare(qq{select datasource_id, comments 
        from ossmole_merged.datasources 
        where datasource_id >= ? 
        and forge_id=?});
    $sth1->execute($datasource_id, $forge_id);
    my $filesInDB = $sth1->fetchall_arrayref;
    $sth1->finish();

    foreach my $row (@$filesInDB) 
    {
        my ($ds, $fileLoc) = @$row;
        print "==================\n";
        parseFile($dbh2, $dbh3, $ds, $fileLoc);
    }       
    
    $dbh1->disconnect(); 
    $dbh2->disconnect();
    $dbh3->disconnect();
}
else
{
    print "You need both a datasource_id and a date to start on your commandline.";
    exit;
}

# --------------------------------------------------
# subroutine: parseFile
# takes: two database connections (local and remote) and a datasource_id
# purpose:
# --get each file on disk
# --pull out the lines
# --parse out the pieces of the lines
# --write each line to the irc table in both local and remote db
# --------------------------------------------------
sub parseFile($dbh2, $dbh3, $ds, $fileLoc)
{
    my $p_dbh2  = $_[0];
    my $p_dbh3  = $_[1];
    my $p_ds    = $_[2];
    my $p_fileLoc = $_[3];

    #date is in the filename, in the format:
    # 51255/20150406
    my $datelog ="";
    if ($p_fileLoc =~ m{^(.*?)\/(.*?)$}s)
    {              
        my $tempdate = $2;
        print "got [$tempdate] for date\n";
        
        if ($tempdate =~ m{^(\d\d\d\d)(\d\d)(\d\d)}s)
        {
            $datelog = $1 . "-" . $2 . "-" . $3;
        }
    }
    
    # open the file
    print "opening file: $p_fileLoc\n";
    open (FILE, $p_fileLoc);
    undef $/;
    my $filestring = <FILE>;
    my $line_num = 0;
    
    # the perl6 data is in an html table
    # (there's a plaintext version but it only has mention & action, not system messages) 
    if ($filestring =~ m{<table id=\"log\"(.*?)<\/table>}s)
    {
        my $table = $1;
        my @trs = split(/<\/tr>/, $table);
        foreach my $tr (@trs)
        {
            my $send_user = "";
            my $timelog = "";
            my $line_message = "";
            my $type = "";
            $line_num++;
            
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
            
            # first case: system message (blank nick td)
            if ($tr =~ m{class\=\"nick\"\>\<\/td\>}s)
            {
                $send_user = undef;
                $type = "system";
            }
            # second case: regular message
            elsif($tr =~ m{\<td class\=\"msg \&}s)
            {
                $type = "message";
                if ($tr =~ m{class="nick">(.*?)<\/td>}s)
                {
                    $send_user = $1;
                }
            }
            # third case: action message
            elsif($tr =~ m{\<td class\=\"msg act}s)
            {
                $type = "action";
                if ($tr =~ m{class="nick">\*(.*?)<\/td>}s)
                {
                    $send_user = $1;
                    $send_user =~ s/^\W+//; #strip off weird control char in this string!
                }
            }   
                
            # grab timelog: 
            #<td class="time" id="i_-799986"><a href="/perl6/2005-02-26#i_-799986">13:55</a></td>
            if ($tr =~ m{td class=\"time\"(.*?)\>\<(.*?)\>(.*?)\<\/a\>}s)
            {
                $timelog = $3;
            }
            
            #grab message
            #<td class="msg act &#39;&#39;">places a sane-o-meter on the channel, wondering if it'll score above zero.</td>
            if ($tr =~ m{td class=\"msg(.*?)\>(.*?)<\/td\>}s)
            {
                $line_message = $2;
                # clean up html
                $line_message = decode_entities($line_message);
            }
    
            print "inserting row for $line_num...\n";
            
            # insert row into table 
            #======
            # LOCAL
            #======    
            if ($type ne "")
            {           
                my $insert2 = $p_dbh2->prepare(qq{
                                INSERT IGNORE INTO perl6_irc
                                    (datasource_id, 
                                    line_num,
                                    line_message,
                                    date_of_entry,
                                    time_of_entry,
                                    type,
                                    send_user,
                                    last_updated) 
                                VALUES (?,?,?,?,?,?,?,NOW())
                                });
                $insert2->execute($p_ds, $line_num, $line_message, $datelog, $timelog, $type, $send_user)
                    or die "Couldn't execute statement: " . $insert2->errstr;
                $insert2->finish();
    
                #======
                # REMOTE
                #======
                my $insert3 = $p_dbh3->prepare(qq{
                                INSERT IGNORE INTO perl6_irc
                                    (datasource_id, 
                                    line_num,
                                    line_message,
                                    date_of_entry,
                                    time_of_entry,
                                    type,
                                    send_user,
                                    last_updated) 
                                VALUES (?,?,?,?,?,?,?,NOW())
                                });
                $insert3->execute($p_ds, $line_num, $line_message, $datelog, $timelog, $type, $send_user)
                    or die "Couldn't execute statement on REMOTE: " . $insert3->errstr;
                $insert3->finish();
            }
        } 
    }
}
        
