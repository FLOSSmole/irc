#!/usr/bin/perl
## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl itself.
## Please see the Perl Artistic License.
## 
## Copyright (C) 2004-2014 Megan Squire <msquire@elon.edu>
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
## FLOSSmole (2004-2014) FLOSSmole: a project to provide academic access to data 
## and analyses of open source projects.  Available at http://flossmole.org 
#
################################################################
# usage:
# > perl parseChatzillaTopicList.pl <datasource_id> <file> <mode>
#
#
# example usage:
# > perl parseChatzillaTopicList.pl 8441 log.txt PROD
#
# purpose: 
# open a chatzilla log file with /list in it, and parse out the 
# topics, channels, and number of users; put these in table in db
#
# REQUIRES:
#
# dbInfo.pl is a text file in the same directory as this file
# It should include your database connection info,
# Including username, password, and dsn in the following format:
# host
# port
# username
# password
# database  
#
################################################################
use strict;
use DBI;

my $datasource_id = shift @ARGV;
my $file = shift @ARGV;
my $DEBUG = shift @ARGV;
my $DBFILE;

if ($DEBUG && $datasource_id && $file)
{
	# connect to db
	
	if ($DEBUG eq "DEBUG")
	{
		$DBFILE = "dbInfoTest.txt";
		print "\n\nDEBUG run\n";
	}
	else
	{
		$DBFILE = "dbInfo.txt";
		print "\n\nPRODUCTION run\n";
	}
	open (INPUT, $DBFILE);
	my @dbinfo = <INPUT>;
	close (INPUT);

	my $host      = $dbinfo[0];
	my $port      = $dbinfo[1];
	my $username  = $dbinfo[2];
	my $password  = $dbinfo[3];
	my $database  = $dbinfo[4];
	chomp($host);
	chomp($port);
	chomp($username);
	chomp($password);
	chomp($database);
	
	# dsn takes the format of "DBI:mysql:ossmole_merged:grid6.cs.elon.edu"
	my $dsn = "DBI:mysql:" . $database . ":" . $host;
	
	my $dbh = DBI->connect($dsn, $username, $password, {RaiseError=>1});
	
	
    # read in file
    print "opening file: $file\n";
    open my $info, $file or die "Could not open $file: $!";
    # undef $/;
    # here is a typical line:
    # [2014-03-25 09:42:12] === #softuni   3   https://softuni.bg/ - Software University Bulgaria

    while( my $line = <$info>)  
    {   
        # parse out channel name
        $line =~ m{^(.*?)===\s#(.*?)\s+(.*?)\s+(.*?)$};
        my $junk = $1;
        my $channel_name = $2;
        my $num_users    = $3;
        my $topic        = $4;
        
        # remove high ascii from the topic
        $topic =~ s/[^!-~\s]//g;
        
        print "channel name: $channel_name\n";
        print "num users: $num_users\n";
        print "topic: $topic\n";
        print "===\n";
        # insert into database
        if (($DEBUG eq "PROD"))
        {
            my $insert = $dbh->prepare(qq{
                                INSERT IGNORE INTO fn_irc_channels (
                                    channel_name, 
                                    num_users, 
                                    topic, 
                                    datasource_id, 
                                    last_updated)
                                VALUES (?,?,?,?,NOW())
                                });
                $insert->execute($channel_name, $num_users, $topic, $datasource_id)
                    or die "Couldn't execute statement: " . $insert->errstr;
                $insert->finish();
        }
    }

    close $info;

    $dbh->disconnect(); 
}
else
{
	print "You need a datasource_id, a file, and a debug mode on your commandline.";
	exit;
}
