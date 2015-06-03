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
# > perl 2parseDjangoLogs.pl <datasource_id> 
#
# example usage:
# > perl 2parseDjangoIRCLogs.pl 51146 
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
my $forge_id = 42;

if ($datasource_id)
{
	# connect to db (once at local grid6, and once at Syracuse)
	# dsn takes the format of "DBI:mysql:ossmole_merged:host"
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
    # 51146/2015apr09
    my $datelog ="";
    if ($p_fileLoc =~ m{^(.*?)\/(.*?)$}s)
    {              
        my $tempdate = $2;
        print "got [$tempdate] for date\n";
        
        if ($tempdate =~ m{^(\d\d\d\d)(\w\w\w)(\d\d)}s)
        {
        	#convert three letter abbr to 2-digit month
			my %mm3 = (
				'jan'=>'01',
				'feb'=>'02',
				'mar'=>'03',
				'apr'=>'04',
				'may'=>'05',
				'jun'=>'06',
				'jul'=>'07',
				'aug'=>'08',
				'sep'=>'09',
				'oct'=>'10',
				'nov'=>'11',
				'dec'=>'12');
        	$datelog = $1 . "-" . $mm3{$2} . "-" . $3;
        }
    }
    
    # open the file
    print "opening file: $p_fileLoc\n";
    open (FILE, $p_fileLoc);
    undef $/;
    my $filestring = <FILE>;

	if ($filestring =~ m{<ul id=\"ll\">(.*?)<\/ul>}s)
	{
		my $ul = $1;
		my @lis = split(/<\/li>/, $ul);
		foreach my $li (@lis)
		{
			my $send_user = "";
			my $line_message = "";
			my $type = "";
			# here is the pattern
			#<li class="le" rel="frege"><a href="#1462000" name="1462000">#</a> <span style="color:#481e7c;8" class="username" rel="frege">&lt;frege&gt;</span> I have a south question </li>
			if ($li =~ m{<li class=\"le\" rel=\"(.*?)\">(.*?)name=\"(.*?)\">(.*?)</span>(.*?)$}s)
			{
				my $send_user = $1;
				my $junk2      = $2;
				my $line_num   = $3;
				my $junk4      = $4;
				my $line_message = $5;

				# clean up html
				my $clean_line_message = decode_entities($line_message);
			
				print "inserting row for $line_num...\n";
				# insert row into table 
				#======
				# LOCAL
				#======               
				my $insert2 = $p_dbh2->prepare(qq{
									INSERT IGNORE INTO django_irc
										(datasource_id, 
										line_num,
										full_line_text,
										line_message,
										date_of_entry,
										type,
										send_user,
										last_updated) 
									VALUES (?,?,?,?,?,'message',?,NOW())
									});
				$insert2->execute($p_ds, $line_num, $li, $clean_line_message, $datelog, $send_user)
					or die "Couldn't execute statement on LOCAL: " . $insert2->errstr;
				$insert2->finish();
		
				#======
				# REMOTE
				#======
				my $insert3 = $p_dbh3->prepare(qq{
									INSERT IGNORE INTO django_irc
										(datasource_id, 
										line_num,
										full_line_text,
										line_message,
										date_of_entry,
										type,
										send_user,
										last_updated) 
									VALUES (?,?,?,?,?,'message',?,NOW())
									});
				$insert3->execute($p_ds, $line_num, $li, $clean_line_message, $datelog, $send_user)
					or die "Couldn't execute statement on REMOTE: " . $insert3->errstr;
				$insert3->finish();
			} # if
		} # foreach
	} # if
}	# sub
		
