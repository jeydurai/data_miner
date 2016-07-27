package modules::migrators::sql_to_nosql;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
use Time::Progress;
use modules::helpers::connections::mongo_connection;
use modules::helpers::constants::db_connection_strings;
use modules::helpers::connections::mysql_connection;
use List::MoreUtils qw(uniq);
use Try::Tiny;
use v5.14;
use experimental;
#no warnings qw(experimental::smartmatch experimental::lexical_topic experimental::regex_sets experimental::lexical_subs);

our $sqlConnection = modules::helpers::connections::mysql_connection->new();
our $mongoConnection = modules::helpers::connections::mongo_connection->new();  

sub new {
	my $class = shift;
	my $self = {};
	
	bless $self, $class;
	return $self;
}

sub insertUniqueNodesInMongo {
	my %hashSQLDataSubSCMS;
	my %hashSQLDataGTMu;
	my %hashSQLDataRegion;
	my %hashSQLDataSalesLevel6;
	my %hashSQLDataSalesAgent;
	
	my $self = shift ;
	
	my $dbh = $sqlConnection->getMySQLConnection();
	my $sth = $dbh->prepare("SELECT DISTINCT sub_scms, gtmu, region, sales_level_6, tbm FROM booking_dump");
	
	$sth->execute() or die "$DBI::errstr";
	
	my $total_no_of_records = $sth->rows;
	my $progress = new Time::Progress;
	
	$progress->attr( min => 0, max => $total_no_of_records );
	
	
	
	print $total_no_of_records, " No(s) records to be processed...\n";
	my $rec_no = 0;
	print "Fetching SQL Data into Hashes...\n";
	while (my $a = $sth->fetchrow_hashref()) {
		$rec_no++;
		$hashSQLDataSubSCMS{$rec_no} = $a->{"sub_scms"};
		$hashSQLDataGTMu{$rec_no} = $a->{"gtmu"};
		$hashSQLDataRegion{$rec_no} = $a->{"region"};
		$hashSQLDataSalesLevel6{$rec_no} = $a->{"sales_level_6"};
		$hashSQLDataSalesAgent{$rec_no} = $a->{"tbm"};
		print $progress->report("%45b %p\r", $rec_no);
	}
	print $progress->report("done %p elapsed: %L (%l sec)", $rec_no);
	
	
	
	
	%hashSQLDataGTMu = ();
	%hashSQLDataRegion = ();
	%hashSQLDataSalesAgent = ();
	%hashSQLDataSalesLevel6 = ();
	%hashSQLDataSubSCMS = ();
	
} #End of the Subroutine

1;
