package modules::helpers::connections::mysql_connection;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use modules::helpers::constants::db_connection_strings;
use DBI;
use strict;
use warnings;

our $db_conn_string;
our $driver;
our $database;
our $user_name;
our $password;
our $dsn;


sub new {
	my $class = shift;
	my $self = {};
	our $db_conn_string = modules::helpers::constants::db_connection_strings->new();
	our $driver = $db_conn_string->getMySQLDriver;
	our $database = $db_conn_string->getMySQLSourceDatabase;
	our $user_name = $db_conn_string->getMySQLUserName;
	our $password = $db_conn_string->getMySQLPassword;
	our $dsn = "DBI:$driver:database=$database";

	bless $self, $class;
	return $self;
}

sub getMySQLConnection {
	my($self) = @_;
	return DBI->connect($dsn, $user_name, $password);
}

sub getDatabaseName {
	my ($self) = @_;
	return $database;
}

sub disconnectMySQLConnection {
	my ($self, $dbh) = @_;
	$dbh->disconnect;
}

1;