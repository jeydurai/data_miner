package modules::helpers::connections::mongo_connection;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use modules::helpers::constants::db_connection_strings;
use strict;
use warnings;
use MongoDB;

our $db_conn_string;
our $host;
our $port;
our $db_name;
our $coll_name;

sub new {
	my $class = shift;
	my $self = {};
	our $db_conn_string = modules::helpers::constants::db_connection_strings->new();
	our $host = $db_conn_string->getMongoHost();
	our $port = $db_conn_string->getMongoPort();
	our $coll_name = $db_conn_string->getCollectionNodes();

	bless $self, $class;
	return $self;
}

sub getMongoClient {
	my $self = shift;
	return MongoDB::MongoClient->new(host => $host, port => $port);;
}

sub getMongoDatabase {
	my $self = shift;
	my $db_name = shift; 
	my $client = $self->getMongoClient();
	return $client->get_database($db_name);
}

1;
