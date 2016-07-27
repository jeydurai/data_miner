package modules::helpers::constants::db_connection_strings;
use strict;
use warnings;
use Exporter;
our @EXPORT_OK = qw/getMySQLDriver getMySQLSourceDatabase getMySQLUserName getMySQLPassword getMongoHost getMongoPort getNameOfDatabaseBI getCollectionNodes/;

sub new {
	my $class = shift;
	my $self = {};
	bless $self, $class;
	return $self;
}

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ MySQL Strings $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

sub getMySQLDriver {
	my ($self) = @_;
	return "mysql";
}

sub getMySQLSourceDatabase {
	my ($self) = @_;
	return "mysourcedata";
}

sub getMySQLUserName {
	my ($self) = @_;
	return "root";
}

sub getMySQLPassword {
	my ($self) = @_;
	return "Jey03\$78";
}

sub getTableNameBookingDump {
	my $self = shift;
	return "booking_dump";
}

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ MongoDB Strings $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
sub getMongoHost {
	my $self = shift;
	return "localhost";
}

sub getMongoPort {
	my $self = shift;
	return "27017";
}

sub getNameOfDatabaseBI {
	my $self = shift;
	return "truenorth";
}

sub getCollectionNodes {
	my $self = shift;
	return "truenorth_nodes";
}

1;
