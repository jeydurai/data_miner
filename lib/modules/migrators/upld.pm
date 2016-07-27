package modules::migrators::upld;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use modules::migrators::mysql_to_csv;
use strict;
use warnings;

sub new {
	my $class = shift;
	my $self = {};
	bless $self, $class;
	return $self;
}

sub uploadCSV_Into_MySQL {
	print "Destination MySQL Table: ";
	my $table_name = <STDIN>;
	print "Source TXT/CSV file path: ";
	my $source_dir = <STDIN>;
	print "Source TXT/CSV file name: ";
	my $source_file = <STDIN>;

	my $uploader = modules::migrators::mysql_to_csv->new();
	if ((defined chomp($table_name)) && (defined chomp($source_dir)) && defined (chomp($source_file))) {
		$uploader->upload($table_name, $source_dir, $source_file);
	} else {
		print "Input is not a valid strings!\n";
	}

}
1;