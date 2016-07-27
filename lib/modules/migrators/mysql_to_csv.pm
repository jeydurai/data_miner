package modules::migrators::mysql_to_csv;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use modules::helpers::connections::mysql_connection;
use strict;
use warnings;
use Exporter;
our @EXPORT_OK = qw/upload/;

sub new {						# Class Constructor
	my $class = shift;		# Class Definition
	my $self = {};			
	bless $self, $class;
	return $self;
}

sub upload {
	my $conn_obj = modules::helpers::connections::mysql_connection->new();
	my $database = $conn_obj->getDatabaseName();

	print ".\n.\n.\n";
	print "Acquiring $database database connection...\n";
	my $dbh = $conn_obj->getMySQLConnection();
	print "$database database connection acquired!\n";
	print "Deleting $_[1] table contents...\n";
	my $database_return = $dbh->do(qq{DELETE FROM $_[1]}) 
		or die $DBI::errstr;
	print "$database_return row(s) deleted in $database.$_[1] table \n";

	my $file_path = $_[2].'/'.$_[3].'.txt';
	my $query = qq{LOAD DATA LOCAL INFILE 
														?
												 INTO TABLE $_[1]
												 CHARACTER SET latin1
												 FIELDS TERMINATED BY ','
												 ENCLOSED BY '"'
												 LINES TERMINATED BY '\r\n'
												 IGNORE 1 LINES};
							
	print "Uploading $_[1] table ...\n";
	$database_return = $dbh->do($query, undef, $file_path) 
		or die $DBI::errstr;
	print $database_return, " row(s) appended in $database.$_[1] table\n";
	print "Upload completed successfully!\n";

	print ".\n.\n.\n Want to upload in master table as well (Yes/No): ";
	my $can_I_proceed_upload_the_master_table = <STDIN>;

	my $master_table_name = undef;
	if (defined chomp ($can_I_proceed_upload_the_master_table)) {
		if ($_[1] =~ /^newdata$/) {
			$master_table_name = "dump_from_finance";
		} elsif ($_[1] =~ /^newdata1$/) {
			$master_table_name = "dump_from_finance_nri";
		}
	my $query = qq{LOAD DATA LOCAL INFILE 
														?
												 INTO TABLE $master_table_name
												 CHARACTER SET latin1
												 FIELDS TERMINATED BY ','
												 ENCLOSED BY '"'
												 LINES TERMINATED BY '\r\n'
												 IGNORE 1 LINES};
		
		if ($can_I_proceed_upload_the_master_table =~ /^yes$/i) {
			print "Uploading $master_table_name table ...\n";
			$database_return = $dbh->do($query, undef, $file_path) 
				or die $DBI::errstr;
			print $database_return, " row(s) appended in $database.$master_table_name table\n";
			print "Upload completed in master table $database.$master_table_name successfully!\n";
		} else {
			print "You do not want to proceed uploading master table $database.$master_table_name!\n";
		}
		

	} else {
		print "Input for proceeding upload in master table is Incorrect\n";
		print "Cannot upload master table $database.$master_table_name! \n";
	}
	$dbh->disconnect();
	$conn_obj->disconnectMySQLConnection();
}
1;