package modules::migrators::xlsx_to_csv_txt;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use DBI;
use modules::helpers::connections::mysql_connection;
use strict;
use warnings;
use Spreadsheet::XLSX;
use Exporter;
our @EXPORT_OK = qw/convert/;

sub new {
	my $class = shift;
	my $self = {
		_sourceDirName		=>	shift,
		_sourceFileName		=>	shift,
		_sourceSheetName	=>	shift,
		_outputFileName		=>	shift,
	};
	bless $self, $class;
	return $self;
}

sub convert {
	my $conn_obj = modules::helpers::connections::mysql_connection->new();
	my $database = $conn_obj->getDatabaseName();
	print "\n.\n.\n.\n";
	print "Acquiring $database database connection...\n";
	my $dbh = $conn_obj->getMySQLConnection();
	print "$database database connection acquired!\n";

	my $table_name = undef;
	print "Identifying as to which table upload to happen...\n";
	if ($_[4] =~ /^newdata$/i) {
		$table_name = "dump_from_finance";
	} elsif ($_[4] =~ /^newdata1$/i) {
		$table_name = "dump_from_finance_nri";
	} else {
		print "Incorrect output file name mentioned, Please try again!";
	}
	print "Identified $table_name as the table to be uploaded into!\n";
	
	print "Acquiring statement handle to locate the Max ID...\n";
	my $query = qq{SELECT MAX(ID) as max_id from $table_name};
	my $sth = $dbh->prepare($query);
	$sth->execute() 
		or die $DBI::errstr;
	print "Acquired the statement handle!\n";
	my (@row_max) = $sth->fetchrow_array();
	my ($max_id) = @row_max;
	$sth->finish();
	$dbh->disconnect();
	$conn_obj->disconnectMySQLConnection();
	print "Max ID in table $table_name: ", $max_id, "\n";
	if ($max_id != 0) {
		my $output_file_name = $_[4].'.txt';
		print ".\n.\n.\nSearching $_[2] file in $_[1] directory...\n";
		my $dump_file = Spreadsheet::XLSX->new($_[1].'\\'.$_[2])
			or die "Can't open file!\n";
		print "Opened and parsed XLSX file successfully!\n";
		open my $fh_csv, ">", $output_file_name 
			or die "$output_file_name: $!\n";
		print "File Handle created for '$output_file_name'!\n";
		my $line;
		my $first_column_value = 1;
		my $row_counter += $max_id;

		foreach my $data_sheet (@{$dump_file->{Worksheet}}) {
			if ($data_sheet->{Name} =~ /data/i) {
				print "Recognized the '$_[3]' sheet in $_[2] file!\n";
				print "Conversion on progress....\n";
				$data_sheet->{MaxRow} ||= $data_sheet->{MinRow};
				foreach my $row ($data_sheet->{MinRow} .. $data_sheet->{MaxRow}) {
					$data_sheet->{MaxCol} ||= $data_sheet->{MinCol};
					$first_column_value = 1;
					foreach my $col ($data_sheet->{MinCol} .. $data_sheet->{MaxCol}) {
						my $cell = $data_sheet->{Cells}[$row][$col];
						if ($first_column_value == 1) {
							$cell->{Val} = ++$row_counter unless $cell->{Val} eq "ID";
							$first_column_value = 0;
						}
						if ($cell) {
							$line .= "\"".$cell->{Val}."\",";
						} else {
							$line .= "\"".""."\",";
						}
					}
					chomp($line);
					print $fh_csv "$line\n";
					$line='';
				}
			}
		}

		close $fh_csv or die "$output_file_name: $!";
		print "'$output_file_name' has been successfully created under C:\\jeyaraj\\analysis\\pbg_dashboards\\excel_dashboard\n";
	} else {
		print "No data found in $database.$table_name, Cannot continue!\n";
	}
	
}
1;
