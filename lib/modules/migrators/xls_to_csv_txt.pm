package modules::migrators::xls_to_csv_txt;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use modules::helpers::connections::mysql_connection;
use strict;
use warnings;
use Spreadsheet::ParseExcel;
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
	my $row_counter = undef;
	my $max_id  = 0;
	print "\n.\n.\n.\n";
	print "Acquiring $database database connection...\n";
	my $dbh = $conn_obj->getMySQLConnection();
	print "$database database connection acquired!\n";

	my $table_name = "";
	print "Identifying as to which table upload to happen...\n";
	if ($_[4] =~ /^newdata$/i) {
		$table_name = "dump_from_finance";
	} elsif ($_[4] =~ /^newdata1$/i) {
		$table_name = "dump_from_finance_nri";
	} else {
		$table_name = "others";
	}
	
	if (defined $table_name && $table_name ne "others") {
		print "Identified $table_name as the table to be uploaded into!\n";
		
		print "Acquiring statement handle to locate the Max ID...\n";
		my $query = qq{SELECT MAX(ID) as max_id from $table_name};
		my $sth = $dbh->prepare($query);
		$sth->execute() 
			or die $DBI::errstr;
		print "Acquired the statement handle!\n";
		my (@row_max) = $sth->fetchrow_array();
		($max_id) = @row_max;
		$sth->finish();
		$dbh->disconnect();
		$conn_obj->disconnectMySQLConnection();
		print "Max ID in table $table_name: ", $max_id, "\n";
		$row_counter += $max_id;
	} else {
		$row_counter = 0;
	}
	my $output_file_name = $_[4].'.txt';
	print ".\n.\n.\nSearching $_[2] file in $_[1] directory...\n";
	my $parser = Spreadsheet::ParseExcel->new();
	my $work_book = $parser->parse($_[1].'\\'.$_[2]);
	if (!defined $work_book) {
		die $parser->error(), ".\n";
	}
	print "Opened and parsed XLSX file successfully!\n";
	open my $fh_csv, ">", $output_file_name 
		or die "$output_file_name: $!\n";
	print "File Handle created for '$output_file_name'!\n";
	my $line;
	my $first_column_value = 1;

	for my $data_sheet ($work_book->worksheets()) {
		if (lc($data_sheet->get_name()) eq lc( $_[3])) {
			print "Recognized the '$_[3]' sheet in $_[2] file!\n";
			print "Conversion on progress....\n";
			my ($row_min, $row_max) = $data_sheet->row_range();
			for my $row ($row_min .. $row_max) {
				my ($col_min, $col_max) = $data_sheet->col_range();
				$first_column_value = 1;
				foreach my $col ($col_min .. $col_max) {
					my $cell = $data_sheet->get_cell($row, $col);
					my $cell_value  = "";
					if ($cell) {
						$cell_value = $cell->value();
					}
					if ($first_column_value == 1) {
						unless ($cell_value eq "ID") {
							++$row_counter;
							$cell_value = $row_counter;
						}
						$first_column_value = 0;
					}
					if ($cell_value) {
						$line .= "\"".$cell_value."\",";
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
	
}
1;