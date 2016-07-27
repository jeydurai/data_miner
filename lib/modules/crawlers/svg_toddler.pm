package modules::crawlers::svg_toddler;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
no warnings 'uninitialized';
use v5.14;
use Try::Tiny;
use List::MoreUtils qw(uniq);
use Win32::OLE;
use Excel::Writer::XLSX;
use Spreadsheet::WriteExcel::Utility;
use modules::reports::report_utility;
use Scalar::Util qw(looks_like_number);
use XML::Dumper;
#use Algorithm::Permute;
use Array::Utils qw(:all);
use Text::Trim qw(trim);

#no warnings qw(experimental::smartmatch experimental::lexical_topic experimental::regex_sets experimental::lexical_subs);
our @ISA = qw(modules::reports::report_utility);

sub new {
	my $class = shift;
	my $self = {};
	bless $self, $class;
	return $self;
}

sub import_into_excel {
	my $self = shift;
	my $dir_name = "C:\/Jeyaraj\/Analysis\/Nal\/Rankings";
	my $excel_file_name = "BT_500.xlsx";
	my $path_string = $dir_name . '/';
	my $file_ab_path = $path_string . $excel_file_name;
	
	# Opening an Excel Sheet
	
	print "Opening an excel to store the data...\n";
	my $xl_app = $self->getXLApp();
	$xl_app->{Visible} = 1;
	$xl_app->{DisplayAlerts} = 0;
	my $xl_book = $xl_app->Workbooks->open($file_ab_path);
	my $main_sheet = $xl_book->Sheets("main");
	print "Successfully opened the excel!\n";
	my $starting_row = 4;
	my $row_counter = $starting_row;
	foreach (1 .. 10) {
		my $file_name_with_ext = "bt_p" . $_ .".txt";
		my $ab_path = $path_string . $file_name_with_ext;
		open (my $fh, '<:encoding(UTF-8)', $ab_path) or die "Could not open the file '$ab_path' $!\n";
		print "Successfully opened the $file_name_with_ext file!\n";
		print "processing file $file_name_with_ext ...\n";
		my $loop_counter = 0;
		while (my $row = <$fh>) {
			my $counter = 1;
			my @matches;
			$loop_counter++;
			print "Line # $loop_counter in $file_name_with_ext ...\n";
			chomp $row;
			if ($_ == 7 || $_ == 10) {
				(@matches) = ($row =~ /(?:.*?<text.*?>(?:<.*?>)*(.*?)<\/text>|<text(.*?)\/>)/ig);
				$counter = 30;
			} else {
				(@matches) = ($row =~ /.*?<text.*?>(?:<.*?>)*(.*?)<\/text>/ig);
			}
		
			my $col_counter = 0;
			foreach my $match (@matches) {
				if ($_ == 7 || $_ == 10) {
					if ($match eq "" || !defined $match) {
						next;
					} else {
						$match = 0 unless defined $match;
					}
				}
				
				if (++$counter > 29) {
					$col_counter++;
					print "<$match-$col_counter>|" if $col_counter == 1;
					$main_sheet->Cells($row_counter, $col_counter)->{Value} = $match if $col_counter == 1;
					my $every = ($col_counter % 20);
					if ($every == 0) {
						$main_sheet->Cells($row_counter, $col_counter)->{Value} = $match;
						print "<$match-$col_counter>|";
						print "\n" ;
						$row_counter++;
						$col_counter = 0;
						$xl_app->ActiveWindow->SmallScroll({Down => 1}) if ($counter > 330 || $_ > 1);
					} else {
						$main_sheet->Cells($row_counter, $col_counter)->{Value} = $match;
						print "<$match-$col_counter>|";
					}
				}
				#my $dummy = <STDIN> if ($_ == 7);
			}	
		}
		close($fh);
		print "File $file_name_with_ext processed!\n";
	}
	print "Please check the data and confirm to close it by saving (Yes/No) [Default is No]: ";
	my $confirm = <STDIN>;
	
	if (defined chomp($confirm)) {
		if ($confirm =~ /^yes$/i) {
			$xl_app->{DisplayAlerts} = 0;
			$xl_book->Save;
			$xl_book->Close;
			$xl_app->{DisplayAlerts} = 1;
			$xl_app->Quit;			
		}
	}
	$xl_app->{DisplayAlerts} = 1;
	$xl_app = 0;
	$xl_book = 0;
	
}


1;
