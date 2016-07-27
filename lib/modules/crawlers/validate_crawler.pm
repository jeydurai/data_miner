package modules::crawlers::validate_crawler;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
no warnings 'uninitialized';
use modules::reports::report_maker;
use modules::helpers::calculator;
use Scalar::Util qw(looks_like_number);
use v5.14;
use XML::Dumper;
use Algorithm::Permute;
use Array::Utils qw(:all);
use Text::Trim qw(trim);
use Data::Dumper;
use IO::Handle;
use Excel::Writer::XLSX;
use Excel::Writer::XLSX::Utility;
use Spreadsheet::WriteExcel::Utility;
use XML::Dumper;
use Try::Tiny;
use List::MoreUtils qw(uniq);

our @ISA = qw(modules::reports::report_maker);



no warnings qw(experimental::smartmatch experimental::lexical_topic experimental::regex_sets experimental::lexical_subs);

sub new {
    my $class = shift;
    my $self = {};
    bless $self, $class;
    return $self;
}


sub validatePOSDuplication {
    my $self = shift;

	my $DEFAULT_MAPPING_SOURCE_CHOICE = 3;
	my $mapping_source_string;
	
	print "De serializing the file information in to a variable...\n";
	my $dump = new XML::Dumper;
	my $file_name_of_input_data = "input_data.xml";
	my $ab_path; my $unique_customers_sheet_name; my $unique_partners_sheet_name; my $unique_technologies_sheet_name; my $unique_sales_agents_sheet_name;
	
	if (-f $file_name_of_input_data) {
		my $input_data = $dump->xml2pl($file_name_of_input_data);
		my $sheet_name;
		foreach (@{$input_data}) {
			$ab_path = $_->{absolute_path};
			$unique_customers_sheet_name = $_->{unique_customers};
			$unique_partners_sheet_name = $_->{unique_partners};
			$unique_technologies_sheet_name = $_->{unique_technologies};
			$unique_sales_agents_sheet_name = $_->{unique_sales_agents};
		}
	} else {
		print "\nPath of the Source file: ";
		my $dir_name = <STDIN>;
		print "Source file name: ";
		my $file_name = <STDIN>;
		print "Source file sheet name: ";
		my $unique_customers_sheet_name = <STDIN>;
	
		if (defined chomp($dir_name) && defined chomp($file_name) && defined chomp($unique_customers_sheet_name)) {
			# Make ready of absolute path for the file 
			my $path_string = $dir_name . '/';
			$ab_path = $path_string . $file_name;
			
			my $input_data = [
				{
					absolute_path => $ab_path,
					unique_customers => $unique_customers_sheet_name,
					unique_partners => "unique_partners",
					unique_technologies => "unique_technologies",
					unique_sales_agents => "unique_sales_agents",
				}
			];
			
			my $file_name_of_input_data = "input_data.xml";
			my $dump = new XML::Dumper;
			$dump->pl2xml($input_data, $file_name_of_input_data);
		} else {
			print "Input strings on the file name and path are Incorrect!\n";
			return;
		}
	}

	print "Completed de serializing the file information in to a variable!\n";

	# Opening the Required Excel Sheet
	# ================================
	print "Opening $ab_path file...\n";
	my $xl_app = $self->getXLApp();
	$xl_app->{Visible} = 1;
	$xl_app->{DisplayAlerts} = 0;
	my $xl_book = $xl_app->Workbooks->open($ab_path);
	my $unique_sheet = $xl_book->Sheets($unique_customers_sheet_name);
	my $last_row_in_unique_sheet = $unique_sheet->UsedRange->Rows->{'Count'};
	my $start_row = 2; my $dealid_col = 11; my $partner_name_col = 17; my $customer_name_col = 24; my $posid_col = 13; my $pos_batchid_col = 5;
	my $row_counter = $start_row;
	my $last_row_with_short = $last_row_in_unique_sheet;
    while ($row_counter <= $last_row_in_unique_sheet) {
        my $dealid=""; my $partner_name=""; my $customer_name=""; my $posid="";
        my $pos_batchid="";
        try {
            $dealid = $unique_sheet->Cells($row_counter, $dealid_col)->{Value};
            $partner_name = $unique_sheet->Cells($row_counter, $partner_name_col)->{Value};
            $customer_name = $unique_sheet->Cells($row_counter, $customer_name_col)->{Value};
            $posid = $unique_sheet->Cells($row_counter, $posid_col)->{Value};
            $pos_batchid = $unique_sheet->Cells($row_counter, $pos_batchid_col)->{Value};
        } catch {
            $dealid = "";
            $partner_name = "";
            $customer_name = "";
            $posid = "";
            $pos_batchid = "";
        };
    } # while
    print "Deal ID: $dealid, Partner Name: $partner_name\n";
    print "\n";
}

1;
