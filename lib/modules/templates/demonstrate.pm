package modules::templates::demonstrate;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
use v5.14;
use experimental;
#no warnings qw(experimental::smartmatch experimental::lexical_topic experimental::regex_sets experimental::lexical_subs);

our %dem_hash;

sub new {
	my $class = shift;
	my $self = {};
	our %dem_hash = (
			'cvrt'	=>	'Convert Command--used to convert files',
			'upld'	=>	'Upload Command--used to upload files',
			'cln'	=>	'Clean Command--used to clean data',
			'todd'	=>	'Toddle Command--used to Toddle across the data',
			'quit'	=>	'Exit from CDH application',
			'dem'	=>	'Demonstrated Command--lists commands available in CDH',
			'pcf'	=>	'Poly Control F--helps to map one Excel list with the other',
			'rpt'	=>	'Report Maker--helps to obtain automated reports'
	);
	bless $self, $class;
	return $self;
}


sub dem{
	my $header = "CDH command list:";
	my $head_border = "=" x length $header;
	my $string_length = 0;
	print "\n$header\n$head_border\n";
	foreach my $key (sort keys %dem_hash) {
		my $value = $dem_hash{$key};
		my $print_text = "$key ==> $value";
		unless ($string_length > length $print_text) {
			$string_length = length $print_text;
		}
		print $print_text, "\n";
	}
	print "=" x $string_length, "\n\n";
}

sub demSpecific {
		my ($self, $option) = @_;
		given ($option) {
			when (/^rpt$/) {
				my %dem_sub_hash = (
							1	=>	"Syntax\t\t\t\t==>\trpt\{space\}[--option]",
							2	=>	"[--option]\t\t\t==>\t[--epm\{space\}[email_ID]]\t[--epm]\t[--ep]",
							3	=>	"[--epm\{space\}[email_ID]]\t==>\t[e]--Excel Report\t[p]--Pivot Table\t[m]--Mailing\t[email_ID]--a valid email ID (Save it as a File as well)",
							4	=>	"[--epm]\t\t\t\t==>\t[e]--Excel Report\t[p]--Pivot Table\t[m]--Mailing",
							5	=>	"[--ep]\t\t\t\t==>\t[e]--Excel Report\t[p]--Pivot Table and NOT saving Workbook",
							6	=>	"[--eps]\t\t\t\t==>\t[e]--Excel Report\t[ps]--Pivot Table and Save it as File only",
							7	=>	"[--epse]\t\t\t==>\t[e]--Excel Report\t[pse]--Pivot Table, Save and Email"
						   );
				$self->printDetails(\%dem_sub_hash, $option);
			}
			when (/^pcf$/) {
				my %dem_sub_hash = (
							1	=>	"Syntax\t\t==>\tpcf\{space\}[-option]",
							2	=>	"[-option]\t==>\t[-m]\t[-s]",
							3	=>	"[-m]\t\t==>\t[m]--Multiple under Looping",
							4	=>	"[-md]\t\t==>\t[m]--Multiple\t[d] --De-dupping",
							5	=>	"[-s]\t\t==>\t[s]--Single"
						   );
				$self->printDetails(\%dem_sub_hash, $option);
			}
			when (/^cvrt$/) {
				my %dem_sub_hash = (
							1	=>	"Syntax\t\t==>\tcvrt\{space\}<xls|xlsx>\{space\}>>\{space\}<txt|csv>",
							2	=>	"<xls>\t\t==>\txls--In File as Excel File of 97-2003 format",
							3	=>	"<xlsx>\t\t==>\txlsx--In File as Excel File of >=2007 format",
							4	=>	">>\t\t==>\t>>--Double Greater than sign to tell system to consider out stream",
							5	=>	"<txt>\t\t==>\ttxt--Out File as .txt format with comma separated valued",
							6	=>	"<csv>\t\t==>\tcsv--Out File as .csv format (will be in effect for Win32 only--currently deprecated)",
						   );
				$self->printDetails(\%dem_sub_hash, $option);
			}
			when (/^upld$/) {
				my %dem_sub_hash = (
							1	=>	"Syntax\t\t==>\tupld\{space\}<mysql|SQL>\{space\}<<\{space\}<csv|others>",
							2	=>	"<mysql>\t\t==>\tmysql--Data into MySQL Server",
							3	=>	"<SQL>\t\t==>\tSQL--Data into other SQL Server (Currently Deprecated)",
							4	=>	"<<\t\t==>\t<<--Double Less than sign to tell system to consider In stream",
							5	=>	"<others>\t==>\tOthers--In File as any format (Currently Deprecated)",
							6	=>	"<csv>\t\t==>\tcsv--In File as .csv format",
						   );
				$self->printDetails(\%dem_sub_hash, $option);
			}
			when (/^cln$/) {
				my %dem_sub_hash = (
							1	=>	"Syntax\t\t\t\t==>\tcln\{space\}<UDO>",
							2	=>	"<UDO>\t\t\t\t==>\tUser Defined Option",
							3	=>	"<findump -ri | -nri>\t\t==>\tClean Finance Booking Dump and upload in MySQL Table",
							4	=>	"<sql to nosql>\t\t\t==>\tProcess Data from MySQL and preapre NoSQL Database",
							5	=>	"<findump -xl |-xls|-xlsx >\t==>\tMakes a clean Excel uploadable sheets",
							6	=>	"<findump -uf |-uniquefinder >\t==>\tUnique Names Finder",
						   );
				$self->printDetails(\%dem_sub_hash, $option);
			}
			when (/^todd$/) {
				my %dem_sub_hash = (
							1	=>	"Syntax\t\t\t\t==>\ttodd\{space\}<UDO>",
							2	=>	"<UDO>\t\t\t\t==>\tUser Defined Option",
							3	=>	"<-xluf |-xlsuf | -xlsxuf >\t==>\tUnique Names Finder",
							4	=>	"<-svg >\t==>\tToddling a SVG table file and converts in to Excel",
						   );
				$self->printDetails(\%dem_sub_hash, $option);
			}
			when (/^quit$/) {
				my %dem_sub_hash = (
							1	=>	"Syntax\t\t==>\tquit",
							2	=>	"quit\t\t==>\tis a explicit command to exit CDH application",
						   );
				$self->printDetails(\%dem_sub_hash, $option);
			}
			default {
				print "Option Received is $option!\n";
			}
		}
}

sub printDetails {
	my($self, $hash_ref, $option) = @_;
	my $dis_string = $dem_hash{$option};
	my $string_length = length $dis_string;
	print "\n", $dis_string, "\n";
	print "=" x $string_length, "\n";

	$string_length = 0;
	foreach my $key (sort keys %$hash_ref) {
		my $value = $hash_ref->{$key};
		unless ($string_length > length $value) {
			$string_length = length $value;
		}
		print $value, "\n";
	}
	print "=" x $string_length, "\n\n";
}

1;
