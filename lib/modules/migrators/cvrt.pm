package modules::migrators::cvrt;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use modules::migrators::xlsx_to_csv_txt;
use modules::migrators::xls_to_csv_txt;
use strict;
use warnings;

sub new {
	my $class = shift;
	my $self = {};
	bless $self, $class;
	return $self;
}

sub convertXLSX_To_TXT {
	print "Source Folder Name (Absolute Path): ";
	my $folder_name = <STDIN>;
	print "Source File Name (with Extension Name): ";
	my $file_name = <STDIN>;
	print "Source File Sheet/Tab Name (Default is 'Data'): ";
	my $sheet_name = <STDIN>;
	print "Output File Name: ";
	my $output_file_name = <STDIN>;

	$sheet_name = 'Data' unless (defined chomp($sheet_name));

	my $file_converter = modules::migrators::xlsx_to_csv_txt->new();
	if ((defined chomp($folder_name)) && (defined chomp($file_name)) &&
		(defined chomp($output_file_name))) {
		$file_converter->convert($folder_name, $file_name, $sheet_name, $output_file_name);
	} else {
		print "You have not specified either 'Source Folder Name', 'Source File Name', or 'Output File Name'\n";
	}
}

sub convertXLS_To_TXT {
	print "Source Folder Name (Absolute Path): ";
	my $folder_name = <STDIN>;
	print "Source File Name (with Extension Name): ";
	my $file_name = <STDIN>;
	print "Source File Sheet/Tab Name (Default is 'Data'): ";
	my $sheet_name = <STDIN>;
	print "Output File Name: ";
	my $output_file_name = <STDIN>;

	$sheet_name = 'Data' unless (defined chomp($sheet_name));

	my $file_converter = modules::migrators::xls_to_csv_txt->new();
	if ((defined chomp($folder_name)) && (defined chomp($file_name)) &&
		(defined chomp($output_file_name))) {
		$file_converter->convert($folder_name, $file_name, $sheet_name, $output_file_name);
	} else {
		print "You have not specified either 'Source Folder Name', 'Source File Name', or 'Output File Name'\n";
	}
}


1;