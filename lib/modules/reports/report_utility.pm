package modules::reports::report_utility;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
use Win32::OLE;
use Win32::OLE::Const;
use modules::helpers::connections::mysql_connection;
use Scalar::Util qw(looks_like_number);
use Spreadsheet::WriteExcel::Utility;

sub new {
	my $class = shift;
	my $self = {
		_queryString 				=>	shift,
		_sthString					=>	shift,
		_xlApplication				=>	shift,
		_xlBook						=>	shift,
		_pivotSheet					=>	shift,
		_rawDataSheet				=>	shift,
		_queryHashReference			=>	shift,
	};
	bless $self, $class;
	return $self;
}

sub getXLConstantObject {
	my ($self) = @_;
	return Win32::OLE::Const->Load("Microsoft Excel");
}

sub setQueryStringAdvanced {
	my ($self, $table_name, $field_string, $where_clause, $groupby_srting) = @_;
	print "Preparing Query String and getting assigned...\n";
	my $query = "SELECT ";
	$query = $query.$field_string;
	$query = 	$query." FROM ".$table_name;
	$query = $query." ".$where_clause." ".$groupby_srting;
	$self->{_queryString} = qq{$query};
	print "Full Query String is $self->{_queryString}\n";
	print "Query String has been successfully prepared!\n";
}

sub setQueryString {
	my ($self, $query_string) = @_;
	#print "Preparing Query String and getting assigned...\n";
	$self->{_queryString} = $query_string;
	#print "Query String has been successfully prepared!\n";
}

sub prepareSTH {
	my ($self, $switch_off_process) = @_;
	print "Acquiriing DB Statement Handle...\n"  unless ($switch_off_process);
	my $sth = $self->{_dbiConnection}->prepare($self->{_queryString});
	$sth->execute() or die $DBI::errstr;
	$self->{_sthString} = $sth;
	print "DB Statement Handle has been successfully acquired!\n"  unless ($switch_off_process);;
}

sub connectAndPrepareSTH {
	my ($self) = @_;
	$self->prepareDBIConnection();
	print "Acquiriing DB Statement Handle...\n";
	my $sth = $self->{_dbiConnection}->prepare($self->{_queryString});
	$sth->execute() or die $DBI::errstr;
	$self->{_sthString} = $sth;
	print "DB Statement Handle has been successfully acquired!\n";
	return $self->{_sthString};
}

sub prepareParamSTH {
	my ($self, %param_hash, @cols) = @_;
	print "Acquiriing DB Statement Handle...\n";
	my $sth = undef;
	$sth = $self->{_dbiConnection}->prepare($self->{_queryString});
	foreach my $key (sort keys %param_hash) {
		print "Parameter-$key => $param_hash{$key}\n";
		if (looks_like_number($key)) {
			$sth->bind_param($key, $param_hash{$key});
		}
	}
	
	for my $col (1..$#cols+1) {
		$sth->bind_col($col, \$cols[$col-1]);
	}
	$sth->execute();
	$self->{_sthString} = $sth;
	print "DB Statement handle has been successfully acquired!\n";
}

sub prepareXLApp {
	my ($self) = @_;
	print "Opening Excel Application...\n";
	my $xlApp = Win32::OLE->new('Excel.Application');
	$xlApp->{Visible} = 1;
	$xlApp->{DisplayAlerts} = 0;
	$self->{_xlApplication} = $xlApp;
	print "Excel Application has been successfully opened!\n";
}

sub prepareXLBook {
	my ($self) = @_;
	print "Preparing XL WorkBook...\n";
	$self->{_xlBook} = $self->{_xlApplication}->Workbooks->Add;
	print "XL WorkBook successfully prepared!\n";
	
}

sub preparePivotSheet {
	my ($self) = @_;
	print "Adding a Pivot Sheet in Excel Application...\n";
	my $xlSheet = $self->{_xlBook}->Sheets(2);
#	$xlSheet->{Name} = "Pivot";
	$self->{_pivotSheet} = $xlSheet;
	print "Pivot Sheet has been successfully added!\n";
}

sub prepareRawDataSheet {
	my ($self) = @_;
	print "Adding a Raw Data Sheet in Excel Application...\n";
	my $xlSheet = $self->{_xlBook}->Sheets(1);
	$xlSheet->{Name} = "Raw_Data";
	$self->{_rawDataSheet} = $xlSheet;
	print "Raw Data Sheet has been successfully added!\n";
}

sub setReportUtility {
	my ($self) = @_;
	$self->prepareSTH();
	$self->prepareXLApp();
	$self->preparePivotSheet();
}

sub getMySQLDBH {
	my $self = shift;
	my $obj = modules::helpers::connections::mysql_connection->new();
	return $obj->getMySQLConnection();
}

sub getSimpleSTH {
	my ($self, $dbh, $query_string, %param_hash) = @_;
	my $sth = undef;
	$sth = $dbh->prepare($query_string);
	
	if (%param_hash) {
		foreach my $key (sort keys %param_hash) {
			if (looks_like_number($key)) {
				$sth->bind_param($key, $param_hash{$key});
			}
		}
	}
	$sth->execute() or die $DBI::errstr;
	return $sth;
}
sub getSTH {
	my ($self) = @_;
	return $self->{_sthString};
}

sub getXLApp {
	my ($self) = @_;
	my $xlApp = Win32::OLE->new('Excel.Application');
	$xlApp->{Visible} = 1;
	$xlApp->{DisplayAlerts} = 0;
	return $xlApp;
}

sub getXLBook {
	my ($self, $xlApp) = @_;
	my $xlBook = $xlApp->Workbooks->Add;
	return $xlBook;
}

sub getXLSheet {
	my ($self, $xlBook, $sheetNo) = @_;
	my $xlSheet = $xlBook->Sheets($sheetNo);
	return $xlSheet;
}

sub getPivotSheet {
	my ($self) = @_;
	return $self->{_pivotSheet};
}
sub getRawDataSheet {
	my ($self) = @_;
	return $self->{_rawDataSheet};
}
1;
