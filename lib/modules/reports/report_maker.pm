package modules::reports::report_maker;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
use modules::reports::report_utility;
use Spreadsheet::WriteExcel::Utility;
use v5.14;
use experimental;
#no warnings qw(experimental::smartmatch experimental::lexical_topic experimental::regex_sets experimental::lexical_subs);
#use modules::mailers::outlook;
our @ISA = qw(modules::reports::report_utility);

sub new {
	my ($class) = @_;
	my $self = $class->SUPER::new($_[1], $_[2], $_[3], $_[4], $_[5], $_[6], $_[7], $_[8], $_[9]);
	$self = {
		_xlPivot			=>	shift,
	};
	bless $self, $class;
	return $self;
}

sub getFinDataByStatePS {
    my ($self, $year, $month_from, $month_to, $state_regex, $ps) = @_;
    my $qq_string; my $query_string; my %param_hash; my $sth; my @empty_arr = ();
    my %hsh;
    my $calc = modules::helpers::calculator->new();
    my $obj = modules::reports::report_utility->new();
    my $dbh = $obj->getMySQLDBH();
    $qq_string = "SELECT SUM(Booking_Net) AS booking, SUM(Base_List) AS baselist,
    SUM(standard_cost) AS std_cost FROM mysourcedata.booking_dump 
    WHERE (FP_Year=? AND (FP_Month>=? AND FP_Month<=?)) AND Sales_Level_6 REGEXP ? AND prod_ser=?";
    $query_string = qq{$qq_string};
    $param_hash{1} = $year; $param_hash{2} = $month_from;
    $param_hash{3} = $month_to; $param_hash{4} = $state_regex;
    $param_hash{5} = $ps;
    $sth = $obj->getSimpleSTH($dbh, $query_string, %param_hash);
    while (my $href = $sth->fetchrow_hashref()) { 
        $hsh{c_yr_booking} = $href->{booking};
        $hsh{c_yr_baselist} = $href->{baselist};
        $hsh{c_yr_stdcost} = $href->{std_cost};
    } # while
    $sth->finish;

    # Acquiring the data for Previous Year
    $param_hash{1} = $year-1;
    $sth = $obj->getSimpleSTH($dbh, $query_string, %param_hash);
    while (my $href = $sth->fetchrow_hashref()) { 
        $hsh{p_yr_booking} = $href->{booking};
        $hsh{p_yr_baselist} = $href->{baselist};
        $hsh{p_yr_stdcost} = $href->{std_cost};
    } # while
    $hsh{c_yr_discount} = $calc->getDiscount($hsh{c_yr_booking},$hsh{c_yr_baselist});
    $hsh{p_yr_discount} = $calc->getDiscount($hsh{p_yr_booking}, $hsh{p_yr_baselist});
    $hsh{c_yr_stdmargin} = $calc->getDiscount($hsh{c_yr_stdcost}, $hsh{c_yr_booking});
    $hsh{p_yr_stdmargin} = $calc->getDiscount($hsh{p_yr_stdcost}, $hsh{p_yr_booking});

    $hsh{yoy_booking} = $calc->getGrowth($hsh{c_yr_booking}, $hsh{p_yr_booking});
    $hsh{yoy_discount} = $calc->getGrowth($hsh{c_yr_discount}, $hsh{p_yr_discount});
    $hsh{yoy_stdmargin} = $calc->getGrowth($hsh{c_yr_stdmargin}, $hsh{p_yr_stdmargin});

    # Formatted Strings for Screen display
    $hsh{f_c_yr_booking} = $calc->formatUSD($hsh{c_yr_booking});
    $hsh{f_c_yr_list} = $calc->formatUSD($hsh{c_yr_baselist});
    $hsh{f_c_yr_stdcost} = $calc->formatUSD($hsh{c_yr_stdcost});
    $hsh{f_p_yr_booking} = $calc->formatUSD($hsh{p_yr_booking});
    $hsh{f_p_yr_list} = $calc->formatUSD($hsh{p_yr_baselist});
    $hsh{f_p_yr_stdcost} = $calc->formatUSD($hsh{p_yr_stdcost});

    $hsh{f_yoy_booking} = $calc->formatPercent($hsh{yoy_booking});
    $hsh{f_yoy_discount} = $calc->formatPercent($hsh{yoy_discount});
    $hsh{f_yoy_stdmargin} = $calc->formatPercent($hsh{yoy_stdmargin});
    return %hsh;
}

sub getBookingDumpLatestYear {
    my $self = shift; 
    my $latest_year;
    my @empty_arr = ();
    my $obj = modules::reports::report_utility->new();
    my $dbh = $obj->getMySQLDBH();
    print "Finding latest year...\n";
    my $qq_string = "SELECT MAX(FP_Year) as year FROM booking_dump";
    my $query_string = qq{$qq_string};
    my $sth = $obj->getSimpleSTH($dbh, $query_string, @empty_arr);
    while (my $href = $sth->fetchrow_hashref()) { $latest_year = $href->{year}; }
    $sth->finish;
    print "Latest year has been acquired!\n";
    return ($latest_year, $latest_year-1);
}

sub dropTable {
    my ($self, $table_name) = @_;
    my @empty_arr = ();
    my $obj = modules::reports::report_utility->new();
    my $dbh = $obj->getMySQLDBH();

    print "Dropping existing $table_name table...\n";
    my $qq_string = "DROP TABLE ". $table_name;
    my $query_string = qq{$qq_string};
    my $sth = $obj->getSimpleSTH($dbh, $query_string, @empty_arr);
    $sth->finish;
    print "latest_year_booking_dump table has been dropped!\n";
    return $table_name;
}

sub createLikeTable {
    my ($self, $existing_tbl_name, $like_tbl_name) = @_;
    my @empty_arr = ();
    my $obj = modules::reports::report_utility->new();
    my $dbh = $obj->getMySQLDBH();

    print "Creating a new table with the name $existing_tbl_name...\n";
    my $qq_string = "CREATE TABLE IF NOT EXISTS ".$existing_tbl_name." LIKE ". $like_tbl_name;
    my $query_string = qq{$qq_string};
    my $sth = $obj->getSimpleSTH($dbh, $query_string, @empty_arr);
    $sth->finish;
    print "latest_year_booking_dump table has been created!\n";
    return $like_tbl_name;
}

sub copyTable {
    my ($self, $existing_tbl_name, $like_tbl_name, $year) = @_;
    my @empty_arr = ();
    my $obj = modules::reports::report_utility->new();
    my $dbh = $obj->getMySQLDBH();
    my %param_hash;

    print "Copying latest booking dump data into a new table...\n";
    my $qq_string = "INSERT INTO ".$existing_tbl_name." SELECT * FROM ".$like_tbl_name." WHERE FP_Year=?";
    my $query_string = qq{$qq_string};
    $param_hash{1} = $year;
    my $sth = $obj->getSimpleSTH($dbh, $query_string, %param_hash);
    $sth->finish;
    print "Latest booking dump data has been copied into a new table!\n";
}

sub getUniqueArray {
    my ($self, $field_name, $tbl_name) = @_;
    my @empty_arr = ();
    my @array;
    my $obj = modules::reports::report_utility->new();
    my $dbh = $obj->getMySQLDBH();
    print "Preparing for fetching in Array...\n";
    my $qq_string = "SELECT DISTINCT ".$field_name." FROM ".$tbl_name;
    my $query_string = qq{$qq_string};
    my $sth = $obj->getSimpleSTH($dbh, $query_string, @empty_arr);
    while (my $href = $sth->fetchrow_hashref()) { push @array, $href->{$field_name}; }
    $sth->finish;
    print "All Unique data copied into an Array!\n";
    return @array;
}

sub getSubSCMSHash { 
    return (
        "select" => "_SL_TM",
        "MM" => "_MM_",
        "GEO N" => "_GEO_NM",
        "GEO N-NM" => "_GEO_N_NM"
    );
}


sub getStateHash {
    my $self = shift;
    my $node_level = shift;
    my %nodes;

    given ($node_level) {
        when (/comm|india/i) {
            %nodes = (
                "EU1" => 'SOUTH',
                "EU2" => "WEST",
                "EU3" => "EAST|NORTH|SAARC",
                "Others" => "COMM|COM|MISC"
            );
        }
        when (/south|eu1/i) {
            %nodes = (
                "Karnataka" => 'BLR|KK|KKR',
                "Kerala" => "KEL|KL",
                "Tamilnadu" => "TN|CHN",
                "APTS" => "AP|TS|HYD"
            );
        }
        when (/west|eu2/i) {
            %nodes = (
                "Mumbai" => 'MUM',
                "Pune" => "PUN",
                "Gujarat" => "GUJ",
                "Others" => "PRG|ROM|WEST"
            );
        }
        when (/eu3/i) {
            %nodes = (
                "Nepal & Bhutan" => 'NEBH|NP',
                "West Bengal" => "WB",
                "East-Others" => "JBOC|NORTH_EAST",
                "Chandigarh" => 'CHD',
                "Delhi & NCR" => "N_DL|NCR",
                "NOI & GGN" => "NOI|GGN",
                "MPRJ" => "MPRJ|MP|RJ",
                "UPUK" => "UP|UPUK",
                "Bangladesh" => '_BD',
                "Sri Lanka" => "SRLK|SR_COL|_COL|SL_MD|NE_SAARC",
                "NOI & GGN" => "NOI|GGN",
                "MPRJ" => "MPRJ|MP|RJ",
            );
        }
        when (/east/i) {
            %nodes = (
                "Nepal & Bhutan" => 'NEBH|NP',
                "West Bengal" => "WB",
                "Others" => "JBOC|NORTH_EAST"
            );
        }
        when (/north/i) {
            %nodes = (
                "Chandigarh" => 'CHD',
                "Delhi & NCR" => "N_DL|NCR",
                "NOI & GGN" => "NOI|GGN",
                "MPRJ" => "MPRJ|MP|RJ",
                "UPUK" => "UP|UPUK"
            );
        }
        when (/saarc/i) {
            %nodes = (
                "Bangladesh" => '_BD',
                "Sri Lanka" => "SRLK|SR_COL|_COL|SL_MD|NE_SAARC",
            );
        }
    }
    return %nodes;
}


sub getExcelPivotReport {
	my ($self, $mysql_table_name, $field_text, $where_clause, $groupby_text, 
	$params, $field_hash, $field_array, $pivot_fields, $pivot_field_orientation, 
	$pivot_field_position, $pivot_datafield_function, $option) = @_;
	my $total_cols = scalar(@{$field_array});
	$self->setQueryStringAdvanced($mysql_table_name, $field_text, $where_clause, $groupby_text);
	if (%$params) {
		$self->prepareParamSTH(%$params, @$field_array);
	} else {
		$self->prepareSTH();
	}
	my $sth_final = $self->getSTH();
	my $total_rows_in_sth = ($sth_final->rows)+1;
	print "Query returned $total_rows_in_sth row(s)\n";

	$self->prepareXLApp();
	my $xl_app = $self->getXLApp();
	$self->prepareXLBook();
	my $xl_book = $self->getXLBook();
	$self->prepareRawDataSheet();
	$self->preparePivotSheet();
	my $raw_sheet = $self->getRawDataSheet();
	my $pivot_sheet = $self->getPivotSheet();
	my $raw_row = 1;
	my @full_range_array = ();
	my @row_array = ();
	for my $col (1..$total_cols) {
		$raw_sheet->Cells($raw_row, $col)->{Value} = ${$field_array}[$col-1];
		$row_array[$col-1] = ${$field_array}[$col-1];
	}
	$full_range_array[$raw_row-1] = [@row_array];
	while (my $a = $sth_final->fetchrow_hashref()) {
		$raw_row++;
		@row_array = ();
		for my $col (1..$total_cols) {
			if (defined $a->{${$field_array}[$col-1]}) {
				$row_array[$col-1] = $a->{${$field_array}[$col-1]};
			}
		}
		$full_range_array[$raw_row-1] = [@row_array];
		
	}
	my $last_row = ($sth_final->rows)+1;
	my $last_row_ref = xl_rowcol_to_cell($last_row-1, $total_cols-1); # Excel Cell to Row-Col Conversion
	$sth_final->finish();
	$self->prepareXLConstantObject();
	my $pivot_rng = $raw_sheet->Range("A1:$last_row_ref");
	$pivot_rng->{Value} = [@full_range_array];
	my $pivot_table = $pivot_sheet->PivotTableWizard(1, $pivot_rng, "", "ReportPivot");
#	my $pivot_table = $pivot_sheet->PivotTableWizard(1, [@full_range_array], "", "ReportPivot");
	$self->{_xlPivot} = $pivot_table;
	$pivot_table->{InGridDropZones} = 'False';

	my $total_fields = keys %$pivot_fields;
	for my $index (1..$total_fields) {
		if (${$pivot_field_orientation}{$index} =~ /^DATA_FIELD$/i) {
			print "Data Field Now!\n";
			$self->setDataFields(${$pivot_fields}{$index}, ${$pivot_datafield_function}{$index});
		} else {
			$self->setPivotFields(${$pivot_fields}{$index});
		}
	}
	for my $index (1..$total_fields) {
		unless (${$pivot_field_orientation}{$index} =~ /^DATA_FIELD$/i) {
			$self->setPivotFieldsOrientation(${$pivot_fields}{$index}, 
			${$pivot_field_orientation}{$index}, ${$pivot_field_position}{$index});
		}
	}
	$xl_app->{Visible} = 1;
	$xl_book->ActiveSheet->{Name} = "Pivot";
	$raw_sheet->delete;
	$xl_app->{DisplayAlerts} = 0;
	$xl_app->ActiveWindow->{Zoom} = 80;
	$xl_app->ActiveWindow->{DisplayGridlines} = 'False';
	if ($option =~ /^eps$/i) {
		$xl_book->SaveAs("c:\\pivot_report.xlsx");
		$xl_app->{DisplayAlerts} = 1;
	} elsif ($option =~ /^epse$/i) {
		$xl_book->SaveAs("c:\\pivot_report.xlsx");
		$xl_app->{DisplayAlerts} = 1;
		my $loopTerminator = 1;
		my @ToArray = ();
		my @CcArray = ();
		my @BccArray = ();
		while ($loopTerminator) {
			print "Check the Pivot if it's ok!\n";
			my $counter = 1;
			print "Email To $counter: ";
			while (my $to = <STDIN>) {
				chomp($to);
				if ($to) {
					push @ToArray, $to;
				}
				$counter++;
			print "Email To $counter: ";
			}
			print "Email IDs of 'To' have been received!\n";
			$counter = 1;
			print "Email Cc $counter: ";
			while (my $cc = <STDIN>) {
				chomp($cc);
				if ($cc) {
					push @CcArray, $cc;
				}
				$counter++;
			print "Email Cc $counter: ";
			}
			print "Email IDs of 'Cc' have been received!\n";
			$counter = 1;
			print "Email Bcc $counter: ";
			while (my $bcc = <STDIN>) {
				chomp($bcc);
				if ($bcc) {
					push @BccArray, $bcc;
				}
				$counter++;
			print "Email Bcc $counter: ";
			}
			print "Email IDs of 'Bcc' have been received!\n";
			print "Email Subject: ";
			my $subject = <STDIN>;
			chomp($subject);
			print "Email Body: ";
			my $body = <STDIN>;
			chomp($body);
			my $attachment = "c:\\pivot_report.xlsx";
			my $email = Email::OUTLOOK->new(\@ToArray, \@CcArray, \@BccArray, 
			$subject, $body, $attachment, 1);
			my $is_email_sent = $email->emailWithAttachmentOutlook();
			if ($is_email_sent) {
				print "Email sent successfully!\n";
				$loopTerminator = 0;
			} else {
				print "Email could not be sent due to following reasons!\n";
				foreach my $key (keys %{$email->{_errorHash}}) {
					print "$key\t=>\t${$email->{_errorHash}}{$key}\n";
				}
				print "Do you wanna try again (N)? : ";
				my $continue = <STDIN>;
				chomp $continue;
				if ((!defined $continue) || ($continue =~ /^Y$/i)) {
					$loopTerminator = 1;
				} else {
					$loopTerminator = 0;
					print "You do not want to try!....Exiting....\n";
				}
			}
			
		}
	}
	
	$xl_app = 0;
	$xl_book = 0;

	
}



sub setPivotFields {
	my ($self, $fields) = @_;
	print "Initiating 'Add Pivot Field' for $fields..\n";
	$self->{_xlPivot}->AddFields($fields);
	print "Added Pivot Field for $fields!\n";
}

sub setPivotFieldsOrientation {
	my ($self, $fields, $orientation_const, $position) = @_;
	my $orientation = undef;
		given ($orientation_const) {
			when (/^ROW_FIELD$/i) {
				$orientation = $self->{_xlConst}->{xlRowField};
			}
			when (/^PAGE_FIELD$/i) {
				$orientation = $self->{_xlConst}->{xlPageField};
			}
			when (/^COLUMN_FIELD$/i) {
				$orientation = $self->{_xlConst}->{xlColumnField};
			}
			default {
				print "No Pattern match on Pivot Field found!\n";
			}
		}
	print "Initiating 'Set Pivot Orientation' for $fields with Orientation $orientation...\n";
		$self->{_xlPivot}->PivotFields($fields)->{Orientation} = $orientation;
	print "Pivot Orientation is SET for $fields with Orientation $orientation!\n";
	print "Initiating 'Set Pivot Position' for $fields with Orientation $position...\n";
		$self->{_xlPivot}->PivotFields($fields)->{Position} = $position;
	print "Pivot Position is SET for $fields with Position $position!\n";
}

sub setDataFields {
	my ($self, $fields, $function) = @_;
	print "Initiating 'Set Data Field' for $fields...\n";
	$self->{_xlPivot}->PivotFields($fields)->{Orientation} = 
						$self->{_xlConst}->{xlDataField};
	$self->{_xlPivot}->PivotFields($function)->{NumberFormat} = 
						"\$#,##0_);[Red](\$#,##0)";
	$self->{_xlPivot}->{TableStyle2} = "PivotStyleLight16";
	print "Data Field' for $fields!\n";
}

1;
