package modules::validators::input_validator;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
use Scalar::Util qw(looks_like_number);
#use modules::mailers::outlook;
use v5.14;
use experimental;
#no warnings qw(experimental::smartmatch experimental::lexical_topic experimental::regex_sets experimental::lexical_subs);
use modules::reports::report_utility;
our @ISA = qw(modules::reports::report_utility);

sub new {
	my $class = shift;
	my $self = {
		_selectedSQLFields		=>	shift,
		_pivotField				=> 	shift,
		_pivotFieldOrientation	=>	shift,
		_pivotFieldPosition		=>	shift,
		_pivotDataFieldFunction	=>	shift,
		_tableField				=>	shift,
		_userField				=>	shift,
		_actualField			=>	shift,
		_fieldArrayRef			=>	shift,
		_isPivotFieldOk			=>	shift,
		_isPivotOrientationOk	=>	shift,
		_isPivotPositionOk		=>	shift,
		_emailAddresses			=>	shift,
	};
	bless $self, $class;
	return $self;
}

sub getPivotProperty {
	my ($self, $user_pivot_input) = @_;
	my $field = undef;
	my $orientation = undef;
	my $position = undef;
	my $decide_vali_pivot_field_property = undef;
	my $decide_vali_pivot_field_orientation = undef;
	my $decide_vali_pivot_field_position = undef;
	if ($user_pivot_input =~ /^(.*?)\|(.*?)\|(.*?)$/i) {
		$field = $1;
		$orientation = $2;
		$position = $3;
		$decide_vali_pivot_field_property = $self->validatePivotFieldProperty($field);
		$decide_vali_pivot_field_orientation = $self->validatePivotFieldOrientation($orientation);
		$decide_vali_pivot_field_position = $self->validatePivotFieldPosition($position);
		if ($decide_vali_pivot_field_property && $decide_vali_pivot_field_orientation 
			&& $decide_vali_pivot_field_position) {
			return 1;
		} else {
			return 0;
		}
	} else {
		return 0;
	}
}

sub validateFieldProperty {
	my ($self, $input) = @_;
	my $arith = "";
	my $table_field = "";
	my $user_field = "";
	my $actual_field = "";
	my %field_names = ();
	my %temp_hash = ();
	if ($input =~ /^(.*?)\((.*?)\)\s+as\s+(.*?)$/i) {
		print "Data Field Chosen is: $2\n";
		print "Display Field: $3\n";
		$arith = $1;
		$table_field = $2;
		$user_field = $3;
		$actual_field = $input;
		if ($arith =~ /^(sum|count)$/) {
			print "Math function for the data field $actual_field is $1\n";
		} else {
			print "Math Function is NOT correct!\n";
			return 0;
		}
	} elsif ($input =~ /^(.*?)\((.*?)\)$/i) {
		print "Data Field Chosen is: $2\n";
		print "Display Field: $2\n";
		$arith = $1;
		$table_field = $2;
		$user_field = $2;
		$actual_field = $input;
		if ($arith =~ /^(sum|count)$/) {
			print "Math function for the data field $actual_field is $1\n";
		} else {
			print "Math Function is NOT correct!\n";
			return 0;
		}
	} elsif ($input =~ /^(.*?)\s+as\s+(.*?)$/i) {
		print "Data Field Chosen is: $2\n";
		$table_field = $1;
		$user_field = $2;
		$actual_field = $input;
	} else {
		$table_field = $input;
		$actual_field = $input;
		$user_field = $input;
	}
	
	print "Table Field: $table_field\n";
	print "Actual Field: $actual_field\n";
	print "User Field: $user_field\n";
	
	$self->{_tableField} = $table_field;
	$self->{_actualField} = $actual_field;
	$self->{_userField} = $user_field;
	return 1;
}

sub validatePivotFieldProperty {
	my ($self, $field) = @_;
	my $field_text = undef;
	my $data_field_function = undef;
	my $col_ref = $self->{_selectedSQLFields};
	print "Column Reference Array:\n";
	foreach my $content (@{$col_ref}) {
		print "Content - $content\n";
	}
	
	my $error_key = 0;
	my $error_text = "";
	my %error = ();
	if ($field =~ /^(.*?)\((.*?)\)$/i) {
		$data_field_function = $1;
		$field_text = $2;
		print "Field Text: $field_text\n";
		if (grep {$_ eq $field_text} @{$col_ref}) {
			if ($data_field_function =~ /^(SUM|COUNT)$/i) {
				$self->{_pivotField} = $field_text;
				if ($1 =~ /^SUM$/i) {
					$self->{_pivotDataFieldFunction} = "Sum of $field_text";
				} else {
					$self->{_pivotDataFieldFunction} = "Count of $field_text";
				}
				$error_key = 1;
				$error_text = "Pivot Field Name and Function are CORRECT!";
			} else {
				$error_key = 0;
				$error_text = "Incorrect Data Field Function!";
			}
		} else {
				$error_key = 0;
				$error_text = "Incorrect Field Name!";
		}
		
	} else {
		$field_text = $1;
		$self->{_pivotField} = $field_text;
		$self->{_pivotDataFieldFunction} = undef;
		$error_key = 1;
		$error_text = "Pivot Field Name is CORRECT!";
	}
		$error{$error_key} = $error_text;
		$self->{_isPivotFieldOk} = \%error;
		if ($error_key) {
			return 1;
		} else {
			return 0;
		}
	
}

sub validatePivotFieldOrientation {
	my ($self, $orientation) = @_;
	my $error_key = 0;
	my $error_text = "";
	my %error = ();
	if ($orientation =~ /^(ROW|PAGE|COLUMN|ROW_FIELD|PAGE_FIELD|COLUMN_FIELD|COL|COL_FIELD|DATA|DATA_FIELD)$/i) {
		given ($1) {
			when (/^(ROW|ROW_FIELD)$/i) {
				$self->{_pivotFieldOrientation} = "ROW_FIELD";
				$error_key = 1;
				$error_text = "Pivot Field Orientation is CORRECT as 'ROW_FIELD'!";
			}
			when (/^(PAGE|PAGE_FIELD)$/i) {
				$self->{_pivotFieldOrientation} = "PAGE_FIELD";
				$error_key = 1;
				$error_text = "Pivot Field Orientation is CORRECT as 'PAGE_FIELD'!";
			}
			when (/^(COL|COLUMN_FIELD|COL_FIELD)$/i) {
				$self->{_pivotFieldOrientation} = "COLUMN_FIELD";
				$error_key = 1;
				$error_text = "Pivot Field Orientation is CORRECT as 'COLUMN_FIELD'!";
			}
			when (/^(DATA|DATA_FIELD)$/i) {
				if ($self->{_pivotDataFieldFunction}) {
					$self->{_pivotFieldOrientation} = "DATA_FIELD";
					$error_key = 1;
					$error_text = "Pivot Field Orientation is CORRECT as 'DATA_FIELD' and FUNCTION is CORRECT as well!";
				} else {
					$self->{_pivotFieldOrientation} = "DATA_FIELD";
					$error_key = 0;
					$error_text = "Pivot Field Orientation is CORRECT as 'DATA_FIELD', but FUNCTION is WRONG!";
				}
			
			}
			default {
				$self->{_pivotFieldOrientation} = undef;
				$error_key = 0;
				$error_text = "INCORRECT Pivot Field Orientation!";
			}
			
		}
	
	} else {
		$self->{_pivotFieldOrientation} = undef;
		$error_key = 0;
		$error_text = "INCORRECT Pivot Field Orientation!";
	}
	$error{$error_key} = $error_text;
	$self->{_isPivotOrientationOk} = \%error;
	if ($error_key) {
		return 1;
	} else {
		return 0;
	}
	
}

sub validatePivotFieldPosition {
	my ($self, $position) = @_;
	my $error_key = 0;
	my $error_text = "";
	my %error = ();
	if (looks_like_number($position)) {
		$self->{_pivotFieldPosition} = int($position);
		$error_key = 1;
		$error_text = "Pivot Field Position is CORRECT as $self->{_pivotFieldPosition}!";
	} else {
		$error_key = 0;
		$error_text = "INCORRECT Pivot Field Position!";
	}
	$error{$error_key} = $error_text;
	$self->{_isPivotPositionOk} = \%error;
	if ($error_key) {
		return 1;
	} else {
		return 0;
	}
}

sub validateRPT_EPM {
	my ($self, $email_input, $sub_levels_input) = @_;
	my $is_success = 1;
	my %what_happened = ();
	print "Input-1: $email_input\n";
	print "Input-2: $sub_levels_input\n";

	# =====================================================
	# Email ID Validation
	# =====================================================
	
	my @ToArray = ();
	my @CcArray = ();
	my @BccArray = ();
	
#	my @email_input_array = split(/&&/, $email_input);
	my @email_input_array = split(/[&,#,\$,^,!,~,`,\/,%,\|]+/, $email_input);
	my $is_error = $self->parseEmailID(\@email_input_array);
	foreach my $key (sort keys %{$is_error}) {
		if ($key) {
			print ${$is_error}{$key},"\n";
			$what_happened{${$is_error}{$key}} = "Email Can't be sent!";
			$is_success = 0;
		}
	}
	my $to_counter = 0;
	my $cc_counter = 0;
	my $bcc_counter = 0;
	foreach my $key (keys %{$self->{_emailAddresses}}) {
		print "$key => ${$self->{_emailAddresses}}{$key}\n";
		$key =~ /^\d+:(.*?)$/i;
		given ($1) {
			when (/^to$/i) {
				$ToArray[$to_counter] = ${$self->{_emailAddresses}}{$key};
				$to_counter++;
			}
			when (/^cc$/i) {
				$CcArray[$cc_counter] = ${$self->{_emailAddresses}}{$key};
				$cc_counter++;
			}
			when (/^bcc$/i) {
				$BccArray[$bcc_counter] = ${$self->{_emailAddresses}}{$key};
				$bcc_counter++;
			}
		}
	}
	# =====================================================
	# Sales Level Validation
	# =====================================================
	my @sub_levels_input_array = split(/[&,#,\$,^,@,!,~,`,\/,%,\|]+/, $sub_levels_input);
	my ($error, $error_hash_ref, $sub_level) = $self->parseSubLevels(\@sub_levels_input_array);
	if ($error) {
		print "Error in the Sub Level as following:\n";
		foreach my $key (keys %{$error_hash_ref}) {
			print "$key\t=>\t${$error_hash_ref}{$key}\n";
			$what_happened{$key} = ${$error_hash_ref}{$key};
			$is_error = 0;
			$is_success = 0;
		}
	} else {
		print "Sub Level String is: $sub_level\n";
	}
	return ($is_success, \%what_happened, \@ToArray, \@CcArray, \@BccArray, $sub_level);
}


sub parseEmailID {
	my ($self, $array) = @_;
	my %is_there_error = ();
	my $error_key = undef;
	my $error_string = undef;
	my %email_address = ();
	my $counter = 1;
	foreach my $item (@{$array}) {
		if ($item =~ /^(to|cc|bcc):(.*?)$/i) {
			print "Email String to be parsed: $1=>$2\n";
			my $helper = Email::OUTLOOK->new();
			my $is_valid = $helper->isEmailIDValid($2, $1);
			if ($is_valid) {
				$email_address{$counter.":".$1} = $2;
				$counter++;
				$error_key = 0;
				$error_string = "$2 is a VALID email ID!";
			} else {
				$error_key = 1;
				$error_string = "$2 is NOT a valid email ID!";
			}
		} else {
				$error_key = 1;
				$error_string = "Syntax ERROR in Email Input String!";
		}
	}
	$is_there_error{$error_key} = $error_string;
	$self->{_emailAddresses} = \%email_address;
	return \%is_there_error;
}

sub parseSubLevels {
	my ($self, $array) = @_;
	my %is_there_error = ();
	my $error_key = undef;
	my $error_string = undef;
	my $sub_level_string = "";
	my $region_string = "";
	my $eu_string = "";
	my $sl6_string = "";
	my $sub_scms_string = "";
	my $fp_year_string = "";
	my $fp_quarter_string = "";
	my $fp_month_string = "";
	my $fp_week_string = "";
	my $region_counter = 1;
	my $eu_counter = 1;
	my $sl6_counter = 1;
	my $sub_scms_counter = 1;
	my $fp_year_counter = 1;
	my $fp_quarter_counter = 1;
	my $fp_month_counter =1;
	my $fp_week_counter =1;
	my $array_size = scalar(@{$array});
	foreach my $item (@{$array}) {
		print "Sub Levels to be parsed: $item\n";
		given ($item) {
			when (/^(EAST|NORTH|SOUTH|WEST|SAARC)$/i) {
				if ($region_counter == 1) {
					$region_string = $region_string."region = "."'".uc $1."'";
				} else {
					$region_string = $region_string." OR region = "."'".uc $1."'";
				}
				$region_counter++;
				$error_key = $item;
				$error_string = "Parsed!";
			}
			when (/^(PL|PLV|PL_V|PLS|PL_S)$/i) {
				my $sub_scms_input = ""; 
				if ($1 =~ /^PLV|PLS|PL_V|PL_S$/i) {
					$sub_scms_input = "PL_S";
				} else {
					$sub_scms_input = "PL";
				}
				if ($sub_scms_counter == 1) {
					$sub_scms_string = $sub_scms_string."sub_scms = "."'".uc $sub_scms_input."'";
				} else {
					$sub_scms_string = $sub_scms_string." OR sub_scms = "."'".uc $sub_scms_input."'";
				}
				$sub_scms_counter++;
				$error_key = $item;
				$error_string = "Parsed!";
			}
			when (/^(EU1|EU2|EU3|COMM)$/i) {
				my $get_string = $self->getRegionsForEU($1);
				if ($eu_counter == 1) {
					$eu_string = $eu_string.uc $get_string;
				} else {
					$eu_string = $eu_string." OR ".uc $get_string;
				}
				$eu_counter++;
				$error_key = $item;
				$error_string = "Parsed!";
			}
			when (/^(\d\d\d\d|\d\d\d\d-\d\d\d\d|>=\d\d\d\d|>\d\d\d\d|=\d\d\d\d|<=\d\d\d\d|<\d\d\d\d|FY\d\d|FY\d\d-FY\d\d|FY\d\d-\d\d|>=FY\d\d|>FY\d\d|=FY\d\d|<=FY\d\d|<FY\d\d)$/i) {
				my $get_string = $self->parseYearString($1);
				if ($fp_year_counter == 1) {
					$fp_year_string = $fp_year_string.$get_string;
				} else {
					$fp_year_string = $fp_year_string." OR ".$get_string;
				}
				$fp_year_counter++;
				$error_key = $item;
				$error_string = "Parsed!";
			}
			when (/^(Q\d|\dQ|\dQ|=\dQ|=Q\d|Q\d-Q\d|>=Q\d|>Q\d|<=Q\d|<Q\d|\dQ-\dQ|>=\dQ|>\dQ|<=\dQ|<\dQ)$/i) {
				my $get_string = $self->parseQuarterString($1);
				if ($fp_quarter_counter == 1) {
					$fp_quarter_string = $fp_quarter_string.$get_string;
				} else {
					$fp_quarter_string = $fp_quarter_string." OR ".$get_string;
				}
				$fp_quarter_counter++;
				$error_key = $item;
				$error_string = "Parsed!";
			}
			when (/^(M\d+|\d+M|\d+M|=\d+M|=M\d+|M\d+-M\d+|>=M\d+|>M\d+|<=M\d+|<M\d+|\d+M-\d+M|>=\d+M|>\d+M|<=\d+M|<\d+M)$/i) {
				my $get_string = $self->parseMonthString($1);
				if ($fp_month_counter == 1) {
					$fp_month_string = $fp_month_string.$get_string;
				} else {
					$fp_month_string = $fp_month_string." OR ".$get_string;
				}
				$fp_month_counter++;
				$error_key = $item;
				$error_string = "Parsed!";
			}
			when (/^(W\d+|\d+W|\d+W|=\d+W|=W\d+|W\d+-W\d+|>=W\d+|>W\d+|<=W\d+|<W\d+|\d+W-\d+W|>=\d+W|>\d+W|<=\d+W|<\d+W)$/i) {
				my $get_string = $self->parseWeekString($1);
				if ($fp_week_counter == 1) {
					$fp_week_string = $fp_week_string.$get_string;
				} else {
					$fp_week_string = $fp_week_string." OR ".$get_string;
				}
				$fp_week_counter++;
				$error_key = $item;
				$error_string = "Parsed!";
			}
			when (/(.*?)/i) {
				my $does_exist = $self->validateSL6($1);
				if ($does_exist) {
					if ($sl6_counter == 1) {
						$sl6_string = $sl6_string."sales_level_6 = "."'".uc $item."'";
					} else {
						$sl6_string = $sl6_string." OR "."sales_level_6 = "."'".uc $item."'";
					}
					$sl6_counter++;
					$error_key = $item;
					$error_string = "Parsed!";
				} else {
					$error_key = $item;
					$error_string = "is NOT either a valid Region Name, GTMu name or Sales_Level_6 ID!";
				}
			}
			default {
				$error_key = $item;
				$error_string = "Either the String is EMPTY or Syntax ERROR!";
			}
		}
		$is_there_error{$error_key} = $error_string;
	}
	if ($region_string eq "" && $eu_string eq "" && $sl6_string eq "") {
		$sub_level_string = "";
		return (1, \%is_there_error, $sub_level_string);
	} else {
		if ($region_string ne "") {
			$region_string = "(".$region_string.")";
			$sub_level_string = $sub_level_string."(".$region_string;
		}
		if ($eu_string ne "") {
			$eu_string = "(".$eu_string.")";
			if ($sub_level_string ne "") {
				$sub_level_string = "(".$sub_level_string." AND ".$eu_string;
			} else {
				$sub_level_string = $sub_level_string.$eu_string;
			}
		}
		if ($sub_scms_string ne "") {
			$sub_scms_string = "(".$sub_scms_string.")";
			if ($sub_level_string ne "") {
				$sub_level_string = $sub_level_string." AND ".$sub_scms_string;
			} else {
				$sub_level_string = $sub_level_string.$sub_scms_string;
			}
		}
		if ($fp_year_string ne "") {
			$fp_year_string = "(".$fp_year_string.")";
			if ($sub_level_string ne "") {
				$sub_level_string = $sub_level_string." AND ".$fp_year_string;
			} else {
				$sub_level_string = $sub_level_string.$fp_year_string;
			}
		}
		if ($fp_quarter_string ne "") {
			$fp_quarter_string = "(".$fp_quarter_string.")";
			if ($sub_level_string ne "") {
				$sub_level_string = $sub_level_string." AND ".$fp_quarter_string;
			} else {
				$sub_level_string = $sub_level_string.$fp_quarter_string;
			}
		}
		if ($fp_month_string ne "") {
			$fp_month_string = "(".$fp_month_string.")";
			if ($sub_level_string ne "") {
				$sub_level_string = $sub_level_string." AND ".$fp_month_string;
			} else {
				$sub_level_string = $sub_level_string.$fp_month_string;
			}
		}
		if ($fp_week_string ne "") {
			$fp_week_string = "(".$fp_week_string.")";
			if ($sub_level_string ne "") {
				$sub_level_string = $sub_level_string." AND ".$fp_week_string;
			} else {
				$sub_level_string = $sub_level_string.$fp_week_string;
			}
		}
		if ($sl6_string ne "") {
			$sl6_string = "(".$sl6_string.")";
			if ($sub_level_string ne "") {
				if ($sub_level_string =~ /^\(\(/i) {
					$sub_level_string = $sub_level_string." AND ".$sl6_string.")";
				} else {
					$sub_level_string = $sub_level_string." AND ".$sl6_string;
				}
			} else {
				if ($sub_level_string =~ /^\(\(/i) {
					$sub_level_string = $sub_level_string.$sl6_string.")";
				}
			}
		} else {
			if ($sub_level_string ne "") {
				if ($sub_level_string =~ /^\(\(/i) {
					$sub_level_string = $sub_level_string.")";
				}
			}
		}
		return (0, \%is_there_error, $sub_level_string);
	}
}

sub getRegionsForEU {
	my ($self, $string) = @_;
	my $result_string = "";
	given ($string) {
		when (/^EU1$/i) {
			$result_string = "region = 'SOUTH'";
		}
		when (/^EU2$/i) {
			$result_string = "region = 'WEST'";
		}
		when (/^EU3$/i) {
			$result_string = "region = 'EAST' OR region = 'NORTH' OR region = 'SAARC'";
		}
		when (/^COMM$/i) {
			$result_string = "region = 'SOUTH' OR region = 'WEST' OR region = 'EAST' OR region = 'NORTH' OR region = 'SAARC'";
		}
	}
	return $result_string
}

sub validateSL6 {
	my ($self, $string) = @_;
		my $qq_string = "SELECT DISTINCT sales_level_6 FROM booking_dump GROUP BY sales_level_6";
		my $query_string = qq{$qq_string};
		$self->prepareDBIConnection();
		$self->setQueryString($query_string);
		$self->prepareSTH();
		my $sth = $self->getSTH();
		my $total_rows_in_sth = ($sth->rows)+1;
		print "$total_rows_in_sth row(s) returned!\n";
		my $a = $sth->fetchrow_hashref();
		my @cols = $a->{"sales_level_6"};
		$sth->finish;
		if (grep {$_ eq $string} @cols) {
			print "$string Exists in booking_dump table!\n";
			return 1;
		} else {
			print "$string does NOT Exist in booking_dump table!\n";
			return 0;
		}
}

sub parseYearString {
	my ($self, $period) = @_;
	my $period_string = "";
	given ($period) {
		when (/^(\d\d\d\d)$/i) {
			$period_string = "fp_year = $period";
		}
		when (/^(\d\d\d\d)-(\d\d\d\d)$/i) {
			$period_string = "fp_year >= $1 AND fp_year <= $2";
		}
		when (/^>=(\d\d\d\d)$/i) {
			$period_string = "fp_year >= $1";
		}
		when (/^>(\d\d\d\d)$/i) {
			$period_string = "fp_year > $1";
		}
		when (/^=(\d\d\d\d)$/i) {
			$period_string = "fp_year = $1";
		}
		when (/^<=(\d\d\d\d)$/i) {
			$period_string = "fp_year <= $1";
		}
		when (/^<(\d\d\d\d)$/i) {
			$period_string = "fp_year < $1";
		}
		when (/^FY(\d\d)$/i) {
			my $year = int("20".$1);
			$period_string = "fp_year = $year";
		}
		when (/^FY(\d\d)-FY(\d\d)$/i) {
			my $f_year = int("20".$1);
			my $t_year = int("20".$2);
			$period_string = "fp_year >= $f_year AND fp_year <= $t_year";
		}
		when (/^FY(\d\d)-(\d\d)$/i) {
			my $f_year = int("20".$1);
			my $t_year = int("20".$2);
			$period_string = "fp_year >= $f_year AND fp_year <= $t_year";
		}
		when (/^>=FY(\d\d)$/i) {
			my $year = int("20".$1);
			$period_string = "fp_year >= $year";
		}
		when (/^>FY(\d\d)$/i) {
			my $year = int("20".$1);
			$period_string = "fp_year > $year";
		}
		when (/^=FY(\d\d)$/i) {
			my $year = int("20".$1);
			$period_string = "fp_year = $year";
		}
		when (/^<=FY(\d\d)$/i) {
			my $year = int("20".$1);
			$period_string = "fp_year <= $year";
		}
		when (/^<FY(\d\d)$/i) {
			my $year = int("20".$1);
			$period_string = "fp_year < $year";
		}
	}
	return $period_string;
}

sub parseQuarterString {
	my ($self, $period) = @_;
	my $period_string = "";
	given ($period) {
		when (/^Q(\d)$/i) {
			$period_string = "fp_quarter = 'Q$1'";
		}
		when (/^(\d)Q$/i) {
			$period_string = "fp_quarter = 'Q$1'";
		}
		when (/^(\d)Q$/i) {
			$period_string = "fp_quarter = 'Q$1'";
		}
		when (/^=(\d)Q$/i) {
			$period_string = "fp_quarter = 'Q$1'";
		}
		when (/^=Q(\d)$/i) {
			$period_string = "fp_quarter = 'Q$1'";
		}
		when (/^Q(\d)-Q(\d)$/i) {
			$period_string = "fp_quarter >= 'Q$1' AND fp_quarter <= 'Q$2'";
		}
		when (/^(\d)Q-(\d)Q$/i) {
			$period_string = "fp_quarter >= 'Q$1' AND fp_quarter <= 'Q$2'";
		}
		when (/^>=Q(\d)$/i) {
			$period_string = "fp_quarter >= 'Q$1'";
		}
		when (/^>=(\d)Q$/i) {
			$period_string = "fp_quarter >= 'Q$1'";
		}
		when (/^>Q(\d)$/i) {
			$period_string = "fp_quarter > 'Q$1'";
		}
		when (/^>(\d)Q$/i) {
			$period_string = "fp_quarter > 'Q$1'";
		}
		when (/^<=Q(\d)$/i) {
			$period_string = "fp_quarter <= 'Q$1'";
		}
		when (/^<=(\d)Q$/i) {
			$period_string = "fp_quarter <= 'Q$1'";
		}
		when (/^<Q(\d)$/i) {
			$period_string = "fp_quarter < 'Q$1'";
		}
		when (/^<(\d)Q$/i) {
			$period_string = "fp_quarter < 'Q$1'";
		}
	}
	return $period_string;
}

sub parseMonthString {
	my ($self, $period) = @_;
	my $period_string = "";
	given ($period) {
		when (/^M(\d+)$/i) {
			$period_string = "fp_month = $1";
		}
		when (/^(\d+)M$/i) {
			$period_string = "fp_month = $1";
		}
		when (/^(\d+)M$/i) {
			$period_string = "fp_month = $1";
		}
		when (/^=(\d+)M$/i) {
			$period_string = "fp_month = $1";
		}
		when (/^=M(\d+)$/i) {
			$period_string = "fp_month = $1";
		}
		when (/^M(\d+)-M(\d+)$/i) {
			$period_string = "fp_month >= $1 AND fp_month <= $2";
		}
		when (/^(\d+)M-(\d+)M$/i) {
			$period_string = "fp_month >= $1 AND fp_month <= $2";
		}
		when (/^>=M(\d+)$/i) {
			$period_string = "fp_month >= $1";
		}
		when (/^>=(\d+)M$/i) {
			$period_string = "fp_month >= $1";
		}
		when (/^>M(\d+)$/i) {
			$period_string = "fp_month > $1";
		}
		when (/^>(\d+)M$/i) {
			$period_string = "fp_month > $1";
		}
		when (/^<=M(\d+)$/i) {
			$period_string = "fp_month <= $1";
		}
		when (/^<=(\d+)M$/i) {
			$period_string = "fp_month <= $1";
		}
		when (/^<M(\d+)$/i) {
			$period_string = "fp_month < $1";
		}
		when (/^<(\d+)M$/i) {
			$period_string = "fp_month < $1";
		}
	}
	return $period_string;
}

sub parseWeekString {
	my ($self, $period) = @_;
	my $period_string = "";
	given ($period) {
		when (/^W(\d+)$/i) {
			$period_string = "fp_week = $1";
		}
		when (/^(\d+)W$/i) {
			$period_string = "fp_week = $1";
		}
		when (/^(\d+)W$/i) {
			$period_string = "fp_week = $1";
		}
		when (/^=(\d+)W$/i) {
			$period_string = "fp_week = $1";
		}
		when (/^=W(\d+)$/i) {
			$period_string = "fp_week = $1";
		}
		when (/^W(\d+)-W(\d+)$/i) {
			$period_string = "fp_week >= $1 AND fp_week <= $2";
		}
		when (/^(\d+)W-(\d+)W$/i) {
			$period_string = "fp_week >= $1 AND fp_week <= $2";
		}
		when (/^>=W(\d+)$/i) {
			$period_string = "fp_week >= $1";
		}
		when (/^>=(\d+)W$/i) {
			$period_string = "fp_week >= $1";
		}
		when (/^>W(\d+)$/i) {
			$period_string = "fp_week > $1";
		}
		when (/^>(\d+)W$/i) {
			$period_string = "fp_week > $1";
		}
		when (/^<=W(\d+)$/i) {
			$period_string = "fp_week <= $1";
		}
		when (/^<=(\d+)W$/i) {
			$period_string = "fp_week <= $1";
		}
		when (/^<W(\d+)$/i) {
			$period_string = "fp_week < $1";
		}
		when (/^<(\d+)W$/i) {
			$period_string = "fp_week < $1";
		}
	}
	return $period_string;
}


1;
