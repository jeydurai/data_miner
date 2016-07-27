package modules::reports::rpt;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
use modules::reports::report_maker;
use Spreadsheet::WriteExcel::Utility;
use modules::helpers::connections::mysql_connection;
use Scalar::Util qw(looks_like_number);
use modules::validators::input_validator;
our @ISA = qw(modules::reports::report_utility);

sub new {
	my $class = shift;
	my $self = {};
	bless $self, $class;
	return $self;
}

sub  getXLPivot{
	my ($self, $option) = @_;
	my $obj = modules::reports::report_maker->new();
	$obj->prepareDBIConnection();
	my $dbh = $obj->{_dbiConnection};
	my @table_list = $dbh->tables(); # get all the table names available in an array
	my $hash_key = 1;
	my %table_hash = ();
	my $table_name_length = 0;
	my $mysql_table_name = undef;
	my $hash_key_text = undef;
	foreach my $value (@table_list) { # Transfer tables names from array to hash
		$table_hash{$hash_key} = $value;
		$hash_key++;
		my $hash_key_text = ("" x (3-length $hash_key)).$hash_key;
		if ($table_name_length < length($hash_key_text."=>".$value)) { # find out the maximum text length for the printing tab width
			$table_name_length = length ($hash_key_text."=>".$value);
		}
	}

	print "MySQL Table Name: ";
	while ($mysql_table_name = <STDIN>) { # prompt to choose table name or mention it or list and then mention
		chomp $mysql_table_name;
		if ($mysql_table_name =~ /^list$/) {
			$hash_key = 1;
			foreach my $value (@table_list) {
				$hash_key_text = ("" x (3-length $hash_key)).$hash_key;
				if ($hash_key % 2 == 0) {
					print $hash_key_text."=>".$value, "\n";
				} else {
					print $hash_key_text."=>".$value, " " x ($table_name_length-length ($value)-length ($hash_key_text)); 
				}
				$hash_key++;
			}
			print "\nMySQL Table Name: ";
		} else {
			last;
		}
	};

	if (looks_like_number($mysql_table_name)) {
		$mysql_table_name = $table_hash{$mysql_table_name};
	}
	
	# Display chosen table's 'Filed names
	if (grep {($_ eq "`mysourcedata`.`".$mysql_table_name."`") || ($_ eq $mysql_table_name)} @table_list) { # check if the chosen or mentioned table name does exist in the database
		my $qq_string = "SELECT * FROM $mysql_table_name WHERE 1=0";
		my $query_string = qq{$qq_string};
		$obj->setQueryString($query_string);
		$obj->prepareSTH();
		my $sth = $obj->getSTH();
		my @cols = @{$sth->{NAME_lc}};
		$sth->finish;
		$hash_key = 1;
		my %field_hash = ();
		my $field_name_length = 0;
		foreach my $value (@cols) {
			$field_hash{$hash_key} = $value;
			$hash_key++;
			my $hash_key_text = ("" x (3-length $hash_key)).$hash_key;
			if ($field_name_length < length($hash_key_text."=>".$value)) {
				$field_name_length = length ($hash_key_text."=>".$value);
			}
		}
		$hash_key = 1;
		foreach my $value (@cols) {
			$hash_key_text = ("" x (3-length $hash_key)).$hash_key;
			if ($hash_key % 2 == 0) {
				print $hash_key_text."=>".$value, "\n";
			} else {
				print $hash_key_text."=>".$value, " " x ($field_name_length-length ($value)-length ($hash_key_text)); 
			}
			$hash_key++;
		}
		
		# Get user input for field selection
		#====================================
		print "\n\nChoose the field names in '$mysql_table_name':\n";
		my %field_names = ();
		my $counter = 1;
		my $validator = modules::validators::input_validator->new();
		print "Field-$counter: ";
		my $input_cleaned = undef;
		my $table_field = undef;
		my $actual_field = undef;
		my $user_field = undef;
		my @field_array = ();
		my @user_field_array = ();
		my $array_index = 0;
		while (my $input = <STDIN>) {
			chomp($input);
			my $decide = $validator->validateFieldProperty($input);
			
			if ($decide == 0) {
				last;
			} else {
				$table_field = $validator->{_tableField};
				$actual_field = $validator->{_actualField};
				$user_field = $validator->{_userField};
				
				if ($actual_field eq '') {
					last;
				} elsif (grep {$_ eq $table_field} @cols) {
					if ($actual_field) {
						$field_names{$actual_field} = $user_field;
						$field_array[$array_index] = $actual_field;
						$user_field_array[$array_index] = $user_field;
						$array_index++;
						print "Hash: ", $actual_field, " => ", $user_field, "\n";
					}
				} elsif ($actual_field =~ /^remove$/i) {
						print "\nType in the field names to be removed:\n";
						while (my $temp_input = <STDIN>) {
							chomp ($temp_input);
							if ($temp_input) {
								my @matched_keys = grep {$field_names{$_} eq $temp_input} keys %field_names;
								@user_field_array = grep {$field_names{$_} ne $temp_input} @matched_keys;
								foreach my $key (@matched_keys) {
									delete $field_names{$key};
								}
								my $index = 0;
								foreach my $key (sort keys %field_names) {
									$field_array[$index] = $key;
									$user_field_array[$index] = $field_names{$key};
									$index++;
								}
							} else {
								last;
							}
						}
				} else {
					print "\n$table_field field does not exit in '$mysql_table_name'!\n";
					print "Field-$counter: ";
					next;
				}
			}
			
			$counter++;
			print "Field-$counter: ";
		}
		my $size = keys %field_names;
		print "$size field names have been chosen!\n";
		foreach my $key (sort keys %field_names) {
			if ($field_names{$key}) {
				print $field_names{$key}, "\n";
			}
		}
		my $field_text = join(', ', @field_array);
		print "Field Text => ", $field_text, "\n"; #Field names for the query
		# ==================================================================================


		# Get user input for GROUP BY field selection
		# ===========================================
		print "\n\nChoose the field names in '$mysql_table_name' for GROUP BY:\n";
		my @field_names_groupby = ();
		$counter = 1;
		print "Field-$counter: ";
		$input_cleaned = undef;
		while (my $input = <STDIN>) {
			chomp($input);
			if ($input eq '') {
				last;
			} elsif (grep {$_ eq $input} @cols) {
				if ($input) {
					push @field_names_groupby, $input;
				}
			} elsif ($input =~ /^remove$/i) {
					print "\nType in the field names to be removed:\n";
					print "Field to be removed: ";
					while (my $temp_input = <STDIN>) {
						chomp ($temp_input);
						if ($temp_input) {
							@field_names_groupby = grep {$_ ne $temp_input} @field_names_groupby;
						} else {
							print "\n$temp_input field does not exit in '$mysql_table_name'!\n";
							print "Field to be removed: ";
							last;
						}
					}
			} else {
				print "\n$input field does not exit in '$mysql_table_name'!\n";
				print "Field-$counter: ";
				next;
			}
			$counter++;
			print "Field-$counter: ";
		}
		
		print $#field_names_groupby+1, " field names have been chosen for GROUP BY!\n";
		foreach my $value (@field_names_groupby) {
			if ($value) {
				print $value, "\n";
			}
		}
		my $groupby_text = join(', ', @field_names_groupby);
		$groupby_text = "GROUP BY ".$groupby_text;
		print "'GROUP BY' Text => ", $groupby_text, "\n"; #Field names for the query
		# ====================================================================================
		
		# Get user input for QUERY CONDITIONS
		# =============================
		print "\n\nKey in PATTERN for QUERY CONDITIONS:\n";
		my $condition_string = "";
		my %string_condi = ();
		my $key_string_condi = 1;
		print "=> ";
		while (my $input = <STDIN>) {
			chomp($input);
			
			if ($input eq '') {
				last;
			} elsif ($input =~ m{^(\(|\s+\(|\s+\(\(|\(\(|\s+\(\(|\s+AND\s+\(|\s+OR\s+\(|\)\s+AND\s+\(|\)\s+OR\s+\()+(.*)\s+(.*)\s+\?(\)|\)\))+$}i) {
				print "Q1: $1\n";
				print "Q2: $2\n";
				print "Q3: $3\n";
				my $temp_field_string = $2;
				my $operator = $3;
				if ($operator =~ /^(=|!=|>=|<=|LIKE|>|<)$/i) {
					print "$operator is a VALID SQL operator!\n";
				} else {
					print "$operator is a VALID SQL operator!\n";
					print "=>";
					next;
				}
				if (grep{$_ eq $temp_field_string} @cols) {
					$condition_string = $condition_string.$input;
					$string_condi{$key_string_condi} = $input;
					$key_string_condi++;
				} else {
					print "$temp_field_string does not exist in table!\n";
					print "=> ";
					next;
				}
				
			} else {
				print "\n$input is NOT a valid query condition Syntax!\n";
				print "=> ";
				next;
			}
			print "Query Condition PATTERN => ", $condition_string, "\n"; #Condition for the query
			print "=> ";
		}
		
		my $recur_open_paren = () = $condition_string =~ /\(/g;
		my $recur_close_paren = () = $condition_string =~ /\)/g;
		print "\n", "=" x 50, "\n";
		print "Final Query Condition PATTERN => ", $condition_string, "\n"; #Condition for the query
		print "Final Query Condition PATTERN in Sequence:\n";
		print "==========================================\n";
		foreach my $key (sort keys %string_condi) {
			print $key, "=>", $string_condi{$key}, "\n";
		}
		print "==========================================\n";
		print "Number of Opening Parenthesis => $recur_open_paren\n";
		print "Number of Closing Parenthesis => $recur_close_paren\n";
		print "==========================================\n";
		if ($recur_open_paren != $recur_close_paren) {
			print "Number of Opening Parentheses do NOT match with Closing Parentheses!\n\n"
		} else {
			print "Parentheses Syntax are Correct!\n\n"
		}
		print "You are allowed to correct CONDITION PATTERN, if you like to...\n";
		print "Key in the SEQUENCE NUMBER of condition pattern (or type zero to Proceed...):\n";
		print "=============================================================================\n";
		print "SEQUENCE NUMBER=> ";
		my $loop_terminator = 1;
	
		while ($loop_terminator) {
			while (my $which_one = <STDIN>) {
				chomp ($which_one);
				if (!looks_like_number($which_one)) {
					print "SEQUENCE NUMBER should be NUMBERIC [0 to exit]!\n";
					print "=> ";
					next;
				} elsif ($which_one != 0) {
					print "You wanted to edit '$string_condi{$which_one}'\n";
					print "Go ahead and replace $string_condi{$which_one}...\n";
					print "=> ";
					while (my $input = <STDIN>) {
						chomp($input);
							
						if ($input eq '') {
							last;
						} elsif ($input =~ m{^(\(|\s+\(|\s+\(\(|\(\(|\s+\(\(|\s+AND\s+\(|\s+OR\s+\(|\)\s+AND\s+\(|\)\s+OR\s+\()+(.*)\s+(.*)\s+\?(\)|\)\))+$}i) {
							print "Q1: $1\n";
							print "Q2: $2\n";
							print "Q3: $3\n";
							my $temp_field_string = $2;
							my $operator = $3;
							if ($operator =~ /^(=|!=|>=|<=|LIKE|>|<)$/i) {
								print "$operator is a VALID SQL operator!\n";
							} else {
								print "$operator is a NOT VALID SQL operator!\n";
								print "=>";
								next;
							}
							if (grep{$_ eq $temp_field_string} @cols) {
								$string_condi{$which_one} = $input;
							} else {
								print "$temp_field_string does not exist in table!\n";
								print "=> ";
								next;
							}
							
						} else {
							print "\n$input is NOT a valid query condition Syntax!\n";
							print "=> ";
							next;
						}
						$condition_string = "";
						foreach my $key (sort keys %string_condi) {
							$condition_string = $condition_string.$string_condi{$key};
						}
						print "Query Condition PATTERN => ", $condition_string, "\n"; #Condition for the query
						print "=> ";
					}
				} else {
					$loop_terminator = 0;
					last;
				}
				$recur_open_paren = () = $condition_string =~ /\(/g;
				$recur_close_paren = () = $condition_string =~ /\)/g;
				print "\n", "=" x 50, "\n";
				print "Final Query Condition PATTERN => ", $condition_string, "\n"; #Condition for the query
				print "Final Query Condition PATTERN in Sequence:\n";
				print "==========================================\n";
				foreach my $key (sort keys %string_condi) {
					print $key, "=>", $string_condi{$key}, "\n";
				}
				print "==========================================\n";
				print "Number of Opening Parenthesis => $recur_open_paren\n";
				print "Number of Closing Parenthesis => $recur_close_paren\n";
				print "==========================================\n";
				if ($recur_open_paren != $recur_close_paren) {
					print "Number of Opening Parentheses do NOT match with Closing Parentheses!\n\n";
					$loop_terminator = 1;
				} else {
					print "Parenthesis Syntax is Correct!\n\n";
					$loop_terminator = 0;
				}
				print "You are allowed to correct CONDITION PATTERN, if you like to...\n";
				print "Key in the SEQUENCE NUMBER of condition pattern (or type zero to Proceed...):\n";
				print "=============================================================================\n";
				print "=> ";
			}
			if ($loop_terminator == 1) {
				print "Value of Loop Terminater is $loop_terminator\n";
				print "==========================================\n";
				print "Number of Opening Parenthesis => $recur_open_paren\n";
				print "Number of Closing Parenthesis => $recur_close_paren\n";
				print "==========================================\n";
				print "Parentheses Syntax errors in Condition String IS NOT ALLOWED!\n";
			} else {
				print "Your Condition Strings have been validated successfully!\n";
			}
		}
		# =============================
		# Get user input for Parameters
		# =============================
		my $where_clause = "";
		if ($condition_string) {
			$where_clause = "WHERE ".$condition_string;
		}
		my $number_of_params = () = $condition_string =~ /\?/g;
		print "There are $number_of_params parameters to be given for the Query\n";
		my %params = ();
		$loop_terminator = 1;
		while ($loop_terminator <= $number_of_params) {
			print "Parameter - $loop_terminator: ";
			my $inp = <STDIN>;
			chomp $inp;
			if ($inp) {
				$params{$loop_terminator} = $inp;
				$loop_terminator++;
			} else {
				print "Parameter cannot be empty!\n";
			}
		}
		foreach my $key (sort keys %params) {
			print "$key => $params{$key}\n";
		}
		# ====================================================================================
		# Get Pivot Field Input
		# =========================================================================
		my %pivot_fields = ();
		my %pivot_field_orientation = ();
		my %pivot_field_position = ();
		my %pivot_datafield_function = ();
		$validator = Validator::INPUT_VALIDATOR->new(\@user_field_array);
		$counter = 1;
		print "Pivot Field Configuration(Field|Field Orientaion|Field Position): ";
		while (my $input = <STDIN>) {
			chomp($input);
			my $decide = $validator->getPivotProperty($input);
			
			if ($decide == 0) {
				foreach my $key (keys %{$validator->{_isPivotFieldOk}}) {
					print "${$validator->{_isPivotFieldOk}}{$key}\n";
				}
				foreach my $key (keys %{$validator->{_isPivotOrientationOk}}) {
					print "${$validator->{_isPivotOrientationOk}}{$key}\n";
				}
				foreach my $key (keys %{$validator->{_isPivotPositionOk}}) {
					print "${$validator->{_isPivotPositionOk}}{$key}\n";
				}
				last;
			}
			foreach my $key (keys %{$validator->{_isPivotFieldOk}}) {
				print "${$validator->{_isPivotFieldOk}}{$key}\n";
			}
			foreach my $key (keys %{$validator->{_isPivotOrientationOk}}) {
				print "${$validator->{_isPivotOrientationOk}}{$key}\n";
			}
			foreach my $key (keys %{$validator->{_isPivotPositionOk}}) {
				print "${$validator->{_isPivotPositionOk}}{$key}\n";
			}
			$pivot_fields{$counter} = $validator->{_pivotField};
			$pivot_field_orientation{$counter} = $validator->{_pivotFieldOrientation};
			$pivot_field_position{$counter} = $validator->{_pivotFieldPosition};
			$pivot_datafield_function{$counter} = $validator->{_pivotDataFieldFunction};
			print "Pivot Field Configuration(Field|Field Orientaion|Field Position) $counter: ";
			$counter++;
		}
		# Pivot Preparation
		# =================
		foreach my $key (keys %pivot_fields) {
			print "Pivot Fields $key => $pivot_fields{$key}\n";
		}
		foreach my $key (keys %pivot_field_orientation) {
			print "Pivot Orientation $key => $pivot_field_orientation{$key}\n";
		}
		foreach my $key (keys %pivot_field_position) {
			print "Pivot Position $key => $pivot_field_position{$key}\n";
		}
		foreach my $key (keys %pivot_datafield_function) {
			if ($pivot_datafield_function{$key}) {
				print "Pivot Position $key => $pivot_datafield_function{$key}\n";
			} else {
				print "Pivot Position $key => Null\n";
			}
		}
		
		$obj->getExcelPivotReport($mysql_table_name, $field_text, $where_clause, 
								  $groupby_text, \%params, \%field_names, \@user_field_array, 
								  \%pivot_fields, \%pivot_field_orientation, 
								  \%pivot_field_position, \%pivot_datafield_function, $option);
	} else {
		print "\nEither table name not defined or table does not exist!\n"
	}
	$dbh->disconnect();
	$obj = undef;
}

sub emailXLPivot {
	my ($self, $to_array, $cc_array, $bcc_array, $sub_level) = @_;
	my $option = 0;
	my @pivot_input = ();
	my $obj = modules::reports::report_maker->new();
	$obj->prepareDBIConnection();
	my $dbh = $obj->{_dbiConnection};
	my $mysql_table_name = undef;

	$mysql_table_name = "booking_dump";
		
	# Get All fields
	#====================================
	my %field_names = ();
	my $input_cleaned = undef;
	my $table_field = undef;
	my $actual_field = undef;
	my $user_field = undef;
	my $qq_string = "SELECT * FROM $mysql_table_name WHERE 1=0";
	my $query_string = qq{$qq_string};
	$obj->setQueryString($query_string);
	$obj->prepareSTH();
	my $sth = $obj->getSTH();
	my @field_array = @{$sth->{NAME_lc}};
	$sth->finish;
	my @user_field_array = @field_array;
	$field_names{$actual_field} = $user_field;
		my $size = keys %field_names;
		print "$size field names have been chosen!\n";
		foreach my $key (sort keys %field_names) {
			if ($field_names{$key}) {
				print $field_names{$key}, "\n";
			}
		}
	my $field_text = join(', ', @field_array);
	print "Field Text => ", $field_text, "\n"; #Field names for the query
	# ==================================================================================
	my $validator = modules::validators::input_validator->new(\@user_field_array);


	# GROUP BY field selection
	# ===========================================
	my $groupby_text = "";
	
	# Get user input for QUERY CONDITIONS
	# =============================
	my $condition_string = $sub_level;
	
	# =============================
	# Get user input for Parameters
	# =============================
	my $where_clause = "WHERE ".$condition_string;
	my %params = ();
	# ====================================================================================
	# Get Pivot Field Preparation
	# =========================================================================
	
	$pivot_input[0] = "fp_quarter|page|1";
	$pivot_input[1] = "fp_month|page|2";
	$pivot_input[2] = "fp_week|page|3";
	$pivot_input[3] = "sub_scms|page|4";
	$pivot_input[4] = "partner_name|page|5";
	$pivot_input[5] = "customer_name|page|6";
	$pivot_input[6] = "sales_level_6|page|7";
	$pivot_input[7] = "region|page|8";
	$pivot_input[8] = "mapped_type|row|1";
	$pivot_input[9] = "tbm|row|2";
	$pivot_input[10] = "arch2|col|1";
	$pivot_input[11] = "sum(booking_net)|data|1";
	
	my %pivot_fields = ();
	my %pivot_field_orientation = ();
	my %pivot_field_position = ();
	my %pivot_datafield_function = ();
	my $counter = 1;
	print "Pivot Field Configuration is under parsing...\n";
	foreach my $input (@pivot_input) {
		my $decide = $validator->getPivotProperty($input);
		
		if ($decide == 0) {
			foreach my $key (keys %{$validator->{_isPivotFieldOk}}) {
				print "${$validator->{_isPivotFieldOk}}{$key}\n";
			}
			foreach my $key (keys %{$validator->{_isPivotOrientationOk}}) {
				print "${$validator->{_isPivotOrientationOk}}{$key}\n";
			}
			foreach my $key (keys %{$validator->{_isPivotPositionOk}}) {
				print "${$validator->{_isPivotPositionOk}}{$key}\n";
			}
			last;
		}
		foreach my $key (keys %{$validator->{_isPivotFieldOk}}) {
			print "${$validator->{_isPivotFieldOk}}{$key}\n";
		}
		foreach my $key (keys %{$validator->{_isPivotOrientationOk}}) {
			print "${$validator->{_isPivotOrientationOk}}{$key}\n";
		}
		foreach my $key (keys %{$validator->{_isPivotPositionOk}}) {
			print "${$validator->{_isPivotPositionOk}}{$key}\n";
		}
		$pivot_fields{$counter} = $validator->{_pivotField};
		$pivot_field_orientation{$counter} = $validator->{_pivotFieldOrientation};
		$pivot_field_position{$counter} = $validator->{_pivotFieldPosition};
		$pivot_datafield_function{$counter} = $validator->{_pivotDataFieldFunction};
		$counter++;
	}
	# Pivot Preparation
	# =================
		
	$obj->getExcelPivotReport($mysql_table_name, $field_text, $where_clause, 
							  $groupby_text, \%params, \%field_names, \@user_field_array, 
							  \%pivot_fields, \%pivot_field_orientation, 
							  \%pivot_field_position, \%pivot_datafield_function, $option);
	$dbh->disconnect();
}

sub getPerformanceReport {
	
	# Receive all Parameters
	print "Receiving Arguments...\n";
	my $self = shift;
	my $gtmu_as_param = shift; # Receive what EU the report is needed for
	my $emailID = shift; # Receive whom to send this in an email to
	print "Arguments have been assigned into lexical variables!\n";
	
	# Other variables' declaration
	my $obj = modules::reports::report_maker->new();
	my $dbh = $obj->getMySQLDBH();
	my $qq_string;
	my $query_string;
	my %param_hash;
	my @technologies;
	my @archs;
	my @verticals;
	my @period = qw(Q1 Q2 Q3 Q4 H1 H2 YTD);
	my @field_header_array;
	my $sth;
	my @empty_array = ();

	# Dropping latest_year_booking_dump table if exists
	print "Dropping table latest_year_booking_dump...\n";
	$qq_string = "DROP TABLE IF EXISTS latest_year_booking_dump";
	$query_string = qq{$qq_string};
	$sth = $obj->getSimpleSTH($dbh, $query_string, @empty_array);
	$sth->finish;
	print "Table latest_year_booking_dump has been dropped successfully!\n";
	
	# Create a table to copy all latest years data from booking_dump table
	print "Creating a new Table...\n";
	$qq_string = "CREATE TABLE  IF NOT EXISTS latest_year_booking_dump LIKE booking_dump_template";
	$query_string = qq{$qq_string};
	$sth = $obj->getSimpleSTH($dbh, $query_string, @empty_array);
	$sth->finish;
	print "New Table successfully created!\n";

	# Copy all latest years data into latest_year_booking_dump from booking_dump table
	print "Copying all latest years data in to latest years Table...\n";
	$qq_string = "INSERT INTO latest_year_booking_dump SELECT * FROM booking_dump WHERE fp_year=2015 AND prod_ser='product'";
	$query_string = qq{$qq_string};
	$sth = $obj->getSimpleSTH($dbh, $query_string, @empty_array);
	$sth->finish;
	print "Latest years data has been copied successfully in to latest years Table!\n";

	
	# Prepare for fetching all fields of Goal Sheet in Array
	$qq_string = "SELECT * FROM goal_sheet WHERE 1=0";
	$query_string = qq{$qq_string};
	$sth = $obj->getSimpleSTH($dbh, $query_string, @empty_array);
	my @cols = @{$sth->{NAME_lc}};
	$sth->finish;
	
	my $last_col = pop @cols;
	print "Last Column $last_col is popped out!\n";

	#push all columns in the header array
	push @field_header_array, @cols;
		

	# Prepare for fetching all Technologies in Array
	print "Preparing for fetching all Technologies in Array...\n";
	$qq_string = "SELECT DISTINCT Tech_Name FROM latest_year_booking_dump";
	$query_string = qq{$qq_string};
	$sth = $obj->getSimpleSTH($dbh, $query_string, @empty_array);
	while (my $hash_ref_tech_name = $sth->fetchrow_hashref()) {
		push @technologies, $hash_ref_tech_name->{"Tech_Name"};
	}
	print "All Unique Technologies fetched and assigned into an array!\n";
	$sth->finish();
	# Prepare for fetching all Architectures in Array
	print "Preparing for fetching all Architectures in Array...\n";
	$qq_string = "SELECT DISTINCT arch2 FROM latest_year_booking_dump";
	$query_string = qq{$qq_string};
	$sth = $obj->getSimpleSTH($dbh, $query_string, @empty_array);
	while (my $hash_ref_tech_name = $sth->fetchrow_hashref()) {
		push @archs, $hash_ref_tech_name->{"arch2"};
	}
	print "All Unique Architectures fetched and assigned into an array!\n";
	$sth->finish();

	# Prepare for fetching all Verticals in Array
	print "Preparing for fetching all Verticals in Array...\n";
	$qq_string = "SELECT DISTINCT Vertical FROM latest_year_booking_dump";
	$query_string = qq{$qq_string};
	$sth = $obj->getSimpleSTH($dbh, $query_string, @empty_array);
	while (my $hash_ref_tech_name = $sth->fetchrow_hashref()) {
		push @verticals, $hash_ref_tech_name->{"Vertical"};
	}
	$sth->finish();
	print "All Unique Verticals fetched and assigned into an array!\n";

	# Prepare query strings
	if (uc($gtmu_as_param) eq 'ALL') {
		$qq_string = "SELECT * FROM goal_sheet";
	} else {
		$qq_string = "SELECT * FROM goal_sheet WHERE gtmu=?";
		$param_hash{1} = $gtmu_as_param; 
	}
	
	$query_string = qq{$qq_string};
	my $sth_goal_sheet = $obj->getSimpleSTH($dbh, $query_string, %param_hash);	
	
	my $total_num_of_recs = $sth_goal_sheet->rows;
	my $counter = 0;
	my $xl_app = $self->getXLApp();
	my $xl_book = $self->getXLBook($xl_app);
	
	$xl_book->Sheets->Add;
	$xl_book->Sheets->Add;
	$xl_book->Sheets->Add;
	$xl_book->Sheets->Add;
	$xl_book->Sheets->Add;
	$xl_book->Sheets->Add;
	my $q1_sheet = $self->getXLSheet($xl_book, 1);
	my $q2_sheet = $self->getXLSheet($xl_book, 2);
	my $q3_sheet = $self->getXLSheet($xl_book, 3);
	my $q4_sheet = $self->getXLSheet($xl_book, 4);
	my $h1_sheet = $self->getXLSheet($xl_book, 5);
	my $h2_sheet = $self->getXLSheet($xl_book, 6);
	my $ytd_sheet = $self->getXLSheet($xl_book, 7);
	
	push @field_header_array, "booking_all";
	push @field_header_array, "actual_vs_goal";
	push @field_header_array, "list_all";
	push @field_header_array, "discount_all";
	push @field_header_array, "billed_customers";
	push @field_header_array, "billed_partners";
	push @field_header_array, "yld per customer";
	push @field_header_array, "fy15_pos_runrate";
	push @field_header_array, "pos_runrate_on_total_booking";
	push @field_header_array, "fy14_pos_runrate";
	push @field_header_array, "yoy_pos_runrate";
	push @field_header_array, "fy15_pos_b2b";
	push @field_header_array, "pos_b2b_on_total_booking";
	push @field_header_array, "fy14_pos_b2b";
	push @field_header_array, "yoy_pos_b2b";
	
	foreach (@technologies) {push @field_header_array, "booking_".$_; push @field_header_array, "list_".$_; push @field_header_array, "discount_".$_;}
	foreach (@archs) {push @field_header_array, "booking_".$_; push @field_header_array, "list_".$_; push @field_header_array, "discount_".$_;}
	foreach (@verticals) {push @field_header_array, "booking_".$_; push @field_header_array, "list_".$_; push @field_header_array, "discount_".$_;}
	my $header_count = scalar(@field_header_array);
	push  my @full_header_range_array, [@field_header_array];
	my $last_row = 1;
	my $last_row_ref = xl_rowcol_to_cell($last_row-1, $header_count-1); # Excel Cell to Row-Col Conversion
	print "Last Row Reference: $last_row_ref | Header Count: $header_count\n";
	my $pivot_rng = $q1_sheet->Range("A1:$last_row_ref");
	$pivot_rng->{Value} = [@full_header_range_array];
	$pivot_rng = $q2_sheet->Range("A1:$last_row_ref");
	$pivot_rng->{Value} = [@full_header_range_array];
	$pivot_rng = $q3_sheet->Range("A1:$last_row_ref");
	$pivot_rng->{Value} = [@full_header_range_array];
	$pivot_rng = $q4_sheet->Range("A1:$last_row_ref");
	$pivot_rng->{Value} = [@full_header_range_array];
	$pivot_rng = $h1_sheet->Range("A1:$last_row_ref");
	$pivot_rng->{Value} = [@full_header_range_array];
	$pivot_rng = $h2_sheet->Range("A1:$last_row_ref");
	$pivot_rng->{Value} = [@full_header_range_array];
	$pivot_rng = $ytd_sheet->Range("A1:$last_row_ref");
	$pivot_rng->{Value} = [@full_header_range_array];

	
	my $row_count = 2;
	while (my $rec_hash_ref = $sth_goal_sheet->fetchrow_hashref()) { # Iterating through Goal Sheet table data hash reference
		# Variable declaration
		my $xl_sheet;
		my $goal = 0;
		my $col_count = 0;
		my @field_value_array;
		my @full_range_array;
	
		
		# Get all the fields from Goal Sheet in lexical variables
		my $mapped_sales_level_6 = $rec_hash_ref->{"sales_level_6"};
		my $id_l5 = $rec_hash_ref->{"id_l5"};
		my $goal_q1 = $rec_hash_ref->{"goal_q1"};
		my $goal_q2 = $rec_hash_ref->{"goal_q2"};
		my $goal_q3 = $rec_hash_ref->{"goal_q3"};
		my $goal_q4 = $rec_hash_ref->{"goal_q4"};
		my $goal_h1 = $rec_hash_ref->{"goal_h1"};
		my $goal_h2 = $rec_hash_ref->{"goal_h2"};
		my $goal_ytd = $rec_hash_ref->{"goal_ytd"};
		$counter++;
		
		$col_count = scalar(@cols);
		
		print "$counter) $mapped_sales_level_6 | $id_l5 data extraction is on...\n";
		print "===================================================================\n";
		
		my $period_counter = 0;
		foreach my $qtr (@period) {
			# Variable declaration and initialization
			my $booking_all = 0; my $base_list_all = 0; my $billed_customers = 0; my $billed_partners = 0;
			my $fy15_pos_runrate = 0; my $fy14_pos_runrate;
			my $fy15_pos_b2b = 0; my $fy14_pos_b2b = 0;
			foreach my $col (@cols) {
				push @field_value_array, $rec_hash_ref->{$col};
			}		

			# Parameters Preparation to match and fetch from latest_year_booking_dump table		
			%param_hash = ();
			$param_hash{1} = $mapped_sales_level_6;
			$param_hash{2} = $id_l5;

			print "$counter) $mapped_sales_level_6 | $id_l5 data extraction for $qtr is on...\n";
			my $qq_string_ext;
			# Preparing qq_string extension for the priod parameter
			if ($qtr eq 'Q1') {
				$qq_string_ext = " AND fp_quarter='Q1'";
				$xl_sheet = $q1_sheet;
				$goal = $goal_q1;
			} elsif ($qtr eq 'Q2') {
				$qq_string_ext = " AND fp_quarter='Q2'";
				$xl_sheet = $q2_sheet;
				$goal = $goal_q2;
			} elsif ($qtr eq 'Q3') {
				$qq_string_ext = " AND fp_quarter='Q3'";
				$xl_sheet = $q3_sheet;
				$goal = $goal_q3;
			} elsif ($qtr eq 'Q4') {
				$qq_string_ext = " AND fp_quarter='Q4'";
				$xl_sheet = $q4_sheet;
				$goal = $goal_q4;
			} elsif ($qtr eq 'H1') {
				$qq_string_ext = " AND (fp_quarter='Q1' OR fp_quarter ='Q2')";
				$xl_sheet = $h1_sheet;
				$goal = $goal_h1;
			} elsif ($qtr eq 'H2') {
				$qq_string_ext = " AND (fp_quarter='Q3' OR fp_quarter ='Q4')";
				$xl_sheet = $h2_sheet;
				$goal = $goal_h2;
			} else  {
				$qq_string_ext = "";
				$xl_sheet = $ytd_sheet;
				$goal = $goal_ytd;
			}
			
			# Fetching Booking, Base List & Discount Data for Overall
			# =======================================================
			print "Fetching Full Booking for $counter) $mapped_sales_level_6 | $id_l5 | $qtr...\n";
			$qq_string = "SELECT SUM(Booking_Net) as booking, SUM(Base_List) as Base_List, 
							COUNT(DISTINCT customer_name) as billed_customer, COUNT(DISTINCT partner_name) as billed_partner
							 FROM latest_year_booking_dump WHERE Mapped_Sales_Level_6=? AND Mapped_id=?" . $qq_string_ext;
			$query_string = qq{$qq_string};
			my $sth = $obj->getSimpleSTH($dbh, $query_string, %param_hash);
			while (my $rec_hash_ref_sub = $sth->fetchrow_hashref()) {
				$booking_all = $booking_all + $rec_hash_ref_sub->{"booking"} if ($rec_hash_ref_sub->{"booking"});
				$base_list_all = $base_list_all + $rec_hash_ref_sub->{"Base_List"} if ($rec_hash_ref_sub->{"Base_List"});
				$billed_customers = $billed_customers + $rec_hash_ref_sub->{"billed_customer"} if ($rec_hash_ref_sub->{"billed_customer"});
				$billed_partners = $billed_partners + $rec_hash_ref_sub->{"billed_partner"} if ($rec_hash_ref_sub->{"billed_partner"});
			}
			$sth->finish();
			# Booking for Overall
			push @field_value_array, $booking_all;
			# Calculating Actual vs. Goal
			if ($goal != 0) {push @field_value_array, ($booking_all/$goal);} else {push @field_value_array, "No Goal";}
			# Base List for Overall
			push @field_value_array, $base_list_all;
			# Calculating Discount for Overall
			if ($base_list_all != 0) {push @field_value_array, 1-($booking_all/$base_list_all);} else {push @field_value_array, 0;}
			# Billed Customers for Overall
			push @field_value_array, $billed_customers;
			# Billed Partners for Overall
			push @field_value_array, $billed_partners;
			# Calculating Yield per Customer
			if ($billed_customers != 0) {push @field_value_array, ($booking_all/$billed_customers);} else {push @field_value_array, "No Customers";}
			print "Full Booking for $counter) $mapped_sales_level_6 | $id_l5 | $qtr completed\n";


			# Fetching POS Booking Breakups
			# =======================================================
			print "Fetching POS Booking for $counter) $mapped_sales_level_6 | $id_l5 | $qtr...\n";
			$qq_string = "SELECT 
							SUM(CASE WHEN (`booking_dump`.`fp_year`=2015 AND `booking_dump`.`ERP_Deal_ID`='' AND `booking_dump`.`Booking_Adjustment`='POS') THEN `booking_dump`.`Booking_Net` ELSE 0 END) as fy15_pos_runrate, 
							SUM(CASE WHEN (`booking_dump`.`fp_year`=2014 AND `booking_dump`.`ERP_Deal_ID`='' AND `booking_dump`.`Booking_Adjustment`='POS') THEN `booking_dump`.`Booking_Net` ELSE 0 END) as fy14_pos_runrate, 
							SUM(CASE WHEN (`booking_dump`.`fp_year`=2015 AND `booking_dump`.`ERP_Deal_ID`<>'') THEN `booking_dump`.`Booking_Net` ELSE 0 END) as fy15_pos_b2b, 
							SUM(CASE WHEN (`booking_dump`.`fp_year`=2014 AND `booking_dump`.`ERP_Deal_ID`<>'') THEN `booking_dump`.`Booking_Net` ELSE 0 END) as fy14_pos_b2b 
							 FROM booking_dump WHERE Mapped_Sales_Level_6=? AND Mapped_id=?" . $qq_string_ext;
			$query_string = qq{$qq_string};
			$sth = $obj->getSimpleSTH($dbh, $query_string, %param_hash);
			while (my $rec_hash_ref_sub = $sth->fetchrow_hashref()) {
				$fy15_pos_runrate = $fy15_pos_runrate + $rec_hash_ref_sub->{"fy15_pos_runrate"} if ($rec_hash_ref_sub->{"fy15_pos_runrate"});
				$fy14_pos_runrate = $fy14_pos_runrate + $rec_hash_ref_sub->{"fy14_pos_runrate"} if ($rec_hash_ref_sub->{"fy14_pos_runrate"});
				$fy15_pos_b2b = $fy15_pos_b2b + $rec_hash_ref_sub->{"fy15_pos_b2b"} if ($rec_hash_ref_sub->{"fy15_pos_b2b"});
				$fy14_pos_b2b = $fy14_pos_b2b + $rec_hash_ref_sub->{"fy14_pos_b2b"} if ($rec_hash_ref_sub->{"fy14_pos_b2b"});
			}
			$sth->finish();
			# Booking for Overall
			push @field_value_array, $fy15_pos_runrate;
			# Calculating pos_runrate on total_booking
			if ($booking_all == 0) {push @field_value_array, 'UNDEF';} else {push @field_value_array, $fy15_pos_runrate/$booking_all;}
			push @field_value_array, $fy14_pos_runrate;
			# Calculating pos_runrate YoY
			if ($fy14_pos_runrate == 0) {push @field_value_array, 'INFIN';} elsif ((($fy14_pos_runrate < 0) && ($fy15_pos_runrate > 0)) || (($fy14_pos_runrate > 0) && ($fy15_pos_runrate < 0))) {push @field_value_array, 'UNDEF';} else {push @field_value_array, ($fy15_pos_runrate-$fy14_pos_runrate)/$fy14_pos_runrate;}
			push @field_value_array, $fy15_pos_b2b;
			# Calculating pos_b2b on total_booking
			if ($booking_all == 0) {push @field_value_array, 'UNDEF';} else {push @field_value_array, $fy15_pos_b2b/$booking_all;}
			push @field_value_array, $fy14_pos_b2b;
			# Calculating pos_b2b YoY
			if ($fy14_pos_b2b == 0) {push @field_value_array, 'INFIN';} elsif ((($fy14_pos_b2b < 0) && ($fy15_pos_b2b > 0)) || (($fy14_pos_b2b > 0) && ($fy15_pos_b2b < 0))) {push @field_value_array, 'UNDEF';} else {push @field_value_array, ($fy15_pos_b2b-$fy14_pos_b2b)/$fy14_pos_b2b;}
			print "POS Booking for $counter) $mapped_sales_level_6 | $id_l5 | $qtr completed\n";


			# Fetching Technology wise Booking, Base List & Discount Data
			print "Fetching Technology wise Booking for $counter) $mapped_sales_level_6 | $id_l5 | $qtr...\n";
			foreach my $element (@technologies) {
				$param_hash{3} = $element;
				my $metric = 0;
				my $metric2 = 0;
				$qq_string = "SELECT SUM(Booking_Net) as booking, SUM(Base_List) as Base_List 
								FROM latest_year_booking_dump WHERE Mapped_Sales_Level_6=? AND Mapped_id=? AND Tech_Name=?" . $qq_string_ext;
				$query_string = qq{$qq_string};
				$sth = $obj->getSimpleSTH($dbh, $query_string, %param_hash);
				while (my $rec_hash_ref_sub = $sth->fetchrow_hashref()) {
					$metric = $metric + $rec_hash_ref_sub->{"booking"} if ($rec_hash_ref_sub->{"booking"});
					$metric2 = $metric2 + $rec_hash_ref_sub->{"Base_List"} if ($rec_hash_ref_sub->{"Base_List"});
				}
				$sth->finish();
				# Technology wise Booking
				push @field_value_array, $metric; 
				# Technology wise Base List
				push @field_value_array, $metric2;
				# Technology wise Discount
				if ($metric2 != 0) {push @field_value_array, 1-($metric/$metric2);} else {push @field_value_array, 0;}
			}
			print "Technology wise Booking for $counter) $mapped_sales_level_6 | $id_l5 | $qtr completed!\n";

			# Fetching Architecture wise Booking, Base List & Discount Data
			print "Fetching Architecture wise Booking for $counter) $mapped_sales_level_6 | $id_l5 | $qtr...\n";
			foreach my $element (@archs) {
				$param_hash{3} = $element;
				my $metric = 0;
				my $metric2 = 0;
				$qq_string = "SELECT SUM(Booking_Net) as booking, SUM(Base_List) as Base_List 
								FROM latest_year_booking_dump WHERE Mapped_Sales_Level_6=? AND Mapped_id=? AND arch2=?" . $qq_string_ext;
				$query_string = qq{$qq_string};
				$sth = $obj->getSimpleSTH($dbh, $query_string, %param_hash);
				while (my $rec_hash_ref_sub = $sth->fetchrow_hashref()) {
					$metric = $metric + $rec_hash_ref_sub->{"booking"} if ($rec_hash_ref_sub->{"booking"});
					$metric2 = $metric2 + $rec_hash_ref_sub->{"Base_List"} if ($rec_hash_ref_sub->{"Base_List"});
				}
				$sth->finish();
				# Architecture wise Booking
				push @field_value_array, $metric; 
				# Architecture wise Base List
				push @field_value_array, $metric2;
				# Architecture wise Discount
				if ($metric2 != 0) {push @field_value_array, 1-($metric/$metric2);} else {push @field_value_array, 0;}
			}
			print "Architecture wise Booking for $counter) $mapped_sales_level_6 | $id_l5 | $qtr completed!\n";

			# Fetching Vertical wise Booking, Base List & Discount Data
			print "Fetching Vertical wise Booking for $counter) $mapped_sales_level_6 | $id_l5 | $qtr...\n";
			foreach my $element (@verticals) {
				$param_hash{3} = $element;
				my $metric = 0;
				my $metric2 = 0;
				$qq_string = "SELECT SUM(Booking_Net) as booking, SUM(Base_List) as Base_List FROM latest_year_booking_dump WHERE Mapped_Sales_Level_6=? AND Mapped_id=? AND Vertical=?" . $qq_string_ext;
				$query_string = qq{$qq_string};
				$sth = $obj->getSimpleSTH($dbh, $query_string, %param_hash);
				while (my $rec_hash_ref_sub = $sth->fetchrow_hashref()) {
					$metric = $metric + $rec_hash_ref_sub->{"booking"} if ($rec_hash_ref_sub->{"booking"});
					$metric2 = $metric2 + $rec_hash_ref_sub->{"Base_List"} if ($rec_hash_ref_sub->{"Base_List"});
				}
				$sth->finish();
				# Vertical wise Booking
				push @field_value_array, $metric; 
				# Vertical wise Base List
				push @field_value_array, $metric2;
				# Vertical wise Discount
				if ($metric2 != 0) {push @field_value_array, 1-($metric/$metric2);} else {push @field_value_array, 0;}
			}
			print "Vertical wise Booking for $counter) $mapped_sales_level_6 | $id_l5 | $qtr completed!\n";
						
			$col_count = scalar(@field_value_array); # Get the number of Value columns
			print "Vertical wise Base_List for $counter) $mapped_sales_level_6 | $id_l5 | $qtr completed!\n";
			push @full_range_array,  [@field_value_array];

			$last_row = $row_count;
			$last_row_ref = xl_rowcol_to_cell($row_count-1, $col_count-1); # Excel Cell to Row-Col Conversion
			$pivot_rng = $xl_sheet->Range("A$row_count:$last_row_ref");
			$pivot_rng->{Value} = [@full_range_array];
			
			print "$counter) $mapped_sales_level_6 | $id_l5 data extraction for $qtr completed!\n";
			@field_value_array = ();
			@full_range_array = ();
		} # end of Period foreach loop
		$row_count++;
		print "$counter) $mapped_sales_level_6 | $id_l5 data extraction completed!\n";
		print "===================================================================\n";
	} # End of sth_goal_sheet while loop

	$q1_sheet->Activate();
	$q1_sheet->{Name} = 'Q1';
	$q2_sheet->Activate();
	$q2_sheet->{Name} = 'Q2';
	$q3_sheet->Activate();
	$q3_sheet->{Name} = 'Q3';
	$q4_sheet->Activate();
	$q4_sheet->{Name} = 'Q4';
	$h1_sheet->Activate();
	$h1_sheet->{Name} = 'H1';
	$h2_sheet->Activate();
	$h2_sheet->{Name} = 'H2';
	$ytd_sheet->Activate();
	$ytd_sheet->{Name} = 'YTD';
	
	print "Entire Process Completed!\n";
	print "=========================\n\n";
		
}

sub emailXLPivotToMany {
	my ($self) = @_;

}
1;
