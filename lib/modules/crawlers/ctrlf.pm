package modules::crawlers::ctrlf;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
use Try::Tiny;
use YAML;
use JSON;
use UNIVERSAL;
use List::MoreUtils qw(uniq);
use modules::reports::report_utility;
use Scalar::Util qw(looks_like_number);
our @ISA = qw(modules::reports::report_utility);

sub new {
	my ($class) = @_;
	my $self = $class->SUPER::new($_[1], $_[2], $_[3], $_[4], $_[5], $_[6], $_[7], $_[8], $_[9]);
	$self = {};
	bless $self, $class;
	return $self;
}

sub controlF {
	my ($self) = @_;
	my $dbh=undef;
	my $sth=undef;
	my %source_hash = ();
	my %map_hash = ();
	print "\nPath of the Source file: ";
	my $dir_name = <STDIN>;
	print "Source file name: ";
	my $file_name = <STDIN>;
	print "Source file sheet name: ";
	my $sheet_name = <STDIN>;
	print "Starting Row Number in the source file sheet: ";
	my $start_row = <STDIN>;
	print "Source Column Number in the source file sheet: ";
	my $start_col = <STDIN>;
	print "Source ALTERNATE Column Number in the source file sheet: ";
	my $start_alt_col = <STDIN>;
	print "Mapping Table (Booking Table  OR Other) [default is Booking Table]: ";
	my $mapping_table_name = <STDIN>;


	# Starting Row Number Validation
	if (defined chomp($start_row)) {
		if (looks_like_number($start_row)) {
			$start_row = int($start_row);
		} else {
			print "\n Starting Row is not a number!\n";
			return;
		}
	} else {
		$start_row = 2;
	}

	# Starting Column Number Validation
	if (defined chomp($start_col)) {
		if (looks_like_number($start_col)) {
			$start_col = int($start_col);
		} else {
			print "\n Starting Col is not a number!\n";
			return;
		}
	} else {
			print "\n Starting Column is undefined or empty!\n";
			return;
	}

	# Alternative Starting Column Number Validation
	if (defined chomp($start_alt_col)) {
		if (looks_like_number($start_alt_col)) {
			$start_alt_col = int($start_alt_col);
		} else {
			print "\n Alternative Starting Col is not a number!\n";
		}
	} else {
			print "\n Alternative Starting Column is undefined or empty!\n";
	}

	# Mapping Table Validation
	if (defined chomp($mapping_table_name)) {
		if (lc $mapping_table_name eq "other") {
			$mapping_table_name = "customer_names_variable";
		} else {
			$mapping_table_name = "customer_names";
		}
	} else {
			$mapping_table_name = "customer_names";
	}


	if (defined chomp($dir_name) && defined chomp($file_name) && 
		defined chomp($sheet_name)) { # Directory, File Name & Sheet Name Validation
		$self->prepareXLApp();
		my $xl_app = $self->getXLApp();
		$xl_app->{Visible} = 1;
		$xl_app->{DisplayAlerts} = 0;
		my $path_string = $dir_name."/";
		my $ab_path = $path_string.$file_name;
		my $xl_book = $xl_app->Workbooks->Open($ab_path);
		if (defined $xl_book) {
			my $xl_sheet = $xl_book->Sheets($sheet_name);
			my $total_row = $xl_sheet->UsedRange->Rows->{'Count'};
			print "Total Rows: $total_row\n";
			print "Press any key to continue...";
			my $dummy = <STDIN>;
			my $row = $start_row;
			my $col = $start_col;
			
			
			# Excel file store the matched items
			my $row2 = undef;
			my $i_col = 1;
			my $si_col = $i_col+1;
			my $s_col = $si_col+1;
			my $map_col = $s_col+1;
			my $map_unique_col = $map_col+1;
			my $rem_col = $map_unique_col+1;
			my $xl_sheet2 = undef;
			my $last_row_in_result = 0;

			try {
				$xl_sheet2 = $xl_book->Sheets("Mapping");
				$last_row_in_result = $xl_sheet2->UsedRange->Rows->{'Count'};
				$row2 = $last_row_in_result;				
			} catch {
				$xl_sheet2 = $xl_book->Sheets->Add;
				$xl_sheet2->{Name} = "Mapping";
				$row2 = 1;
				$xl_sheet2->Cells($row2, $i_col)->{Value} = "S.No.";
				$xl_sheet2->Cells($row2, $si_col)->{Value} = "Source_Index";
				$xl_sheet2->Cells($row2, $s_col)->{Value} = "Source Unique Name";
				$xl_sheet2->Cells($row2, $map_col)->{Value} = "Mapped Name";
				$xl_sheet2->Cells($row2, $map_unique_col)->{Value} = "Mapped Unique Name";
				$xl_sheet2->Cells($row2, $rem_col)->{Value} = "Remarks";
				$xl_sheet2->Range("C2")->Select;
				$xl_app->ActiveWindow->{FreezePanes} = 'True';
			};
			print "Data is being parsed...\n";
			my @temp_array = ();
			my @source_array = ();
			my $array_index = 0;
			while ($row <= $total_row) {
				my $value = undef;
				$value = $xl_sheet->Cells($row, $col)->Value;
				$temp_array[$array_index] = $value;
				$row++;
				$array_index++;
			}
			@source_array = uniq(@temp_array);
			$array_index = 1;
			foreach my $element (@source_array) {
				$source_hash{$array_index} = $element;
				$array_index++;
			}

			
			print "Data parsing is completed!\n";
			my $hash_size = keys (%source_hash);
			print "Size of Source Data: $hash_size\n";
			my $key = 1;
			# =================================================
			# Getting Mapping Source
			# ================================================
			my $dbh = $self->getMySQLDBH();
			my %map_match = ();
			my $map_unique = undef;
			my $main_index = 0;
			while ($key <= $hash_size) {
				my $source_string = $source_hash{$key};
				my $source_index = $key;
				my @words = split(' ', $source_string);
				my $total_words = $#words+1;
				my $first_word_length = length $words[0];
				my $source_string_length = length $source_string;
				my $counter = $source_string_length;
				my $remarks = "";
				print "\n","=" x 80,"\n";
				print "\n$source_string=>";
				my $loop_terminator = 0;
				my %params = ();
				while ($counter >= $first_word_length) {
					my $try_string = substr($source_string, 0, $counter);
					my $query_string = undef;
					my $query_text = "SELECT DISTINCT account_name, customer_name FROM " . $mapping_table_name . " WHERE customer_name LIKE ?";
					$query_string = qq{$query_text};
					$try_string = "%".$try_string."%";
					$params{1} = $try_string;
					$sth = $self->getSimpleSTH($dbh, $query_string, %params);
					
					if (($sth->rows) == 0) {
						$map_match{1} = "Not Matching";
						$map_unique = "Not Matching";
						#print "No Data found in Customer_Names table!\n";
						
					} else {
					print "Try String is: $try_string\n";
						my $acc_array_index = 1;
						my $cus_array_index = 1;
						my @account_array = ();
						my %unique_data = ();
						my $previous_cus_name = undef;
						my $cus_name = undef;
						my $dummy_counter = 1;
						my $trigger = 0;
						my $last_one = undef;
						while (my $a = $sth->fetchrow_hashref()) {
							if ($trigger) {
								$account_array[$acc_array_index-1] = $last_one;
								$acc_array_index++;
								$account_array[$acc_array_index-1] = $a->{account_name};
								$trigger = 0;
							} else {
								$account_array[$acc_array_index-1] = $a->{account_name};
							}
							$cus_name = $a->{customer_name};
							if ($dummy_counter == 1) {
								$previous_cus_name = $cus_name;
							}
							if ($previous_cus_name ne $cus_name || $dummy_counter == $sth->rows) {
								my $account_name_hash = {};
								$unique_data{$cus_array_index}{$previous_cus_name} = $account_name_hash;
								my $temp_counter = 0;
								my $master_key = 0;
								$last_one = pop @account_array;
								$trigger = 1;
								foreach my $element (@account_array) {
									$master_key = ($temp_counter+1);
									$account_name_hash->{$master_key} = $element;
									$temp_counter++;
								}
								if ($dummy_counter == $sth->rows) {
									if ($previous_cus_name eq $cus_name) {
										$master_key = ($temp_counter+1);
										$account_name_hash->{$master_key} = $last_one;
									} else {
										my $account_name_hash = {};
										$cus_array_index++;
										$unique_data{$cus_array_index}{$cus_name} = $account_name_hash;
										$master_key = 1;
										$account_name_hash->{$master_key} = $last_one;
									}
								}
								$cus_array_index++;
								$acc_array_index = 0;
								$previous_cus_name = $cus_name;
								@account_array = ();
							}
							$acc_array_index++;
							$dummy_counter++;
						} 
					
						print to_json(\%unique_data, {pretty => 1});
						# =============================
						# Searching Algorithms
						# =============================
						print "What key to be considered?: ";
						while (my $what_key = <STDIN>) {
							my $main_key = undef;
							my $sub_key = undef;
							chomp ($what_key);
							if (!defined $what_key || $what_key eq '') {
								print "No keys mentioned!\n";
								print "#" x 25, "\n";
								print "What key to be considered?: ";
								next;
							}
							if ($what_key =~ /^NOTE|RETURN|S$/i) {
									$remarks = "Check" if ($what_key =~ /^NOTE$/i);
									$loop_terminator = 1;
									last;
							}
							my @validate_keys = split(/[&,#,\$,^,!,~,`,\/,%,\|]+/, $what_key);
							my $local_loop_counter = 1;
							foreach my $val_key (@validate_keys) {
								#print "Validated Key=>$val_key\n";
								my @selection_key = split('\.', $val_key);
								my $selection_key_length = $#selection_key+1;
								#print "$selection_key_length strings are in Array | String is: $selection_key[0]\n";
								if ($selection_key_length == 1) {
									$main_key = $selection_key[0];
									if (looks_like_number($main_key)) {
										#print "Main_Key: $main_key\n";
										foreach my $key1 (sort keys %{$unique_data{$main_key}}) {
											my $main_key_1 = $key1;
											if (!ref($main_key_1)) {
												#print "Main_Key_1: $main_key_1\n";
												$map_unique = $main_key_1;
												foreach my $key2 (sort keys %{$unique_data{$main_key}{$main_key_1}}) {
													my $account_match = $unique_data{$main_key}{$main_key_1}{$key2};
													if ($account_match) {
														#print "$key2 => $account_match\n";
														$map_match{$key2} = $account_match;
													}
												}
											}
										}
										$loop_terminator = 1;
								
									} else {
										print "Key is not a number!\n";
										print "What key to be considered?: ";
										next;
									}
								} else {
									my @selection_key2 = split('-', $selection_key[1]);
									$main_key = $selection_key[0];
									if (looks_like_number($main_key)) {
										#print "Main_Key: $main_key\n";
										foreach my $key1 (sort keys %{$unique_data{$main_key}}) {
											my $main_key_1 = $key1;
											if (!ref($main_key_1)) {
												print "Main_Key_1: $main_key_1\n";
												$map_unique = $main_key_1;
												foreach my $key2 (sort keys %{$unique_data{$main_key}{$main_key_1}}) {
													my $account_match = $unique_data{$main_key}{$main_key_1}{$key2};
													if ($account_match) {
														foreach my $indiv (@selection_key2) {
															if ($key2 eq $indiv) {
																#print "$key2 => $account_match\n";
																$map_match{$local_loop_counter} = $account_match;
																$local_loop_counter++;
															}
														}
													}
												}
											}
										}
										$loop_terminator = 1;
										
									} else {
										print "Key is not a number!\n";
										print "What key to be considered?: ";
										next;
									}
								}
							}
								last;

						}

					# ==========================================================	

				}
				if ($loop_terminator) {
					last;
				} else {
					$counter--;
				}
			}
				$sth->finish();
				$key++;
				foreach my $key (sort keys %map_match) {
					$row2++;
					$main_index++;
					$xl_sheet2->Cells($row2, $i_col)->{Value} = $main_index;
					$xl_sheet2->Cells($row2, $si_col)->{Value} = $source_index;
					$xl_sheet2->Cells($row2, $s_col)->{Value} = $source_string;
					$xl_sheet2->Cells($row2, $map_col)->{Value} = $map_match{$key};
					$xl_sheet2->Cells($row2, $map_unique_col)->{Value} = $map_unique;
					$xl_sheet2->Cells($row2, $rem_col)->{Value} = $remarks;
				}
				%map_match = ();
				$xl_app->{DisplayAlerts} = 0;
				$xl_app->ActiveWindow->SmallScroll({Down => 1});
				$xl_book->Save;
				$xl_app->{DisplayAlerts} = 1;
			}
			$xl_book = 0;
			$xl_app = 0;
		}
	} else {
		if (!defined $dir_name) {
			print "\nPath Name cannot be empty string!\n";
		} elsif (!defined $file_name) {
			print "\nFile Name cannot be empty string!\n";
		} else {
			print "\nSheet name cannot be empty string!\n";
		}
	}
}

sub deDupper {
	my ($self) = @_;
	#$self->prepareXLConstantObject();
	my %source_index_hash = ();
	my %alt_hash = ();
	my %share_hash = ();
	my %source_hash = ();
	my %lookup_hash = ();
	my %unique_hash = ();
	my %display_hash = ();
	my $dbh=undef;
	my $sth=undef;

	print "\nPath of the Source file: ";
	my $dir_name = <STDIN>;
	print "Source file name: ";
	my $file_name = <STDIN>;
	print "Source file sheet name: ";
	my $sheet_name = <STDIN>;
	print "Starting Row Number in the source file sheet: ";
	my $start_row = <STDIN>;
	print "Source Column Number in the source file sheet: ";
	my $start_col = <STDIN>;
	print "Source ALTERNATE Column Number in the source file sheet: ";
	my $start_alt_col = <STDIN>;


	# Starting Row Number Validation
	if (defined chomp($start_row)) {
		if (looks_like_number($start_row)) {
			$start_row = int($start_row);
		} else {
			print "\n Starting Row is not a number!\n";
			return;
		}
	} else {
		$start_row = 2;
	}

	# Starting Column Number Validation
	if (defined chomp($start_col)) {
		if (looks_like_number($start_col)) {
			$start_col = int($start_col);
		} else {
			print "\n Starting Col is not a number!\n";
			return;
		}
	} else {
			print "\n Starting Column is undefined or empty!\n";
			return;
	}

	# Alternative Starting Column Number Validation
	if (defined chomp($start_alt_col)) {
		if (looks_like_number($start_alt_col)) {
			$start_alt_col = int($start_alt_col);
		} else {
			print "\n Alternative Starting Col is not a number!\n";
		}
	} else {
			print "\n Alternative Starting Column is undefined or empty!\n";
	}



	if (defined chomp($dir_name) && defined chomp($file_name) && 
		defined chomp($sheet_name)) { # Directory, File Name & Sheet Name Validation
		$self->prepareXLApp();
		my $xl_app = $self->getXLApp();
		$xl_app->{Visible} = 1;
		$xl_app->{DisplayAlerts} = 0;
		my $path_string = $dir_name."/";
		my $ab_path = $path_string.$file_name;
		my $xl_book = $xl_app->Workbooks->Open($ab_path);
		if (defined $xl_book) {
			my $xl_sheet = $xl_book->Sheets($sheet_name);
			my $total_row = $xl_sheet->UsedRange->Rows->{'Count'};
			print "Total Rows: $total_row\n";
			print "Press any key to continue...";
			my $dummy = <STDIN>;
			my $row = $start_row;
			my $col = $start_col;
			
			
			# Excel file store the matched items
			my $row2 = undef;
			my $i_col = 1;
			my $si_col = $i_col+1;
			my $s_col = $si_col+1;
			my $alt_r_col = $s_col+1;
			my $r_col = $alt_r_col+1;
			my $rem_col = $r_col+1;
			my $xl_sheet2 = undef;
			my $last_row_in_result = 0;
			try {
				$xl_sheet2 = $xl_book->Sheets("Result2");
				$last_row_in_result = $xl_sheet2->UsedRange->Rows->{'Count'};
				$row2 = $last_row_in_result;
				my $row_result = 2;
				my @temp_array = ();
				my $temp_counter = 0;
				while ($row_result <= $last_row_in_result) {
					my $value = $xl_sheet2->Cells($row_result, 5)->{Value};
					$temp_array[$temp_counter] = $value;
					$temp_counter++;
					$row_result++;
				}
				my @temp_array2 = uniq @temp_array;
				$temp_counter = 1;
				foreach my $val (@temp_array2) {
					$unique_hash{$temp_counter} = $val;
					print $unique_hash{$temp_counter},"\n";
					$temp_counter++;
				}
				
			} catch {
				$xl_sheet2 = $xl_book->Sheets->Add;
				$xl_sheet2->{Name} = "Result2";
				$row2 = 1;
				$xl_sheet2->Cells($row2, $i_col)->{Value} = "S.No.";
				$xl_sheet2->Cells($row2, $si_col)->{Value} = "Source_Index";
				$xl_sheet2->Cells($row2, $s_col)->{Value} = "Source Field Name";
				$xl_sheet2->Cells($row2, $alt_r_col)->{Value} = "Source Alternative Name";
				$xl_sheet2->Cells($row2, $r_col)->{Value} = "Unique Name";
				$xl_sheet2->Cells($row2, $rem_col)->{Value} = "Remarks";
				$xl_sheet2->Range("C2")->Select;
				$xl_app->ActiveWindow->{FreezePanes} = 'True';
			};
			print "Data is being parsed...\n";
			while ($row <= $total_row) {
				my $key = undef;
				my $value = undef;
				my $alt_value = undef;
				my $orig_value = undef;
				$key = $xl_sheet->Cells($row, 1)->{Value};
				$orig_value = $xl_sheet->Cells($row, $col)->{Value};
				$alt_value = $xl_sheet->Cells($row, $start_alt_col)->{Value};
				if ($orig_value) {
					$value = $orig_value;
				} elsif ($alt_value) {
					$value = $alt_value;
				} else {
					$value = "Empty Cell";
				}
				$source_index_hash{$row-$start_row+1} = $key;
				$share_hash{$row-$start_row+1} = $value;
				if ($alt_value) {
					$alt_hash{$row-$start_row+1} = $alt_value;
				} else {
					$alt_hash{$row-$start_row+1} = "";
				}
				$row++;
			}
			my $hash_size = keys (%share_hash);
			print "Size: $hash_size\n";
			my $key = 1;
			
			# =============================
			# Searching Algorithms
			# =============================
			%source_hash = %share_hash;
			%lookup_hash = %share_hash;
			while ($key <= $hash_size) {
				my $is_mapped = 0;
				my $source_string = $source_hash{$key};
				my $source_index = $source_index_hash{$key};
				my $unique_string = undef;
				my $lookup_string = $lookup_hash{$key};
				delete $lookup_hash{$key};
				my $source_string_length = length $source_string;
				my $counter = $source_string_length;
				my $remarks = "";
				my @words = split(' ', $source_string);
				my $total_words = $#words+1;
				my $first_word_length = length $words[0];
				print "\n","=" x 80,"\n";
				print "\n$source_string=>";
				my $loop_terminator = 0;
#				while ($counter >= 2) {
				while ($counter >= $first_word_length) {
					my @matched_keys = undef;
					my $try_string = substr($source_string, 0, $counter);
					if ($counter > 2) {
						@matched_keys = grep{$unique_hash{$_} =~ /\Q$try_string/i} keys %unique_hash;
					} else {
						@matched_keys = grep{$unique_hash{$_} =~ /^\Q$try_string/i} keys %unique_hash;
					}
					if (@matched_keys) {
						print "'***$try_string***'\n";
						print "=" x 80,"\n";
						%display_hash = ();
						foreach my $key (@matched_keys) {
							$display_hash{$key} = $unique_hash{$key};
						}
						foreach my $key (sort{lc($display_hash{$a}) cmp lc($display_hash{$b})} keys %display_hash) {
							print "$key => $display_hash{$key}\n";
						}
						print "What key to be considered?: ";
						while (my $what_key = <STDIN>) {
							chomp ($what_key);
							if (!defined $what_key || $what_key eq '') {
								print "No keys mentioned!\n";
								print "#" x 25, "\n";
								print "What key to be considered?: ";
								$loop_terminator = 0;
								next;
							}
							if (grep{$_ eq $what_key} @matched_keys) {
								$unique_string = $unique_hash{$what_key};
								$loop_terminator = 1;
								$is_mapped = 1;
								last;
							}
							if ($what_key =~ /^NOTE|RETURN|S|\+$/i) {
								$remarks = "Check" if ($what_key =~ /^NOTE$/i);
								print "Check\n";
								if ($counter > 2) {
									@matched_keys = grep{$lookup_hash{$_} =~ /\Q$try_string/i} keys %lookup_hash;
								} else {
									@matched_keys = grep{$lookup_hash{$_} =~ /^\Q$try_string/i} keys %lookup_hash;
								}
								if (@matched_keys) {
									print "'***$try_string***'\n";
									print "=" x 80,"\n";
									%display_hash = ();
									foreach my $key (@matched_keys) {
										$display_hash{$key} = $lookup_hash{$key};
									}
									foreach my $key (sort{lc($display_hash{$a}) cmp lc($display_hash{$b})} keys %display_hash) {
										print "$key => $display_hash{$key}\n";
									}
									print "What key to be considered?: ";
									while (my $what_key2 = <STDIN>) {
										chomp ($what_key2);
										if (!defined $what_key2 || $what_key2 eq '') {
											print "No keys mentioned!\n";
											print "#" x 25, "\n";
											print "What key to be considered?: ";
											$loop_terminator = 0;
											next;
										}
										if (grep{$_ eq $what_key2} @matched_keys) {
											$unique_string = $lookup_hash{$what_key2};
											$unique_hash{$key} = $lookup_hash{$what_key2};
											delete $lookup_hash{$what_key2};
											$loop_terminator = 1;
											$is_mapped = 1;
											last;
										}
										if ($what_key2 =~ /^NOTE|RETURN|S|\+$/i) {
											$remarks = "Check" if ($what_key2 =~ /^NOTE$/i);
											$unique_string = $source_string;
											$loop_terminator = 1;
											last;
										}
									}
								}
								$loop_terminator = 1;
								last;
							}
						}
					} else {
						if ($counter > 2) {
							@matched_keys = grep{$lookup_hash{$_} =~ /\Q$try_string/i} keys %lookup_hash;
						} else {
							@matched_keys = grep{$lookup_hash{$_} =~ /^\Q$try_string/i} keys %lookup_hash;
						}
						if (@matched_keys) {
							print "'***$try_string***'\n";
							print "=" x 80,"\n";
							%display_hash = ();
							foreach my $key (@matched_keys) {
								$display_hash{$key} = $lookup_hash{$key};
							}
							foreach my $key (sort{lc($display_hash{$a}) cmp lc($display_hash{$b})} keys %display_hash) {
								print "$key => $display_hash{$key}\n";
							}
							print "What key to be considered?: ";
							while (my $what_key2 = <STDIN>) {
								chomp ($what_key2);
								if (!defined $what_key2 || $what_key2 eq '') {
									print "No keys mentioned!\n";
									print "#" x 25, "\n";
									print "What key to be considered?: ";
									$loop_terminator = 0;
									next;
								}
								if (grep{$_ eq $what_key2} @matched_keys) {
									$unique_string = $lookup_hash{$what_key2};
									$unique_hash{$key} = $lookup_hash{$what_key2};
									delete $lookup_hash{$what_key2};
									$loop_terminator = 1;
									$is_mapped = 1;
									last;
								}
								if ($what_key2 =~ /^NOTE|RETURN|S|\+$/i) {
									$remarks = "Check" if ($what_key2 =~ /^NOTE$/i);
									$unique_string = $source_string;
									$loop_terminator = 1;
									last;
								}
							}
						} else {
							$unique_string = $source_string;
							$loop_terminator = 0;
							$counter--;
						}

					}
					if ($loop_terminator) {
						last;
					}
				}
				my @matched_keys = undef;
				$is_mapped = 1 if ($remarks =~ /^check$/i);
				unless ($is_mapped) {
					print "If you like to try by 'OTHER TEXT',\nkey in your 'TEXT': ";
					while (my $text = <STDIN>) {
						chomp ($text);
						if (!defined $text || $text eq '') {
							print "Text keyed in is empty or undefined!\n";
							print "#" x 25, "\n";
							print "key in your 'TEXT': ";
							next;
						} elsif ($text =~ /^NOTE|RETURN|S|\+$/i) {
							$remarks = "Check" if ($text =~ /^NOTE$/i);
							$unique_string = $source_string;
							last;
						} elsif (@matched_keys = grep{$unique_hash{$_} =~ /\Q$text/i} keys %unique_hash) {
							print "'***$text***'\n";
							print "=" x 80,"\n";
							%display_hash = ();
							foreach my $key (@matched_keys) {
								$display_hash{$key} = $unique_hash{$key};
							}
							foreach my $key (sort{lc($display_hash{$a}) cmp lc($display_hash{$b})} keys %display_hash) {
								print "$key => $display_hash{$key}\n";
							}
							print "What key to be considered?: ";
							while (my $what_key2 = <STDIN>) {
								chomp ($what_key2);
								if (!defined $what_key2 || $what_key2 eq '') {
									print "No keys mentioned!\n";
									print "#" x 25, "\n";
									print "What key to be considered?: ";
									next;
								}
								if (grep{$_ eq $what_key2} @matched_keys) {
									$unique_string = $unique_hash{$what_key2};
									last;
								}
								if ($what_key2 =~ /^NOTE|RETURN|S|\+$/i) {
									$remarks = "Check" if ($what_key2 =~ /^NOTE$/i);
									$unique_string = $source_string;
								}
							}
								if (@matched_keys = grep{$lookup_hash{$_} =~ /\Q$text/i} keys %lookup_hash) {
									print "'***$text***'\n";
									print "=" x 80,"\n";
									%display_hash = ();
									foreach my $key (@matched_keys) {
										$display_hash{$key} = $lookup_hash{$key};
									}
									foreach my $key (sort{lc($display_hash{$a}) cmp lc($display_hash{$b})} keys %display_hash) {
										print "$key => $display_hash{$key}\n";
									}
									print "What key to be considered?: ";
									while (my $what_key2 = <STDIN>) {
										chomp ($what_key2);
										if (!defined $what_key2 || $what_key2 eq '') {
											print "No keys mentioned!\n";
											print "#" x 25, "\n";
											print "What key to be considered?: ";
											next;
										}
										if (grep{$_ eq $what_key2} @matched_keys) {
											$unique_string = $lookup_hash{$what_key2};
											$unique_hash{$key} = $lookup_hash{$what_key2};
											delete $lookup_hash{$what_key2};
											last;
										}
										if ($what_key2 =~ /^NOTE|RETURN|S|\+$/i) {
											$remarks = "Check" if ($what_key2 =~ /^NOTE$/i);
											$unique_string = $source_string;
											last;
										}
									}
									last;
								} else {
									print "Your 'TEXT does not match!...you can try again or exit!'\n";
									print "If you like to try by 'OTHER TEXT',\nkey in your 'TEXT': ";
								}
						} elsif (@matched_keys = grep{$lookup_hash{$_} =~ /\Q$text/i} keys %lookup_hash) {
								print "'***$text***'\n";
								print "=" x 80,"\n";
								%display_hash = ();
								foreach my $key (@matched_keys) {
									$display_hash{$key} = $lookup_hash{$key};
								}
								foreach my $key (sort{lc($display_hash{$a}) cmp lc($display_hash{$b})} keys %display_hash) {
									print "$key => $display_hash{$key}\n";
								}
								print "What key to be considered?: ";
								while (my $what_key2 = <STDIN>) {
									chomp ($what_key2);
									if (!defined $what_key2 || $what_key2 eq '') {
										print "No keys mentioned!\n";
										print "#" x 25, "\n";
										print "What key to be considered?: ";
										next;
									} elsif (grep{$_ eq $what_key2} @matched_keys) {
										$unique_string = $lookup_hash{$what_key2};
										$unique_hash{$key} = $lookup_hash{$what_key2};
										delete $lookup_hash{$what_key2};
										last;
									} elsif ($what_key2 =~ /^NOTE|RETURN|S|\+$/i) {
										$remarks = "Check" if ($what_key2 =~ /^NOTE$/i);
										$unique_string = $source_string;
										last;
									}
								}
								last;
							} else {
								print "Your 'TEXT does not match!...you can try again or exit!'\n";
								print "If you like to try by 'OTHER TEXT',\nkey in your 'TEXT': ";
							}
						} 
				}
				unless ($unique_string) {
					print "No Matching found!\n";
					$unique_string = $source_string;
				}
				$row2++;
				$xl_sheet2->Cells($row2, $i_col)->{Value} = $row2-1;
				$xl_sheet2->Cells($row2, $si_col)->{Value} = $source_index;
				$xl_sheet2->Cells($row2, $s_col)->{Value} = $source_hash{$key};
				$xl_sheet2->Cells($row2, $alt_r_col)->{Value} = $alt_hash{$key};
				$xl_sheet2->Cells($row2, $r_col)->{Value} = $unique_string;
				$xl_sheet2->Cells($row2, $rem_col)->{Value} = $remarks;
				$key++;
				if ($row > 30) {
					$xl_app->ActiveWindow->SmallScroll({Down => 1});
				}
				$xl_app->{DisplayAlerts} = 0;
				if ($row2 % 20 == 0) {
				
					$last_row_in_result = $xl_sheet2->UsedRange->Rows->{'Count'};
					my $row_result = 2;
					my $temp_counter = 0;
					my @temp_array = ();
					my @temp_array2 = ();
					while ($row_result <= $last_row_in_result) {
						my $value = $xl_sheet2->Cells($row_result, 5)->{Value};
						$temp_array[$temp_counter] = $value;
						$temp_counter++;
						$row_result++;
					}
					@temp_array2 = uniq @temp_array;
					$temp_counter = 1;
					%unique_hash = ();
					foreach my $val (@temp_array2) {
						$unique_hash{$temp_counter} = $val;
						$temp_counter++;
					}
					$xl_book->Save;
					print "Data Saved!\n";
				}
				$xl_app->{DisplayAlerts} = 1;
			}
			# =============================
			
			
			$xl_app->{DisplayAlerts} = 1;
			$xl_book = 0;
			$xl_app = 0;
		}
	} else {
		if (!defined $dir_name) {
			print "\nPath Name cannot be empty string!\n";
		} elsif (!defined $file_name) {
			print "\nFile Name cannot be empty string!\n";
		} else {
			print "\nSheet name cannot be empty string!\n";
		}
	}
}

1;