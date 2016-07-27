package modules::migrators::converter_utility;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use modules::reports::report_utility;
use modules::helpers::constants::db_connection_strings;
use strict;
use warnings;
use v5.14;
use experimental;
no warnings qw(experimental::smartmatch experimental::lexical_topic experimental::regex_sets experimental::lexical_subs);
use List::MoreUtils qw(uniq);

our $rpt_util_obj;
our $sql_string;

sub new {
	my $class = shift;
	my $self = {_rptUtilObj => shift,};

	# MySQL Report Utility Object and Database Handle
	our $rpt_util_obj = modules::reports::report_utility->new();
	our $sql_string = modules::helpers::constants::db_connection_strings->new();

	bless $self, $class;
	return $self;
}

# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# $$$$$$$$$$$$ BEGIN Object Preparation Block $$$$$$$$$$$$
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$


# method to prepare Partners object
sub fetchObjectCustomers {
	my $self =shift;
	my $where_clause = shift;
	my $param_year = shift;
	my $param_quarter = shift;
	my $param_month = shift;
	my $param_week = shift;
	my $dbh = $self->getMySQLDBHandle();

	my $object_string = 'region';
	my $table_name = $sql_string->getTableNameBookingDump();
	my %hash_final;

	my %hash_gtmu;
	# Sending query details to a helper method and receive Region array reference
	my $array_ref_region = $self->_runQueryReturnArrayRef($table_name, $object_string, $where_clause, 
			$param_year, $param_quarter, $param_month, $param_week,);
	my $array_ref_gtmu = $self->getArrayRefGTMu($array_ref_region);
	$object_string = 'sales_level_6';
	# Sending query details to a helper method and receive Sales Level 6 array reference
	my $array_ref_sl6 = $self->_runQueryReturnArrayRef($table_name, $object_string, $where_clause, 
			$param_year, $param_quarter, $param_month, $param_week,);

	$hash_gtmu{"All"} = $array_ref_region;
	foreach my $element (@{$array_ref_gtmu}) {
		$hash_gtmu{$element} = $self->getArrayRefRegion($element);	
	}

	foreach my $hash_key (keys %hash_gtmu) { # hash_key is element like All, EU1, EU2...
		# Each array reference contains Regions
		#print "$hash_key processing ...\n";
		print ".";
		$hash_final{$hash_key} = $self->getArrayRefCustomers($hash_gtmu{$hash_key}, $where_clause, 
			$param_year, $param_quarter, $param_month, $param_week, "by region"); 
		#print "$hash_key processed!\n";
	}

	foreach my $array_element (@{$array_ref_region}) {
		#print "$array_element processing ...\n";
		$hash_final{$array_element} = $self->getArrayRefCustomers([$array_element], $where_clause,
		$param_year, $param_quarter, $param_month, $param_week, "by region");
		#print "$array_element processed!\n";
	}

	foreach my $array_element (@{$array_ref_sl6}) {
		#print "$array_element processing ...\n";
		$hash_final{$array_element} = $self->getArrayRefCustomers([$array_element], $where_clause,
		$param_year, $param_quarter, $param_month, $param_week, "by sl6");
		#print "$array_element processed!\n";
	}

	return \%hash_final;
}



# method to prepare Partners object
sub fetchObjectPartners {
	my $self =shift;
	my $where_clause = shift;
	my $param_year = shift;
	my $param_quarter = shift;
	my $param_month = shift;
	my $param_week = shift;
	my $dbh = $self->getMySQLDBHandle();

	my $object_string = 'region';
	my $table_name = $sql_string->getTableNameBookingDump();
	my %hash_final;

	my %hash_gtmu;
	# Sending query details to a helper method and receive Region array reference
	my $array_ref_region = $self->_runQueryReturnArrayRef($table_name, $object_string, $where_clause, 
			$param_year, $param_quarter, $param_month, $param_week,);
	my $array_ref_gtmu = $self->getArrayRefGTMu($array_ref_region);
	$object_string = 'sales_level_6';
	# Sending query details to a helper method and receive Sales Level 6 array reference
	my $array_ref_sl6 = $self->_runQueryReturnArrayRef($table_name, $object_string, $where_clause, 
			$param_year, $param_quarter, $param_month, $param_week,);

	$hash_gtmu{"All"} = $array_ref_region;
	foreach my $element (@{$array_ref_gtmu}) {
		$hash_gtmu{$element} = $self->getArrayRefRegion($element);	
	}

	foreach my $hash_key (keys %hash_gtmu) { # hash_key is element like All, EU1, EU2...
		# Each array reference contains Regions
		#print "$hash_key processing ...\n";
		print ".";
		$hash_final{$hash_key} = $self->getArrayRefPartners($hash_gtmu{$hash_key}, $where_clause, 
			$param_year, $param_quarter, $param_month, $param_week, "by region"); 
		#print "$hash_key processed!\n";
	}

	foreach my $array_element (@{$array_ref_region}) {
		#print "$array_element processing ...\n";
		$hash_final{$array_element} = $self->getArrayRefPartners([$array_element], $where_clause,
		$param_year, $param_quarter, $param_month, $param_week, "by region");
		#print "$array_element processed!\n";
	}

	foreach my $array_element (@{$array_ref_sl6}) {
		#print "$array_element processing ...\n";
		$hash_final{$array_element} = $self->getArrayRefPartners([$array_element], $where_clause,
		$param_year, $param_quarter, $param_month, $param_week, "by sl6");
		#print "$array_element processed!\n";
	}

	return \%hash_final;
}


# method to prepare Sales Agent object
sub fetchObjectSalesAgents {
	my $self =shift;
	my $where_clause = shift;
	my $param_year = shift;
	my $param_quarter = shift;
	my $param_month = shift;
	my $param_week = shift;
	my $dbh = $self->getMySQLDBHandle();

	my $object_string = 'region';
	my $table_name = $sql_string->getTableNameBookingDump();
	my %hash_final;

	my %hash_gtmu;
	# Sending query details to a helper method and receive Region array reference
	my $array_ref_region = $self->_runQueryReturnArrayRef($table_name, $object_string, $where_clause, 
			$param_year, $param_quarter, $param_month, $param_week,);
	my $array_ref_gtmu = $self->getArrayRefGTMu($array_ref_region);
	$object_string = 'sales_level_6';
	my $array_ref_sl6 = $self->_runQueryReturnArrayRef($table_name, $object_string, $where_clause, 
			$param_year, $param_quarter, $param_month, $param_week,);

	$hash_gtmu{"All"} = $array_ref_region;
	foreach my $element (@{$array_ref_gtmu}) {
		$hash_gtmu{$element} = $self->getArrayRefRegion($element);	
	}

	foreach my $hash_key (keys %hash_gtmu) { # hash_key is element like All, EU1, EU2...
		# Each array reference contains Regions
		#print "$hash_key processing ...\n";
		print ".";
		$hash_final{$hash_key} = $self->getArrayRefSalesAgents($hash_gtmu{$hash_key}, $where_clause, 
			$param_year, $param_quarter, $param_month, $param_week, "by region"); 
		#print "$hash_key processed!\n";
	}

	foreach my $array_element (@{$array_ref_region}) {
		#print "$array_element processing ...\n";
		$hash_final{$array_element} = $self->getArrayRefSalesAgents([$array_element], $where_clause,
		$param_year, $param_quarter, $param_month, $param_week, "by region");
		#print "$array_element processed!\n";
	}

	foreach my $array_element (@{$array_ref_sl6}) {
		#print "$array_element processing ...\n";
		$hash_final{$array_element} = $self->getArrayRefSalesAgents([$array_element], $where_clause,
		$param_year, $param_quarter, $param_month, $param_week, "by sl6");
		#print "$array_element processed!\n";
	}

	return \%hash_final;
}


# method to prepare SL6 object
sub fetchObjectSL6 {
	my $self =shift;
	my $where_clause = shift;
	my $param_year = shift;
	my $param_quarter = shift;
	my $param_month = shift;
	my $param_week = shift;
	my $dbh = $self->getMySQLDBHandle();

	my $object_string = 'region';
	my $table_name = $sql_string->getTableNameBookingDump();
	my %hash_final;

	my %hash_gtmu;
	# Sending query details to a helper method and receive Region array reference
	my $array_ref_region = $self->_runQueryReturnArrayRef($table_name, $object_string, $where_clause, 
			$param_year, $param_quarter, $param_month, $param_week,);
	my $array_ref_gtmu = $self->getArrayRefGTMu($array_ref_region);

	$hash_gtmu{"All"} = $array_ref_region;
	foreach my $element (@{$array_ref_gtmu}) {
		$hash_gtmu{$element} = $self->getArrayRefRegion($element);	
	}

	foreach my $hash_key (keys %hash_gtmu) { # hash_key is element like All, EU1, EU2...
		# Each array reference contains Regions
		#print "$hash_key processing ...\n";
		print ".";
		$hash_final{$hash_key} = $self->getArrayRefSL6($hash_gtmu{$hash_key}, $where_clause, 
			$param_year, $param_quarter, $param_month, $param_week,); 
		#print "$hash_key processed!\n";
	}

	foreach my $array_element (@{$array_ref_region}) {
		#print "$array_element processing ...\n";
		$hash_final{$array_element} = $self->getArrayRefSL6([$array_element], $where_clause,
		$param_year, $param_quarter, $param_month, $param_week,);
		#print "$array_element processed!\n";
	}

	return \%hash_final;
}

# method to prepare Region object
sub fetchObjectRegion {
	my $self =shift;
	my $where_clause = shift;
	my $param_year = shift;
	my $param_quarter = shift;
	my $param_month = shift;
	my $param_week = shift;
	my $dbh = $self->getMySQLDBHandle();

	my $object_string = 'region';
	my $table_name = $sql_string->getTableNameBookingDump();

	my %hash_final;
	# Sending query details to a helper method and receive Region array reference
	my $array_ref_region = $self->_runQueryReturnArrayRef($table_name, $object_string, $where_clause, 
	$param_year, $param_quarter, $param_month, $param_week,);
	my $array_ref_gtmu = $self->getArrayRefGTMu($array_ref_region);

	print ".";
	$hash_final{"All"} = $array_ref_region;
	foreach my $element (@{$array_ref_gtmu}) {
		$hash_final{$element} = $self->getArrayRefRegion($element);	
	}

	return \%hash_final;
}

# method to prepare GTMu array reference for the objct preparation
sub fetchArrayRefGTMu {
	my $self =shift;
	my $where_clause = shift;
	my $param_year = shift;
	my $param_quarter = shift;
	my $param_month = shift;
	my $param_week = shift;
	my $dbh = $self->getMySQLDBHandle();

	my $object_string = 'region';
	my $table_name = $sql_string->getTableNameBookingDump();

	# Sending query details to a helper method and receive Region array reference
#	print "fetchArrayrefGTMu: $where_clause\n";	
	print ".";
	my $array_ref_region = $self->_runQueryReturnArrayRef($table_name, $object_string, $where_clause, 
	$param_year, $param_quarter, $param_month, $param_week,);

	return $self->getArrayRefGTMu($array_ref_region);
}
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# $$$$$$$$$$$$$ END Object Preparation Block $$$$$$$$$$$$$
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$



# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# $$$$$$$$$$$$$$$$$ BEGIN Private Block $$$$$$$$$$$$$$$$$$
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

# method to help sub class to have MySQL Database Handle 
sub getMySQLDBHandle {
	my $self = shift;
	$self->{_rptUtilObj} = $rpt_util_obj->getMySQLDBH();
	return $self->{_rptUtilObj};
}

sub getCleanArrayRef {
	my ($self, @array) = @_;

	{ # Block to control the unnecessary 'uninitialized' warnings
		no warnings 'uninitialized';
		@array = uniq(@array);
		@array = grep{defined && /[^\s]/} @array;
		@array = sort @array;
	}
	return \@array;
}

sub _runQueryReturnArrayRef {
	my ($self, $table_name, $object_string, $where_clause, $param_year,												$param_quarter, $param_month, $param_week, $param) = @_;
	
	my $dbh = $self->getMySQLDBHandle();
	my @array_final;
	print ".";
	#print "runqueryreturn: $where_clause\n";	
	my $query = "SELECT DISTINCT " . $object_string . " from " . $table_name . $where_clause;
	my $qq_string = qq{$query};
	my $sth = $dbh->prepare($qq_string);
	#print "$where_clause\n";


	if (defined $param_year) {
		$sth->bind_param(1, $param_year);
		$sth->bind_param(2, $param) if defined $param;
	}
	if (defined $param_quarter) {
		$sth->bind_param(1, $param_year);
		$sth->bind_param(2, $param_quarter);
		$sth->bind_param(3, $param) if defined $param;
	}
	if (defined $param_month) {
		$sth->bind_param(1, $param_year);
		$sth->bind_param(2, $param_quarter);
		$sth->bind_param(3, $param_month);
		$sth->bind_param(4, $param) if defined $param;
	}
	if (defined $param_week) {
		$sth->bind_param(1, $param_year);
		$sth->bind_param(2, $param_quarter);
		$sth->bind_param(3, $param_month);
		$sth->bind_param(4, $param_week);
		$sth->bind_param(5, $param) if defined $param;
	}
	if (!defined $param_year && !defined $param_quarter && !defined $param_month && !defined $param_week) {
		$sth->bind_param(1, $param) if defined $param;
	}


	# Executing SQL query for Region
	$sth->execute() or die $DBI::errstr;

	while (my $href_mysql = $sth->fetchrow_hashref()) { # Iteration through Object Hash reference from MySQL
		push @array_final, $href_mysql->{$object_string}; # Copying iterated Object Strings into a lexical variable
	} # sth while loop end
	$sth->finish();

	return $self->getCleanArrayRef(@array_final);
}

# Method to prepare GTMu in an array reference
sub getArrayRefGTMu {
	my $self = shift;
	my $argument_arrayRef = shift;
	my @return_array;
	
	foreach my $region (@{$argument_arrayRef}) {
		given ($region) {
			when (/^SOUTH$/i) {
				push @return_array, "EU1";
			}
			when (/^WEST$/i) {
				push @return_array, "EU2";
			}
			when (/^(?:EAST|NORTH|SAARC|EAST&BDESH)$/i) {
				push @return_array, "EU3";
			}
			default {
				push @return_array, "COMM";
			}
		}
	}
	return $self->getCleanArrayRef(@return_array);
}

sub getArrayRefRegion {
	my $self = shift;
	my $gtmu = shift;
	given ($gtmu) {
		when (/^EU1$/i) {
			return ['SOUTH'];
		}
		when (/^EU2$/i) {
			return ['WEST'];
		}
		when (/^EU3$/i) {
			return ['EAST', 'NORTH', 'SAARC'];
		}
		when (/^COMM$/i) {
			return ['COMM'];
		}
		default {
			return ['COMM'];
		}
	}
}

# Method to prepare SL6 in an array reference
sub getArrayRefSL6 {
	my $self = shift;
	my $argument_arrayRef = shift;
	my $where_clause = shift;
	my $param_year = shift;
	my $param_quarter = shift;
	my $param_month = shift;
	my $param_week = shift;
	my @return_array;
	my $object_string = "sales_level_6";
	my $table_name = $sql_string->getTableNameBookingDump();
	my $array_ref_temp;
	my $dbh = $self->getMySQLDBHandle;
	my $where_clause_final;
	
	foreach my $region (@{$argument_arrayRef}) {
		if ((! defined $where_clause) || $where_clause eq " " || $where_clause eq "") {
			$where_clause_final = $where_clause . " WHERE region = ?";	
		} else {
			$where_clause_final = $where_clause . " AND region = ?";	
		}
		my $array_ref_temp = $self->_runQueryReturnArrayRef($table_name, $object_string, 								$where_clause_final, $param_year, $param_quarter, $param_month, $param_week, $region);
		push @return_array, @{$array_ref_temp};
	}
	return $self->getCleanArrayRef(@return_array);
}

# Method to prepare SL6 in an array reference
sub getArrayRefSalesAgents {
	my $self = shift;
	my $argument_arrayRef = shift;
	my $where_clause = shift;
	my $param_year = shift;
	my $param_quarter = shift;
	my $param_month = shift;
	my $param_week = shift;
	my $option = shift;
	my @return_array;
	my $object_string = "tbm";
	my $table_name = $sql_string->getTableNameBookingDump();
	my $array_ref_temp;
	my $dbh = $self->getMySQLDBHandle;
	my $where_clause_final;
	
	foreach my $element (@{$argument_arrayRef}) {
		if ((! defined $where_clause) || $where_clause eq " " || $where_clause eq "") {
			$where_clause_final = $where_clause . " WHERE region = ?" if $option eq "by region";
			$where_clause_final = $where_clause . " WHERE sales_level_6 = ?" if $option eq "by sl6";
		} else {
			$where_clause_final = $where_clause . " AND region = ?" if $option eq "by region";
			$where_clause_final = $where_clause . " AND sales_level_6 = ?" if $option eq "by sl6";
		}
		my $array_ref_temp = $self->_runQueryReturnArrayRef($table_name, $object_string, 								$where_clause_final, $param_year, $param_quarter, $param_month, $param_week, $element);
		push @return_array, @{$array_ref_temp};
	}
	return $self->getCleanArrayRef(@return_array);
}


# Method to prepare Partners in an array reference
sub getArrayRefPartners {
	my $self = shift;
	my $argument_arrayRef = shift;
	my $where_clause = shift;
	my $param_year = shift;
	my $param_quarter = shift;
	my $param_month = shift;
	my $param_week = shift;
	my $option = shift;
	my @return_array;
	my $object_string = "partner_name";
	my $table_name = $sql_string->getTableNameBookingDump();
	my $array_ref_temp;
	my $dbh = $self->getMySQLDBHandle;
	my $where_clause_final;
	
	foreach my $element (@{$argument_arrayRef}) {
		if ((! defined $where_clause) || $where_clause eq " " || $where_clause eq "") {
			$where_clause_final = $where_clause . " WHERE region = ?" if $option eq "by region";
			$where_clause_final = $where_clause . " WHERE sales_level_6 = ?" if $option eq "by sl6";
		} else {
			$where_clause_final = $where_clause . " AND region = ?" if $option eq "by region";
			$where_clause_final = $where_clause . " AND sales_level_6 = ?" if $option eq "by sl6";
		}
		my $array_ref_temp = $self->_runQueryReturnArrayRef($table_name, $object_string, 								$where_clause_final, $param_year, $param_quarter, $param_month, $param_week, $element);
		push @return_array, @{$array_ref_temp};
	}
	return $self->getCleanArrayRef(@return_array);
}

# Method to prepare Customers in an array reference
sub getArrayRefCustomers {
	my $self = shift;
	my $argument_arrayRef = shift;
	my $where_clause = shift;
	my $param_year = shift;
	my $param_quarter = shift;
	my $param_month = shift;
	my $param_week = shift;
	my $option = shift;
	my @return_array;
	my $object_string = "customer_name";
	my $table_name = $sql_string->getTableNameBookingDump();
	my $array_ref_temp;
	my $dbh = $self->getMySQLDBHandle;
	my $where_clause_final;
	
	foreach my $element (@{$argument_arrayRef}) {
		if ((! defined $where_clause) || $where_clause eq " " || $where_clause eq "") {
			$where_clause_final = $where_clause . " WHERE region = ?" if $option eq "by region";
			$where_clause_final = $where_clause . " WHERE sales_level_6 = ?" if $option eq "by sl6";
		} else {
			$where_clause_final = $where_clause . " AND region = ?" if $option eq "by region";
			$where_clause_final = $where_clause . " AND sales_level_6 = ?" if $option eq "by sl6";
		}
		my $array_ref_temp = $self->_runQueryReturnArrayRef($table_name, $object_string, $where_clause_final, $param_year, $param_quarter, $param_month, $param_week, $element);
		push @return_array, @{$array_ref_temp};
	}
	return $self->getCleanArrayRef(@return_array);
}


# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# $$$$$$$$$$$$$$$$$$ END Private Block $$$$$$$$$$$$$$$$$$$
# $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$


1;

