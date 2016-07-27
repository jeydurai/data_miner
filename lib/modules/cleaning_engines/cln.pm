package modules::cleaning_engines::cln;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use modules::helpers::connections::mysql_connection;
use Benchmark;
use strict;
use warnings;

sub new {
	my $class = shift;
	my $self = {};
	bless $self, $class;
	return $self;
}

sub cleanInit {
	# This is a Perl script to Initialize the cleaning
	# =================================================
	# Database Details
	# ================
	# Database: mysourcedata
	# Source Table: dump_from_finance and dump_from_finance_nri
	# tech_grand_master (get technology, arch1, arch2)

	# tech_grand_master is a sum of tech_spec(for <=FY12) and tech_spec1(for >=FY13)
	# tech_spec is TMS_Level_1 based and tech_spec1 is Bus_sub_entity based
	# While running script, either '-ri' OR '-nri' should be given as arguments
	# ========================================================================
	my ($self, $argv) = @_;
	my $conn_obj = modules::helpers::connections::mysql_connection->new();

	# Preparing Database Handle for MySQL.mysourcedata database
	my $dbh = $conn_obj->getMySQLConnection();

	# ===================================================================
	# Preliminary clean up
	# ===================================================================

	my $sth = undef;
	print "Deleting tech_master Table for clean up...\n";
	$sth = $dbh->prepare("DELETE FROM tech_master");
	$sth->execute();
	print "", $sth->rows, "row(s) got affected!", "\n";
	print "tech_master Table has been deleted!\n";

	print "Inserting new data in to tech_master Table...\n";
	$sth = $dbh->prepare("INSERT INTO tech_master SELECT DISTINCT
			RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) AS Tech_Code, 
			IFNULL((SELECT ts.Tech_Name_1 FROM tech_spec AS ts 
			WHERE RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) = ts.tech_code),'Others') AS Tech_Name_1, 
			IFNULL((SELECT ts2.arch1 FROM tech_spec AS ts2 
			WHERE RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) = ts2.tech_code),'Other') AS arch1, 
			IFNULL((SELECT ts3.arch2 FROM tech_spec AS ts3 
			WHERE RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) = ts3.tech_code),'Others') AS arch2 
			FROM dump_from_finance AS df ORDER BY tech_code");
	$sth->execute();
	print "", $sth->rows, "row(s) got affected!", "\n";
	print "New data in to tech_master Table has been inserted!\n";


	print "Deleting tech_master1 Table for clean up...\n";
	$sth = $dbh->prepare("DELETE FROM tech_master1");
	$sth->execute();
	print "", $sth->rows, "row(s) got affected!", "\n";
	print "tech_master1 Table has been deleted!\n";

	print "Inserting new data in to tech_master1 Table...\n";
	$sth = $dbh->prepare("INSERT INTO tech_master1 SELECT DISTINCT Internal_Sub_Business_Entity_Name AS Tech_Code, 
			IFNULL((SELECT ts.Tech_Name_1 FROM tech_spec1 AS ts WHERE Internal_Sub_Business_Entity_Name = ts.tech_code),'Others') AS Tech_Name_1, 
			IFNULL((SELECT ts2.arch1 FROM tech_spec1 AS ts2 WHERE Internal_Sub_Business_Entity_Name = ts2.tech_code),'Others') AS arch1, 
			IFNULL((SELECT ts3.arch2 FROM tech_spec1 AS ts3 WHERE Internal_Sub_Business_Entity_Name = ts3.tech_code),'Others') AS arch2	
			FROM dump_from_finance AS df ORDER BY tech_code");
	$sth->execute();
	print "", $sth->rows, "row(s) got affected!", "\n";
	print "New data in to tech_master Table has been inserted!\n";


	print "Deleting tech_grand_master Table for clean up...\n";
	$sth = $dbh->prepare("DELETE FROM tech_grand_master");
	$sth->execute();
	print "", $sth->rows, "row(s) got affected!", "\n";
	print "tech_grand_master Table has been deleted!\n";

	print "Inserting new data in to tech_grand_master Table...\n";
	$sth = $dbh->prepare("INSERT INTO mysourcedata.tech_grand_master SELECT * FROM mysourcedata.tech_master
							UNION ALL SELECT * FROM mysourcedata.tech_master1");
	$sth->execute();
	print "", $sth->rows, "row(s) got affected!", "\n";
	print "New data in to tech_grand_master Table has been inserted!\n";

	print "Dropping booking_dump Table...\n";
	$sth = $dbh->prepare("DROP TABLE booking_dump");
	$sth->execute();
	print "", $sth->rows, "row(s) got affected!", "\n";
	print "booking_dump Table has been dropped!\n";

	print "Dropping booking_dump_nri Table...\n";
	$sth = $dbh->prepare("DROP TABLE booking_dump_nri");
	$sth->execute();
	print "", $sth->rows, "row(s) got affected!", "\n";
	print "booking_dump_nri Table has been dropped!\n";


	print "Creating booking_dump Table...\n";
	$sth = $dbh->prepare("CREATE TABLE booking_dump LIKE booking_dump_template");
	$sth->execute();
	print "", $sth->rows, "row(s) got affected!", "\n";
	print "booking_dump Table has been created!\n";

	print "Creating booking_dump_nri Table...\n";
	$sth = $dbh->prepare("CREATE TABLE booking_dump_nri LIKE booking_dump_template");
	$sth->execute();
	print "", $sth->rows, "row(s) got affected!", "\n";
	print "booking_dump_nri Table has been created!\n";
	# Garbage Collection
	$sth->finish();	# get rid of "sth_source" Statement handle
	$conn_obj->disconnectMySQLConnection($dbh);
	$dbh = undef;
	print "===============================================================\n";
	print "===============================================================\n";
	print "\nCleaning Initialization completes!\n";
}

sub cleanFinanceBookingDump {
	# This is a Perl script to clean the finance raw dump and insert data into
	# fresh booking dump
	# ========================================================================
	# Database Details
	# ================
	# Database: mysourcedata
	# Source Table: dump_from_finance and dump_from_finance_nri
	# Resultant Table: booking_dump and booking_dump_nri
	# Supporting Table: customer_names(get unique customer name),
	# partner_names (get unique partner name), week_master(get financial week),
	# tech_grand_master (get technology, arch1, arch2)

	# tech_grand_master is a sum of tech_spec(for <=FY12) and tech_spec1(for >=FY13)
	# tech_spec is TMS_Level_1 based and tech_spec1 is Bus_sub_entity based
	# While running script, either '-ri' OR '-nri' should be given as arguments
	# ========================================================================
	my ($self, $argv) = @_;
	my $conn_obj = modules::helpers::connections::mysql_connection->new();

	# Preparing Database Handle for MySQL.mysourcedata database
	my $dbh = $conn_obj->getMySQLConnection();

	# ===================================================================
	# Table "booking_dump" clean up
	# ===================================================================

	my $sth_check_booking_dump_empty = undef;
	my $temp_table_name = undef;
	if ($argv eq "") {
		print "\nScript won't run without Arguments! Quiting now... Try again!\n";
	}
	if ($argv=~/-ri/i ) {
		$sth_check_booking_dump_empty = $dbh->prepare("SELECT * FROM booking_dump LIMIT 0,1");
		$temp_table_name = " Row(s) found in booking_dump Table \n";
	} elsif ($argv=~/-nri/i) {
		$sth_check_booking_dump_empty = $dbh->prepare("SELECT * FROM booking_dump_nri LIMIT 0,1");
		$temp_table_name = " Row(s) found in booking_dump_nri Table \n";
	} else {
		print "\nIncorrect Arguments! Quiting now... Try again!\n";
	}


	$sth_check_booking_dump_empty->execute()
		or die $DBI::errstr;
	if (my @temp_row = $sth_check_booking_dump_empty->fetchrow_array()) {
		print "", $sth_check_booking_dump_empty->rows, $temp_table_name;
		$sth_check_booking_dump_empty->finish();
		print "Proceed clean up contents in booking_dump Table (Yes/No)?: ";
		
		$_ = <STDIN>;	# Get user Input as either "Yes" or "No"
		chomp;
		
		if ($_ ne "") {	# If user doesn't give any input, then terminate
			# Clean Table "booking_dump"
			if (/^yes$/i) {	# Check for case-insensitive Input as "yes"
				print "DELETING Old data from 'booking_dump table'...\n";
				if ($argv eq "") {
					print "\nScript won't run without Arguments! Quiting now... Try again!\n";
					return;
				}
				if ($argv=~/-ri/i ) {
					$dbh->do("DELETE FROM booking_dump");
					print "DELETED Old data from 'booking_dump table'!\n";
				} elsif ($argv=~/-nri/i) {
					$dbh->do("DELETE FROM booking_dump_nri");
					print "DELETED Old data from 'booking_dump_nri table'!\n";
				} else {
					print "\nIncorrect Arguments! Quiting now... Try again!\n";
				}
			} else {
				print "Without cleaning up booking_dump Table, Inserting will be IMPOSSIBLE\n";
				print "Quiting...\n";
			}
		} else {
			print "\nIncorrect Answer to the question!\n";
		}
	}
	# ===================================================================
	# ===================================================================


		
	# Preparing Statement Handle for fetching the whole "dump_from_finance Table"
	my $sth_source=undef;
	if ($argv=~/-ri/i ) {
		$sth_source = $dbh->prepare("SELECT 	
						ID,
						AT_Attach,
						customer_name,
						ERP_Deal_ID,
						Sales_Order_Number_Detail,
						fiscal_period_id,
						fiscal_quarter_id,
						fiscal_period_id,
						fiscal_week_id,
						partner_name, 
						TBM,
						sales_level_5,
						Sales_Level_6,
						SCMS,
						Sub_SCMS,
						TMS_Level_1_Sales_Allocated,
						Technology_Group,
						Product_ID,
						Partner_Tier_Code,
						Ship_To_City,
						Booking_Net,
						TMS_Sales_Allocated_Bookings_Base_List,
						TMS_Sales_Allocated_Bookings_Quantity,
						Internal_Business_Entity_Name,
						Internal_Sub_Business_Entity_Name,
						Bill_To_Site_City,
						Product_Family,
						Product_ID,
						Bookings_Adjustments_Type,
						Partner_Certification,
						Partner_Type,
						prod_ser,
                        standard_cost
				FROM dump_from_finance WHERE Booking_Net<>0"
		);
	} elsif ($argv=~/-nri/i) {
		$sth_source = $dbh->prepare("SELECT 
						ID,
						AT_Attach,
						customer_name,
						ERP_Deal_ID,
						Sales_Order_Number_Detail,
						fiscal_period_id,
						fiscal_quarter_id,
						fiscal_period_id,
						fiscal_week_id,
						partner_name, 
						TBM,
						sales_level_5,
						Sales_Level_6,
						SCMS,
						Sub_SCMS,
						TMS_Level_1_Sales_Allocated,
						Technology_Group,
						Product_ID,
						Partner_Tier_Code,
						Ship_To_City,
						Booking_Net,
						TMS_Sales_Allocated_Bookings_Base_List,
						TMS_Sales_Allocated_Bookings_Quantity,
						Internal_Business_Entity_Name,
						Internal_Sub_Business_Entity_Name,
						Bill_To_Site_City,
						Product_Family,
						Product_ID,
						Bookings_Adjustments_Type,
						Partner_Certification,
						Partner_Type,
						prod_ser,
                        standard_cost
				FROM dump_from_finance_nri WHERE Booking_Net<>0"
		);
	}
	# Fetching the whole "dump_from_finance" Table records
	$sth_source->execute()
		or die $DBI::errstr;

	my $total_dump_from_finance_recs = $sth_source->rows;
	my $t0 = Benchmark->new;	# Record the time before entering the master loop
	my $start = time();			# Record the Start Time (for Real Time information)
	my $print_timer = 1;		# Print breaker to ensure for every interval only one time the info gets printed
	my $prev_exe_time = 0;		# Variable to store previous Execution Time for print breaker 
	my $prev_so_far_data=1;		# Variable to store previous intervals data processed
	my $rec_counter = 0;		# Source Database Record Counter
	print $total_dump_from_finance_recs, " Record(s) found: ", "\n";

	# This while loop will run as much as times the handle "sth_source" 
	# holds the records
	while (my @row = $sth_source->fetchrow_array()) {	# Assign fetched row by row in the array @row

		# ============================================================
		# Preparing statement handle for fetching the matched unique customer
		#	from customer_names table
		# ============================================================
		my $sth_customer_name = $dbh->prepare("SELECT unique_names, vertical FROM universal_unique_names WHERE names = ?");
		# Parameter Binding and Fetching the matched 
		# "customer_names" table
		$sth_customer_name->execute($row[2]) 
			or die $DBI::errstr;
		my (@row_unique_customer) = $sth_customer_name->fetchrow_array(); # Assign only the first record to the array @row2
		my ($unique_customer_name, $vertical) = @row_unique_customer; # Assign the unique customer in a local variable
		$sth_customer_name->finish();	# get rid of "sth_customer_name" Statement handle 
		# ============================================================
		
		# ============================================================
		# Sub string financial periods in simple readable form
		# ============================================================
		my $fp_year = substr($row[5], 0, 4);
		my $fp_quarter = substr($row[6], -2, 2);
		my $fp_month = substr($row[5], -2, 2);
		# ============================================================
		
		# ============================================================
		# Preparing statement handle for fetching the matched week_id
		# 	from week_master table
		# ============================================================
		my $sth_fiscal_week = $dbh->prepare("SELECT 
							fp_week
							FROM week_master
							WHERE fp_quarter = ?
							AND week_in_database = ?"
			);
		
		# From the source data table, extracting only last two letters of 
		# the fields...
		my $quarter_field_last_two = substr($row[6], -2); # $row[6] is "fiscal_quarter_id"
		my $week_field_last_two	   = substr($row[8], -2); # $row[8] is "fiscal_week_id"
		
		# Parameter Binding and Fetching the matched 
		# "week_master" table records 
		$sth_fiscal_week->execute($quarter_field_last_two, $week_field_last_two)
			or die $DBI::errstr;
		my (@row_fp_week) = $sth_fiscal_week->fetchrow_array(); # Assign only the first record to the array @row2
		my ($fp_week) = @row_fp_week; # Assign the unique week in a local variable
		$sth_fiscal_week->finish();	# get rid of "sth_fiscal_week" Statement handle 
		# ============================================================

		# ============================================================
		# Preparing statement handle for fetching the matched unique partner
		# 	from "partner_names" table
		# ============================================================
		my $sth_partner_name = $dbh->prepare("SELECT 
							unique_names
							FROM universal_unique_names 
							WHERE names = ?"
			);
		# Parameter Binding and Fetching the matched 
		# "partner_names" table records
		$sth_partner_name->execute($row[9])
			or die $DBI::errstr;
		my (@row_unique_partner) = $sth_partner_name->fetchrow_array(); # Assign only the first record to the array @row2
		my ($unique_partner_name) = @row_unique_partner; # Assign the unique customer in a local variable
		$sth_partner_name->finish();	# get rid of "sth_partner_name" Statement handle 
		# ============================================================

		# ============================================================
		# Fetch Sales_Level_5 to obtain Region & GTMu
		# ============================================================
		my $sth_gtmu = $dbh->prepare("SELECT region, gtmu FROM node_mapper WHERE sales_level_5 = ?");
		# Parameter Binding and Fetching the matched region & gtmu from
        # node_mapper table
		$sth_gtmu->execute($row[11]) 
			or die $DBI::errstr;
		my (@row_sth_gtmu) = $sth_gtmu->fetchrow_array(); # Assign only the first record to the array @row2
		my ($region, $unique_gtmu) = @row_sth_gtmu; # Assign the region and gtmu in a local variable
		$sth_gtmu->finish();	# get rid of "sth_gtmu" Statement handle 
		# ============================================================
		
		# ============================================================
		# Sub string Sub_SCMS to remove "COMM" text
		# ============================================================
		my $sub_scms = substr($row[14], index($row[14], "_")+1, 
							  length($row[14])-index($row[14], "_")
							 );
		# ============================================================
		
		# ============================================================
		# Sub string Technology code from TMS_Level_1
		# ============================================================
		my $tech_code = "";
		if ($fp_year <= 2012) {
			$tech_code = substr($row[15], index($row[15], "-")+1, 
								   length($row[15])-index($row[15], "-")
								  );
		} else {
			$tech_code = $row[24];
		}
		# Preparing statement handle for fetching the matched technology name
		# 	from "tech_master" table
		my $sth_tech_name = $dbh->prepare("SELECT 
							tech_name_1,
							arch1,
							arch2
					   FROM tech_grand_master 
					   WHERE tech_code = ?"
			);
		$sth_tech_name->execute($tech_code)
			or die $DBI::errstr;
		my (@row_tech_name) = $sth_tech_name->fetchrow_array(); # Assign only the first record to the array @row2
		my ($tech_name, $arch1, $arch2) = @row_tech_name;  # Assign the unique customer in a local variable
		$sth_tech_name->finish();	# get rid of "sth_tech_name" Statement handle 
		# ============================================================

		# ============================================================
		# TAM/VSAM from Unique_Sales_Agents
		# ============================================================
		my $mapped_id;
		my $mapped_name;
		my $mapped_type;
		my $vs_team_node;
		my $mapped_sales_level_6;
		my $mapped_sub_scms;
		my $mapped_region;
		my $mapped_gtmu;
		my $mapped_id_l4;
		my $mapped_name_l4;
		my $mapped_type_l4;
		my $mapped_id_l3;
		my $mapped_name_l3;
		my $mapped_type_l3;
		my $mapped_id_l2;
		my $mapped_name_l2;
		my $mapped_type_l2;
		my $mapped_id_l1;
		my $mapped_name_l1;
		my $mapped_type_l1;
		my $mapped_id_l0;
		my $mapped_name_l0;
		my $mapped_type_l0;
		
		# Preparing statement handle for fetching the matched technology name
		# 	from "tech_master" table
		my $sth_tam_vsam = $dbh->prepare("SELECT * FROM unique_sales_agents WHERE sales_agent_name = ? AND sales_level_6=?");
		$sth_tam_vsam->execute($row[10], $row[12])
			or die $DBI::errstr;
		# Get mysql data rows in a scalar variable as hash reference

		while (my $tam_vsam = $sth_tam_vsam->fetchrow_hashref()) {
			$mapped_sales_level_6 = $tam_vsam->{"Mapped_Sales_Level_6"};
			$mapped_sub_scms = $tam_vsam->{"Mapped_Sub_SCMS"};
			$mapped_region = $tam_vsam->{"Mapped_Region"};
			$mapped_gtmu = $tam_vsam->{"Mapped_GTMu"};
			$mapped_id = $tam_vsam->{"Mapped_id"};
			$mapped_name = $tam_vsam->{"Mapped_Name"};
			$mapped_type = $tam_vsam->{"Mapped_Type"};
			$vs_team_node = $tam_vsam->{"VS_team_node"};
			$mapped_id_l4 = $tam_vsam->{"Mapped_id_L4"};
			$mapped_name_l4 = $tam_vsam->{"Mapped_Name_L4"};
			$mapped_type_l4 = $tam_vsam->{"Mapped_Type_L4"};
			$mapped_id_l3 = $tam_vsam->{"Mapped_id_L3"};
			$mapped_name_l3 = $tam_vsam->{"Mapped_Name_L3"};
			$mapped_type_l3 = $tam_vsam->{"Mapped_Type_L3"};
			$mapped_id_l2 = $tam_vsam->{"Mapped_id_L2"};
			$mapped_name_l2 = $tam_vsam->{"Mapped_Name_L2"};
			$mapped_type_l2 = $tam_vsam->{"Mapped_Type_L2"};
			$mapped_id_l1 = $tam_vsam->{"Mapped_id_L1"};
			$mapped_name_l1 = $tam_vsam->{"Mapped_Name_L1"};
			$mapped_type_l1 = $tam_vsam->{"Mapped_Type_L1"};
			$mapped_id_l0 = $tam_vsam->{"Mapped_id_L0"};
			$mapped_name_l0 = $tam_vsam->{"Mapped_Name_L0"};
			$mapped_type_l0 = $tam_vsam->{"Mapped_Type_L0"};
		}

		# Assign lexical variable with fetched hash reference data
		$sth_tam_vsam->finish();	# get rid of "sth_tam_vsam" Statement handle 
		# ============================================================

		# ============================================================
		# IOT Portfolio from IOT Portfolios
		# ============================================================
		my $iot_portfolio = "";
		# Preparing statement handle for fetching the matched IOT Portfolio
		# 	from "iot_portfolios" table
		my $sth_iot = $dbh->prepare("SELECT 
						iot_portfolio
					   FROM iot_portfolios 
					   WHERE Product_Fam_id = ?
					   OR Product_Fam_id = ?"
			);
		$sth_iot->execute($row[26], $row[27])
			or die $DBI::errstr;
		if ($sth_iot->rows != 0) {
			my (@row_iot) = $sth_iot->fetchrow_array(); # Assign only the first record to the array @row2
			($iot_portfolio) = @row_iot;  # Assign the IOT portfolio in a local variable
		} else {
			$iot_portfolio = "NA";
		}
		$sth_iot->finish();	# get rid of "sth_iot" Statement handle 
		# ============================================================

		# ===================================================================
		# Unique City, State and Country information from unique_cities table
		# ===================================================================
		my $unique_city = "";
		my $unique_state = "";
		my $unique_country = "";
		# Preparing statement handle for fetching the matched Unique Cities
		# 	from "unique_cities" table
		my $sth_unique_cities = $dbh->prepare("SELECT 
						unique_city, unique_state, unique_country
					   FROM unique_cities 
					   WHERE sales_level_6 = ?
					   AND sales_agent = ? 
					   AND Bill_To_Site_City = ? 
					   AND Ship_To_City = ?"
			);
		$sth_unique_cities->execute($row[12], $row[10], $row[25], $row[19])
			or die $DBI::errstr;
		if ($sth_unique_cities->rows != 0) {
			my (@row_cities) = $sth_unique_cities->fetchrow_array(); # Assign only the first record to the array @row2
			($unique_city, $unique_state, $unique_country) = @row_cities;  # Assign the City information in a local variable
		} else {
			$unique_city = "NA";
			$unique_state = "NA";
			$unique_country = "NA";
		}
		$sth_unique_cities->finish();	# get rid of "$sth_unique_cities" Statement handle 
		# ============================================================



		# ============================================================
		# Handling Float Numbers Accuracy of Booking Net and Base List
		# ============================================================
		my $booking_net = sprintf("%53.10f", $row[20]);
		$booking_net =~ s/^\s+//;
		my $base_list = sprintf("%53.10f", $row[21]);
		$base_list =~ s/^\s+//;
		# ============================================================
		
		
		# ==============================================================
		# Processing Time Handler
		# ==============================================================
		my $inbetween = time();	# Record current time inside the loop
		my $exe_time = $inbetween - $start;	# Difference between initial time and current time
		$rec_counter++;
		if ($prev_exe_time != $exe_time) { # Trigger $print_timer to 1 if the execution time has changed to new interval
			$print_timer = 1;
		}
		
		if ((($exe_time % 5) == 0) && ($print_timer == 1)) { # Check whether the same interval is repeated and print the processing information accordingly
		
			my $process_speed_in_recs = ($rec_counter-$prev_so_far_data+1)/5;	# Processing speed - No/. Records per Second
			my $process_speed_in_secs = 5/($rec_counter-$prev_so_far_data+1);	# Processing speed - No/. Seconds per Record
			my $exp_secs = $process_speed_in_secs*($total_dump_from_finance_recs-$rec_counter+1); # Expected Time in seconds
			my $exp_hour = int($exp_secs/(60*60));	# Expected Time in Hours
			my $exp_min = int($exp_secs/60);	# Expected Time in Minutes
			$prev_so_far_data = $rec_counter;	# Current so far data is stored as Previous So far data
			# Printing the Processing Information
			printf "Elapsed %6d", $exe_time;
			print " secs/prcssd: (";
			printf "%10d", $rec_counter;
			print ")/(";
			printf "%10d", $total_dump_from_finance_recs;
			print ") @ ";
			printf "%5d", $process_speed_in_recs;
			print " RPS|";
			printf "%6.5f", $process_speed_in_secs;
			print " SPR/ET: ";
			printf "%2d hrs|%4d mins|%5d secs\n", $exp_hour, $exp_min, $exp_secs;
			
			
			$prev_exe_time = $exe_time;
			$print_timer = 0;	# $print_timer is made 0 so that in a definite interval, the information is not repeatedly printed
		}

		
	# ============================================================
	# Preparing statement handle to Insert cleaned data in to 
	#	"booking_dump" table
	# ============================================================
		my $sth_booking_dump_insert=undef;
		if ($argv=~/-ri/i ) {
			$sth_booking_dump_insert = $dbh->prepare("INSERT INTO booking_dump
								  (ID,
								   At_Attach,
								   Account_Name,
								   Customer_Name,
								   ERP_Deal_ID,
								   Sales_Order_Number_Detail,
								   FP_Year,
								   FP_Quarter,
								   FP_Month,
								   FP_Week,
								   Partner,
								   Partner_Name,
								   TBM,
								   Region,
								   Sales_Level_6,
								   SCMS,
								   Sub_SCMS,
								   TMS_Level_1_Sales_Allocated,
								   Tech_Name,
								   Tech_Code,
								   Technology_Group,
								   Partner_Tier_Code,
								   Ship_To_City,
								   Booking_Net,
								   Base_List,
								   TMS_Sales_Allocated_Bookings_Quantity,
								   Internal_Business_Entity_Name,
								   Internal_Sub_Business_Entity_Name,
								   arch1,
								   arch2,
								   Product_ID,
								   Mapped_id,
								   Mapped_name,
								   Mapped_type,
								   VS_team_node,
								   Bill_To_Site_City,
								   Vertical,
								   iot_portfolio,
								   GTMu,
								   Product_Family,
								   Booking_Adjustment,
								   Partner_Certification,
								   Partner_Type,
								   Mapped_Sales_Level_6,
								   Mapped_Sub_SCMS,
								   Mapped_Region,
								   Mapped_GTMu,
								   Mapped_id_L4,
								   Mapped_Name_L4,
								   Mapped_Type_L4,
								   Mapped_id_L3,
								   Mapped_Name_L3,
								   Mapped_Type_L3,
								   Mapped_id_L2,
								   Mapped_Name_L2,
								   Mapped_Type_L2,
								   Mapped_id_L1,
								   Mapped_Name_L1,
								   Mapped_Type_L1,
								   Mapped_id_L0,
								   Mapped_Name_L0,
								   Mapped_Type_L0,
								   unique_city,
								   unique_state,
								   unique_country,
								   prod_ser,
                                   standard_cost
								   )
								  VALUES
								   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
								?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
								?, ?, ?, ?, ?, ?, ?, ?, 
								?, ?, ?, ?, ?, ?, ?, ?,
								?, ?, ?, ?, ?, ?, ?, ?, ?,
								?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
								?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
								?, ?
								   )"
			);
		} elsif ($argv=~/-nri/i) {
			$sth_booking_dump_insert = $dbh->prepare("INSERT INTO booking_dump_nri
								  (ID,
								   At_Attach,
								   Account_Name,
								   Customer_Name,
								   ERP_Deal_ID,
								   Sales_Order_Number_Detail,
								   FP_Year,
								   FP_Quarter,
								   FP_Month,
								   FP_Week,
								   Partner,
								   Partner_Name,
								   TBM,
								   Region,
								   Sales_Level_6,
								   SCMS,
								   Sub_SCMS,
								   TMS_Level_1_Sales_Allocated,
								   Tech_Name,
								   Tech_Code,
								   Technology_Group,
								   Partner_Tier_Code,
								   Ship_To_City,
								   Booking_Net,
								   Base_List,
								   TMS_Sales_Allocated_Bookings_Quantity,
								   Internal_Business_Entity_Name,
								   Internal_Sub_Business_Entity_Name,
								   arch1,
								   arch2,
								   Product_ID,
								   Mapped_id,
								   Mapped_name,
								   Mapped_type,
								   VS_team_node,
								   Bill_To_Site_City,
								   Vertical,
								   iot_portfolio,
								   GTMu,
								   Product_Family,
								   Booking_Adjustment,
								   Partner_Certification,
								   Partner_Type,
								   Mapped_Sales_Level_6,
								   Mapped_Sub_SCMS,
								   Mapped_Region,
								   Mapped_GTMu,
								   Mapped_id_L4,
								   Mapped_Name_L4,
								   Mapped_Type_L4,
								   Mapped_id_L3,
								   Mapped_Name_L3,
								   Mapped_Type_L3,
								   Mapped_id_L2,
								   Mapped_Name_L2,
								   Mapped_Type_L2,
								   Mapped_id_L1,
								   Mapped_Name_L1,
								   Mapped_Type_L1,
								   Mapped_id_L0,
								   Mapped_Name_L0,
								   Mapped_Type_L0,
								   unique_city,
								   unique_state,
								   unique_country,
								   prod_ser,
                                   standard_cost
								   )
								  VALUES
								   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
								?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
								?, ?, ?, ?, ?, ?, ?, ?, 
								?, ?, ?, ?, ?, ?, ?, ?,
								?, ?, ?, ?, ?, ?, ?, ?, ?,
								?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
								?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
								?, ?)"
			);
		}
		$sth_booking_dump_insert->execute($row[0], $row[1], $row[2], 
						  $unique_customer_name, $row[3],
						  $row[4], $fp_year, $fp_quarter,
						  $fp_month, $fp_week, $row[9],
						  $unique_partner_name, $row[10],
						  $region, $row[12], $row[13], $sub_scms,
						  $row[15], $tech_name, $tech_code,
						  $row[16], $row[18], $row[19], 
						  $booking_net, $base_list, $row[22],
						  $row[23], $row[24], $arch1, $arch2, $row[17],
						  $mapped_id, $mapped_name, $mapped_type,
						  $vs_team_node, $row[25], $vertical, 
						  $iot_portfolio, $unique_gtmu, $row[26], $row[28],
						  $row[29], $row[30], 
						  $mapped_sales_level_6, $mapped_sub_scms, $mapped_region,
						  $mapped_gtmu, $mapped_id_l4, $mapped_name_l4, $mapped_type_l4,
						  $mapped_id_l3, $mapped_name_l3, $mapped_type_l3,
						  $mapped_id_l2, $mapped_name_l2, $mapped_type_l2,
						  $mapped_id_l1, $mapped_name_l1, $mapped_type_l1,
						  $mapped_id_l0, $mapped_name_l0, $mapped_type_l0,
						  $unique_city, $unique_state, $unique_country, $row[31],
                          $row[32]
			)
			or die $DBI::errstr;
		$sth_booking_dump_insert->finish();
	}

	# =================================================================
	# =====================================================================
	# Print final processing time
	# =====================================================================
	my $t1 = Benchmark->new; # Record the time after the master loop completion
	my $td = timediff($t1,$t0);	# Assign the loop running time in a local variable
	print "\n\nTotal time elapsed to update 'booking_dump table': ", timestr($td), "\n";
	# =====================================================================

	# Garbage Collection
	$sth_source->finish();	# get rid of "sth_source" Statement handle
	$conn_obj->disconnectMySQLConnection($dbh);
	$dbh = undef;
}
1;
