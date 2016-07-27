package modules::reports::comm_reports;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
no warnings 'uninitialized';
use modules::reports::report_maker;
use modules::helpers::calculator;
use Scalar::Util qw(looks_like_number);
use v5.14;
use XML::Dumper;
#use Algorithm::Permute;
use Array::Utils qw(:all);
use Text::Trim qw(trim);
use Data::Dumper;
use IO::Handle;
use Excel::Writer::XLSX;
use Excel::Writer::XLSX::Utility;

our @ISA = qw(modules::reports::report_maker);



#no warnings qw(experimental::smartmatch experimental::lexical_topic experimental::regex_sets experimental::lexical_subs);

sub new {
    my $class = shift;
    my $self = {};
    bless $self, $class;
    return $self;
}

sub printProgress {
    my ($self, $counter, $loop_count) = @_;
    my $calc = modules::helpers::calculator->new();
    my $process = $calc->getRatio($counter++, $loop_count);
    my $process_text = "Processing $process% ...";
    unless ($counter == 1) {
        my $text_length = length $process_text;
        $text_length += 10;
        my $erase_text = "\b" x $text_length;
        print $erase_text;
    }
    print $process_text;
}

sub parseReportPeriod {
    my $self = shift;
    my $period = shift;

    my $month_to;
    my $month_from = 1;

    given ($period) {
        when(/m1/i) {
            $month_to = 1;
        }
        when(/m2/i) {
            $month_to = 2;
        }
        when(/m3/i) {
            $month_to = 3;
        }
        when(/m4/i) {
            $month_to = 4;
        }
        when(/m5/i) {
            $month_to = 5;
        }
        when(/m6/i) {
            $month_to = 6;
        }
        when(/m7/i) {
            $month_to = 7;
        }
        when(/m8/i) {
            $month_to = 8;
        }
        when(/m9/i) {
            $month_to = 9;
        }
        when(/m10/i) {
            $month_to = 10;
        }
        when(/m11/i) {
            $month_to = 11;
        }
        when(/m12/i) {
            $month_to = 12;
        }
        when(/q1/i) {
            $month_to = 3;
        }
        when(/q2/i) {
            $month_from = 4;
            $month_to = 6;
        }
        when(/q3/i) {
            $month_from = 7;
            $month_to = 9;
        }
        when(/q4/i) {
            $month_from = 10;
            $month_to = 12;
        }
        when(/h1/i) {
            $month_to = 6;
        }
        when(/h2/i) {
            $month_from = 7;
            $month_to = 12;
        }
    }
    return ($month_from, $month_to);
}


sub getCommSnapshot {
    
    #Receive all Parameters
    my ($self, $node_level, $period) = @_;
    print "Option1: $node_level\n"; print "Option2: $period\n";

    # Variable Declaration & Initialization
    my $data_hash; my $calc = modules::helpers::calculator->new();
    
    my %sub_scmss = $self->getSubSCMSHash(); 

    my ($month_from, $month_to) = $self->parseReportPeriod($period);
    my %nodes = $self->getStateHash($node_level); # Fetch all State wise data
    my ($latest_year, $prev_year) = $self->getBookingDumpLatestYear();
    my $latest_tbl_name = $self->dropTable("latest_year_booking_dump");
    my $book_dump_tbl_name = $self->createLikeTable($latest_tbl_name,"booking_dump");
    $self->copyTable($latest_tbl_name, $book_dump_tbl_name, $latest_year); 
    my @techs = $self->getUniqueArray("Tech_Name", $latest_tbl_name); 
    my @archs = $self->getUniqueArray("arch2", $latest_tbl_name); 
    my @verticals = $self->getUniqueArray("vertical", $latest_tbl_name); 
    my @prod_sers = $self->getUniqueArray("prod_ser", $latest_tbl_name); 
    
    my $loop_count = $calc->permuteArrayAndHash(@prod_sers, %nodes);

    my $counter = 2 ;
    my $loop_count2 = scalar keys %nodes;
    my $break = $loop_count2 + $counter;
    STDOUT->autoflush(1);
    my $file_name = 'C:\\Jeyaraj\\Analysis\\Prem\\'. $node_level.'.xlsx';
    my $xl_book = Excel::Writer::XLSX->new($file_name);
    my $xl_sheet = $xl_book->add_worksheet();
    my $format_header = $xl_book->add_format();
    my $format = $xl_book->add_format();
    my $format_num = $xl_book->add_format();
    my $format_per = $xl_book->add_format();
    $format_header->set_bold(); $format_header->set_border(1);
    $format_header->set_bg_color('#800080'); $format_header->set_fg_color('white');
    $format->set_border(1);
    $format_num->set_num_format('$#,##0'); $format_num->set_border(1);
    $format_per->set_num_format('0.00%'); $format_per->set_border(1);
    my $header_trigger = 1;
    foreach my $prod_ser (@prod_sers) {
        foreach my $state (keys %nodes) {
            my $key = $state."|".$prod_ser;
            my %hsh = $self->getFinDataByStatePS($latest_year, $month_from, $month_to, $nodes{$state}, $prod_ser);
            # Current year Data
            $data_hash->{$key}{state} = $state;
            $data_hash->{$key}{prod_ser} = $prod_ser;
            $data_hash->{$key}{c_yr_booking} = $hsh{c_yr_booking};
            $data_hash->{$key}{c_yr_baselist} = $hsh{c_yr_baselist};
            $data_hash->{$key}{c_yr_stdcost} = $hsh{c_yr_stdcost};

            #Previous Year Data
            $data_hash->{$key}{p_yr_booking} = $hsh{p_yr_booking};
            $data_hash->{$key}{p_yr_baselist} = $hsh{p_yr_baselist};
            $data_hash->{$key}{p_yr_stdcost} = $hsh{p_yr_stdcost};

            #Derived Data
            $data_hash->{$key}{c_yr_discount} = $hsh{c_yr_discount} ;
            $data_hash->{$key}{p_yr_discount} = $hsh{p_yr_discount};
            $data_hash->{$key}{c_yr_stdmargin} = $hsh{c_yr_stdmargin};
            $data_hash->{$key}{p_yr_stdmargin} = $hsh{p_yr_stdmargin};
            $data_hash->{$key}{yoy_booking} = $hsh{yoy_booking};
            $data_hash->{$key}{yoy_discount} = $hsh{yoy_discount};
            $data_hash->{$key}{yoy_stdmargin} = $hsh{yoy_stdmargin};
            #$self->printProgress($counter++, $loop_count);

            my $gtmu_rng = "B".$counter; 
            my $act_booking_rng = "C".$counter;
            my $yoy_booking_rng = "D".$counter; 
            my $of_forecast_rng = "E".$counter; 
            my $of_plan_rng = "F".$counter;
            my $act_discount_rng = "H".$counter; 
            my $yoy_discount_rng = "I".$counter; 
            my $act_stdmargin_rng = "K".$counter; 
            my $yoy_stdmargin_rng = "L".$counter;
            my $counter_rng = "N".$counter;
            my $break_rng = "O".$counter;
            my $reminder_rng = "P".$counter;

            if ($header_trigger) {
                $header_trigger = 0;
                $xl_sheet->write($gtmu_rng, "GTMu", $format_header); 
                $xl_sheet->write($act_booking_rng, "Actual", $format_header);
                $xl_sheet->write($yoy_booking_rng, "YoY", $format_header);
                $xl_sheet->write($of_forecast_rng, "%Fcst", $format_header); 
                $xl_sheet->write($of_plan_rng, "%Plan", $format_header);
                $xl_sheet->write($act_discount_rng, "Actual", $format_header); 
                $xl_sheet->write($yoy_discount_rng, "YoY", $format_header);
                $xl_sheet->write($act_stdmargin_rng, "Actual", $format_header); 
                $xl_sheet->write($yoy_stdmargin_rng, "YoY", $format_header);
                #$xl_sheet->write($counter_rng, $counter, $format_header);
                #$xl_sheet->write($break_rng, $break, $format_header);
                #$xl_sheet->write($reminder_rng, $counter % $break, $format_header);

                $counter++;
                $gtmu_rng = "B".$counter; 
                $act_booking_rng = "C".$counter;
                $yoy_booking_rng = "D".$counter; 
                $of_forecast_rng = "E".$counter; 
                $of_plan_rng = "F".$counter;
                $act_discount_rng = "H".$counter; 
                $yoy_discount_rng = "I".$counter; 
                $act_stdmargin_rng = "K".$counter; 
                $yoy_stdmargin_rng = "L".$counter;
                $counter_rng = "N".$counter;
                $break_rng = "O".$counter;
                $reminder_rng = "P".$counter;

                $xl_sheet->write($gtmu_rng, $data_hash->{$key}{state}, $format); 
                $xl_sheet->write($act_booking_rng, $data_hash->{$key}{c_yr_booking}, $format_num);
                $xl_sheet->write($yoy_booking_rng, $data_hash->{$key}{yoy_booking}, $format_per);
                #$xl_sheet->write($of_forecast_rng, $data_hash->{$key}{}; 
                #$xl_sheet->write($of_plan_rng, $data_hash->{$key}{};
                $xl_sheet->write($act_discount_rng, $data_hash->{$key}{c_yr_discount}, $format_per); 
                $xl_sheet->write($yoy_discount_rng, $data_hash->{$key}{yoy_discount}, $format_per);
                $xl_sheet->write($act_stdmargin_rng, $data_hash->{$key}{c_yr_stdmargin}, $format_per); 
                $xl_sheet->write($yoy_stdmargin_rng, $data_hash->{$key}{yoy_stdmargin}, $format_per);
                #$xl_sheet->write($counter_rng, $counter, $format);
                #$xl_sheet->write($break_rng, $break, $format);
                #$xl_sheet->write($reminder_rng, $counter % $break, $format);
            } else {
                $xl_sheet->write($gtmu_rng, $data_hash->{$key}{state}, $format); 
                $xl_sheet->write($act_booking_rng, $data_hash->{$key}{c_yr_booking}, $format_num);
                $xl_sheet->write($yoy_booking_rng, $data_hash->{$key}{yoy_booking}, $format_per);
                #$xl_sheet->write($of_forecast_rng, $data_hash->{$key}{}, $format); 
                #$xl_sheet->write($of_plan_rng, $data_hash->{$key}{}, $format);
                $xl_sheet->write($act_discount_rng, $data_hash->{$key}{c_yr_discount}, $format_per); 
                $xl_sheet->write($yoy_discount_rng, $data_hash->{$key}{yoy_discount}, $format_per);
                $xl_sheet->write($act_stdmargin_rng, $data_hash->{$key}{c_yr_stdmargin}, $format_per); 
                $xl_sheet->write($yoy_stdmargin_rng, $data_hash->{$key}{yoy_stdmargin}, $format_per);
                #$xl_sheet->write($counter_rng, $counter, $format);
                #$xl_sheet->write($break_rng, $break, $format);
                #$xl_sheet->write($reminder_rng, $counter % $break, $format);
            }
            my $fmtd_c_yr_booking = $hsh{f_c_yr_booking};
            my $fmtd_c_yr_list = $hsh{f_c_yr_list};
            my $fmtd_c_yr_std_cost = $hsh{f_c_yr_stdcost};
            my $fmtd_p_yr_booking = $hsh{f_p_yr_booking};
            my $fmtd_p_yr_list = $hsh{f_p_yr_list};
            my $fmtd_p_yr_std_cost = $hsh{f_p_yr_stdcost};

            my $fmtd_yoy_booking = $hsh{f_yoy_booking};
            my $fmtd_yoy_discount = $hsh{f_yoy_discount};
            my $fmtd_yoy_stdmargin = $hsh{f_yoy_stdmargin};

            print "Key: $key\n";
            print "Product/Service: $data_hash->{$key}{prod_ser}\n";
            print "State: $data_hash->{$key}{state}\n";
            print "Current Yr. Booking: $fmtd_c_yr_booking\n";
            print "YoY Booking: $fmtd_yoy_booking \n";
            print "Current Yr. BaseList: $fmtd_c_yr_list\n";
            print "YoY Discount $fmtd_yoy_discount\n";
            print "Current Year Std. Cost: $fmtd_c_yr_std_cost\n";
            print "YoY STD. Margin $fmtd_yoy_stdmargin\n";
            print "Previous Yr. Booking: $fmtd_p_yr_booking\n";
            print "Previous Yr. BaseList: $fmtd_p_yr_list\n";
            print "Previous Year Std. Cost: $fmtd_p_yr_std_cost\n";
            print "\n";
            if ($break <= $counter && ($counter % $break == 0)) {
                $counter += 2;
                $header_trigger = 1;
            } else {
                $counter++ ;
                $header_trigger = 0;
            }
        } # foreach
    } # foreach
    print "\nAll state wise data has been acquired!\n";
    print "\n";
} # sub


sub getCommSnapshot2 {
    
    #Receive all Parameters
    my ($self, $node_level, $period) = @_;
    print "Option1: $node_level\n"; print "Option2: $period\n";

    # Variable Declaration & Initialization
    
    my %sub_scmss = $self->getSubSCMSHash(); 

    my ($month_from, $month_to) = $self->parseReportPeriod($period);
    my %nodes = $self->getStateHash($node_level); # Fetch all State wise data
    my ($latest_year, $prev_year) = $self->getBookingDumpLatestYear();
    my $latest_tbl_name = $self->dropTable("latest_year_booking_dump");
    my $book_dump_tbl_name = $self->createLikeTable($latest_tbl_name,"booking_dump");
    $self->copyTable($latest_tbl_name, $book_dump_tbl_name, $latest_year); 
    my @techs = $self->getUniqueArray("Tech_Name", $latest_tbl_name); 
    my @archs = $self->getUniqueArray("arch2", $latest_tbl_name); 
    my @verticals = $self->getUniqueArray("vertical", $latest_tbl_name); 
    my @prod_sers = $self->getUniqueArray("prod_ser", $latest_tbl_name); 
  
    my %periods = (
        "latest_year" => $latest_year,
        "prev_year" => $prev_year,
        "month_from" => $month_from,
        "month_to" => $month_to
    ); 

    my $file_name = 'C:\\Jeyaraj\\Analysis\\Prem\\'. $node_level.'.xlsx';

    # Write in Excel
    print "Writing in Excel...\n";
    my $hsh = $self->writeSnapShotAsExcel(\%nodes, \@prod_sers, \%periods, $file_name);
    print "Completed Writing in Excel!\n";
    STDOUT->autoflush(1);
    foreach my $prod_ser (@prod_sers) {
        foreach my $state (keys %nodes) {
            my $key = $state."|".$prod_ser;
            my $fmtd_c_yr_booking = $hsh->{$key}{f_c_yr_booking};
            my $fmtd_c_yr_list = $hsh->{$key}{f_c_yr_list};
            my $fmtd_c_yr_std_cost = $hsh->{$key}{f_c_yr_stdcost};
            my $fmtd_p_yr_booking = $hsh->{$key}{f_p_yr_booking};
            my $fmtd_p_yr_list = $hsh->{$key}{f_p_yr_list};
            my $fmtd_p_yr_std_cost = $hsh->{$key}{f_p_yr_stdcost};

            my $fmtd_yoy_booking = $hsh->{$key}{f_yoy_booking};
            my $fmtd_yoy_discount = $hsh->{$key}{f_yoy_discount};
            my $fmtd_yoy_stdmargin = $hsh->{$key}{f_yoy_stdmargin};

            print "Key: $key\n";
            print "Product/Service: $prod_ser\n";
            print "State: $state}\n";
            print "Current Yr. Booking: $fmtd_c_yr_booking\n";
            print "YoY Booking: $fmtd_yoy_booking \n";
            print "Current Yr. BaseList: $fmtd_c_yr_list\n";
            print "YoY Discount $fmtd_yoy_discount\n";
            print "Current Year Std. Cost: $fmtd_c_yr_std_cost\n";
            print "YoY STD. Margin $fmtd_yoy_stdmargin\n";
            print "Previous Yr. Booking: $fmtd_p_yr_booking\n";
            print "Previous Yr. BaseList: $fmtd_p_yr_list\n";
            print "Previous Year Std. Cost: $fmtd_p_yr_std_cost\n";
            print "\n";
        } # foreach
    } # foreach
    print "\nAll state wise data has been acquired!\n";
    print "\n";
} # sub

sub writeSnapShotAsExcel {
    my ($self, $nds, $prods, $perds, $file_name) = @_;
    my %nodes = %{$nds};
    my @prod_sers = @{$prods};
    my %periods = %{$perds};

    my $xl_book = Excel::Writer::XLSX->new($file_name);
    my $xl_sheet = $xl_book->add_worksheet();
    my $latest_year = $periods{latest_year};
    my $prev_year = $periods{prev_year};
    my $month_from = $periods{month_from};
    my $month_to = $periods{month_to};
    my $data_hash;
    my $calc = modules::helpers::calculator->new();
    my $loop_count = $calc->permuteArrayAndHash(@prod_sers, %nodes);
    my $counter = 2 ;
    my $loop_count2 = scalar keys %nodes;
    my $break = $loop_count2 + $counter;
    my $header_trigger = 1;
    my $format_header = $xl_book->add_format();
    my $format = $xl_book->add_format();
    my $format_num = $xl_book->add_format();
    my $format_per = $xl_book->add_format();
    $format_header->set_bold(); $format_header->set_border(1);
    $format_header->set_bg_color('#800080'); $format_header->set_fg_color('white');
    $format->set_border(1);
    $format_num->set_num_format('$#,##0'); $format_num->set_border(1);
    $format_per->set_num_format('0.00%'); $format_per->set_border(1);

    foreach my $prod_ser (@prod_sers) {
        foreach my $state (keys %nodes) {
            my $key = $state."|".$prod_ser;
            my %hsh = $self->getFinDataByStatePS($latest_year, $month_from, $month_to, $nodes{$state}, $prod_ser);
            # ==================================================
            # Storing the Data in a Hash for this sub to return
            # ==================================================
            # Current year Data
            $data_hash->{$key}{state} = $state;
            $data_hash->{$key}{prod_ser} = $prod_ser;
            $data_hash->{$key}{c_yr_booking} = $hsh{c_yr_booking};
            $data_hash->{$key}{c_yr_baselist} = $hsh{c_yr_baselist};
            $data_hash->{$key}{c_yr_stdcost} = $hsh{c_yr_stdcost};

            #Previous Year Data
            $data_hash->{$key}{p_yr_booking} = $hsh{p_yr_booking};
            $data_hash->{$key}{p_yr_baselist} = $hsh{p_yr_baselist};
            $data_hash->{$key}{p_yr_stdcost} = $hsh{p_yr_stdcost};

            #Derived Data
            $data_hash->{$key}{c_yr_discount} = $hsh{c_yr_discount} ;
            $data_hash->{$key}{p_yr_discount} = $hsh{p_yr_discount};
            $data_hash->{$key}{c_yr_stdmargin} = $hsh{c_yr_stdmargin};
            $data_hash->{$key}{p_yr_stdmargin} = $hsh{p_yr_stdmargin};
            $data_hash->{$key}{yoy_booking} = $hsh{yoy_booking};
            $data_hash->{$key}{yoy_discount} = $hsh{yoy_discount};
            $data_hash->{$key}{yoy_stdmargin} = $hsh{yoy_stdmargin};

            # Formatted Data
            $data_hash->{$key}{f_c_yr_booking} = $hsh{f_c_yr_booking};
            $data_hash->{$key}{f_c_yr_list} = $hsh{f_c_yr_list};
            $data_hash->{$key}{f_c_yr_stdcost} = $hsh{f_c_yr_stdcost};
            $data_hash->{$key}{f_p_yr_booking} = $hsh{f_p_yr_booking};
            $data_hash->{$key}{f_p_yr_list} = $hsh{f_p_yr_list};
            $data_hash->{$key}{f_p_yr_stdcost} = $hsh{f_p_yr_stdcost};
            $data_hash->{$key}{f_yoy_booking} = $hsh{f_yoy_booking};
            $data_hash->{$key}{f_yoy_discount} = $hsh{f_yoy_discount};
            $data_hash->{$key}{f_yoy_stdmargin} = $hsh{f_yoy_stdmargin};

            #$self->printProgress($counter++, $loop_count);
            my $gtmu_rng = "B".$counter; 
            my $act_booking_rng = "C".$counter;
            my $yoy_booking_rng = "D".$counter; 
            my $of_forecast_rng = "E".$counter; 
            my $of_plan_rng = "F".$counter;
            my $act_discount_rng = "H".$counter; 
            my $yoy_discount_rng = "I".$counter; 
            my $act_stdmargin_rng = "K".$counter; 
            my $yoy_stdmargin_rng = "L".$counter;
            my $counter_rng = "N".$counter;
            my $break_rng = "O".$counter;
            my $reminder_rng = "P".$counter;

            if ($header_trigger) {
                $header_trigger = 0;
                $xl_sheet->write($gtmu_rng, "GTMu", $format_header); 
                $xl_sheet->write($act_booking_rng, "Actual", $format_header);
                $xl_sheet->write($yoy_booking_rng, "YoY", $format_header);
                $xl_sheet->write($of_forecast_rng, "%Fcst", $format_header); 
                $xl_sheet->write($of_plan_rng, "%Plan", $format_header);
                $xl_sheet->write($act_discount_rng, "Actual", $format_header); 
                $xl_sheet->write($yoy_discount_rng, "YoY", $format_header);
                $xl_sheet->write($act_stdmargin_rng, "Actual", $format_header); 
                $xl_sheet->write($yoy_stdmargin_rng, "YoY", $format_header);
                #$xl_sheet->write($counter_rng, $counter, $format_header);
                #$xl_sheet->write($break_rng, $break, $format_header);
                #$xl_sheet->write($reminder_rng, $counter % $break, $format_header);

                $counter++;
                $gtmu_rng = "B".$counter; 
                $act_booking_rng = "C".$counter;
                $yoy_booking_rng = "D".$counter; 
                $of_forecast_rng = "E".$counter; 
                $of_plan_rng = "F".$counter;
                $act_discount_rng = "H".$counter; 
                $yoy_discount_rng = "I".$counter; 
                $act_stdmargin_rng = "K".$counter; 
                $yoy_stdmargin_rng = "L".$counter;
                $counter_rng = "N".$counter;
                $break_rng = "O".$counter;
                $reminder_rng = "P".$counter;

                $xl_sheet->write($gtmu_rng, $state, $format); 
                $xl_sheet->write($act_booking_rng, $hsh{c_yr_booking}, $format_num);
                $xl_sheet->write($yoy_booking_rng, $hsh{yoy_booking}, $format_per);
                #$xl_sheet->write($of_forecast_rng, $hsh{}); 
                #$xl_sheet->write($of_plan_rng, $hsh{});
                $xl_sheet->write($act_discount_rng, $hsh{c_yr_discount}, $format_per); 
                $xl_sheet->write($yoy_discount_rng, $hsh{yoy_discount}, $format_per);
                $xl_sheet->write($act_stdmargin_rng, $hsh{c_yr_stdmargin}, $format_per); 
                $xl_sheet->write($yoy_stdmargin_rng, $hsh{yoy_stdmargin}, $format_per);
                #$xl_sheet->write($counter_rng, $counter, $format);
                #$xl_sheet->write($break_rng, $break, $format);
                #$xl_sheet->write($reminder_rng, $counter % $break, $format);
            } else {
                $xl_sheet->write($gtmu_rng, $state, $format); 
                $xl_sheet->write($act_booking_rng, $hsh{c_yr_booking}, $format_num);
                $xl_sheet->write($yoy_booking_rng, $hsh{yoy_booking}, $format_per);
                #$xl_sheet->write($of_forecast_rng, $hsh{}, $format); 
                #$xl_sheet->write($of_plan_rng, $hsh{}, $format);
                $xl_sheet->write($act_discount_rng, $hsh{c_yr_discount}, $format_per); 
                $xl_sheet->write($yoy_discount_rng, $hsh{yoy_discount}, $format_per);
                $xl_sheet->write($act_stdmargin_rng, $hsh{c_yr_stdmargin}, $format_per); 
                $xl_sheet->write($yoy_stdmargin_rng, $hsh{yoy_stdmargin}, $format_per);
                #$xl_sheet->write($counter_rng, $counter, $format);
                #$xl_sheet->write($break_rng, $break, $format);
                #$xl_sheet->write($reminder_rng, $counter % $break, $format);
            }
            if ($break <= $counter && ($counter % $break == 0)) {
                $counter += 2;
                $header_trigger = 1;
            } else {
                $counter++ ;
                $header_trigger = 0;
            }
        }
    }
    return $data_hash;
} #Sub



1;
