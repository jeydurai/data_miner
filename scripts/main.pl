use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use modules::migrators::cvrt;
use modules::migrators::upld;
use modules::migrators::sql_to_nosql;
use modules::templates::Greeter;
use modules::templates::demonstrate;
use modules::cleaning_engines::cln;
use modules::crawlers::ctrlf;
use modules::crawlers::excel_toddler;
use modules::reports::rpt;
use modules::reports::comm_reports;
use modules::helpers::practise;
use modules::crawlers::svg_toddler;
use strict;
use warnings;
use v5.14;
use experimental;
#no warnings qw(experimental::smartmatch experimental::lexical_topic experimental::regex_sets experimental::lexical_subs);


my $cons_cdm= modules::templates::Greeter->new();

$cons_cdm->printWelcomeMessage();
$cons_cdm->printPrompt();

while (my $cdm = <STDIN>) {
	chomp $cdm;
	my $what_entered = undef;
	my $args = undef;
	my $obj = undef;
	given ($cdm) {
		when (/^prac -p$/) {
			$obj = modules::helpers::practise->new();
			$obj->pratisePerl();
		}
		when (/^cvrt xlsx >> txt$/) {
			$obj = modules::migrators::cvrt->new();
			$obj->convertXLSX_To_TXT();
		}
		when (/^cvrt xls >> txt$/) {
			$obj = modules::migrators::cvrt->new();
			$obj->convertXLS_To_TXT();
		}
		when (/^cvrt sql >> nosql$/) {
			$obj = modules::migrators::sql_to_nosql->new();
			$obj->insertUniqueNodesInMongo();
		}
		when (/^upld mysql << csv$/) {
			$obj = modules::migrators::upld->new();
			$obj->uploadCSV_Into_MySQL();
		}
		when (/^cln findump/) {
			$cdm = ~ /^\w+\s\w+\s(-\w+)$/a ;
			if (defined $1) {
				$args = $1;
				if ($args =~ /(-ri|-nri)/) {
					$obj = modules::cleaning_engines::cln->new();
					$obj->cleanFinanceBookingDump($args);
				} elsif ($args =~ /(-xl|-xls|xlsx)/) {
					$obj = modules::crawlers::excel_toddler->new();
					$obj->crawlAndCleanBookingDump();
				} elsif ($args =~ /(-init)/) {
					$obj = modules::cleaning_engines::cln->new();
					$obj->cleanInit();
				} else {
					print "Unrecognized 'cln' command option!\n\n";
				}
			} else {
				print "'cln' command requires option!...\n\n";
			}
		}
		when (/^todd/) {
			$cdm = ~ /^\w+\s(-\w+)$/a ;
			if (defined $1) {
				$args = $1;
				if ($args =~ /(-xluf|-xlsuf|-xlsxuf)/) {
					$obj = modules::crawlers::excel_toddler->new();
					$obj->uniqueNamesFinder();
				} elsif ($args =~ /(-xlvf|-xlsvf|-xlsxvf)/) {
					$obj = modules::crawlers::excel_toddler->new();
					$obj->verticalFinder();
				} elsif ($args =~ /(-svg)/) {
					$obj = modules::crawlers::svg_toddler->new();
					$obj->import_into_excel();
				} else {
					print "Unrecognized 'todd' command option!\n\n";
				}
			} else {
				print "'todd' command requires option!...\n\n";
			}
		}
		when (/^pcf\s+-(md|s|m)$/a) {
			$obj = modules::crawlers::ctrlf->new();
			if ($1 eq "s") {
				print "\nMethod is under constuction\n.\n.\n.\n";
				
			} elsif ($1 eq "md") {
				$obj->deDupper()
			} else {
				$obj->controlF();
			}
		}
		when (/^rpt2(.*)$/a) {
            given($1) {
                when (/^\s+-snap\s+(east|north|saarc|south|west|comm|eu1|eu2|eu3)\s+(m1|m2|m3|m4|m5|m6|m7|m8|m9|m10|m11|m12|q1|q2|q3|q4|h1|h2|ytd)$/a) {
                    $obj = modules::reports::comm_reports->new();
                    $obj->getCommSnapshot2($1, $2);
                }
				default {
					print "Incorrect 'rpt2' command option!\n";
				}
            }
        }
		when (/^rpt(.*)/) {
			given ($1) {
				when (/^\s+((-epm)(\s+.*)|(-epm))$/) {
					if ($1 =~ /^-epm\s+(.*)/) {
						if ($1 =~ /^(.*@.*\.(com|org|in|net|edu|co.in))$/i) {
							print "'$1' is a valid email id\n";
						} else {
							print "'$1' is NOT a valid email id\n";
						}
					} else {
						print "Inner Pattern-1.1 Matched: $1: Email Id option to be listed\n";
					}
				}
				when (/^\s+(-ep)$/) {
					$obj = modules::reports::rpt->new();
					$obj->getXLPivot();
				}
				when (/^\s+(-eper)$/) {
					$obj = modules::reports::rpt->new();
					$obj->getPerformanceReport('ALL');
				}
				default {
					print "Incorrect 'rpt' command option!\n";
				}
			}
		}
		when (/^dem(.*)/) {
			$obj = modules::templates::demonstrate->new();
			given ($1) {
				when (/^(\s+--.*)$/) {
					if ($1 =~ /^\s+--(.*)$/) {
						$obj->demSpecific($1);
					} else {
						print "Invalid 'dem' option!\n";
					}
				} 
				when (/^(.*)$/) {
					if ($1 ne "") {
						print "Invalid 'dem' option!\n";
					} else {
						$obj->dem();
					}
				}
				default {
					print "Invalid 'dem' command!\n";
				}
			}
		}
		when (/^quit$/i) {
			last;
		}
		default {
			print "Not a cdm command!...\n\n";
		}
	}
	$cons_cdm->printPrompt();
}
$cons_cdm->printExitingMessage();
$cons_cdm = undef;

1;
