package modules::crawlers::excel_toddler;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
no warnings 'uninitialized';
use v5.14;
use Try::Tiny;
use List::MoreUtils qw(uniq);
use Win32::OLE;
use Excel::Writer::XLSX;
use Spreadsheet::WriteExcel::Utility;
use modules::reports::report_utility;
use Scalar::Util qw(looks_like_number);
use XML::Dumper;
#use Algorithm::Permute;
use Array::Utils qw(:all);
use Text::Trim qw(trim);

#no warnings qw(experimental::smartmatch experimental::lexical_topic experimental::regex_sets experimental::lexical_subs);
our @ISA = qw(modules::reports::report_utility);

sub new {
	my $class = shift;
	my $self = {};
	bless $self, $class;
	return $self;
}

sub testRedundancy {
	my $self = shift;
	my $base_name = shift;
	my $search_string = shift;
	my $REDUNDANT_OFFICIAL_WORDS_REF = shift;
	my $contrac_len = shift;
	
	$search_string = trim($search_string);
	my @truncated_dedundant_words = map{substr($_, 0, $contrac_len)} @{$REDUNDANT_OFFICIAL_WORDS_REF};
	my @temp_base_words = split / /, $search_string;
	@temp_base_words = map {trim($_)} @temp_base_words;
	my $search_string2 = trim($temp_base_words[0]);
	my $go_ahead = 0;
	my @reduntancy_array;	
	
	try {
		if (my ($matched) = grep{$_ =~ /^$search_string/i} @{$REDUNDANT_OFFICIAL_WORDS_REF}) {
			my $temp_string = $search_string . "<" . $base_name . ">";
			push @reduntancy_array, $temp_string;
		} elsif (my ($matched_1) = grep{/^$_\s*.{1,3}.*?/i =~ $search_string} @{$REDUNDANT_OFFICIAL_WORDS_REF}) {
			my $temp_string = $search_string . "<" . $base_name . ">";
			push @reduntancy_array, $temp_string;
		} elsif (my ($matched2) = grep{/^$_\s*.{1,3}.*?/i =~ $search_string} @truncated_dedundant_words) {
			my $temp_string = $search_string . "<" . $base_name . ">";
			push @reduntancy_array, $temp_string;
		} elsif (my ($matched2_1) = grep{$_ =~ /^$search_string/i} @truncated_dedundant_words) {
			my $temp_string = $search_string . "<" . $base_name . ">";
			push @reduntancy_array, $temp_string;
		} else {
			my $temp_string = "Non Redundant " . $search_string . "<" . $base_name . ">";
			push @reduntancy_array, $temp_string;
			$go_ahead = 1;
		}
	} catch {
			my $temp_string = "Error Caught Non Redundant " . $search_string . "<" . $base_name . ">";
			push @reduntancy_array, $temp_string;
			$go_ahead = 1;
	};

	return ($go_ahead, \@reduntancy_array);	
	
}

sub checkIfBeginsWithSingleChar {
	my $self = shift;
	my $base_words = shift;
	
	my $string = "";
	my $is_first_set_of_words_over = 0;
	my $temp_counter = 0;
	my $one_more_test = 0;
	print "Base Words: @{$base_words}\n";
	foreach my $word (@${base_words}) {
		$temp_counter++;
		last if (length $word != 1 && $temp_counter == 1);
		$one_more_test = 1;
		unless ($is_first_set_of_words_over) {
			if (length $word == 1) {
				$string = $string . $word;
			} elsif (length $word != 1 && $temp_counter == 2) {
				$string = $string . $word;
			} else {
				$is_first_set_of_words_over = 1;
				$string = $string . " " . $word;
			}
		} else {
			$string = $string . " " . $word;
		}
		#print "<$word>|$string\n";
	}
	return ($one_more_test, $string);
}

sub mapAndGetMatchedUnique {
	my $self = shift;
	my $unique_data_hash = shift;
	my $patterns_ref = shift;

	my %matched_hash = map {
		my $data = ${$unique_data_hash}{$_};
		my $corporate_name = ${$data}{"corporate_name"};
		my $string_to_compare = $_;
		$string_to_compare = trim($string_to_compare);
		if (($string_to_compare ~~ @{$patterns_ref}) || $corporate_name ~~ @{$patterns_ref}) { # check if the base_name match(es) any of the patterns
			$_, $data;
		} else {
			();
		}
	} keys %{$unique_data_hash};

	my $howmany_matches = keys %matched_hash;
	return (\%matched_hash, $howmany_matches);
}

sub searchBySentenceLonger {
	my $self = shift;
	my $unique_data_hash = shift;
	my $base_name = shift;
	my $contrac_len = shift;
	my $which_round = shift;
	my $REDUNDANT_OFFICIAL_WORDS_REF = shift;
	
	my $MATCH_TYPE;
	my $base_name_original = $base_name;
	$base_name = trim($base_name);
	my $base_name_length = length $base_name;
	my @base_words = split / /, $base_name;
	@base_words = map {trim($_)} @base_words;
	my $number_of_words = scalar(@base_words);	
	
	my $counter = 0;
	my $MATCH_BENCHMARK_LENGTH = 15;
	my $matched_hashref="";
	my $howmany_matches=0;
	
	my @truncated_dedundant_words = map{substr($_, 0, $contrac_len)} @{$REDUNDANT_OFFICIAL_WORDS_REF};
	
	# First set of patterns (Check for perfect match - Vlookup)
	my @patterns = (qr/(?i:^\Q$base_name_original\E$)/);
	($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);
	
	
	if ($howmany_matches == 0) {
		@patterns = ();
		@patterns = (
			qr/(?i:^\Q$base_name\E)/,
			qr/(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])+(?i:\Q$base_name\E)/,
			qr/(?i:group)(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:of)(?i:ms|m\/s|[^a-zA-Z]|dr|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:\Q$base_name\E)/,
			qr/(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:\Q$base_name\E)(?i:ms|m\/s|[^a-zA-Z]|dr|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:group)/,
		);
		foreach (1 .. $base_name_length) {
			$counter++;
			last if ($base_name_length-$_ > 0) && ($base_name_length-$_ < $MATCH_BENCHMARK_LENGTH);
			my $search_string = trim(substr($base_name, 0, $base_name_length-$_)); # if the last letter is Dot, search string to be except that
			my ($go_ahead, $reduntancy_arrayref) = $self->testRedundancy($base_name, $search_string, $REDUNDANT_OFFICIAL_WORDS_REF, $contrac_len);
			 
			if ($go_ahead) {
				push @patterns, qr/(?i:^\Q$search_string\E)/;
				push @patterns, qr/(?i:global[^a-zA-Z]*|india\s+|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])+(?i:\Q$search_string\E)/;
			}
			
		}	
		($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);
	} else {
		if ($which_round == 1) {
			$MATCH_TYPE = "VLOOKUP" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_QUICK_MATCH" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_QUICK_MATCH" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
	}
	
	
	if ($howmany_matches == 0) {
		@patterns = ();
		foreach ($counter .. $base_name_length) {
			last unless ($base_name_length-$_ >= $contrac_len) && ($base_name_length-$_ < $MATCH_BENCHMARK_LENGTH);
			my $search_string = trim(substr($base_name, 0, length($base_name)-$_)); # if the last letter is Dot, search string to be except that
			my @temp_base_words = split / /, $search_string;
			@temp_base_words = map {trim($_)} @temp_base_words;
			my $search_string2 = trim($temp_base_words[0]);
			my ($go_ahead, $reduntancy_arrayref) = $self->testRedundancy($base_name, $search_string, $REDUNDANT_OFFICIAL_WORDS_REF, $contrac_len);
			 
			if ($go_ahead) {
					push @patterns, qr/(?i:^\Q$search_string\E)/;
				}
		}	
		($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);
	} else {
		if ($which_round == 1) {
			$MATCH_TYPE = "SIMPLE_LENGTH_CONTRACTION" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_LENGTH_CONTRACTION" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_LENGTH_CONTRACTION" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
	}


	if ($howmany_matches > 0) {
		print "There are $howmany_matches match(es) found!\n";
		if ($which_round == 1) {
			$MATCH_TYPE = "NON_DOT_LENGTH_CONTRACTION" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_NON_DOT_LENGTH_CONTRACTION" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_NON_DOT_LENGTH_CONTRACTION" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
		foreach (keys %{$matched_hashref}) {
			my $data_ref = ${$matched_hashref}{$_};
			my $corporate_name = ${$data_ref}{"corporate_name"};
			my $vertical = ${$data_ref}{"vertical"};
			print "$base_name | $_ | <$corporate_name> | <$vertical>\n";
		}
	} else {
		print "Zero match(es) don't display for $base_name\n" if $howmany_matches == 0;
	}

	return ($matched_hashref, $howmany_matches, $MATCH_TYPE);
}

sub searchBySentenceShorter {
	my $self = shift;
	my $unique_data_hash = shift;
	my $base_name = shift;
	my $contrac_len = shift;
	my $which_round = shift;
	my $REDUNDANT_OFFICIAL_WORDS_REF = shift;
	
	my $MATCH_TYPE;
	my $base_name_original = $base_name;
	$base_name = trim($base_name);
	my $base_name_length = length $base_name;
	my @base_words = split / /, $base_name;
	@base_words = map {trim($_)} @base_words;
	my $number_of_words = scalar(@base_words);	
	
	print "Inside searchBySentenceShorter method\n";
	print "$base_name is a Short Sentence!\n";

	# First set of patterns (Check for perfect match - Vlookup)
	my @patterns = (qr/(?i:^\Q$base_name_original\E$)/);
	my ($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);


	if ($howmany_matches == 0) {
		@patterns = ();
		@patterns = (
			qr/(?i:^\Q$base_name\E)/,
			qr/(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])+(?i:\Q$base_name\E)/,
			qr/(?i:group)(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:of)(?i:ms|m\/s|[^a-zA-Z]|dr|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:\Q$base_name\E)/,
			qr/(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:\Q$base_name\E)(?i:ms|m\/s|[^a-zA-Z]|dr|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:group)/,
		);
		($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);
	} else {
		if ($which_round == 1) {
			$MATCH_TYPE = "VLOOKUP" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_QUICK_MATCH" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_QUICK_MATCH" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
	}

	if ($howmany_matches == 0) {
		my $separators_arrayref = ["", " ", "\.", "\. ", "-", "- ", "~", "~ ", "|", "| ", "%", "% ", "*", "* ", "^", "^ ", "\$", "\$ ", "\@", "\@ ", "\&", "\& ", "(", ")", "_", "_ "];
		foreach (@{$separators_arrayref}) {
			@patterns = ();
			my $search_string = join($_, @base_words);
			@patterns = (
				qr/(?i:^\Q$search_string\E)/,
				qr/(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])+(?i:\Q$search_string\E)/,
				qr/(?i:group)(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:of)(?i:ms|m\/s|[^a-zA-Z]|dr|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:\Q$search_string\E)/,
				qr/(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:\Q$search_string\E)(?i:ms|m\/s|[^a-zA-Z]|dr|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:group)/,
			);
		}
		($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);
	} else {
		if ($which_round == 1) {
			$MATCH_TYPE = "SIMPLE_PREFIXING_ALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_PREFIXING_ALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_PREFIXING_ALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
	}


	if ($howmany_matches > 0) {
		print "There are $howmany_matches match(es) found!\n";
		if ($which_round == 1) {
			$MATCH_TYPE = "SIMPLE_PREFIXING_NONALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_PREFIXING_NONALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_PREFIXING_NONALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
		foreach (keys %{$matched_hashref}) {
			my $data_ref = ${$matched_hashref}{$_};
			my $corporate_name = ${$data_ref}{"corporate_name"};
			my $vertical = ${$data_ref}{"vertical"};
			print "$base_name | $_ | <$corporate_name> | <$vertical>\n";
		}
	} else {
		print "Zero match(es) don't display for $base_name\n" if $howmany_matches == 0;
	}

	return ($matched_hashref, $howmany_matches, $MATCH_TYPE);

}

sub searchByOneWordLonger {
	my $self = shift;
	my $unique_data_hash = shift;
	my $base_name = shift;
	my $contrac_len = shift;
	my $which_round = shift;
	my $REDUNDANT_OFFICIAL_WORDS_REF = shift;

	my $MATCH_TYPE;
	my $base_name_original = $base_name;
	$base_name = trim($base_name);
	my $base_name_length = length $base_name;
	my @base_words = split / /, $base_name;
	@base_words = map {trim($_)} @base_words;
	my $number_of_words = scalar(@base_words);	
	my @each_characters;
	
	print "Inside searchByOneWordLonger method\n";
	print "$base_name is a Longer Word!\n";

	# First set of patterns (Check for perfect match - Vlookup)
	my @patterns = (qr/(?i:^\Q$base_name_original\E$)/);
	my ($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);


	if ($howmany_matches == 0) {
		@patterns = ();
		@patterns = (
			qr/(?i:^\Q$base_name\E)/,
			qr/(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])+(?i:\Q$base_name\E)/,
			qr/(?i:group)(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:of)(?i:ms|m\/s|[^a-zA-Z]|dr|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:\Q$base_name\E)/,
			qr/(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:\Q$base_name\E)(?i:ms|m\/s|[^a-zA-Z]|dr|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:group)/,
		);
		($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);
	} else {
		if ($which_round == 1) {
			$MATCH_TYPE = "VLOOKUP" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_QUICK_MATCH" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_QUICK_MATCH" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
	}
	

	if ($howmany_matches == 0) {
		@each_characters = split "", $base_name;
		my $total_chars = scalar @each_characters;
		my @running_array = @each_characters;
		my @left_chars;
	
		@patterns = ();
		print "Matching One Word with various Combinations...\n";
		foreach (@each_characters) {
			my $removed_char = shift @running_array;
			push @left_chars, $removed_char;
			my @right_chars = @running_array;
			my $separators_arrayref = ["", " ", "\.", "\. ", "-", "- ", "~", "~ ", "|", "| ", "%", "% ", "*", "* ", "^", "^ ", "\$", "\$ ", "\@", "\@ ", "\&", "\& ", "(", ")", "_", "_ "];
			foreach (@{$separators_arrayref}) {
				my $search_string = join("", @left_chars) . $_ . join("", @right_chars);
				#print "One Word Longer: <$search_string>\n";
				push @patterns, qr/(?i:^\Q$search_string\E)/;
				push @patterns, qr/(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])+(?i:\Q$search_string\E)/;
			} # end of separators
		}
		($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);
		print "Matching One Word with various Combinations is over!\n";
	} else {
		if ($which_round == 1) {
			$MATCH_TYPE = "SIMPLE_PREFIXING_ALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_PREFIXING_ALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_PREFIXING_ALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
	}


	if ($howmany_matches > 0) {
		print "There are $howmany_matches match(es) found!\n";
		if ($which_round == 1) {
			$MATCH_TYPE = "SIMPLE_LEFT_RIGHT_JOINING_WORDS" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_LEFT_RIGHT_JOINING_WORDS" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_LEFT_RIGHT_JOINING_WORDS" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
		foreach (keys %{$matched_hashref}) {
			my $data_ref = ${$matched_hashref}{$_};
			my $corporate_name = ${$data_ref}{"corporate_name"};
			my $vertical = ${$data_ref}{"vertical"};
			print "$base_name | $_ | <$corporate_name> | <$vertical>\n";
		}
	} else {
		print "Zero match(es) don't display for $base_name\n" if $howmany_matches == 0;
	}

	return ($matched_hashref, $howmany_matches, $MATCH_TYPE);
}

sub searchByOneWordShorter {
	my $self = shift;
	my $unique_data_hash = shift;
	my $base_name = shift;
	my $contrac_len = shift;
	my $which_round = shift;
	my $REDUNDANT_OFFICIAL_WORDS_REF = shift;

	my $MATCH_TYPE;
	my $base_name_original = $base_name;
	$base_name = trim($base_name);
	my $base_name_length = length $base_name;
	my @base_words = split / /, $base_name;
	@base_words = map {trim($_)} @base_words;
	my $number_of_words = scalar(@base_words);	
	my @each_characters;
	
	print "Inside searchByOneWordShorter method\n";
	print "$base_name is just a Short Word!\n";

	# First set of patterns (Check for perfect match - Vlookup)
	my @patterns = (qr/(?i:^\Q$base_name_original\E$)/);
	my ($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);

	if ($howmany_matches == 0) {
		@patterns = ();
		@patterns = (
			qr/(?i:^\Q$base_name\E)/,
			qr/(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])+(?i:\Q$base_name\E)/,
			qr/(?i:group)(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:of)(?i:ms|m\/s|[^a-zA-Z]|dr|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:\Q$base_name\E)/,
			qr/(?i:global[^a-zA-Z]*|india\s+|the|ms|m\/s|[^a-zA-Z]|dr|the[^a-zA-Z]|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:\Q$base_name\E)(?i:ms|m\/s|[^a-zA-Z]|dr|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])*?(?i:group)/,
		);
		($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);
	} else {
		if ($which_round == 1) {
			$MATCH_TYPE = "VLOOKUP" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_QUICK_MATCH" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_QUICK_MATCH" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
	}

	if ($howmany_matches == 0) {
		@each_characters = split "", $base_name;
		my $total_chars = scalar @each_characters;
		my @running_array = @each_characters;
		my @left_chars;
	
		print "Matching One Word with various Combinations...\n";
		@patterns = ();
		foreach (@each_characters) {
			my $removed_char = shift @running_array;
			push @left_chars, $removed_char;
			my @right_chars = @running_array;
			my $separators_arrayref = ["", " ", "\.", "\. ", "-", "- ", "~", "~ ", "|", "| ", "%", "% ", "*", "* ", "^", "^ ", "\$", "\$ ", "\@", "\@ ", "\&", "\& ", "(", ")", "_", "_ "];
			foreach (@{$separators_arrayref}) {
				my $search_string = join("", @left_chars) . $_ . join("", @right_chars);
				#print "One Word Longer: <$search_string>\n";
				push @patterns, qr/(?i:^\Q$search_string\E)/;
				push @patterns, qr/(?i:global[^a-zA-Z]*|india\s+|the|the[^a-zA-Z]|ms|m\/s|[^a-zA-Z]|dr|ms[^a-zA-Z]|m\/s[^a-zA-Z]|dr[^a-zA-Z])+(?i:\Q$search_string\E)/;
			} # end of separators
		}
		($matched_hashref, $howmany_matches) = $self->mapAndGetMatchedUnique($unique_data_hash, \@patterns);
		print "Matching One Word with various Combinations is over!\n";
	} else {
		if ($which_round == 1) {
			$MATCH_TYPE = "SIMPLE_PREFIXING_ALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_PREFIXING_ALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_PREFIXING_ALPHA" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
	}


	if ($howmany_matches > 0) {
		print "There are $howmany_matches match(es) found!\n";
		if ($which_round == 1) {
			$MATCH_TYPE = "SIMPLE_LEFT_RIGHT_JOINING_WORDS" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 2) {
			$MATCH_TYPE = "FIRST_IS_A_CHAR_AND_LEFT_RIGHT_JOINING_WORDS" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		} elsif ($which_round == 3) {
			$MATCH_TYPE = "SPECIAL_GROUPING_AND_LEFT_RIGHT_JOINING_WORDS" if ((!defined $MATCH_TYPE) || $MATCH_TYPE eq "");
		}
		foreach (keys %{$matched_hashref}) {
			my $data_ref = ${$matched_hashref}{$_};
			my $corporate_name = ${$data_ref}{"corporate_name"};
			my $vertical = ${$data_ref}{"vertical"};
			print "$base_name | $_ | <$corporate_name> | <$vertical>\n";
		}
	} else {
		print "Zero match(es) don't display for $base_name\n" if $howmany_matches == 0;
	}

	return ($matched_hashref, $howmany_matches, $MATCH_TYPE);

}

sub getCleanedOption {
	my $self = shift;
	my $option = shift;
	my $DEFAULT_OPTION = shift;

	if (defined chomp($option)) {
	$option = $DEFAULT_OPTION if (!looks_like_number($option) || $option > $DEFAULT_OPTION);
	print "\nEntered choice is not a number, hence default option is applicable!!!\n";
	} else {
		$option = $DEFAULT_OPTION;
		print "\nDefault option is applicable!!!\n";
	}
	return $option;
}

sub getSourceFileCredentials {
	my $self = shift;
	my $file_name_of_input_data = shift;
	my $is_new = shift;
	my $not_source_exist = shift;
	
	my $ab_path; my $dir_name; my $file_name; my $sheet_name;
	if ($is_new) {
		print "\nSource does not exist!!!!!\n" if $not_source_exist;
		print "Path of the Source file: ";
		$dir_name = <STDIN>;
		print "Source file name: ";
		$file_name = <STDIN>;
		print "Source file sheet name: ";
		$sheet_name = <STDIN>;
		
		if (defined chomp($dir_name) && defined chomp($file_name) && defined chomp($sheet_name)) {
			my $path_string = $dir_name . '/';
			$ab_path = $path_string . $file_name;
			
			my $input_data = [
				{
					absolute_path => $ab_path,
					unique_customers => $sheet_name
				}
			];
			my $dump = new XML::Dumper;
			$dump->pl2xml($input_data, $file_name_of_input_data);
			
		} else {
			print "Input strings on the file name and path are Incorrect!\n";
		}
	} else {
		print "De serializing the file information in to a variable...\n";
		my $dump = new XML::Dumper;
		if (-f $file_name_of_input_data) {
			my $input_data = $dump->xml2pl($file_name_of_input_data);
			foreach (@{$input_data}) {
				$ab_path = $_->{absolute_path};
				$sheet_name = $_->{unique_customers};
			}
		} else {
			$ab_path = $self->getSourceFileCredentials("input_data.xml", 1, 1);
		}
	
		print "Completed de serializing the file information in to a variable!\n";
	}
	
	return ($ab_path, $sheet_name);
}

sub uniqueNamesFinder {
	my $self = shift;

	my $SEARCH_METHOD = "";
	my $DEFAULT_STRING_THRESHOLD = 10;
	my $DEFAULT_MAPPING_SOURCE_CHOICE = 3;
	my $DEFAULT_UPLOAD_CHOICE = 2;
	my $DEFAULT_SOURCE_METHOD = 3;
	my $mapping_source_string;
	

	print "\nSource File Input method:\n";
	print "===========================\n";
	print "1. Already existing formatted Booking Data File\n";
	print "2. Source Details in a New XML\n";
	print "3. Command Prompt Input\n";
	print "Enter your choice from 1 to 3 [DEFAULT 3]: ";
	my $source_input_methods = <STDIN>;
	$source_input_methods = $self->getCleanedOption($source_input_methods, $DEFAULT_SOURCE_METHOD);
	
	my $ab_path; my $sheet_name; my $input_data_file_name; my $is_file_new;
	given ($source_input_methods) {
		when (1) {
			$is_file_new = 0; $input_data_file_name = "input_data.xml";
		} 
		when (2) {
			$is_file_new = 0; $input_data_file_name = "input_data2.xml";
		} 
		default {
			$is_file_new = 1; $input_data_file_name = "input_data2.xml";
		}
	}
	($ab_path, $sheet_name) = $self->getSourceFileCredentials($input_data_file_name, $is_file_new, 0);
	
	print "Specify the Minimum String threshold [DEFUALT is $DEFAULT_STRING_THRESHOLD]: ";
	my $contrac_len = <STDIN>;
	
	$contrac_len = $self->getCleanedOption($contrac_len, $DEFAULT_STRING_THRESHOLD);
	
	print "\nThe following are the Mapping Source...\n";
	print "=========================================\n";
	print "1. Booking Data Customer Names\n";
	print "2. Booking Data Partner Names\n";
	print "3. Universal Unique Names\n";
	print "Enter your choice from 1 to 3 [DEFAULT 3]: ";
	my $mapping_source_choice = <STDIN>;

	$mapping_source_choice = $self->getCleanedOption($mapping_source_choice, $DEFAULT_MAPPING_SOURCE_CHOICE);
	
	given ($mapping_source_choice) {
		when (1) {$mapping_source_string = "Booking_Data_Customers"} 
		when (2) {$mapping_source_string = "Booking_Data_Partners"} 
		default {
			# Nothing to do
		}
	}
	
	# Opening the Required Excel Sheet
	# ================================
	print "Opening $ab_path file...\n";
	my $xl_app = $self->getXLApp();
	$xl_app->{Visible} = 1;
	$xl_app->{DisplayAlerts} = 0;
	my $xl_book = $xl_app->Workbooks->open($ab_path);
	my $unique_sheet = $xl_book->Sheets($sheet_name);
	my $last_row_in_unique_sheet = $unique_sheet->UsedRange->Rows->{'Count'};
	my $start_row = 2; my $name_col = 1; my $unique_name_col = 2; my $vertical_col = 3; my $remarks_col = 4; my $match_type_col = 5; my $upload_status_col = 6;
	my $row_counter = $start_row;
	#my $last_row_with_short = int($last_row_in_unique_sheet * 0.12);
	my $last_row_with_short = $last_row_in_unique_sheet;

	my $should_search = 1; 
	my ($names_hashref, $is_first_time) = $self->getExcelColumnInKeysAndValues($unique_sheet, $start_row, $last_row_with_short, $name_col, 
								$unique_name_col, $vertical_col, $remarks_col, $match_type_col, $upload_status_col, "matched", "remarks");
	
	my $tot_names = keys %{$names_hashref};
	
	$unique_sheet->Activate();
	print "$ab_path file is now opened and activated the unique sheet!\n";
	# Excel Sheet opened
	# ================================
	
	my $unique_sheet_rng = $unique_sheet->Range("A1".":"."F".$last_row_with_short);
	$unique_sheet_rng->Columns()->AutoFilter();
	$unique_sheet_rng->Columns()->AutoFilter("4", "=no match") unless $is_first_time;

	my $remaining_names_hashref;
	my $loop_counter = 0;
	my %nomatch_hash;
	my %unique_data_hash_additional;
	my %unique_data_hash = $self->uniqueDataFromSQL($mapping_source_string);
	my $REDUNDANT_OFFICIAL_WORDS_REF = $self->redundantWordsFromSQL();
	
	foreach my $base_name (sort{lc(${$names_hashref}{$a}) cmp lc(${$names_hashref}{$b})} keys %{$names_hashref}) { # Iteration through the excel sheet's contents

		$xl_app->ActiveWindow->SmallScroll({Down => 1}) if ++$loop_counter > 15;
		my $matched_hash_ref = "";
		my $howmany_matches = 0;
		my $MATCH_TYPE;
		my $is_short_circuit = 0;
		my $search_loop_counter = 1;
		my $cell_ref = ${$names_hashref}{$base_name};
		my $name_col_ref = ${$cell_ref}{"name_col_ref"};
		my $unique_name_col_ref = ${$cell_ref}{"unique_name_col_ref"};
		my $vertical_col_ref = ${$cell_ref}{"vertical_col_ref"};
		my $remarks_col_ref = ${$cell_ref}{"remarks_col_ref"};
		my $match_type_col_ref = ${$cell_ref}{"match_type_col_ref"};
		my $upload_status_col_ref = ${$cell_ref}{"upload_status_col_ref"};
		my $base_name_proc = $base_name;
		my $base_name_length = length trim($base_name);
		my @base_words = split / /, trim($base_name);
		@base_words = map {trim($_)} @base_words;
		my $number_of_words = scalar(@base_words);	
		
		while (1) {
			print "Search Looping No.: $search_loop_counter\n";
			print "$base_name-<$name_col_ref>|<$unique_name_col_ref>|<$vertical_col_ref>|<$remarks_col_ref>\n";
			given ($search_loop_counter) {
				when (1) {
					# Nothing to do
				} when (2) {
					my ($one_more_test, $string) = $self->checkIfBeginsWithSingleChar(\@base_words);
					if ($one_more_test) {
						print "\nSecond Round Initiated for $base_name!\n";
						$base_name_proc = trim($string);
					} else {
						$search_loop_counter++;
						next;
						print "\nSecond Round need not be Initiated for $base_name_proc!\n";
					}				
				} when (3) {
					my $temp_base_name = $base_name;
					given ($temp_base_name) {
						when (/(AVPN|EVPN|A\.V\.P\.N|E\.V\.P\.N)/i) {
							print "\nThird Round Initiated for $base_name_proc!\n";
							(my $new = $temp_base_name) =~ s/(AVPN|EVPN|A\.V\.P\.N|E\.V\.P\.N|\([^a-zA-Z]*AVPN[^a-zA-Z]*\)|\([^a-zA-Z]*EVPN[^a-zA-Z]*\)|\([^a-zA-Z]*A\.V\.P\.N[^a-zA-Z]*\)|\([^a-zA-Z]*E\.V\.P\.N[^a-zA-Z]*\))//gi;
							print "New String: $new | Old String: $temp_base_name\n";
							$base_name_proc = trim($new);
						}
						when (/.*?\((.*?)\).*?/i) {
							print "\nThird Round Initiated for $base_name_proc!\n";
							$base_name_proc = trim($1);
						}
						when (/^[^a-zA-Z]*(?:t[^a-zA-Z]*h[^a-zA-Z]*e|m[^a-zA-Z]*s)(.*?)$/i) {
							print "\nThird Round Initiated for $base_name_proc!\n";
							$base_name_proc = trim($1);
						}
						when (/^\([^a-zA-Z]*group[^a-zA-Z]*of[^a-zA-Z]*[^a-zA-Z]*(?:t[^a-zA-Z]*h[^a-zA-Z]*e|m[^a-zA-Z]*s)*(.*?)\).*?$/i) {
							print "\nThird Round Initiated for $base_name_proc!\n";
							$base_name_proc = trim($1);
						}
						when (/^\([^a-zA-Z]*(?:t[^a-zA-Z]*h[^a-zA-Z]*e|m[^a-zA-Z]*s)*(.*?)[^a-zA-Z]*group[^a-zA-Z]*\).*?$/i) {
							print "\nThird Round Initiated for $base_name_proc!\n";
							$base_name_proc = trim($1);
						}
						when (/\&/i) {
							(my $new = $temp_base_name) =~ s/\&/and/gi;
							print "New String: $new | Old String: $temp_base_name\n";
							$base_name_proc = trim($new);
						}
						default {
							$is_short_circuit = 1;
							print "\nThird Round need not be Initiated for $base_name_proc!\n";
						}
					}
				} default {
					$is_short_circuit = 1;
				}
			}

			$base_name_length = length trim($base_name_proc);
			@base_words = split / /, trim($base_name_proc);
			@base_words = map {trim($_)} @base_words;
			$number_of_words = scalar(@base_words);	

			unless ($base_name_length <= 1) {
				if ($base_name_length <= $contrac_len && $number_of_words == 1)  {
					($matched_hash_ref, $howmany_matches, $MATCH_TYPE) = $self->searchByOneWordShorter(\%unique_data_hash, 
					$base_name_proc, $contrac_len, $search_loop_counter, $REDUNDANT_OFFICIAL_WORDS_REF);
				} elsif ($base_name_length <= $contrac_len && $number_of_words > 1) {
					($matched_hash_ref, $howmany_matches, $MATCH_TYPE) = $self->searchBySentenceShorter(\%unique_data_hash, 
					$base_name_proc, $contrac_len, $search_loop_counter, $REDUNDANT_OFFICIAL_WORDS_REF);
				} elsif ($base_name_length > $contrac_len && $number_of_words == 1) {
					($matched_hash_ref, $howmany_matches, $MATCH_TYPE) = $self->searchByOneWordLonger(\%unique_data_hash, 
					$base_name_proc, $contrac_len, $search_loop_counter, $REDUNDANT_OFFICIAL_WORDS_REF);
				} elsif ($base_name_length > $contrac_len && $number_of_words > 1) {
					($matched_hash_ref, $howmany_matches, $MATCH_TYPE) = $self->searchBySentenceLonger(\%unique_data_hash, 
					$base_name_proc, $contrac_len, $search_loop_counter, $REDUNDANT_OFFICIAL_WORDS_REF);
				} else {
					print "No Methods assigned!!!!\n";
				}
				$MATCH_TYPE = "no match" unless $MATCH_TYPE;
				if ($howmany_matches > 0) {
					foreach (keys %{$matched_hash_ref}) {
						my $data_ref = ${$matched_hash_ref}{$_};
						my $corporate_name = ${$data_ref}{"corporate_name"};
						my $vertical = ${$data_ref}{"vertical"};
						$vertical = $self->getIndustryVertical($base_name) if ((!defined $vertical) || $vertical eq "");
						$unique_sheet->Range($unique_name_col_ref)->{Value} = $corporate_name;
						$unique_sheet->Range($vertical_col_ref)->{Value} = $vertical;
						$unique_sheet->Range($remarks_col_ref)->{Value} = "matched";
						$unique_sheet->Range($match_type_col_ref)->{Value} = $MATCH_TYPE;
						$unique_sheet->Range($upload_status_col_ref)->{Value} = "upload" unless $MATCH_TYPE eq "VLOOKUP";
						print "Found a match in search Loop# $search_loop_counter! $base_name <$base_name_length>|$unique_name_col_ref|<$corporate_name>\n";
						last;
					}
					last;
				} else {
					$is_short_circuit = 1 if ($search_loop_counter == 3);
				}
			} else {
				# Do not add in to unique hash, which is what No match is; Mention No match remarks in excel sheet.
				my $vertical = $self->getIndustryVertical($base_name);
				$unique_sheet->Range($remarks_col_ref)->{Value} = "no match";
				$unique_sheet->Range($unique_name_col_ref)->{Value} = $base_name;
				$unique_sheet->Range($match_type_col_ref)->{Value} = $MATCH_TYPE;
				$unique_sheet->Range($vertical_col_ref)->{Value} = $vertical;
				$unique_sheet->Range($upload_status_col_ref)->{Value} = "upload";
				$unique_sheet->Range($name_col_ref)->Interior->{ColorIndex} = 10;
				$unique_sheet->Range($unique_name_col_ref)->Interior->{ColorIndex} = 10;
				$unique_sheet->Range($vertical_col_ref)->Interior->{ColorIndex} = 10;
				$unique_sheet->Range($remarks_col_ref)->Interior->{ColorIndex} = 10;
			}

			if ($is_short_circuit) {
				# Add in to unique hash; Mention No match remarks in excel sheet.
				my $vertical = $self->getIndustryVertical($base_name);
				my $cell_ref = {
					name_col_ref => $name_col_ref,
					unique_name_col_ref => $unique_name_col_ref,
					vertical_col_ref => $vertical_col_ref,
					remarks_col_ref => $remarks_col_ref,
					match_type_col_ref => $match_type_col_ref,
					upload_status_col_ref => $upload_status_col_ref,
				};
				$remaining_names_hashref = {
					$base_name => $cell_ref	
				};
									
				$unique_data_hash{$base_name} = {
					corporate_name => $base_name,
					vertical => $vertical
				};
				$unique_data_hash_additional{$base_name} = {
					corporate_name => $base_name,
					vertical => $vertical
				};
				$unique_sheet->Range($remarks_col_ref)->{Value} = "no match";
				$unique_sheet->Range($match_type_col_ref)->{Value} = $MATCH_TYPE;
				$unique_sheet->Range($unique_name_col_ref)->{Value} = $base_name;
				$unique_sheet->Range($vertical_col_ref)->{Value} = $vertical;
				$unique_sheet->Range($upload_status_col_ref)->{Value} = "upload";
				$unique_sheet->Range($name_col_ref)->Interior->{ColorIndex} = 10;
				$unique_sheet->Range($unique_name_col_ref)->Interior->{ColorIndex} = 10;
				$unique_sheet->Range($vertical_col_ref)->Interior->{ColorIndex} = 10;
				$unique_sheet->Range($remarks_col_ref)->Interior->{ColorIndex} = 10;
				$search_loop_counter++;
				last;
			} else {
					last if ($search_loop_counter == 3);
			}
				
			$matched_hash_ref = "";
			$howmany_matches = 0;
			$search_loop_counter++;
		} # infinite while loop end
		
	} # end of names hash iteration
	
	print "\nIt is the TIME to check your excel sheet and confirm if new names can be uploaded in to SQL database...\n";
	print "=========================================================================================================\n";
	print "1. Upload now\n";
	print "2. Do not upload\n";
	print "Enter your choice from 1 to 2 [DEFAULT 2]: ";
	my $upload_choice = <STDIN>;
	if (defined chomp($upload_choice)) {
		$upload_choice = $DEFAULT_UPLOAD_CHOICE if (!looks_like_number($upload_choice) || $upload_choice > $DEFAULT_UPLOAD_CHOICE);
		print "\nEntered choice is not a number, hence default option is applicable!!!\n";
	} else {
		$upload_choice = $DEFAULT_UPLOAD_CHOICE;
		print "\nDefault option is applicable!!!\n";
	}
	
	if ($upload_choice == 1) {
		# include the subroutine of uploading
	} else {
		print "\nNo uploading the new names in to SQL Database\n";
	}
	
	print "Search Methods completed!\n";
	$xl_app->{DisplayAlerts} = 1;
}

sub crawlAndCleanBookingDump {
	my $self = shift;
	my @empty_array = ();
	my %xldump_cols;
	my %sqldump_cols;
	my %xl_sql_map_cols;
	my $sql_obj = modules::reports::report_maker->new();
	my $dbh = $sql_obj->getMySQLDBH();
	print "\nPath of the Source file: ";
	my $dir_name = <STDIN>;
	print "Source file name: ";
	my $file_name = <STDIN>;
	print "Source file sheet name: ";
	my $sheet_name = <STDIN>;
	my %hash_map_dump_to_actual;

	if (defined chomp($dir_name) && defined chomp($file_name) && defined chomp($sheet_name)) {
		$self->prepareXLApp();
		my $xl_app = $self->getXLApp();
		$xl_app->{Visible} = 1; # Make the excel application visible to the user
		$xl_app->{DisplayAlerts} = 0;
		
		# Make ready of absolute path for the file 
		my $path_string = $dir_name . '/';
		my $ab_path = $path_string . $file_name;
		
		my $input_data = [
			{
				absolute_path => $ab_path,
				main_sheet => $sheet_name,
				unique_customers => "unique_customers",
				unique_partners => "unique_partners",
				unique_technologies => "unique_technologies",
				unique_sales_agents => "unique_sales_agents",
			}
		];
		
		my $file_name_of_input_data = "input_data.xml";
		my $dump = new XML::Dumper;
		$dump->pl2xml($input_data, $file_name_of_input_data);
		
		my $xl_book = $xl_app->Workbooks->Open($ab_path); # Open the Excel Workbook
		if (defined $xl_book) {
			my $dump_sheet = $xl_book->Sheets($sheet_name);
			my $total_excel_rows = $dump_sheet->UsedRange->Rows->{'Count'};
			my $total_excel_cols = $dump_sheet->UsedRange->Columns->{'Count'};
			$dump_sheet->Range("A2")->Select;
			$xl_app->ActiveWindow->{FreezePanes} = 'True';
			print "Total Rows: $total_excel_rows | Total Cols: $total_excel_cols\n";
			print "Press any key to continue...";
			
			# Pull all dump_from_finance table columns
			print "Extracting All dump_from_finance table columns...\n";
			my $qq_string = "SELECT * FROM dump_from_finance WHERE 1=0";
			my $query_string = qq{$qq_string};
			my $sth = $self->getSimpleSTH($dbh, $query_string, @empty_array);
			my @cols = @{$sth->{NAME_lc}};
			$sth->finish();
			print "Extracting All dump_from_finance table columns completed!\n";
			
			# Pull all xldump_to_sqldump_map table columns
			print "Extracting and Assigning All xldump_to_sqldump_map table columns...\n";
			$qq_string = "SELECT * FROM xldump_to_sqldump_map WHERE sqldump_col <> 'NOT_TO_BE_MAPPED'";
			$query_string = qq{$qq_string};
			$sth = $self->getSimpleSTH($dbh, $query_string, @empty_array);
			while (my $hash_ref = $sth->fetchrow_hashref()) {
				$xl_sql_map_cols{$hash_ref->{"xldump_col"}} = $hash_ref->{"sqldump_col"};
			}			
			$sth->finish();
			print "Extracting and Assigning All xldump_to_sqldump_map table columns completed!\n";
			
			my $data_sheet = $xl_book->Sheets->Add;
			$data_sheet->{Name} = "data";
			my @actual_headers_array = [@cols];
			my $last_row = 1;
			my $header_count = scalar @cols;
			my $last_row_ref = xl_rowcol_to_cell($last_row-1, $header_count-1);
			my $header_rng = $data_sheet->Range("A1:$last_row_ref");
			$header_rng->{Value} = [@actual_headers_array];
			
			print "Fetching the column names from $sheet_name sheet...\n";
			my $counter = 1;
			foreach ('A' .. 'XFD') {
				last if $counter > $total_excel_cols;
				my $cell_add =  $_."1";
				my $xldump_col_name = $dump_sheet->Range($cell_add)->{Value};
				$xldump_col_name = lc $xldump_col_name;
				if (defined $xldump_col_name) {
					my $cell_add2 = substr($cell_add,0,length($cell_add)-1) . "2";
					$xldump_cols{$xldump_col_name} = $cell_add2;
				} else {
					print "There is a blank header in the $sheet_name sheet!\n";
					print "Toddler attempt is aborted!\n";
					last;
				}
				$counter++;
			}		
			print "Fetching the column names from $sheet_name sheet completed!\n";

			print "Fetching the column names from Data sheet...\n";
			$counter = 1;
			foreach ('A' .. 'XFD') {
				last if $counter > $header_count;
				my $cell_add =  $_."1";
				my $sqldump_col_name = $data_sheet->Range($cell_add)->{Value};
				$sqldump_col_name = lc $sqldump_col_name;
				if (defined $sqldump_col_name) {
					my $cell_add2 = substr($cell_add,0,length($cell_add)-1) . "2";
					$sqldump_cols{$sqldump_col_name} = $cell_add2;
				} else {
					print "There is a blank header in the Data sheet!\n";
					print "Toddler attempt is aborted!\n";
					last;
				}
				$counter++;
			}

			print "Fetching the column names from Data sheet completed!\n";
			foreach my $xldump_col (sort{lc($xldump_cols{$a}) cmp lc($xldump_cols{$b})} keys %xldump_cols) {
				print "Processing $xldump_col ....\n";
				# Extract only the matched SQL DUMP column name
				my @matched_sql_col = grep{lc $_ eq lc $xldump_col} keys %xl_sql_map_cols;
				
				my $sqldump_col="";
				foreach (@matched_sql_col) {
					$sqldump_col = lc $xl_sql_map_cols{$_};
				}
				
				if ($sqldump_col ne "" && defined $sqldump_col) {
					# Extract xl dump cell's column reference
					my $xldump_cell_col_ref = $xldump_cols{$xldump_col};
					my ($row, $col) = xl_cell_to_rowcol($xldump_cell_col_ref);
					my $last_row_ref2 = xl_rowcol_to_cell($total_excel_rows-1, $col);
					my $dump_rng = $dump_sheet->Range($xldump_cell_col_ref.":".$last_row_ref2);
					my $sqldump_cell_col_ref = "";
					$sqldump_cell_col_ref = $sqldump_cols{$sqldump_col};
					($row, $col) = xl_cell_to_rowcol($sqldump_cell_col_ref);
					my $last_row_ref3 = xl_rowcol_to_cell($total_excel_rows-1, $col);
					my $sql_rng = $data_sheet->Range($sqldump_cell_col_ref.":".$last_row_ref3);
					
					$sql_rng->{Value} = $dump_rng->{Value};
					print "$xldump_col pasted!\n";
				} else {
					print "Processing $xldump_col aborted as it is not a required column!\n";
				}
			}

			# Filtering data
			$data_sheet->Range("A2")->Select;
			$xl_app->ActiveWindow->{FreezePanes} = 'True';
            #$self->autoFilterAndDeleteFilteredRows($total_excel_rows, $header_count, "A1", "A2", "18", "<>INDIA_COMM_1", $data_sheet);

			# Writing Serial Numbers for the data sheet			
			my $total_excel_rows2 = $data_sheet->UsedRange->Rows->{'Count'};
			$self->writeIndexes(1, $total_excel_rows2, "A", $data_sheet);
			
			# Copying Customer Names in a new sheet and removing Duplicates
			my $col_name = "Customer_Name";
			my $xldump_cell_col_ref = $sqldump_cols{lc $col_name};
			$self->copyPasteAndRemoveDuplicates($xl_book, $xldump_cell_col_ref, $col_name, "unique_customers", $total_excel_rows, $data_sheet, $xl_app);
			
			# Copying Partner Names in a new sheet and removing Duplicates
			$col_name = "Partner_Name";
			$xldump_cell_col_ref = $sqldump_cols{lc $col_name};
			$self->copyPasteAndRemoveDuplicates($xl_book, $xldump_cell_col_ref, $col_name, "unique_partners", $total_excel_rows, $data_sheet, $xl_app);

			# Copying Sub Business Entity in a new sheet and removing Duplicates
			$col_name = "Internal_Sub_Business_Entity_Name";
			$xldump_cell_col_ref = $sqldump_cols{lc $col_name};
			$self->copyPasteAndRemoveDuplicates($xl_book, $xldump_cell_col_ref, $col_name, "unique_technologies", $total_excel_rows, $data_sheet, $xl_app);

			# Copying Sales Agents in a new sheet and removing Duplicates
			$col_name = "TBM";
			$xldump_cell_col_ref = $sqldump_cols{lc $col_name};
			$self->copyPasteAndRemoveDuplicates($xl_book, $xldump_cell_col_ref, $col_name, "unique_sales_agents", $total_excel_rows, $data_sheet, $xl_app);


		} else {
			print "There is a problem in the Excel sheet!\n";
			return;
		}
		print "Please check the data and confirm to close it by saving (Yes/No) [Default is No]: ";
		my $confirm = <STDIN>;
		
		if (defined chomp($confirm)) {
			if ($confirm =~ /^yes$/i) {
				$xl_app->{DisplayAlerts} = 0;
				$xl_book->Save;
				$xl_book->Close;
				$xl_app->{DisplayAlerts} = 1;
				$xl_app->Quit;			
			}
		}
		$xl_app->{DisplayAlerts} = 1;
		$xl_app = 0;
		$xl_book = 0;
	} else {
		if (!defined $dir_name) {
			print "\nPath Name cannot be empty string!\n";
		} elsif (!defined $file_name) {
			print "\nFile Name cannot be empty string!\n";
		} else {
			print "\nSheet name cannot be empty string!\n";
		}
	}
	$dbh->disconnect();
	print "Parsing is completed!\n";
	return;
}

sub copyPasteAndRemoveDuplicates {
	my $self = shift;
	my $xl_book = shift;
	my $xl_cell_ref = shift;
	my $col_name = shift;
	my $new_sheet_name = shift;
	my $total_excel_rows = shift;
	my $data_sheet = shift;
	my $xl_app = shift;
	
	my $xl_const_obj = $self->getXLConstantObject();			
	my $col_names = $col_name . "s";
	print "Copying Unique $col_names to a new sheet...\n";
	my ($row, $col) = xl_cell_to_rowcol($xl_cell_ref);
	my $first_row_ref = xl_rowcol_to_cell($row-1, $col);
	#print "Data Range first row ref: $first_row_ref | Row No.: $row\n ";
	my $last_row_ref = xl_rowcol_to_cell($total_excel_rows-1, $col);
	my $rng = $data_sheet->Range($first_row_ref . ":" . $last_row_ref);
	my $new_sheet = $xl_book->Sheets->Add;
	$new_sheet->{Name} = $new_sheet_name;
	
	($row, $col) = xl_cell_to_rowcol("A1");
	$first_row_ref = xl_rowcol_to_cell($row, $col);
	#print "New Range first row ref: $first_row_ref | Row No.: $row\n ";
	$last_row_ref = xl_rowcol_to_cell($total_excel_rows-1, $col);
	my $dest_rng = $new_sheet->Range($first_row_ref . ":" . $last_row_ref);
	
	$dest_rng->{Value} = $rng->{Value};
	print "Copying Unique $col_names to a new sheet completed!\n";

	print "Removing Duplicates from $col_names...\n";
	$dest_rng->RemoveDuplicates("1", $xl_const_obj->{xlYes});
	print "Duplicates from $col_names Removed!\n";
	
	# Autofit the column
	$dest_rng->Columns()->Autofit();

	# Sort the column
	$dest_rng->Sort({Key1 => $dest_rng, Order1 => $xl_const_obj->{xlAscending}, Header => $xl_const_obj->{xlYes}});

	$new_sheet->Range("B1")->{Value} = "Unique_Names"; # Name the Second column
	$new_sheet->Range("C1")->{Value} = "Vertical" if $col_name =~ /customer/i; # Name the Third column
	$new_sheet->Range("D1")->{Value} = "Remarks"; # Name the Fourth column
	$new_sheet->Range("E1")->{Value} = "Matching_Methods"; # Name the Fifth column
	$new_sheet->Range("F1")->{Value} = "SQL_Upload"; # Name the Fifth column
	$new_sheet->Activate();
	$new_sheet->Range("A2")->Select;
	$xl_app->ActiveWindow->{FreezePanes} = 'True';

}

sub autoFilterAndDeleteFilteredRows {
	my $self = shift;
	my $total_excel_rows = shift;
	my $total_excel_columns = shift;
	my $first_cell_ref = shift;
	my $first_data_cell_ref = shift;
	my $filter_col_number_as_string = shift;
	my $criterion = shift;
	my $data_sheet = shift;
	
	my $xl_const_obj = $self->getXLConstantObject();			
	print "Filtering Data for $criterion complement...\n";
	my $last_row_ref = xl_rowcol_to_cell($total_excel_rows-1, $total_excel_columns-1);
	my $full_data_rng = $data_sheet->Range("$first_cell_ref:$last_row_ref");
	$full_data_rng->Columns()->AutoFilter();
	$full_data_rng->Columns()->AutoFilter($filter_col_number_as_string, $criterion);
	print "Filtering Data for $criterion complement completed!\n";
	print "Block only visible cells for the deleting...\n";
	my $full_filtered_data_rng = $data_sheet->Range("$first_data_cell_ref:$last_row_ref");
	my $visi_rng = $full_filtered_data_rng->Cells()->SpecialCells($xl_const_obj->{xlCellTypeVisible});
	print "Visible cells for the deletion got blocked!\n";
	
	print "Deleting Data other than INDIA_COMM_1...\n";
	$visi_rng->EntireRow->Delete(); # Delete the rows that are not INDIA_COMM
	print "Data other than INDIA_COMM_1 got deleted!\n";
	
	# Remove Filters
	$full_data_rng->Columns()->AutoFilter();
}

sub sortRange {
	my $self = shift;
	
	
}

sub writeIndexes {
	my $self = shift;
	my $start_number = shift;
	my $total_excel_rows = shift;
	my $col_name = shift;
	my $data_sheet = shift;
	
	my $end_number = $start_number + $total_excel_rows;
	my @serial_no = map{[$_]} ($start_number .. $end_number);
	my $last_row = $total_excel_rows;
	my $first_row_ref = $col_name . "2";
	my $last_row_ref = $col_name . $last_row;	
	my $serial_no_rng = $data_sheet->Range($first_row_ref . ":" . $last_row_ref);
	$serial_no_rng->{Value} = [@serial_no];
	
}

sub getExcelColumnInKeysAndValues {
	my $self = shift;
	my $xl_sheet = shift;
	my $start_row = shift;
	my $last_row = shift;
	my $name_col = shift;
	my $unique_name_col = shift;
	my $vertical_col = shift;
	my $remarks_col = shift;
	my $match_type_col = shift;
	my $upload_status_col = shift;
	my $fetching_criterion = shift;
	my $fetching_criterion_field = shift;
	
	my %content_hash;
	my $row_counter = $start_row;
	my $loop_first_time_checker = 1;
	my $is_first_time = 1;
	
	print "Fetching Name as Key and Cell reference as Value...\n";
	while ($row_counter <= $last_row) { # Iterate over the each row and stores the names from the excel sheet
		my $name = ""; my $unique_name = ""; my $remarks = ""; my $upload_status = ""; my $vertical = ""; my $match_type_string = "";
		try {
				$match_type_string = $xl_sheet->Cells($row_counter, $match_type_col)->{Value};
		} catch {
			$match_type_string = "";
		};
		try {
				$upload_status = $xl_sheet->Cells($row_counter, $upload_status_col)->{Value};
		} catch {
			$upload_status = "";
		};
		try {
				$vertical = $xl_sheet->Cells($row_counter, $vertical_col)->{Value};
		} catch {
			$vertical = "";
		};
		try {
				$remarks = $xl_sheet->Cells($row_counter, $remarks_col)->{Value};
		} catch {
			$remarks = "";
		};
		try {
				$unique_name = $xl_sheet->Cells($row_counter, $unique_name_col)->{Value};
		} catch {
			$unique_name = "";
		};
		try {
				$name = $xl_sheet->Cells($row_counter, $name_col)->{Value};
		} catch {
			$name = "";
		};
		my $name_col_ref = xl_rowcol_to_cell($row_counter-1, $name_col-1);
		my $unique_name_col_ref = xl_rowcol_to_cell($row_counter-1, $unique_name_col-1);
		my $vertical_col_ref = xl_rowcol_to_cell($row_counter-1, $vertical_col-1);
		my $remarks_col_ref = xl_rowcol_to_cell($row_counter-1, $remarks_col-1);
		my $match_type_col_ref = xl_rowcol_to_cell($row_counter-1, $match_type_col-1);
		my $upload_status_col_ref = xl_rowcol_to_cell($row_counter-1, $upload_status_col-1);
		my $cell_ref = {
			name_col_ref => $name_col_ref,
			unique_name_col_ref => $unique_name_col_ref,
			vertical_col_ref => $vertical_col_ref,
			remarks_col_ref => $remarks_col_ref,
			match_type_col_ref => $match_type_col_ref,
			upload_status_col_ref => $upload_status_col_ref
		};
		
		given ($fetching_criterion_field) {
			when (/upload/i) {
				$content_hash{$name} = $cell_ref if lc $upload_status eq lc $fetching_criterion && $name ne "";
			}
			when (/vertical/i) {
				$content_hash{$name} = $cell_ref if lc $vertical eq lc $fetching_criterion && $name ne "";
			}
			default {
			$content_hash{$name} = $cell_ref if lc $remarks ne lc $fetching_criterion && $name ne "";
			}
		}
		if ($loop_first_time_checker) {
			if ($match_type_string ne "") {
				$is_first_time = 0 ;
				$loop_first_time_checker = 0;
			}
		}
		$row_counter++;
	} # while loop end
	print "Completed fetching Name as Key and Cell reference as Value!\n";
	return (\%content_hash, $is_first_time);
} 

sub uniqueDataFromSQL {
	my $self = shift;
	my $param_string = shift;

	my %unique_data_hash;
	my @empty_array = ();
	my %params;	
	my $sql_obj = modules::reports::report_maker->new();
	my $dbh = $sql_obj->getMySQLDBH();
	my $qq_string = "SELECT DISTINCT names, unique_names, vertical as supp_data1 FROM universal_unique_names";
	my $where_clause;
	
	if ($param_string || $param_string ne "") {
		$where_clause = " WHERE name_catagory=?";
		$params{1} = $param_string;
	} else {
		$where_clause = "";
	}
	$qq_string = $qq_string . $where_clause;
	my $query_string = qq{$qq_string};
	my $sth = ($param_string || $param_string ne "") ? $self->getSimpleSTH($dbh, $query_string, %params) : $self->getSimpleSTH($dbh, $query_string, @empty_array);
	print "Fetching Unique Data from SQL Database...\n";
	while (my $hash_ref = $sth->fetchrow_hashref()) {
		#print '$hash_ref->{"account_name"} | $hash_ref->{"corporate_name"} | $hash_ref->{"vertical"}\n';
		$unique_data_hash{$hash_ref->{"names"}} = {
			corporate_name => $hash_ref->{"unique_names"},
			vertical => $hash_ref->{"supp_data1"}
		};
	}			
	$sth->finish();
	$dbh->disconnect();
	print "Completed fetching Unique Data from SQL Database!\n";
	return %unique_data_hash;
}

sub redundantWordsFromSQL {
	my $self = shift;

	my @data_array;
	my @empty_array = ();	
	my $sql_obj = modules::reports::report_maker->new();
	my $dbh = $sql_obj->getMySQLDBH();
	my $qq_string = "SELECT DISTINCT redundant_words FROM redundant_words";
	my $query_string = qq{$qq_string};
	my $sth = $self->getSimpleSTH($dbh, $query_string, @empty_array);
	print "Fetching Redundant words from SQL Database...\n";
	while (my $hash_ref = $sth->fetchrow_hashref()) {
		push @data_array, $hash_ref->{"redundant_words"}; 
	}			
	$sth->finish();
	$dbh->disconnect();
	print "Completed fetching Unique Data from SQL Database!\n";
	return \@data_array;
}


sub uploadNewNames {
	my $self = shift;

	my $DEFAULT_MAPPING_SOURCE_CHOICE = 3;
	my $mapping_source_string;
	
	print "De serializing the file information in to a variable...\n";
	my $dump = new XML::Dumper;
	my $file_name_of_input_data = "input_data.xml";
	my $ab_path; my $unique_customers_sheet_name; my $unique_partners_sheet_name; my $unique_technologies_sheet_name; my $unique_sales_agents_sheet_name;
	
	if (-f $file_name_of_input_data) {
		my $input_data = $dump->xml2pl($file_name_of_input_data);
		my $sheet_name;
		foreach (@{$input_data}) {
			$ab_path = $_->{absolute_path};
			$unique_customers_sheet_name = $_->{unique_customers};
			$unique_partners_sheet_name = $_->{unique_partners};
			$unique_technologies_sheet_name = $_->{unique_technologies};
			$unique_sales_agents_sheet_name = $_->{unique_sales_agents};
		}
	} else {
		print "\nPath of the Source file: ";
		my $dir_name = <STDIN>;
		print "Source file name: ";
		my $file_name = <STDIN>;
		print "Source file sheet name: ";
		my $unique_customers_sheet_name = <STDIN>;
	
		if (defined chomp($dir_name) && defined chomp($file_name) && defined chomp($unique_customers_sheet_name)) {
			# Make ready of absolute path for the file 
			my $path_string = $dir_name . '/';
			$ab_path = $path_string . $file_name;
			
			my $input_data = [
				{
					absolute_path => $ab_path,
					unique_customers => $unique_customers_sheet_name,
					unique_partners => "unique_partners",
					unique_technologies => "unique_technologies",
					unique_sales_agents => "unique_sales_agents",
				}
			];
			
			my $file_name_of_input_data = "input_data.xml";
			my $dump = new XML::Dumper;
			$dump->pl2xml($input_data, $file_name_of_input_data);
		} else {
			print "Input strings on the file name and path are Incorrect!\n";
			return;
		}
	}

	print "Completed de serializing the file information in to a variable!\n";

	print "\nThe following are the Mapping Source...\n";
	print "=========================================\n";
	print "1. Booking Data Customer Names\n";
	print "2. Booking Data Partner Names\n";
	print "3. Universal Unique Names\n";
	print "Enter your choice from 1 to 3 [DEFAULT 3]: ";
	my $mapping_source_choice = <STDIN>;
	if (defined chomp($mapping_source_choice)) {
		$mapping_source_choice = $DEFAULT_MAPPING_SOURCE_CHOICE if (!looks_like_number($mapping_source_choice) || $mapping_source_choice > $DEFAULT_MAPPING_SOURCE_CHOICE);
		print "\nEntered choice is not a number, hence default option is applicable!!!\n";
	} else {
		$mapping_source_choice = $DEFAULT_MAPPING_SOURCE_CHOICE;
		print "\nDefault option is applicable!!!\n";
	}
	
	
	given ($mapping_source_choice) {
		when (1) {$mapping_source_string = "Booking_Data_Customers"} 
		when (2) {$mapping_source_string = "Booking_Data_Partners"} 
		default {
			# Nothing to do
		}
	}
	
	
	# Opening the Required Excel Sheet
	# ================================
	print "Opening $ab_path file...\n";
	my $xl_app = $self->getXLApp();
	$xl_app->{Visible} = 1;
	$xl_app->{DisplayAlerts} = 0;
	my $xl_book = $xl_app->Workbooks->open($ab_path);
	my $unique_sheet = $xl_book->Sheets($unique_customers_sheet_name);
	my $last_row_in_unique_sheet = $unique_sheet->UsedRange->Rows->{'Count'};
	my $start_row = 2; my $name_col = 1; my $unique_name_col = 2; my $vertical_col = 3; my $remarks_col = 4; my $match_type_col = 5; my $upload_status_col = 6;
	my $row_counter = $start_row;
	my $last_row_with_short = $last_row_in_unique_sheet;

	my $should_search = 1; 
	my ($names_hashref, $is_first_time) = $self->getExcelColumnInKeysAndValues($unique_sheet, $start_row, $last_row_with_short, $name_col, 
								$unique_name_col, $vertical_col, $remarks_col, $match_type_col, $upload_status_col, "upload", "upload");
	
	my $tot_names = keys %{$names_hashref};
	
	$unique_sheet->Activate();
	print "$ab_path file is now opened and activated the unique_customers sheet!\n";
	# Excel Sheet opened
	# ================================
	
	my $unique_sheet_rng = $unique_sheet->Range("A1".":"."F".$last_row_with_short);
	$unique_sheet_rng->Columns()->AutoFilter();

	my $remaining_names_hashref;
	my $loop_counter = 0;
	
	foreach my $base_name (sort{lc(${$names_hashref}{$a}) cmp lc(${$names_hashref}{$b})} keys %{$names_hashref}) { # Iteration through the excel sheet's contents

		$xl_app->ActiveWindow->SmallScroll({Down => 1}) if ++$loop_counter > 15;
		my $matched_hash_ref = "";
		my $MATCH_TYPE;
		my $search_loop_counter = 1;
		my $cell_ref = ${$names_hashref}{$base_name};
		my $name_col_ref = ${$cell_ref}{"name_col_ref"};
		my $unique_name_col_ref = ${$cell_ref}{"unique_name_col_ref"};
		my $vertical_col_ref = ${$cell_ref}{"vertical_col_ref"};
		my $remarks_col_ref = ${$cell_ref}{"remarks_col_ref"};
		my $match_type_col_ref = ${$cell_ref}{"match_type_col_ref"};
		my $upload_status_col_ref = ${$cell_ref}{"upload_status_col"};
		
		my $names = $unique_sheet->Range($name_col_ref)->{Value};
		my $unique_names = $unique_sheet->Range($unique_name_col_ref)->{Value};
		my $vertical = $unique_sheet->Range($vertical_col_ref)->{Value};
		my $name_catagory = "";
		my %params;
		$params{1} = $names;
		$params{2} = $unique_names;
		$params{3} = $vertical;
		$params{4} = $name_catagory;
# Yet to add code
# =======================================================
# =======================================================
# =======================================================
# =======================================================
# =======================================================
		
	} # end of names hash iteration
	
	print "Search Methods completed!\n";
	$xl_app->{DisplayAlerts} = 1;
}

sub verticalFinder {
	my $self = shift;

	my $DEFAULT_MAPPING_SOURCE_CHOICE = 3;
	my $mapping_source_string;
	
	print "De serializing the file information in to a variable...\n";
	my $dump = new XML::Dumper;
	my $file_name_of_input_data = "input_data.xml";
	my $ab_path; my $unique_customers_sheet_name; my $unique_partners_sheet_name; my $unique_technologies_sheet_name; my $unique_sales_agents_sheet_name;
	
	if (-f $file_name_of_input_data) {
		my $input_data = $dump->xml2pl($file_name_of_input_data);
		my $sheet_name;
		foreach (@{$input_data}) {
			$ab_path = $_->{absolute_path};
			$unique_customers_sheet_name = $_->{unique_customers};
			$unique_partners_sheet_name = $_->{unique_partners};
			$unique_technologies_sheet_name = $_->{unique_technologies};
			$unique_sales_agents_sheet_name = $_->{unique_sales_agents};
		}
	} else {
		print "\nPath of the Source file: ";
		my $dir_name = <STDIN>;
		print "Source file name: ";
		my $file_name = <STDIN>;
		print "Source file sheet name: ";
		my $unique_customers_sheet_name = <STDIN>;
	
		if (defined chomp($dir_name) && defined chomp($file_name) && defined chomp($unique_customers_sheet_name)) {
			# Make ready of absolute path for the file 
			my $path_string = $dir_name . '/';
			$ab_path = $path_string . $file_name;
			
			my $input_data = [
				{
					absolute_path => $ab_path,
					unique_customers => $unique_customers_sheet_name,
					unique_partners => "unique_partners",
					unique_technologies => "unique_technologies",
					unique_sales_agents => "unique_sales_agents",
				}
			];
			
			my $file_name_of_input_data = "input_data.xml";
			my $dump = new XML::Dumper;
			$dump->pl2xml($input_data, $file_name_of_input_data);
		} else {
			print "Input strings on the file name and path are Incorrect!\n";
			return;
		}
	}

	print "Completed de serializing the file information in to a variable!\n";

	# Opening the Required Excel Sheet
	# ================================
	print "Opening $ab_path file...\n";
	my $xl_app = $self->getXLApp();
	$xl_app->{Visible} = 1;
	$xl_app->{DisplayAlerts} = 0;
	my $xl_book = $xl_app->Workbooks->open($ab_path);
	my $unique_sheet = $xl_book->Sheets($unique_customers_sheet_name);
	my $last_row_in_unique_sheet = $unique_sheet->UsedRange->Rows->{'Count'};
	my $start_row = 2; my $name_col = 1; my $unique_name_col = 2; my $vertical_col = 3; my $remarks_col = 4; my $match_type_col = 5; my $upload_status_col = 6;
	my $row_counter = $start_row;
	my $last_row_with_short = $last_row_in_unique_sheet;

	my $should_search = 1; 
	my ($names_hashref, $is_first_time) = $self->getExcelColumnInKeysAndValues($unique_sheet, $start_row, $last_row_with_short, $name_col, 
								$unique_name_col, $vertical_col, $remarks_col, $match_type_col, $upload_status_col, "no match", "vertical");
	
	my $tot_names = keys %{$names_hashref};
	
	$unique_sheet->Activate();
	print "$ab_path file is now opened and activated the unique_customers sheet!\n";
	# Excel Sheet opened
	# ================================
	
	my $unique_sheet_rng = $unique_sheet->Range("A1".":"."F".$last_row_with_short);
	$unique_sheet_rng->Columns()->AutoFilter();

	my $remaining_names_hashref;
	my $loop_counter = 0;
	
	foreach my $base_name (sort{lc(${$names_hashref}{$a}) cmp lc(${$names_hashref}{$b})} keys %{$names_hashref}) { # Iteration through the excel sheet's contents

		$xl_app->ActiveWindow->SmallScroll({Down => 1}) if ++$loop_counter > 15;
		my $matched_hash_ref = "";
		my $MATCH_TYPE;
		my $search_loop_counter = 1;
		my $cell_ref = ${$names_hashref}{$base_name};
		my $name_col_ref = ${$cell_ref}{"name_col_ref"};
		my $unique_name_col_ref = ${$cell_ref}{"unique_name_col_ref"};
		my $vertical_col_ref = ${$cell_ref}{"vertical_col_ref"};
		my $remarks_col_ref = ${$cell_ref}{"remarks_col_ref"};
		my $match_type_col_ref = ${$cell_ref}{"match_type_col_ref"};
		my $upload_status_col_ref = ${$cell_ref}{"upload_status_col"};
		
		my $vertical = $self->getIndustryVertical($base_name);		
		$unique_sheet->Range($vertical_col_ref)->{Value} = $vertical;
		
	} # end of names hash iteration
	
	print "Industry Vertical validation completed!\n";
		print "Please check the data and confirm to close it by saving (Yes/No) [Default is No]: ";
		my $confirm = <STDIN>;
		
		if (defined chomp($confirm)) {
			if ($confirm =~ /^yes$/i) {
				$xl_app->{DisplayAlerts} = 0;
				$xl_book->Save;
			}
		}
		$xl_app->{DisplayAlerts} = 1;
		$xl_app = 0;
		$xl_book = 0;
}

sub getIndustryVertical {
	my $self = shift;
	my $base_name = shift;
		my $vertical = "";
		
		given ($base_name) {
			when (/(school|college|collge|colledge|collegde|colledeg|institu|intitution
					|educat|UNIVERSITY|UNIVERSITIES|academy|VIDYAPEETH|POLYTECHNIC
					|POLY TECHNIC|VIDYALAYA|campus|univ|instt)/i) {
				$vertical = lc "EDUCATION- PUBLIC/PRIVATE";
			}
			when (/(pharma|industries|industry|chemical|engineering|manufacturing|
					manufacture|polymer|packages|packaging|printers|printing|rubber|tyre|cement
					|sugar|ceramic|automobile|component|electrical|electronic|conveyor
					|motor|works|garment|exports|foundry|FOUNDARIES|forging|steel|minarals
					|jewel|leather|tools|automatic|conglom|elevator|machiner
					|life scien|lifescien|METALLURG|brake|braking|aluminiu|lifts|shoe|drugs
					|organic|controls|automotive|semiconduc|semi conduct|organic|products
					|glass|welding|springs|equipment|PLASTIPACK|PLASTI PACK|companies|clothing
					|paints|mills|spinning|iron|autocar|products|foods|ispat|fibre|coats |HYDRAULIC|cables|gadgets|crop scien|cropscien|cycles|rotorcraft |rotor craft|drugs|VEHICLES|knit|alloy|petrochem|petro chem|MACHINER |nuts and bolts|fastener|cement|Surfactant|textile
					|POLYESTER|mineral|industria|PNEUMATIC|Exhaust|metals|tubes|Transformers
					|Rectifiers|MOSAIC|ADDITIVE|TEXTILE|AUTO PART|REFRACTOR|EXTRUSION|ENGINE|GEAR
					|Fitting|Machinery|paper mill|pulp|papermill|AUTO CAR|PIPES|FOOTWEAR|FOOT WEAR|RESINS|BEARING|FABRIC|POWER SYS|POLYECOATER
                    |AUTOMATION|innovati|clothing|plastic|bearing|sponge|piping sys|techno cast|technocast|seating|POLYFIBER
                    |robotic|EXTRACTIONS|COMPOSITE|network|net work|ABRASIVE|copper|axles|POLYOLEFIN|switch gear|switchgear
                    |radiator|DYES|INTERMEDIATES|thermoplas|thermo plas|metaldust|metal dust|strip|seeds|POLYFABRIK
                    |POLYPLAST|POLY PLAST)/i) {
				$vertical = lc "MANUFACTURING";
			}
			when (/(construction|construct|projects|engineer|builder|property
					|developer|properties|building|villas|housing|realty|reality
					|INFRASTRUCTURE|real estate|realestate|HOMES|REALTOR|BULDER|architec|interior)/i) {
				$vertical = lc "CONSTRUCTION";
			}
			when (/(energy|utiliti|utility|gas|petrol|diesel|bridge)/i) {
				$vertical = lc "Energy & Utility";
			}
			when (/(financial|finance|bank|insurance|broker|stock|exchange|capital
				|mortgage|lender|credit|advisor|consulting solu|consultingsolu|SECURITIES
				|PORTFOLIO|PORT FOLIO|broking|brokerage|investment|SECURITIES|capital|money
				|EQUITIES|EQUITY|CHITS)/i) {
				$vertical = lc "FINANCIAL SERVICES";
			}
			when (/(hospital|hotel|inn|motel|lodge|lodging|resort|neurocare|nursing|medical centre
			|medicalcentre|medical center|medicalcenter|clinic|leisure|RESIDENCY|HOSPETIAL)/i) {
				$vertical = lc "Hospitality";
			}
			when (/(health care|healthcare|laboratories|laboratory|bio|NUTRITION|MEDICARE)/i) {
				$vertical = lc "HEALTH CARE";
			}
			when (/(ministry|government|ministries|department|dept\.|director|directorate
					|parliament|forest|tax|income|constitution|navy|army|air force|airforce
					|water board|waterboard|commanding|commands|defense|municipality|panchayat
					|municipal|BUREAU|COMMISSIONERATE|electricity board|govt|consulate
					|METROPOLITAN|WATER SUPPLY|WATERSUPPLY|SEWERAGE|court|CIVIL COURT|CIVILCOURT
					|railway|rail way|COAST GUARD|COASTGUARD)/i) {
				$vertical = lc "GOVERNMENT";
			}
			when (/(news|channel|publication|media|entertainment|movie|cinema
					|tv|television|broadcast|telecast|tele cast|broadcast|broad cast
					|MULTIPLEX|MULTI PLEX)/i) {
				$vertical = lc "MEDIA/ENTERTAINMENT";
			}
			when (/(mall|whole sale|wholesale|retail|shop|furniture|super market|supermarket
					|hyper market|hypermarket|interior|computers|stores|fashions|fashion des
					|restaurant)/i) {
				$vertical = lc "RETAIL";
			}
			when (/(telecommuni|tele communi|internet service|internet|packers & movers
					|packers and movers|management service|corporate service|cable vision|cablevision
                    |broadband|broad band)/i) {
				$vertical = lc "SERVICE PROVIDER";
			}
			when (/(infotech|info tech|technologies|technology|software|soft ware
					|information service|info com|infocom|communicat
					|it soluti|itsoluti|infosyst|info syst|IT SERVICE|TECHNOLOGI|bpo
					|b\.p\.o|call centre|callcentre|call center|callcenter|softech
					|data solu|softek|cyber|softlink|soft link|info servi|infoservi
					|business services|TECHNOLGIES|BPO SER|Business Proces|info soft|infosoft
                    |E CONSULTING|info solut|INFOSERVE|infosys|infratech|labs|)/i) {
				$vertical = lc "Professional/IT services";
			}
			when (/(transport|airport|air port|logistic|travel|shipping|cargo|airline|air line
					|freight|carrier|clearing|express|courier|parcel|fleet|lines|container|liner
					|aviation|DISTRIBUTION|tour)/i) {
				$vertical = lc "Transport";
			}
			default {
				$vertical = lc "OTHERS";
			}
		} 
	return $vertical;
}


1;
