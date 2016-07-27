package modules::templates::Greeter;
use strict;
use warnings;
use Win32::Console;

use Exporter;
our @EXPORT_OK = qw/printWelcomeMessage printExitingMessage printPrompt/;

sub new {
	my $class = shift;
	my $self = {};
	bless $self, $class;
	return $self;
}

sub printWelcomeMessage {
	print "======================================================================================\n";
	print "\n";
	print "**************************** WELCOME TO CISCO_DATA_MINER *****************************\n";
	print "\n";
	print "======================================================================================\n";
	print "###### Version 1.01.01\n##### a Unique  Data Handling Console Application\n";
	print "#### Owner: D. Jeyaraj\n### Divsion: Commercial Sales\n## Profile: Data Analytics\n";
	print "# CDM is meant to be an Internal Console Application which does not have any copyright\n";
	print "======================================================================================\n";
}

sub printPrompt {
	print "\nCDM::jeydurai\@cisco.com> ";
}
sub printExitingMessage {
	print "==================================================================================\n";
	print "\n";
	print "************************* THANK YOU FOR USING CISCO_DATA_MINER *******************\n";
	print "\n";
	print "==================================================================================\n";
	print "###### Hope you have had a fun using CDM\n##### CDM is still under modern development\n";
	print "#### New versions will have more features that will ease you in accomplishing your data oriented job\n";
	print "### NoSQL data handling, XML data parser are examples of new features...\n## 'Imagination' is more Important than 'Knowledge' -- Albert Einstein\n";
	print "# You can write to us your feedbacks to jeydurai\@cisco.com\n";
	print "==================================================================================\n";
}
1;
