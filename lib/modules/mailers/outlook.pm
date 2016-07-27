package modules::mailers::outlook;
use strict;
use warnings;
use Mail::Outlook;

our $email = new Mail::Outlook();
our %error_hash = ();

sub new {
	my $class = shift;
	my $self = {
		_mailTo			=>	shift,
		_mailCC			=>	shift,
		_mailBCC		=>	shift,
		_subject		=>	shift,
		_body			=>	shift,
		_attachment		=>	shift,
		_enableDisplay	=>	shift,
		_toString		=>	shift,
		_ccString		=>	shift,
		_bccString		=>	shift,
		_errorHash		=>	shift,
	};
	bless $self, $class;
	return $self;
}

sub emailWithAttachmentOutlook{
	my ($self) = @_;
	if ($self->validateEmailID()) {
		my $msg = $email->create();
		$msg->To($self->{_toString});
		$msg->Cc($self->{_ccString});
		$msg->Bcc($self->{_bccString});
		$msg->Subject($self->{_subject});
		$msg->Body($self->{_body});
		$msg->Attach($self->{_attachment});
		if ($self->{_enableDisplay}) {
			$msg->display;
		}
		$msg->send;
		return 1;
	} else {
		return 0;
	}
}

sub validateEmailID {
	my ($self) = @_;
	my @ToArray = ();
	my @CcArray = ();
	my @BccArray = ();
	my $ToString = "";
	my $CcString = "";
	my $BccString = "";
	@ToArray = @{$self->{_mailTo}};
	@CcArray = @{$self->{_mailCC}};
	@BccArray = @{$self->{_mailBCC}};
	
	my $counter = 0;
	foreach my $string (@ToArray) {
		if ($self->isEmailIDValid($string, "To")) {
			if ($counter == 0) {
				$ToString = $string;
			} else {
				$ToString = $ToString."; ".$string;
			}
			$counter++;
		}
	}

	$counter = 0;
	foreach my $string (@CcArray) {
		if ($self->isEmailIDValid($string, "To")) {
			if ($counter == 0) {
				$CcString = $string;
			} else {
				$CcString = $CcString."; ".$string;
			}
			$counter++;
		}
	}

	$counter = 0;
	foreach my $string (@BccArray) {
		if ($self->isEmailIDValid($string, "To")) {
			if ($counter == 0) {
				$BccString = $string;
			} else {
				$BccString = $BccString."; ".$string;
			}
			$counter++;
		}
	}
	
	print "Email To: $ToString\n";
	print "Email Cc: $CcString\n";
	print "Email Bcc: $BccString\n";
	$self->{_toString} = $ToString;
	$self->{_ccString} = $CcString;
	$self->{_bccString} = $BccString;
	$self->{_errorHash} = \%error_hash;
	if (($ToString ne "") && ($CcString ne "") && ($BccString ne "")) {
		return 1;
	} else {
		return 0;
	}

}

sub isEmailIDValid {
	my ($self, $email_id, $id_type) = @_;
	if ($email_id =~ /^(.*@.*\.(com|org|in|net|edu|co.in))$/i) {
		return 1;
	} else {
		$error_hash{$id_type." (".$email_id.")"} = "is a NOT valid email id";
		return 0;
	}
	
}

1;
