package modules::helpers::calculator;
use lib 'C:/Jeyaraj/Analysis/PBG_Dashboards/perl_projects/data_miner/lib';
use strict;
use warnings;
no warnings 'uninitialized';
use modules::reports::report_maker;
use Scalar::Util qw(looks_like_number);
use v5.14;

#no warnings qw(experimental::smartmatch experimental::lexical_topic experimental::regex_sets experimental::lexical_subs);


sub new {
    my $class = shift;
    my $self = {};
    bless $self, $class;
    return $self;
}


sub formatUSD {
    my $self = shift;
    my $num = shift;

    my $rounded = $self->roundNum($num, 2);

    #add commas, all the "work" happens in the while condition
    while ($rounded =~ s/^(-?\d+)(\d\d\d)/$1,$2/){};
    return '$'.$rounded;
}

sub permuteArrayAndHash {
    my ($self, @array, %hash) = @_;
    my $count1 = scalar @array;
    my $count2 = scalar keys %hash;
    return $count1 * $count2;
}

sub permuteArrayAndArray {
    my ($self, @array1, @array2) = @_;
    my $count1 = scalar @array1;
    my $count2 = scalar @array2;
    return $count1 * $count2;
}

sub formatPercent {
    my $self = shift;
    my $num = shift;

    $num = $num * 100;
    my $rounded = $self->roundNum($num, 2);

    #add commas, all the "work" happens in the while condition
    return $rounded.'%';
}

sub roundNum {
    my ($self, $num, $dec) = @_;
    my $format = "%.".$dec."f";
    my $rounded = sprintf($format, $num);
    return $rounded;
}

sub getRatio {
    my ($self, $numero, $dinom) = @_;
    my $ratio = 0.0;
    unless ($dinom == 0) {
       $ratio = $numero/$dinom; 
    }
    #$ratio = $ratio*100;
    return $ratio;
}

sub getDiscount {
    my ($self, $net, $list) = @_;
    my $discount = 0.0;
    unless ($list == 0) {
       $discount = 1-($net/$list); 
    }
    #$discount = $discount*100;
    return $discount;
}

sub getMargin {
    my ($self, $cost, $rev) = @_;
    my $margin = 0.0;
    unless ($rev == 0) {
       $margin = 1-($cost/$rev); 
    }
    #$margin = $margin*100;
    return $margin;
}

sub getGrowth {
    my ($self, $cur, $prev) = @_;
    my $growth = 0.0;

    if (($prev == 0) || ($cur < 0 && $prev > 0) || ($cur > 0 && $prev < 0)) {
    } else {
       $growth = 1-($cur/$prev); 
    }
    #$growth = $growth*100;
    return $growth;
}

1;
