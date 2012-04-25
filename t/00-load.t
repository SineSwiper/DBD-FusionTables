#!perl -T
use 5.10.0;
use strict;
use warnings FATAL => 'all';
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'DBD::FusionTables' ) || print "Bail out!\n";
}

diag( "Testing DBD::FusionTables $DBD::FusionTables::VERSION, Perl $], $^X" );
