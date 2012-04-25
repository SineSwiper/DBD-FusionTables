use 5.10.0;
use strict;
use warnings FATAL => 'all';
use inc::Module::Install;

name     'DBD-FusionTables';
all_from 'lib/DBD/FusionTables.pm';
author   q{Brendan Byrd <BBYRD@CPAN.org>};
license  'lgpl3';

perl_version 5.10.0;

tests_recursive('t');

resources (
   #homepage   => 'http://yourwebsitehere.com',
   #IRC        => 'irc://irc.perl.org/#DBD-FusionTables',
   license    => 'http://www.gnu.org/licenses/lgpl-3.0.html',
   #repository => 'git://github.com/Brendan Byrd/DBD-FusionTables.git',
   bugtracker => 'http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBD-FusionTables',
);

configure_requires (
   'Module::Install' => 0,
);

build_requires (
   'Test::More' => 0,
);

requires (
   #'ABC'              => 1.6,
   #'Foo::Bar::Module' => 5.0401,
);

install_as_cpan;
auto_install;
WriteAll;
