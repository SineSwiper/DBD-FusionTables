##############################################################################
# DBD::FusionTables::Override Module                                         #
# E-mail: Brendan Byrd <Perl@resonatorsoft.org>                              #
##############################################################################

##############################################################################
# DBD::FusionTables::Override

# Overrides various functions from various modules that are not part of the
# standard set of overloads for SQL::Statement drivers

package   # hide from PAUSE
   DBD::FusionTables::Override;

use common::sense;  # works every time!
use SQL::Parser;

our $VERSION  = $DBD::FusionTables::VERSION;

1;

##############################################################################
# SQL::Parser

package   # hide from PAUSE
   SQL::Parser;

# fortunately, this is a small sub to overwrite
sub IDENTIFIER {
   my ($self, $id) = @_;
   
   return 1 if $id =~ m/^\?QI(.+)\?$/;
   if ($id =~ m/^(.+)\.([^\.]+)$/) {
      my $schema = $1;    # ignored
      $id = $2;
   }
   return 1 if $id =~ m/^".+?"$/s;    # QUOTED IDENTIFIER
   my $err = "Bad table or column name: '$id' ";    # BAD CHARS
   if ($id =~ /\W/) {
      $err .= "has chars not alphanumeric or underscore!";
      return $self->do_err($err);
   }
   # CSV requires optional start with _
   my $badStartRx = uc($self->{dialect}) eq 'ANYDATA' ? qr/^\d/ : qr/^[_\d]/;
   
   # FusionTables requires all numbers in tables, and has no StartRx requirements
   $badStartRx = uc($self->{dialect}) eq 'FUSIONTABLES' ? qr/^\W/ : $badStartRx;
   
   if ($id =~ $badStartRx) {                        # BAD START
      $err .= "starts with non-alphabetic character!";
      return $self->do_err($err);
   }
   if (length $id > 128) {                          # BAD LENGTH
     $err .= "contains more than 128 characters!";
     return $self->do_err($err);
   }
   $id = uc $id;
   if ($self->{opts}->{reserved_words}->{$id}) {    # BAD RESERVED WORDS
     $err .= "is a SQL reserved word!";
     return $self->do_err($err);
   }
   return 1;
}

# SQL::Parser will error out if these don't exist
sub DESCRIBE {
    my ($self, $str) = @_;
    $self->{struct}->{command} = 'DESCRIBE';
    my ($table_name) = $str =~ /^DESCRIBE (\S+)$/i;
    return $self->do_err('Incomplete DESCRIBE statement!') if !$table_name;
    return undef unless $self->TABLE_NAME($table_name);
    $self->{tmp}->{is_table_name} = { $table_name => 1 };
    $self->{struct}->{table_names} = [$table_name];
    $self->{struct}->{column_defs} = [
                                       {
                                         type  => 'column',
                                         value => '*'
                                       }
                                     ];
    return 1;
}

sub SHOW {
    my ($self, $str) = @_;
    $self->{struct}->{command} = 'SHOW';
    my ($sub_cmd) = $str =~ /^SHOW (\S+)$/i;
    return $self->do_err('Bad SHOW sub-command: $sub_cmd!') unless ($sub_cmd =~ /^TABLES$/i);
    return 1;
}

1;