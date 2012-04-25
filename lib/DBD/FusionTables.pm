##############################################################################
# DBD::FusionTables Module                                                   #
# E-mail: Brendan Byrd <Perl@resonatorsoft.org>                              #
##############################################################################

##############################################################################
# DBD::FusionTables

package DBD::FusionTables;

use common::sense;  # works every time!
use LWP::UserAgent;

our $VERSION  = "0.50.00";
our $drh      = undef;         # holds driver handle once initialized
our $err      = 0;             # DBI::err
our $errstr   = "";            # DBI::errstr
our $sqlstate = "";            # DBI::state

=head1 NAME

DBD::FusionTables - DBI driver abstraction for Google's Fusion Tables

=cut

sub driver {
   return $drh if $drh;      # already created - return same one
   my ($class, $attr) = @_;

   $class .= "::dr";

   DBD::FusionTables::dr->install_method('fust_connect');
   DBD::FusionTables::dr->install_method('fust_refresh_login');
   DBD::FusionTables::dr->install_method('fust_test_login');
   DBD::FusionTables::st->install_method('fust_execute_startup');
   DBD::FusionTables::st->install_method('fust_convert_bind_sql');
   DBD::FusionTables::st->install_method('fust_process_data');
   DBD::FusionTables::st->install_method('fust_read_more_data');
   DBD::FusionTables::st->install_method('fust_calc_cache_size');

   $drh = DBI::_new_drh($class, {
      'Name'        => 'FusionTables',
      'Version'     => $VERSION,
      'Err'         => \$DBD::FusionTables::err,
      'Errstr'      => \$DBD::FusionTables::errstr,
      'State'       => \$DBD::FusionTables::state,
      'Attribution' => 'DBD::FusionTables by Brendan Byrd',
      'UserAgent'   => init_ua(),
   }) || return undef;

   return $drh;
}

sub CLONE {
   undef $drh;
}

sub init_ua {
   # Create a user agent object
   my $ua = LWP::UserAgent->new;
   my $o = ucfirst($^O);
   my $os;
   
   if ($o eq 'MSWin32') {
      my @osver = Win32::GetOSVersion();
      $os = ($osver[0] || Win32::GetOSName()).' v'.join('.', @osver[1..3]);
   }
   else {
      $os = `/bin/uname -srmo`;
      $os =~ s/[\n\r]+|^\s+|\s+$//g;
   }

   $ua->agent("Mozilla/5.0 ($o; $os) ".$ua->_agent.' '.__PACKAGE__."/$VERSION");

   return $ua;
}

1;

##############################################################################

package   # hide from PAUSE
   DBD::FusionTables::dr; # ====== DRIVER ======

use common::sense;  # works every time!
use DBI;
use LWP::UserAgent;
use HTTP::Request;

our @dbh;
our $VERSION  = $DBD::FusionTables::VERSION;
our $imp_data_size = 0;

sub connect {
   my ($drh, $dr_dsn, $user, $auth, $attr) = @_;

   if ($dr_dsn =~ /\;|\=/) {  # is DSN notation
      foreach my $var (split /;/, $dr_dsn) {
         my ($attr_name, $attr_value) = split('=', $var, 2);
         return $drh->set_err($DBI::stderr, "Can't parse DSN part '$var'", '08001') unless (defined $attr_value);
          
         $attr_name = lc($attr_name);
         $attr_name = 'fust_'.$attr_name unless ($attr_name =~ /^fust_/o);
         $attr->{$attr_name} = $attr_value;
      }
   }
   else {                     # not DSN notation
      if    ($dr_dsn =~ /(\w+)\@([\da-zA-Z\.]+)(?:\:(\w+))?/) {
         $attr->{fust_database} = $1;
         $attr->{fust_host}     = $2;
         $attr->{fust_port}     = $3 || 'https';
      }
      elsif ($dr_dsn =~ /^(\w+)$/) {
         $attr->{fust_database} = $1;
      }
   }

   my $db   = delete $attr->{fust_database} || delete $attr->{fust_db} || 'fusiontables';
   my $host = delete $attr->{fust_host} || 'www.google.com';
   my $port = delete $attr->{fust_port} || 'https';
   $attr->{fust_url} = "$port://$host/fusiontables/api/query";

   $drh->fust_connect($user, $auth, $attr) or return $drh->set_err($DBI::stderr, "Can't connect to $dr_dsn", '08004');

   # create a 'blank' dbh (call superclass constructor)
   my ($outer, $dbh) = DBI::_new_dbh($drh, { Name => $dr_dsn });

   $dbh->STORE('Active' => 1);
   push @dbh, $dbh;

   return $outer;
}

sub fust_connect {
   my ($drh, $user, $auth, $attr) = @_;
   my $ua = $drh->{UserAgent};
   
   # FIXME: Check to see if FusionTables requires client IDs or URL signing
   # URL Signing:  (http://code.google.com/apis/maps/documentation/webservices/#URLSigning)
   
   # OAuth 2.0
   if ($attr->{fust_refresh_token}) {
      $attr->{fust_client_id}     ||= $user;
      $attr->{fust_client_secret} ||= $auth;
      return $drh->set_err($DBI::stderr, 'Refresh tokens require a Client ID and Client Secret!', '08001') unless ($attr->{fust_client_id} && $attr->{fust_client_secret});
   }
   elsif ($attr->{fust_access_token}) {
      $attr->{fust_authorization} = 'Bearer '.$attr->{fust_access_token};
   }
   # ClientLogin
   elsif (($attr->{fust_email} && $attr->{fust_passwd}) || ($user && $auth) || $attr->{fust_auth_token}) {
      $attr->{fust_email}  ||= $user;
      $attr->{fust_passwd} ||= $auth;
      $attr->{fust_client_account_type} ||= 'HOSTED_OR_GOOGLE';
      $ua->from( $attr->{fust_email} );
   }
   elsif ($attr->{fust_auth_token}) {
      $attr->{fust_authorization} = 'GoogleLogin auth='.$attr->{fust_access_token};
   }
   # Public only (hope for the best...)
   else {
      $attr->{fust_public_only} = 1;
      return 1;
   }
   
   # Support for most of the LWP::UserAgent attributes/methods
   foreach my $opt (map { s/^fust_lwp_// } grep { /^fust_lwp_/ } keys %$attr) {
      next if ($opt eq 'agent');  # verboten
      no strict 'refs';
      $attr->{fust_lwp_$opt} && &{$ua->$opt}($attr->{fust_lwp_$opt});
   }
   
   # Grab a token (if needed)
   $drh->fust_refresh_login($attr) unless ($attr->{fust_access_token} || $attr->{fust_auth_token});
   return $drh->set_err($DBI::stderr, 'No access token available', '08004') unless ($attr->{fust_authorization});
      
   # Test access token
   $drh->fust_test_login($attr) or do {   # Faulty token; get a new one or die trying...
      for (qw(access_token auth_token authorization)) { delete $attr->{'fust_'.$_}; }
      $drh->fust_refresh_login($attr) unless ($attr->{fust_access_token});
      return $drh->set_err($DBI::stderr, 'No access token available', '08004') unless ($attr->{fust_authorization});
   };
   
   return 1;
}

sub fust_refresh_login {
   my ($drh, $attr) = @_;
   my $ua = $drh->{UserAgent};
   
   if    ($attr->{fust_refresh_token}) {
      # Will need to fetch an access token
      my $req = HTTP::Request->new('POST' => 'https://accounts.google.com/o/oauth2/token');
      my $data = URI->new();
      $data->query_form(
         refresh_token => $attr->{fust_refresh_token},
         client_id     => $attr->{fust_client_id},
         client_secret => $attr->{fust_client_secret},
         grant_type    => 'refresh_token',
      );
      $req->content($data->query);
      $req->content_type('application/x-www-form-urlencoded');
      $req->content_length(length($data->query));

      my $res = $ua->request($req);
      $res->is_success or return $drh->set_err($DBI::stderr, "Google responded with HTML error during access refresh: ".$res->status_line, '08004');

      # (going to forgo the need for JSON support and just parse this manually...)
      if ($res->content =~ /\"access_token\" : \"(\S+)\"/) {
         $attr->{fust_access_token} = $1;
         my $type = ($res->content =~ /\"token_type\" : \"(\S+)\"/) ? $1 : 'Bearer';
         $attr->{fust_authorization} = $type.' '.$attr->{fust_access_token};
      }
      else {
         return $drh->set_err($DBI::stderr, 'Refresh successful, but Google did not respond with an access token', '08004');
      }

      return 1;
   }
   elsif ($attr->{fust_email} && $attr->{fust_passwd}) {
      my $req = HTTP::Request->new('POST' => $attr->{fust_url});
      my $data = URI->new();
      $data->query_form(
         accountType   => $attr->{fust_client_account_type} || 'HOSTED_OR_GOOGLE',
         Email         => $attr->{fust_email},
         Passwd        => $attr->{fust_passwd},
         service       => 'fusiontables',
         source        => 'PERL-DBD::'.$drh->{Name}."-$VERSION",
      );
      $req->content($data->query);
      $req->content_type('application/x-www-form-urlencoded');
      $req->content_length(length($data->query));

      my $res = $ua->request($req);
      $res->is_success or do {
         my $dbi_err = "Google responded with HTML error during ClientLogin refresh: ".$res->status_line;
         if ($res->content =~ /^Error=(\w+)/m) {
            my $err = $1;
            given ($err) {  
               when ('BadAuthentication')  { $dbi_err = 'Google responded with: UN/PW is not recognized'; }  
               when ('NotVerified')        { $dbi_err = 'Google responded with: Account email address has not been verified'; }
               when ('TermsNotAgreed')     { $dbi_err = 'Google responded with: User has not agreed to terms'; }  
               when ('CaptchaRequired')    { $dbi_err = 'Google responded with: CAPTCHA is required  (Go to https://www.google.com/accounts/DisplayUnlockCaptcha to resolve.)'; }
               when ('AccountDeleted')     { $dbi_err = 'Google responded with: Account has been deleted'; }  
               when ('AccountDisabled')    { $dbi_err = 'Google responded with: Account has been disabled'; }  
               when ('ServiceDisabled')    { $dbi_err = 'Google responded with: Service has been disabled'; }  
               when ('ServiceUnavailable') { $dbi_err = 'Google responded with: Service is not available; try again later'; }  
               default                     { $dbi_err = 'Google responded with: Unknown error'; }
            }
         }
         
         return $drh->set_err($DBI::stderr, $dbi_err, '08004');
      };

      if ($res->content =~ /^Auth=(\S+)/m) {
         $attr->{fust_auth_token} = $1;
         $attr->{fust_authorization} = 'GoogleLogin auth='.$attr->{fust_auth_token};
      }
      else {
         return $drh->set_err($DBI::stderr, 'Login successful, but Google did not respond with an Auth token', '08004');
      }
      
      return 1;
   }
   
   return undef;  # silently fail
}

sub fust_test_login {
   my ($drh, $attr) = @_;
   my $ua = $drh->{UserAgent};

   my $req = HTTP::Request->new('GET' => $attr->{fust_url});
   ### TODO: Figure out how to use this request to populate table_info ###
   $req->uri->query_form(sql => 'SHOW TABLES');  # (this kind of request requires proper access)
   $req->header( 'Authorization' => $attr->{fust_authorization} );
   
   my $res = $ua->request($req);   
   return $res->is_success;
}

sub data_sources {
   # Typically no need for parameters, as the defaults work just fine...
   return ('dbi:FusionTables:');
}

sub disconnect_all {
   while (my $dbh = shift @dbh) {
      ref $dbh && $dbh->disconnect;
   }
   return 1;
}

1;

##############################################################################

package   # hide from PAUSE
   DBD::FusionTables::db; # ====== DATABASE ======

use common::sense;  # works every time!
use SQL::Parser;
use SQL::Statement;
use DBD::FusionTables::Override;
use HTTP::Request;
use List::Util qw(first);

our $VERSION = $DBD::FusionTables::VERSION;
our $imp_data_size = 0;
our $cached_col_rows = {};

sub prepare {
   my ($dbh, $statement, @attribs) = @_;

   # create a 'blank' sth
   my ($outer, $sth) = DBI::_new_sth($dbh, { Statement => $statement });

   # Process the non-SELECT statements, since they are easy
   my @params;
   my @labels = qw(NUM_OF_FIELDS NAME TYPE PRECISION SCALE NULLABLE);
   uc($statement) =~ /^\s*(\w+)\s+/;
   my $cmd = $1;
   
   # ReadOnly checks
   ($dbh->FETCH('ReadOnly') && $cmd =~ /UPDATE|DELETE|DROP|CREATE|INSERT/) and
      return $dbh->set_err($DBI::stderr, "Driver is in ReadOnly mode; no active SQL statements allowed!", '42808');
   ($dbh->{fust_public_only} && $cmd ne 'SELECT') and
      return $dbh->set_err($DBI::stderr, "Driver is in public only mode; only SELECT statements on public tables are allowed!", '42808');
   
   given ($cmd) {                             #                               (from dbi_sql.h)
      when (/UPDATE|DELETE|DROP/) { @params = (0, [],                          [],         [],        [],                  []);      }
      when ('CREATE')             { @params = (1, ['tableid'],                 [4],        [10],      [0],                 [0]);     }
      when ('INSERT')             { @params = (1, ['rowid'],                   [4],        [10],      [0],                 [0]);     }
      when ('SHOW')               { @params = (2, ['table id','name'],         [4,12],     [10,255],  [0,undef],           [0,0]);   }
      when ('DESCRIBE')           { @params = (3, ['column id','name','type'], [12,12,12], [6,255,8], [undef,undef,undef], [0,0,0]); }
   }
   if (@params) {
      # for some dumb reason, NUM_OF_FIELDS _has_ to use STORE, and the rest _has_ to use the direct $sth->{} method
      for (0 .. 5) { $_ ? $sth->{$labels[$_]} = $params[$_] : $sth->STORE($labels[$_] => $params[$_]); }
   }
   
   # Remove trailing semi-colon
   $statement =~ s/\;\s*$//;
   ### FIXME: Support for multiple statements ###

   # Parse out the statement solely for setting params early
   my ($parser, $stmt);
   $parser = SQL::Parser->new('FusionTables', {PrintError=>0});  # only for purposes of grabbing certain parameters
   $stmt = SQL::Statement->new($statement, $parser);
   
   # can't figure out the statement, so bomb out without any other STOREs, but we'll still try it out in Fusion
   if (!$stmt || $stmt->{errstr}) {
      warn($stmt ? "SQL::Statement reported error while parsing SQL [$statement]: ".$stmt->{errstr} : "SQL::Statement failed to create for SQL [$statement]") if ($dbh->FETCH('Warn'));
      $sth->{fust_parser}   = $parser;
      $sth->{fust_sql_stmt} = $stmt;
      return $outer;
   }

   my @cols = @{$stmt->column_defs()};
   if ($stmt->command() eq 'SELECT' && @cols) {
      my $tbl;
      # Need more information about the tables in question
      foreach my $table (map { $_->name } $stmt->tables()) {  # probably one, but still...
         unless ($cached_col_rows->{$table} || $dbh->{fust_public_only}) {
            my $csth = $dbh->column_info(undef, undef, $table, '.*');
            $csth and $csth->finish;  # (no need to actually pull the info as it's already in $cached_col_rows)
         }
         $tbl = $table;  # going to assume one table right now...
      }

      my @names = map { lc } grep { $_ } map { $_->{alias} || $_->{value} || $_->{fullorg} } @cols;
      unless (@names ~~ /\*/) {
         $sth->STORE('NUM_OF_FIELDS', scalar @cols);

         # redefine it again to filter out functions
         @names = map { lc } grep { $_ } map { $_->{alias} || $_->{value} || '*' } @cols;
         unless (@names ~~ /\*/) {
            my $cache = $cached_col_rows->{$tbl};
            # FusionTables enforces case-sensitivity, but the likelyhood of conflicts doesn't justify breaking _lc/uc functions
            $sth->{'NAME'}         =             \@names;
            $sth->{'NAME_lc'}      = [ map { lc } @names ];
            $sth->{'NAME_uc'}      = [ map { uc } @names ];
            $sth->{'NAME_hash'}    = { map {    $names[$_]  => $_ } (0 .. (@names - 1)) };
            $sth->{'NAME_lc_hash'} = { map { lc($names[$_]) => $_ } (0 .. (@names - 1)) };
            $sth->{'NAME_us_hash'} = { map { uc($names[$_]) => $_ } (0 .. (@names - 1)) };

            if ($cache) {
               $sth->{'TYPE'}      = [ map { $cache->{$_}[13] } @names ];
               $sth->{'PRECISION'} = [ map { $cache->{$_}[ 6] } @names ];
               $sth->{'SCALE'}     = [ map { $cache->{$_}[ 8] } @names ];
               $sth->{'NULLABLE'}  = [ map { $cache->{$_}[10] } @names ];
            }
         }
      }
   }

   $sth->{'NUM_OF_PARAMS'} = scalar $stmt->params if (defined $stmt->params);

   $sth->{fust_parser}   = $parser;
   $sth->{fust_sql_stmt} = $stmt;
   $sth->{fust_cached_col_rows} = $cached_col_rows;

   return $outer;
}

sub disconnect {
   my ($dbh) = @_;

   $dbh->STORE('Active' => 0);
   for (keys %$dbh) { delete $dbh->{$_}; }
   undef $dbh;
   return 1;
}

sub ping {
   my ($dbh) = @_;
   my $ua = $dbh->{Driver}{UserAgent};
   my $req = HTTP::Request->new( 'GET', $dbh->{fust_url}."?sql=SELECT+animal+FROM+274822+WHERE+animal+=+'cat';" );  # Public table from Google SQL reference
   $dbh->{fust_authorization} and $req->header( 'Authorization' => $dbh->{fust_authorization} );
   my $res = $ua->request($req);

   return $res->is_success || $res->status_line;
}

sub last_insert_id {
   my ($dbh, $catalog, $schema, $table, $field) = @_;
   return $dbh->{fust_last_insert_id}->{$table};
}

# DBD::Oracle has a pretty awesome way of setting this up,
# so I'll just copy it
sub get_info {
   my ($dbh, $info_type) = @_;
   require DBD::FusionTables::GetInfo;
   my $v = $DBD::FusionTables::GetInfo::info{int($info_type)};
   $v = $v->($dbh) if (ref $v eq 'CODE');
   return $v;
}

sub get_avail_tables {
   my $dbh    = $_[0];
   my @tables = ();

   my $cmd = 'SHOW TABLES;';
   my $sth = $dbh->prepare_cached($cmd) || return $dbh->set_err($DBI::stderr, $dbh->errstr, $dbh->state);
   $sth->execute() || return $dbh->set_err($DBI::stderr, $dbh->errstr, $dbh->state);

   while (my $row = $sth->fetchrow_hashref()) {
      push @tables, [ $row->{'table id'}, undef, $row->{'table id'}, "TABLE", $row->{'name'} ];
   }

   return @tables;
}

sub table_info {
   my $dbh = shift;
   my $names = [qw( TABLE_QUALIFIER TABLE_OWNER TABLE_NAME TABLE_TYPE REMARKS )];

   $dbh->{fust_public_only} and
      return $dbh->set_err($DBI::stderr, "Driver is in public only mode; only SELECT statements on public tables are allowed!", '42808');
   
   return sponge_sth_loader($dbh, 'TABLE_INFO', $names, [ $dbh->func("get_avail_tables") ] );
}
    
sub column_info {
   my ($dbh, $catalog, $schema, $table, $column) = @_;
   my $names = [qw(
      TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_SIZE BUFFER_LENGTH DECIMAL_DIGITS
      NUM_PREC_RADIX NULLABLE REMARKS COLUMN_DEF SQL_DATA_TYPE SQL_DATETIME_SUB CHAR_OCTET_LENGTH ORDINAL_POSITION IS_NULLABLE
      CHAR_SET_CAT CHAR_SET_SCHEM CHAR_SET_NAME COLLATION_CAT COLLATION_SCHEM COLLATION_NAME UDT_CAT UDT_SCHEM UDT_NAME
      DOMAIN_CAT DOMAIN_SCHEM DOMAIN_NAME SCOPE_CAT SCOPE_SCHEM SCOPE_NAME MAX_CARDINALITY DTD_IDENTIFIER IS_SELF_REF
   )];

   $dbh->{fust_public_only} and
      return $dbh->set_err($DBI::stderr, "Driver is in public only mode; only SELECT statements on public tables are allowed!", '42808');
   
   my @tables = ($table =~ /^\d+$/) ? [ $table, undef, $table, "TABLE", $table ] : $dbh->func("get_avail_tables");
   my @col_rows = ();
   my $types = $dbh->type_info_all;
   shift(@$types);

   foreach my $tbl (sort { $a->[0] <=> $b->[0] } @tables) {
      next unless ($tbl);
      next unless (!$table || /$table/i ~~ $tbl);

      my $cmd = 'DESCRIBE '.$tbl->[0].';';
      my $sth = $dbh->prepare_cached($cmd) || return $dbh->set_err($DBI::stderr, $dbh->errstr, $dbh->state);
      $sth->execute() || return $dbh->set_err($DBI::stderr, $dbh->errstr, $dbh->state);

      while (my $row = $sth->fetchrow_hashref()) {
         next unless (!$column || $row->{name} =~ /$column/i);
         my $ti = first { $_->[0] eq uc($row->{type}) } @$types;
         my $id = $row->{'column id'};
         $id =~ s/^col//;

         my $col_row = [
            # 0=TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_SIZE BUFFER_LENGTH DECIMAL_DIGITS
            $tbl->[0], undef, $tbl->[0], $row->{name}, uc($row->{type}), $row->{type}, $ti->[2], undef, $ti->[17] ? int($ti->[14] * log($ti->[17])/log(10)) : undef,  # log(r^l) = l * log(r)
            # 9=NUM_PREC_RADIX NULLABLE REMARKS COLUMN_DEF SQL_DATA_TYPE SQL_DATETIME_SUB CHAR_OCTET_LENGTH ORDINAL_POSITION IS_NULLABLE
            $ti->[17], $ti->[6], $row->{'column id'}, undef, $ti->[15], $ti->[16], $ti->[17] ? undef : $ti->[2], $id, $ti->[6] ? 'YES' : 'NO',
            # 18=CHAR_SET_CAT CHAR_SET_SCHEM CHAR_SET_NAME COLLATION_CAT COLLATION_SCHEM COLLATION_NAME UDT_CAT UDT_SCHEM UDT_NAME
            undef, undef, undef, undef, undef, undef, undef, undef, undef,
            # DOMAIN_CAT DOMAIN_SCHEM DOMAIN_NAME SCOPE_CAT SCOPE_SCHEM SCOPE_NAME MAX_CARDINALITY DTD_IDENTIFIER IS_SELF_REF
            undef, undef, undef, undef, undef, undef, undef, undef, undef,
         ];
         
         push @col_rows, $col_row;
         $cached_col_rows->{$tbl->[0]}{$row->{name}} = $col_row;
      }

   }

   return sponge_sth_loader($dbh, 'COLUMN_INFO', $names, \@col_rows);
}

sub primary_key_info {
   # Heh, this one is easy: ROWID
   my ($dbh, $catalog, $schema, $table) = @_;
   my $names = [qw(TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME KEY_SEQ PK_NAME)];

   ($dbh->{fust_public_only} && $table !~ /^\d+$/) and
      return $dbh->set_err($DBI::stderr, "Driver is in public only mode; only SELECT statements on public tables are allowed!", '42808');
   
   my @tables = ($table =~ /^\d+$/) ? [ $table, undef, $table, "TABLE", $table ] : $dbh->func("get_avail_tables");
   my @key_rows = ();

   foreach my $tbl (sort { $a->[0] <=> $b->[0] } @tables) {
      next unless (!$table || /$table/i ~~ $tbl);
      push @key_rows, [ $tbl->[0], undef, $tbl->[0], 'ROWID', 1, undef ];
   }

   return sponge_sth_loader($dbh, 'PRIMARY_KEY_INFO', $names, \@key_rows);
}

sub foreign_key_info {
   return undef;  # what's that?  (technically, it exists in Fusion Tables, but it's not available to look at)
}

sub statistics_info {
   return undef;  # no such thing in Fusion Tables
}

# Oddly enough, table_info has a separate sub for this, but not the others
sub sponge_sth_loader {
   my ($dbh, $tbl_name, $names, $rows) = @_;

   # (mostly a straight copy from DBI::DBD::SqlEngine)

   # Temporary kludge: DBD::Sponge dies if @tables is empty. :-(
   @$rows or return;

   my $dbh2 = $dbh->func("sql_sponge_driver");
   my $sth = $dbh2->prepare(
                            $tbl_name,
                            {
                               rows => $rows,
                               NAME => $names,
                            }
                          );
   $sth or $dbh->set_err( $DBI::stderr, $dbh2->errstr, $dbh2->state );
   return $sth;
}

sub type_info_all {
   return [
      {
         TYPE_NAME          => 0,
         DATA_TYPE          => 1,
         COLUMN_SIZE        => 2,     # was PRECISION originally
         LITERAL_PREFIX     => 3,
         LITERAL_SUFFIX     => 4,
         CREATE_PARAMS      => 5,
         NULLABLE           => 6,
         CASE_SENSITIVE     => 7,
         SEARCHABLE         => 8,
         UNSIGNED_ATTRIBUTE => 9,
         FIXED_PREC_SCALE   => 10,    # was MONEY originally
         AUTO_UNIQUE_VALUE  => 11,    # was AUTO_INCREMENT originally
         LOCAL_TYPE_NAME    => 12,
         MINIMUM_SCALE      => 13,
         MAXIMUM_SCALE      => 14,
         SQL_DATA_TYPE      => 15,
         SQL_DATETIME_SUB   => 16,
         NUM_PREC_RADIX     => 17,
         INTERVAL_PRECISION => 18,
      },
      # Name         DataType             Max    Literals      Params         Null   Case Search Unsign  Fixed  Auto   LocalTypeName   M/M Scale     SQLDataType         DateTime_Sub  Radix  ItvPrec

      # The NUMBER field seems to have pretty much no limits, as it appears to store everything as text...
      [ "NUMBER",    DBI::SQL_NUMERIC(),    1000, undef, undef,        undef,     1,     0,     3,     0,     0,     0,       "Number",     0,  1000, DBI::SQL_NUMERIC(),        undef,    10, undef],
      [ "STRING",    DBI::SQL_VARCHAR(),   10**6,   "'",   "'",        undef,     1,     1,     3, undef, undef, undef,       "String", undef, undef, DBI::SQL_VARCHAR(),        undef, undef, undef],
      [ "LOCATION",  DBI::SQL_VARCHAR(),   10**6,   "'",   "'",        undef,     1,     1,     3, undef, undef, undef,     "Location", undef, undef, DBI::SQL_VARCHAR(),        undef, undef, undef],
      # It's called DATETIME, but realistically, it only seem to support just dates...
      [ "DATETIME",  DBI::SQL_TYPE_DATE(), 10**6,   "'",   "'",        undef,     1,     0,     3, undef, undef, undef,     "DateTime", undef, undef, DBI::SQL_DATE(), DBI::SQL_TYPE_DATE(), undef, undef],
   ];
}

sub commit {
   my ($dbh) = @_;
   warn("Commit ineffective as protocol does not support it") if ($dbh->FETCH('Warn'));
   return 0;
}

sub rollback {
   my ($dbh) = @_;
   warn("Rollback ineffective as protocol does not support it") if ($dbh->FETCH('Warn'));
   return 0;
}

sub STORE {
   my ($dbh, $attr, $val) = @_;
   if ($attr eq 'AutoCommit') {
      die "Can't disable AutoCommit: Protocol does not support transactions!" unless ($val);
      return 1;
   }
   if ($attr =~ m/^fust_/) {
      # Handle only our private attributes here
      # Note that we could trigger arbitrary actions.
      # Ideally we should warn about unknown attributes.
      $dbh->{$attr} = $val; # Yes, we are allowed to do this,
      return 1;             # but only for our private attributes
   }
   # Else pass up to DBI to handle for us
   $dbh->SUPER::STORE($attr, $val);
}

sub FETCH {
   my ($dbh, $attr) = @_;
   return 1 if ($attr eq 'AutoCommit');
   if ($attr =~ m/^fust_/) {
      # Handle only our private attributes here
      # Note that we could trigger arbitrary actions.
      return $dbh->{$attr};
   }
   # Else pass up to DBI to handle
   $dbh->SUPER::FETCH($attr);
}

##############################################################################

package   # hide from PAUSE
   DBD::FusionTables::st; # ====== STATEMENT ======

use common::sense;  # works every time!
use SQL::Parser;
use HTTP::Request;
use Text::CSV;
use List::Util qw(reduce min max);
use URI;
use DBD::FusionTables::Override;

our $VERSION = $DBD::FusionTables::VERSION;
our $imp_data_size = 0;

sub bind_param {
   my ($sth, $pNum, $val, $attr) = @_;
   my $dbh = $sth->{Database};
   $attr = { TYPE => $attr } unless (ref $attr);

   my $type = $attr->{TYPE};
   $type //= DBI::SQL_VARCHAR();
   foreach my $v (ref $val eq 'ARRAY' ? @$val : $val) { $v = $dbh->quote($v, $type); }

   my $params = $sth->{fust_sql_stmt}->{params};  # not using SQL::Eval, so just going to store them here
   $params->[$pNum-1]{val}  = $val;
   $params->[$pNum-1]{type} = $attr;
   
   1;
}
*bind_param_array = \&bind_param;  # works for both

sub fust_execute_startup {
   my ($sth, @bind_values) = @_;
   
   # start of by finishing any previous execution if still active
   $sth->finish if $sth->FETCH('Active');
   delete $sth->{'fust_csv_obj'};

   # add bind values, if specified
   if (@bind_values) {
      foreach (1 .. @bind_values) { $sth->bind_param($_, $bind_values[$_-1]); }
   }
   
   # param sanity checks
   my $params = @{$sth->{fust_sql_stmt}->{params}};
   my $numParam = $sth->FETCH('NUM_OF_PARAMS');
   return $sth->set_err($DBI::stderr, "Wrong number of bind parameters", '07001') if (@$params != $numParam);

   # DBI param variables
   $sth->{'ParamValues'} = { map { $_ => $params->[$_-1]{val}  } (1 .. $numParam) };
   $sth->{'ParamTypes'}  = { map { $_ => $params->[$_-1]{type} } (1 .. $numParam) };
   $sth->{'ParamArrays'} = $sth->FETCH('ParamValues');
}

sub fust_convert_bind_sql {
   my ($sth, $tuple) = @_;
   return $sth->{fust_bound_sql} if ($sth->{fust_bound_sql});
   $tuple ||= [ map { $sth->{fust_sql_stmt}{params}[$_]{val} } (0 .. @{$sth->{fust_sql_stmt}{params}}) ];
   return $sth->set_err($DBI::stderr, "Bound parameter value tuple is not an arrayref", '07002') unless (ref $tuple eq 'ARRAY');
   
   # unfortunately, SQL::Statement doesn't have any way of putting the
   # genie back into an SQL statement, so we have do this manually
   my ($sql, $i) = ($sth->{'Statement'}, 0);
   if (@$tuple) {
      # grab a quote string and put it back, just so it doesn't get used as a val
      $sql =~ s/(\'[^\']*\'|\?)/($1 eq '?') ? $tuple->[$i++] : $1/eg;
   }
   return $sth->{fust_bound_sql} = $sql;
}

sub execute {
   my ($sth, @bind_values) = @_;
   $sth->fust_execute_startup(@bind_values);
   my $sql = $sth->fust_convert_bind_sql;
   my $dbh = $sth->{Database};
   my $drh = $dbh->{Driver};
   
   # now let's communicate to Google
   my $cmd    = $sth->{fust_sql_stmt}->command();   ### FIXME: Can't call method "command" on unblessed reference ###
   my $method = ($cmd =~ /SHOW|DESCRIBE|SELECT/i) ? 'GET' : 'POST';
   my $ua     = $drh->{UserAgent};
   my $req    = HTTP::Request->new($method => $dbh->{fust_url});
   $dbh->{fust_authorization} and $req->header( 'Authorization' => $dbh->{fust_authorization} );
   if ($method eq 'POST') {
      my $data = URI->new();
      $data->query_form(sql => $sql);
      $req->content($data->query);
      $req->content_type('application/x-www-form-urlencoded');
      $req->content_length(length($data->query));
   }
   else {
      $req->uri->query_form(sql => $sql);
   }
   
   # In order to get data in chunks to process and hand back to the user, we can't
   # use the handlers (doesn't return back to user), so we fallback to some hidden
   # non-blocking methods within LWP (which I created :)
   $req->{_nonblocking} = 1 unless ($cmd =~ /UPDATE|DELETE|DROP/i);
      
   my $res = $ua->request($req);
   $sth->STORE('Executed' => 1);
   $res->is_success or do {
      # One of the tokens might have expired
      if ($res->code == 401 && !$dbh->{fust_public_only}) {
         $drh->fust_refresh_login($dbh) or return $sth->set_err($DBI::stderr, "Cannot re-authorize", '58031');
         
         # redo request
         $req->remove_header('Authorization');
         $req->header( 'Authorization' => $dbh->{fust_authorization} );
         $res = $ua->request($req);
      }
   };
   $res->is_success or return $sth->set_err($DBI::stderr, "Google responded with HTML error: ".$res->status_line, '58031');
   
   # Make sure we are getting the right kind of headers
   $res->header('x-auto-login')                   and return $sth->set_err($DBI::stderr, "Google responded with permission error (login redirect)", '42501');
   ($res->content_type() =~ /text\/(?:csv|plain)/) or return $sth->set_err($DBI::stderr, "Google responded with non-text page: ".$res->content, '58031');
   
   unless ($cmd =~ /UPDATE|DELETE|DROP/i) {
      $res->header('content-disposition') or return $sth->set_err($DBI::stderr, "Google responded without a standard attachment header", '58031');
      my $data = $sth->{fust_data} = [];
      $sth->{fust_res} = $res;
      $sth->{Active} = 1;
      
      $sth->fust_process_data();
      return (scalar @$data || '0E0');
   }

   return ($res->content =~ /ok/i) ? -1 : $sth->set_err($DBI::stderr, "Google responded with SQL error: ".$res->content);  # no SQLSTATE; could be anything
}

sub execute_for_fetch {
   my ($sth, $fetch_tuple_sub, $tuple_status) = @_;
   # start with empty status array
   ($tuple_status) ? @$tuple_status = () : $tuple_status = [];

   # command sanity check
   my $cmd = $sth->{fust_sql_stmt}->command();   ### FIXME: Can't call method "command" on unblessed reference ###
   return $sth->set_err($DBI::stderr, "SQL command $cmd not compatible with bulk operations", '07003') if ($cmd =~ /SELECT|SHOW|DESCRIBE/);

   ### FIXME: Need to find out which functions actually work in bulk and change accordingly ###
   my ($err_count) = (0);   
   my ($tuple, %ids);
   do {
      my ($bulk_sql, $cnt);
      
      # Google FusionTables have an upper limit of 500 statements and 1M statement size
      while (length($bulk_sql) <= 1024 * 1024 && $cnt <= 500 && ($tuple = &$fetch_tuple_sub())) {
         my $sql = $sth->fust_convert_bind_sql($tuple);
         $sql =~ s/\;?\s*$/\;\n/;  # add required ; to SQL result
         $bulk_sql .= $sql;
         $cnt++;
         $sth->{fust_bound_sql} = undef;
      }
      
      # Hit the upper limits, or finished with all of the tuples
      if ($bulk_sql) {
         $sth->{fust_bound_sql} = $bulk_sql;
         my $rc = $sth->execute();
         my $batch;
         
         my $ecnt = 0;
         unless ($sth->err) {
            unless ($cmd =~ /UPDATE|DELETE|DROP/i) {
               $batch = $sth->fetchall_arrayref([0]);
               for (@$batch) { $ids{$_->[0]} = 1; };
               $ecnt = @$batch;
            }
            else {
               $ecnt = $sth->err ? 0 : $cnt;
            }
         }
         
         # Record row counts for whatever we got, and errors for the rest
         for (1 .. $ecnt) { push @$tuple_status, $rc; }
         if ($sth->err) {
            for (scalar @$batch + 1 .. $cnt) { push @$tuple_status, [ $sth->err, $sth->errstr, $sth->state ]; $err_count++; }
         }
      }

   } while ($tuple);

   my $tuples = @$tuple_status;
   return $sth->set_err($DBI::stderr, "executing $tuples generated $err_count errors") if $err_count;
   $tuples ||= "0E0";
   return $tuples unless wantarray;
   return ($tuples, scalar keys %ids);
}

sub fust_process_data {
   my $sth = shift;
   my $res = $sth->{fust_res};
   my $dbh = $sth->{Database};
   return undef unless ($res);

   my ($fh, $lines, $first, @data);
   
   # You are in a maze of twisty little constants, all alike.
   state $CSV_TYPES = {
      DBI::SQL_NUMERIC()   => Text::CSV::IV(),
      DBI::SQL_INTEGER()   => Text::CSV::IV(),
      DBI::SQL_VARCHAR()   => Text::CSV::PV(),
      DBI::SQL_TYPE_DATE() => Text::CSV::PV(),
   };

   # check for the Text::CSV object
   my $csv = $sth->{fust_csv_obj};
   unless ($csv) {
      $csv = $sth->{fust_csv_obj} = Text::CSV->new({
         blank_is_undef => 1,  ### XXX: Confirm this ###
         binary => 1,
      }) or return $sth->set_err($DBI::stderr, "Cannot create Text::CSV object: ".Text::CSV->error_diag(), '58005');
      $first++;
   }
   
   # Make sure we have enough data to process
   my $cref = $res->content_ref;
   my $len;
   $csv->parse($$cref);  # in case read_more_data bounces early
   while ($len = $sth->fust_read_more_data() && !$csv->parse($$cref)) { 1; }
   $csv->fields() or return $sth->set_err($DBI::stderr, "Cannot fetch another row because the data is corrupt: ".$csv->error_diag(), '57048');
   
   # Cut the content to the last \n
   if ($len && $$cref =~ /^(.+\n)/s) {
      $lines = $1;
      $$cref = substr($$cref, length($lines));
   }
   else {
      $lines = $$cref;
      $$cref = '';
   }

   # We could grab the lines ourselves, but then quoted newlines might get missed.
   # The best option is to open the content as a IO object and pass it to getline()
   open($fh, '<', \$lines) or return $sth->set_err($DBI::stderr, "Cannot create PerlIO scalar object: $!", '58030');
   
   # header processing
   if ($first) {
      my $header = $csv->getline($fh);
      my $table = $sth->{fust_sql_stmt}->tables()->[0]{name};
      my $cached_col_rows = $sth->{fust_cached_col_rows};

      if ($sth->{fust_sql_stmt}->command() eq 'SELECT') {  ### FIXME: Can't call method "command" on unblessed reference ###
         unless ($cached_col_rows->{$table} || $dbh->{fust_public_only}) {
            my $csth = $dbh->column_info(undef, undef, $table, '.*');
            $csth and $csth->finish;
         }
         my $cache = $cached_col_rows->{$table};

         #say Data::Dumper->Dump([\@names, \@cols], [qw(names cols)]);
         unless ($sth->FETCH('NUM_OF_FIELDS')) {
            $sth->STORE('NUM_OF_FIELDS', scalar @$header);

            my @names = @$header;
            unless ($sth->FETCH('NAME')) {
               # FusionTables enforces case-sensitivity, but the likelyhood of conflicts doesn't justify breaking _lc/uc functions
               $sth->{'NAME'}         =             \@names;
               $sth->{'NAME_lc'}      = [ map { lc } @names ];
               $sth->{'NAME_uc'}      = [ map { uc } @names ];
               $sth->{'NAME_hash'}    = { map {    $names[$_]  => $_ } (0 .. @names) };
               $sth->{'NAME_lc_hash'} = { map { lc($names[$_]) => $_ } (0 .. @names) };
               $sth->{'NAME_us_hash'} = { map { uc($names[$_]) => $_ } (0 .. @names) };
            }
            if ($cache && !$sth->FETCH('TYPE')) {
               $sth->{'TYPE'}      = [ map { $cache->{$_}[13] } @names ];
               $sth->{'PRECISION'} = [ map { $cache->{$_}[ 6] } @names ];
               $sth->{'SCALE'}     = [ map { $cache->{$_}[ 8] } @names ];
               $sth->{'NULLABLE'}  = [ map { $cache->{$_}[10] } @names ];
            }
         }
      }
      
      $csv->column_names($header);
      $csv->types([ map { $CSV_TYPES->{$_} } @{$sth->{'TYPE'}} ]) if (@{$sth->{'TYPE'}});
   }
   
   while (my $row = $csv->getline($fh)) { push @data, $row; }
   close($fh);
   
   # get the average row size
   if (@data) {
      my $l = 'fust_avg_row_size';
      my $sum = reduce { $a + length(join '', @$b) } 0, @data;
      $sth->{$l} = $sth->{$l} ? ($sum / @data + $sth->{$l}) / 2 : $sum / @data;
   }
   
   # It's possible that the last line may have been only a partial line, so
   # remove @data lines from $lines and put that data back into $$cref.
   my @lines = split /\n/, $lines;
   splice @lines, 0, @data;
   $$cref = join("\n", @lines).$$cref if (@lines);

   # handle last_insert_id
   if ($sth->{fust_sql_stmt}->command() =~ /INSERT/) {
      my $lii   = $sth->{Database}{fust_last_insert_id};
      my $table = $sth->{fust_sql_stmt}->tables()->[0]{name};
      $lii->{$table} = $data[-1]->[0];
   }
   
   # clean up if all of the HTTP data has been acquired
   unless ($res->{_collector} && $len) {
      for (qw(csv_obj res)) { delete $sth->{'fust_'.$_}; }
   }
   
   # shove data into the right variables and get out of here
   my $sth_data = $sth->{fust_data};
   push @$sth_data, @data;
   $sth->{RowsInCache} = scalar @$sth_data;
   1;
}

sub fust_read_more_data {
   my $sth = shift;
   my $res = $sth->{fust_res};
   return undef unless ($res);
   my $cache_size = $sth->fust_calc_cache_size();

   my $cref  = $res->content_ref;
   my $proto = $res->request()->{_protocol_obj};
   my $olen  = length($$cref);
   while (length($$cref) < $cache_size && $res->{_collector}) {  # collect_content will remove _collector when it's finished
      $proto->collect_content($res, undef, 1);                   # LWP reads in 1024 byte chunks
   }
   
   return length($$cref) - $olen;  # total bytes gathered
}

sub fust_calc_cache_size {
   my $sth = shift;
   my $res = $sth->{fust_res};
   return undef unless ($res);

   # Look at RowCacheSize for recommended size to grab
   my $ars = $sth->{fust_avg_row_size} || $sth->FETCH('NUM_OF_FIELDS') * 20 || 100;
   my $size = $sth->{Database}{RowCacheSize};
   given ($size <=> 0) {
      when  (1) { $size *= $ars;  }
      when  (0) { $size  = 16384; }
      when (-1) { $size *= -1;    }
   }
   return max(2048, $ars * 2, $size);
}

sub fetchrow_arrayref {
   my $sth = shift;
   my $data = $sth->{fust_data};
   
   if ($sth->{fust_res}) {
      my $ars = $sth->{fust_avg_row_size} || $sth->FETCH('NUM_OF_FIELDS') * 20 || 100;
      my $cache_size = $sth->fust_calc_cache_size();
      my $cache_rows = $cache_size / $ars;  # look at recommended _rows_ to check
      
      # grab more data at the 50% row mark
      $sth->fust_process_data() if (@$data < $cache_rows / 2);
   }

   # fetch a row from cache and send
   my $row = shift @$data;
   unless ($row) {
      $sth->STORE(Active => 0); # mark as no longer active
      return undef;
   }
   $sth->{RowsInCache}--;
   map { $_ =~ s/\s+$//; } @$row if ($sth->FETCH('ChopBlanks'));
   return $sth->_set_fbav($row);
}
*fetch = \&fetchrow_arrayref; # required alias for fetchrow_arrayref

sub finish {
   my $sth = shift;
   for (qw(avg_row_size bound_sql csv_obj res data)) { delete $sth->{'fust_'.$_}; }
   $sth->STORE(Active => 0);
   $sth->SUPER::finish();
}

sub DESTROY {
   my $sth = shift;
   $sth->finish if $sth->FETCH('Active');
}

##############################################################################
package   # hide from PAUSE
   SQL::Dialects::FusionTables;

our $VERSION = $DBD::FusionTables::VERSION;

use SQL::Dialects::Role;

sub get_config {
   return <<EOC;
[VALID COMMANDS]
CREATE
DROP
SELECT
INSERT
UPDATE
DELETE
SHOW
DESCRIBE

[VALID OPTIONS]
SELECT_AGGREGATE_FUNCTIONS

[VALID COMPARISON OPERATORS]
=
<
<=
>
>=
LIKE
MATCHES
STARTS WITH
ENDS WITH
CONTAINS
CONTAINS IGNORING CASE
DOES NOT CONTAIN
NOT EQUAL TO
IN

[VALID DATA TYPES]
NUMBER
STRING
LOCATION
DATETIME

[RESERVED WORDS]
AND
ASC
AVERAGE
BY
CASE
CIRCLE
CONTAIN
COUNT
CREATE
DATETIME
DELETE
DESC
DESCRIBE
DOES
DROP
ENDS
EQUAL
FROM
GROUP
IGNORING
IN
INSERT
INTO
LATLNG
LIKE
LIMIT
LOCATION
MATCHES
MAXIMUM
MINIMUM
NOT
NUMBER
OFFSET
OR
ORDER
RECTANGLE
ROWID
SELECT
SET
SHOW
STARTS
STRING
ST_DISTANCE
ST_INTERSECTS
SUM
TABLE
TABLES
TO
UPDATE
VALUES
VIEW
WHERE
WITH
EOC
}

1;

=pod

=head1 NAME

SQL::Dialects::FusionTables

=head1 SYNOPSIS

  use SQL::Dialects::FusionTables;
  $config = SQL::Dialects::AnyData->get_config();

=head1 DESCRIPTION

This package provides the necessary configuration for Google Fusion Tables SQL.

=head1 FUNCTIONS

=head2 get_config

Returns the configuration for Fusion Tables SQL. The configuration is delivered in
ini-style:

  [VALID COMMANDS]
  ...

  [VALID OPTIONS]
  ...

  [VALID COMPARISON OPERATORS]
  ...

  [VALID DATA TYPES]
  ...

  [RESERVED WORDS]
  ...

=cut