package  # hide from PAUSE
   DBD::FusionTables::GetInfo;

use strict;
use DBD::FusionTables();
use SQL::Dialects::FusionTables;

my $sql_driver = 'FusionTables';
my $sql_ver_fmt = '%02d.%02d.%04d';   # ODBC version string: ##.##.#####

my $sql_driver_ver = sprintf $sql_ver_fmt, split (/\./, "$DBD::FusionTables::VERSION.0.0.0.0.0.0");

sub sql_dbms_version {
   my $dbh = shift;
   return sprintf $sql_ver_fmt, @{DBD::FusionTables::db::fust_server_version($dbh)};
}

sub sql_keywords {
   require SQL::Dialects::FusionTables;
   my $Keywords = SQL::Dialects::FusionTables::get_config();
   $Keywords =~ s/^(.+)\[RESERVED WORDS\]\r?\n//s;

   return join ',', split(/\n/, $Keywords);
}

sub sql_data_source_name {
   my $dbh = shift;
   return "dbi:$sql_driver:" . $dbh->{Name};
}

sub sql_user_name {
   my $dbh = shift;
   # CURRENT_USER is a non-standard attribute, probably undef
   # Username is a standard DBI attribute
   return $dbh->{CURRENT_USER} || $dbh->{Username};
}

our %info = (
     20 => 'Y',                           # SQL_ACCESSIBLE_PROCEDURES
     19 => 'Y',                           # SQL_ACCESSIBLE_TABLES
      0 => 0,                             # SQL_ACTIVE_CONNECTIONS
    116 => 0,                             # SQL_ACTIVE_ENVIRONMENTS
      1 => 0,                             # SQL_ACTIVE_STATEMENTS
    169 => 31,                            # SQL_AGGREGATE_FUNCTIONS
    117 => 0,                             # SQL_ALTER_DOMAIN
     86 => 0,                             # SQL_ALTER_TABLE
  10021 => 1,                             # SQL_ASYNC_MODE
    120 => 0,                             # SQL_BATCH_ROW_COUNT
    121 => 0,                             # SQL_BATCH_SUPPORT
     82 => 64,                            # SQL_BOOKMARK_PERSISTENCE  ???
    114 => 0,                             # SQL_CATALOG_LOCATION
  10003 => 'N',                           # SQL_CATALOG_NAME
     41 => '',                            # SQL_CATALOG_NAME_SEPARATOR
     42 => '',                            # SQL_CATALOG_TERM
     92 => 0,                             # SQL_CATALOG_USAGE
  10004 => 'UTF-8',                       # SQL_COLLATING_SEQUENCE
  10004 => 'UTF-8',                       # SQL_COLLATION_SEQ
     87 => 'N',                           # SQL_COLUMN_ALIAS
     22 => 0,                             # SQL_CONCAT_NULL_BEHAVIOR
     53 => 0,                             # SQL_CONVERT_BIGINT
     54 => 0,                             # SQL_CONVERT_BINARY
     55 => 0,                             # SQL_CONVERT_BIT
     56 => 0,                             # SQL_CONVERT_CHAR
     57 => 0,                             # SQL_CONVERT_DATE
     58 => 0,                             # SQL_CONVERT_DECIMAL
     59 => 0,                             # SQL_CONVERT_DOUBLE
     60 => 0,                             # SQL_CONVERT_FLOAT
     48 => 0,                             # SQL_CONVERT_FUNCTIONS
    173 => 0,                             # SQL_CONVERT_GUID
     61 => 0,                             # SQL_CONVERT_INTEGER
    123 => 0,                             # SQL_CONVERT_INTERVAL_DAY_TIME
    124 => 0,                             # SQL_CONVERT_INTERVAL_YEAR_MONTH
     71 => 0,                             # SQL_CONVERT_LONGVARBINARY
     62 => 0,                             # SQL_CONVERT_LONGVARCHAR
     63 => 0,                             # SQL_CONVERT_NUMERIC
     64 => 0,                             # SQL_CONVERT_REAL
     65 => 0,                             # SQL_CONVERT_SMALLINT
     66 => 0,                             # SQL_CONVERT_TIME
     67 => 0,                             # SQL_CONVERT_TIMESTAMP
     68 => 0,                             # SQL_CONVERT_TINYINT
     69 => 0,                             # SQL_CONVERT_VARBINARY
     70 => 0,                             # SQL_CONVERT_VARCHAR
    122 => 0,                             # SQL_CONVERT_WCHAR
    125 => 0,                             # SQL_CONVERT_WLONGVARCHAR
    126 => 0,                             # SQL_CONVERT_WVARCHAR
     74 => 0,                             # SQL_CORRELATION_NAME
    127 => 0,                             # SQL_CREATE_ASSERTION
    128 => 0,                             # SQL_CREATE_CHARACTER_SET
    129 => 0,                             # SQL_CREATE_COLLATION
    130 => 0,                             # SQL_CREATE_DOMAIN
    131 => 0,                             # SQL_CREATE_SCHEMA
    132 => 1,  # Sadly...just this        # SQL_CREATE_TABLE
    133 => 0,                             # SQL_CREATE_TRANSLATION
    134 => 1,                             # SQL_CREATE_VIEW
     23 => 0,                             # SQL_CURSOR_COMMIT_BEHAVIOR
     24 => 0,                             # SQL_CURSOR_ROLLBACK_BEHAVIOR
  10001 => 0,                             # SQL_CURSOR_SENSITIVITY
     16 => 'FusionTables',                # SQL_DATABASE_NAME
      2 => \&sql_data_source_name,        # SQL_DATA_SOURCE_NAME
     25 => 'N',                           # SQL_DATA_SOURCE_READ_ONLY
    119 => 7,                             # SQL_DATETIME_LITERALS
     17 => 'FusionTables',                # SQL_DBMS_NAME
     18 => \&sql_dbms_version,            # SQL_DBMS_VER
     18 => \&sql_dbms_version,            # SQL_DBMS_VERSION
    170 => 0,                             # SQL_DDL_INDEX
     26 => 0,                             # SQL_DEFAULT_TRANSACTION_ISOLATION
     26 => 0,                             # SQL_DEFAULT_TXN_ISOLATION
  10002 => 'N',                           # SQL_DESCRIBE_PARAMETER
    171 => '03.52.1022.0000',             # SQL_DM_VER
      3 => 147209344,                     # SQL_DRIVER_HDBC
      4 => 147212776,                     # SQL_DRIVER_HENV
      6 => $INC{'DBD/FusionTables.pm'},   # SQL_DRIVER_NAME
     77 => '03.52',                       # SQL_DRIVER_ODBC_VER
      7 => $sql_driver_ver,               # SQL_DRIVER_VER
    136 => 0,                             # SQL_DROP_ASSERTION
    137 => 0,                             # SQL_DROP_CHARACTER_SET
    138 => 0,                             # SQL_DROP_COLLATION
    139 => 0,                             # SQL_DROP_DOMAIN
    140 => 0,                             # SQL_DROP_SCHEMA
    141 => 1,                             # SQL_DROP_TABLE
    142 => 0,                             # SQL_DROP_TRANSLATION
    143 => 0,                             # SQL_DROP_VIEW
    144 => 0,                             # SQL_DYNAMIC_CURSOR_ATTRIBUTES1
    145 => 0,                             # SQL_DYNAMIC_CURSOR_ATTRIBUTES2
     27 => 'Y',                           # SQL_EXPRESSIONS_IN_ORDERBY
     84 => 0,                             # SQL_FILE_USAGE
    146 => 65,                            # SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1
    147 => 128,                           # SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2
     88 => 2,                             # SQL_GROUP_BY
     81 => 15,                            # SQL_GETDATA_EXTENSIONS  ???
     28 => 3,                             # SQL_IDENTIFIER_CASE
     29 => "'",                           # SQL_IDENTIFIER_QUOTE_CHAR
    148 => 0,                             # SQL_INDEX_KEYWORDS
    149 => 4958690,                       # SQL_INFO_SCHEMA_VIEWS
    172 => 1,                             # SQL_INSERT_STATEMENT
     73 => 'N',                           # SQL_INTEGRITY
    150 => 0,                             # SQL_KEYSET_CURSOR_ATTRIBUTES1
    151 => 0,                             # SQL_KEYSET_CURSOR_ATTRIBUTES2
     89 => \&sql_keywords,                # SQL_KEYWORDS
    113 => 'Y',                           # SQL_LIKE_ESCAPE_CLAUSE
  10022 => 0,                             # SQL_MAX_ASYNC_CONCURRENT_STATEMENTS
    112 => 0,                             # SQL_MAX_BINARY_LITERAL_LEN
     34 => 0,                             # SQL_MAX_CATALOG_NAME_LENGTH
    108 => 0,                             # SQL_MAX_CHAR_LITERAL_LEN
     # Using 4096 URL char limit with 5-char column names
     97 => 800,                           # SQL_MAX_COLUMNS_IN_GROUP_BY
     98 => 0,                             # SQL_MAX_COLUMNS_IN_INDEX
     99 => 800,                           # SQL_MAX_COLUMNS_IN_ORDER_BY
    100 => 800,                           # SQL_MAX_COLUMNS_IN_SELECT
    101 => 0,                             # SQL_MAX_COLUMNS_IN_TABLE
     30 => 0,                             # SQL_MAX_COLUMN_NAME_LEN
      1 => 0,                             # SQL_MAX_CONCURRENT_ACTIVITIES
     31 => 0,                             # SQL_MAX_CURSOR_NAME_LENGTH
      0 => 0,                             # SQL_MAX_DRIVER_CONNECTIONS
  10005 => 0,                             # SQL_MAX_IDENTIFIER_LEN
    102 => 0,                             # SQL_MAX_INDEX_SIZE
     33 => 0,                             # SQL_MAX_PROCEDURE_NAME_LEN
     34 => 0,                             # SQL_MAX_QUALIFIER_NAME_LEN
    104 => 1_000_000,                     # SQL_MAX_ROW_SIZE
    103 => 'N',                           # SQL_MAX_ROW_SIZE_INCLUDES_LONG
     32 => 0,                             # SQL_MAX_SCHEMA_NAME_LEN
     # 4096 minus some http://api.google.com... stuff
    105 => 4000,                          # SQL_MAX_STATEMENT_LEN
     35 => 0,                             # SQL_MAX_TABLE_NAME_LEN
    106 => 1,  # No joins                 # SQL_MAX_TABLES_IN_SELECT
    107 => 0,                             # SQL_MAX_USER_NAME_LEN
     37 => 'Y',                           # SQL_MULTIPLE_ACTIVE_TXN
     36 => 'Y',                           # SQL_MULT_RESULT_SETS
    111 => 'N',                           # SQL_NEED_LONG_DATA_LEN
     75 => 0,                             # SQL_NON_NULLABLE_COLUMNS
     85 => 1,                             # SQL_NULL_COLLATION
     49 => 0,  # Sadly, nothing here...   # SQL_NUMERIC_FUNCTIONS
    152 => 1,                             # SQL_ODBC_INTERFACE_CONFORMANCE
     12 => 0,                             # SQL_ODBC_SAG_CLI_CONFORMANCE
     15 => 1,                             # SQL_ODBC_SQL_CONFORMANCE
     10 => '03.52',                       # SQL_ODBC_VER
    115 => 0,  # No outer joins           # SQL_OJ_CAPABILITIES
     90 => 'N',                           # SQL_ORDER_BY_COLUMNS_IN_SELECT
     38 => 'N',                           # SQL_OUTER_JOINS
    153 => 2,                             # SQL_PARAM_ARRAY_ROW_COUNTS
    154 => 3,                             # SQL_PARAM_ARRAY_SELECTS
     21 => 'Y',                           # SQL_PROCEDURES
     40 => 'function',                    # SQL_PROCEDURE_TERM
     93 => 3,                             # SQL_QUOTED_IDENTIFIER_CASE
     11 => 'N',                           # SQL_ROW_UPDATES
     39 => 'owner',                       # SQL_SCHEMA_TERM
     91 => 0,                             # SQL_SCHEMA_USAGE
     44 => 3,                             # SQL_SCROLL_OPTIONS
     14 => '\\',                          # SQL_SEARCH_PATTERN_ESCAPE
     13 => sub {"$_[0]->{Name}"},         # SQL_SERVER_NAME
     # All of them!                       # SQL_SPECIAL_CHARACTERS
     94 => (join '', map { chr; } (32, 33, 35..38, 40..47, 58..64, 91..96, 123..126, 128..255)),
    155 => 0,                             # SQL_SQL92_DATETIME_FUNCTIONS
    156 => 0,                             # SQL_SQL92_FOREIGN_KEY_DELETE_RULE
    157 => 0,                             # SQL_SQL92_FOREIGN_KEY_UPDATE_RULE
    158 => 0,                             # SQL_SQL92_GRANT
    159 => 0,                             # SQL_SQL92_NUMERIC_VALUE_FUNCTIONS
    160 => 13944,                         # SQL_SQL92_PREDICATES
    161 => 0,                             # SQL_SQL92_RELATIONAL_JOIN_OPERATORS
    162 => 0,                             # SQL_SQL92_REVOKE
    163 => 1,                             # SQL_SQL92_ROW_VALUE_CONSTRUCTOR
    164 => 0,                             # SQL_SQL92_STRING_FUNCTIONS
    165 => 0,                             # SQL_SQL92_VALUE_EXPRESSIONS
    118 => 0,                             # SQL_SQL_CONFORMANCE
    166 => 0,                             # SQL_STANDARD_CLI_CONFORMANCE
    167 => 7495,                          # SQL_STATIC_CURSOR_ATTRIBUTES1
    168 => 4224,                          # SQL_STATIC_CURSOR_ATTRIBUTES2
     83 => 0,                             # SQL_STATIC_SENSITIVITY
     50 => 0,  # Sadly, nothing here...   # SQL_STRING_FUNCTIONS
     95 => 0,                             # SQL_SUBQUERIES
     51 => 3,                             # SQL_SYSTEM_FUNCTIONS
     45 => 'table',                       # SQL_TABLE_TERM
    109 => 0,                             # SQL_TIMEDATE_ADD_INTERVALS
    110 => 0,                             # SQL_TIMEDATE_DIFF_INTERVALS
     52 => 0,  # Sadly, nothing here...   # SQL_TIMEDATE_FUNCTIONS
     46 => 0,                             # SQL_TRANSACTION_CAPABLE
     72 => 0,                             # SQL_TRANSACTION_ISOLATION_OPTION
     46 => 0,                             # SQL_TXN_CAPABLE
     72 => 0,                             # SQL_TXN_ISOLATION_OPTION
     96 => 0,                             # SQL_UNION
     96 => 0,                             # SQL_UNION_STATEMENT
     47 => \&sql_user_name,               # SQL_USER_NAME
  10000 => 1995,                          # SQL_XOPEN_CLI_YEAR
);

1;

__END__
