# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .cqlhandling import CqlParsingRuleSet, Hint
from cassandra.metadata import maybe_escape_name


simple_cql_types = set(('ascii', 'bigint', 'blob', 'boolean', 'counter', 'date', 'decimal', 'double', 'duration', 'float',
                        'inet', 'int', 'smallint', 'text', 'time', 'timestamp', 'timeuuid', 'tinyint', 'uuid', 'varchar', 'varint'))
simple_cql_types.difference_update(('set', 'map', 'list'))

from . import helptopics
cqldocs = helptopics.CQL3HelpTopics()


class UnexpectedTableStructure(UserWarning):

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return 'Unexpected table structure; may not translate correctly to CQL. ' + self.msg


SYSTEM_KEYSPACES = ('system', 'system_schema', 'system_traces', 'system_auth', 'system_distributed')
NONALTERBALE_KEYSPACES = ('system', 'system_schema')


class Cql3ParsingRuleSet(CqlParsingRuleSet):

    columnfamily_layout_options = (
        ('bloom_filter_fp_chance', None),
        ('comment', None),
        ('dclocal_read_repair_chance', 'local_read_repair_chance'),
        ('gc_grace_seconds', None),
        ('min_index_interval', None),
        ('max_index_interval', None),
        ('read_repair_chance', None),
        ('default_time_to_live', None),
        ('speculative_retry', None),
        ('memtable_flush_period_in_ms', None),
        ('cdc', None)
    )

    columnfamily_layout_map_options = (
        # (CQL3 option name, schema_columnfamilies column name (or None if same),
        #  list of known map keys)
        ('compaction', 'compaction_strategy_options',
            ('class', 'max_threshold', 'tombstone_compaction_interval', 'tombstone_threshold', 'enabled', 'unchecked_tombstone_compaction', 'only_purge_repaired_tombstones')),
        ('compression', 'compression_parameters',
            ('sstable_compression', 'chunk_length_kb', 'crc_check_chance')),
        ('caching', None,
            ('rows_per_partition', 'keys')),
    )

    obsolete_cf_options = ()

    consistency_levels = (
        'ANY',
        'ONE',
        'TWO',
        'THREE',
        'QUORUM',
        'ALL',
        'LOCAL_QUORUM',
        'EACH_QUORUM',
        'SERIAL'
    )

    @classmethod
    def escape_value(cls, value):
        if value is None:
            return 'NULL'  # this totally won't work
        if isinstance(value, bool):
            value = str(value).lower()
        elif isinstance(value, float):
            return '%f' % value
        elif isinstance(value, int):
            return str(value)
        return "'%s'" % value.replace("'", "''")

    @classmethod
    def escape_name(cls, name):
        if name is None:
            return 'NULL'
        return "'%s'" % name.replace("'", "''")

    @staticmethod
    def dequote_name(name):
        name = name.strip()
        if name == '':
            return name
        if name[0] == '"' and name[-1] == '"':
            return name[1:-1].replace('""', '"')
        else:
            return name.lower()

    @staticmethod
    def dequote_value(cqlword):
        cqlword = cqlword.strip()
        if cqlword == '':
            return cqlword
        if cqlword[0] == "'" and cqlword[-1] == "'":
            cqlword = cqlword[1:-1].replace("''", "'")
        return cqlword


CqlRuleSet = Cql3ParsingRuleSet()

# convenience for remainder of module
completer_for = CqlRuleSet.completer_for
explain_completion = CqlRuleSet.explain_completion
dequote_value = CqlRuleSet.dequote_value
dequote_name = CqlRuleSet.dequote_name
escape_value = CqlRuleSet.escape_value

# BEGIN SYNTAX/COMPLETION RULE DEFINITIONS

syntax_rules = r'''
<Start> ::= <CQL_Statement>*
          ;

<CQL_Statement> ::= [statements]=<statementBody> ";"
                  ;

# the order of these terminal productions is significant:
<endline> ::= /\n/ ;

JUNK ::= /([ \t\r\f\v]+|(--|[/][/])[^\n\r]*([\n\r]|$)|[/][*].*?[*][/])/ ;

<stringLiteral> ::= <quotedStringLiteral>
                  | <pgStringLiteral> ;
<quotedStringLiteral> ::= /'([^']|'')*'/ ;
<pgStringLiteral> ::= /\$\$(?:(?!\$\$).)*\$\$/;
<quotedName> ::=    /"([^"]|"")*"/ ;
<float> ::=         /-?[0-9]+\.[0-9]+/ ;
<uuid> ::=          /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/ ;
<blobLiteral> ::=    /0x[0-9a-f]+/ ;
<wholenumber> ::=   /[0-9]+/ ;
<identifier> ::=    /[a-z][a-z0-9_]*/ ;
<colon> ::=         ":" ;
<star> ::=          "*" ;
<endtoken> ::=      ";" ;
<op> ::=            /[-+=,().]/ ;
<cmp> ::=           /[<>!]=?/ ;
<brackets> ::=      /[][{}]/ ;

<integer> ::= "-"? <wholenumber> ;
<boolean> ::= "true"
            | "false"
            ;

<unclosedPgString>::= /\$\$(?:(?!\$\$).)*/ ;
<unclosedString>  ::= /'([^']|'')*/ ;
<unclosedName>    ::= /"([^"]|"")*/ ;
<unclosedComment> ::= /[/][*].*$/ ;

<term> ::= <stringLiteral>
         | <integer>
         | <float>
         | <uuid>
         | <boolean>
         | <blobLiteral>
         | <collectionLiteral>
         | <functionLiteral> <functionArguments>
         | "NULL"
         ;

<functionLiteral> ::= (<identifier> ( "." <identifier> )?)
                 | "TOKEN"
                 ;

<functionArguments> ::= "(" ( <term> ( "," <term> )* )? ")"
                 ;

<tokenDefinition> ::= token="TOKEN" "(" <term> ( "," <term> )* ")"
                    | <term>
                    ;
<cident> ::= <quotedName>
           | <identifier>
           | <unreservedKeyword>
           ;
<colname> ::= <cident> ;   # just an alias

<collectionLiteral> ::= <listLiteral>
                      | <setLiteral>
                      | <mapLiteral>
                      ;
<listLiteral> ::= "[" ( <term> ( "," <term> )* )? "]"
                ;
<setLiteral> ::= "{" ( <term> ( "," <term> )* )? "}"
               ;
<mapLiteral> ::= "{" <term> ":" <term> ( "," <term> ":" <term> )* "}"
               ;

<anyFunctionName> ::= ( ksname=<cfOrKsName> dot="." )? udfname=<cfOrKsName> ;

<userFunctionName> ::= ( ksname=<nonSystemKeyspaceName> dot="." )? udfname=<cfOrKsName> ;

<refUserFunctionName> ::= udfname=<cfOrKsName> ;

<userAggregateName> ::= ( ksname=<nonSystemKeyspaceName> dot="." )? udaname=<cfOrKsName> ;

<functionAggregateName> ::= ( ksname=<nonSystemKeyspaceName> dot="." )? functionname=<cfOrKsName> ;

<aggregateName> ::= <userAggregateName>
                  ;

<functionName> ::= <functionAggregateName>
                 | "TOKEN"
                 ;

<statementBody> ::= <useStatement>
                  | <selectStatement>
                  | <dataChangeStatement>
                  | <schemaChangeStatement>
                  | <authenticationStatement>
                  | <authorizationStatement>
                  ;

<dataChangeStatement> ::= <insertStatement>
                        | <updateStatement>
                        | <deleteStatement>
                        | <truncateStatement>
                        | <batchStatement>
                        ;

<schemaChangeStatement> ::= <createKeyspaceStatement>
                          | <createColumnFamilyStatement>
                          | <createIndexStatement>
                          | <createMaterializedViewStatement>
                          | <createUserTypeStatement>
                          | <createFunctionStatement>
                          | <createAggregateStatement>
                          | <createTriggerStatement>
                          | <dropKeyspaceStatement>
                          | <dropColumnFamilyStatement>
                          | <dropIndexStatement>
                          | <dropMaterializedViewStatement>
                          | <dropUserTypeStatement>
                          | <dropFunctionStatement>
                          | <dropAggregateStatement>
                          | <dropTriggerStatement>
                          | <alterTableStatement>
                          | <alterKeyspaceStatement>
                          | <alterUserTypeStatement>
                          ;

<authenticationStatement> ::= <createUserStatement>
                            | <alterUserStatement>
                            | <dropUserStatement>
                            | <listUsersStatement>
                            | <createRoleStatement>
                            | <alterRoleStatement>
                            | <dropRoleStatement>
                            | <listRolesStatement>
                            ;

<authorizationStatement> ::= <grantStatement>
                           | <grantRoleStatement>
                           | <revokeStatement>
                           | <revokeRoleStatement>
                           | <listPermissionsStatement>
                           ;

# timestamp is included here, since it's also a keyword
<simpleStorageType> ::= typename=( <identifier> | <stringLiteral> | "timestamp" ) ;

<userType> ::= utname=<cfOrKsName> ;

<storageType> ::= <simpleStorageType> | <collectionType> | <frozenCollectionType> | <userType> ;

# Note: autocomplete for frozen collection types does not handle nesting past depth 1 properly,
# but that's a lot of work to fix for little benefit.
<collectionType> ::= "map" "<" <simpleStorageType> "," ( <simpleStorageType> | <userType> ) ">"
                   | "list" "<" ( <simpleStorageType> | <userType> ) ">"
                   | "set" "<" ( <simpleStorageType> | <userType> ) ">"
                   ;

<frozenCollectionType> ::= "frozen" "<" "map"  "<" <storageType> "," <storageType> ">" ">"
                         | "frozen" "<" "list" "<" <storageType> ">" ">"
                         | "frozen" "<" "set"  "<" <storageType> ">" ">"
                         ;

<columnFamilyName> ::= ( ksname=<cfOrKsName> dot="." )? cfname=<cfOrKsName> ;

<materializedViewName> ::= ( ksname=<cfOrKsName> dot="." )? mvname=<cfOrKsName> ;

<userTypeName> ::= ( ksname=<cfOrKsName> dot="." )? utname=<cfOrKsName> ;

<keyspaceName> ::= ksname=<cfOrKsName> ;

<nonSystemKeyspaceName> ::= ksname=<cfOrKsName> ;

<alterableKeyspaceName> ::= ksname=<cfOrKsName> ;

<cfOrKsName> ::= <identifier>
               | <quotedName>
               | <unreservedKeyword>;

<unreservedKeyword> ::= nocomplete=
                        ( "key"
                        | "clustering"
                        # | "count" -- to get count(*) completion, treat count as reserved
                        | "ttl"
                        | "compact"
                        | "storage"
                        | "type"
                        | "values" )
                      ;

<property> ::= [propname]=<cident> propeq="=" [propval]=<propertyValue>
                ;
<propertyValue> ::= propsimpleval=( <stringLiteral>
                                  | <identifier>
                                  | <integer>
                                  | <float>
                                  | <unreservedKeyword> )
                    # we don't use <mapLiteral> here so we can get more targeted
                    # completions:
                    | propsimpleval="{" [propmapkey]=<term> ":" [propmapval]=<term>
                            ( ender="," [propmapkey]=<term> ":" [propmapval]=<term> )*
                      ender="}"
                    ;

'''


def prop_equals_completer(ctxt, cass):
    if not working_on_keyspace(ctxt):
        # we know if the thing in the property name position is "compact" or
        # "clustering" that there won't actually be an equals sign, because
        # there are no properties by those names. there are, on the other hand,
        # table properties that start with those keywords which don't have
        # equals signs at all.
        curprop = ctxt.get_binding('propname')[-1].upper()
        if curprop in ('COMPACT', 'CLUSTERING'):
            return ()
    return ['=']


completer_for('property', 'propeq')(prop_equals_completer)


@completer_for('property', 'propname')
def prop_name_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_prop_name_completer(ctxt, cass)
    else:
        return cf_prop_name_completer(ctxt, cass)


@completer_for('propertyValue', 'propsimpleval')
def prop_val_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_prop_val_completer(ctxt, cass)
    else:
        return cf_prop_val_completer(ctxt, cass)


@completer_for('propertyValue', 'propmapkey')
def prop_val_mapkey_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_prop_val_mapkey_completer(ctxt, cass)
    else:
        return cf_prop_val_mapkey_completer(ctxt, cass)


@completer_for('propertyValue', 'propmapval')
def prop_val_mapval_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_prop_val_mapval_completer(ctxt, cass)
    else:
        return cf_prop_val_mapval_completer(ctxt, cass)


@completer_for('propertyValue', 'ender')
def prop_val_mapender_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_prop_val_mapender_completer(ctxt, cass)
    else:
        return cf_prop_val_mapender_completer(ctxt, cass)


def ks_prop_name_completer(ctxt, cass):
    optsseen = ctxt.get_binding('propname', ())
    if 'replication' not in optsseen:
        return ['replication']
    return ["durable_writes"]


def ks_prop_val_completer(ctxt, cass):
    optname = ctxt.get_binding('propname')[-1]
    if optname == 'durable_writes':
        return ["'true'", "'false'"]
    if optname == 'replication':
        return ["{'class': '"]
    return ()


def ks_prop_val_mapkey_completer(ctxt, cass):
    optname = ctxt.get_binding('propname')[-1]
    if optname != 'replication':
        return ()
    keysseen = map(dequote_value, ctxt.get_binding('propmapkey', ()))
    valsseen = map(dequote_value, ctxt.get_binding('propmapval', ()))
    for k, v in zip(keysseen, valsseen):
        if k == 'class':
            repclass = v
            break
    else:
        return ["'class'"]
    if repclass in CqlRuleSet.replication_factor_strategies:
        opts = set(('replication_factor',))
    elif repclass == 'NetworkTopologyStrategy':
        return [Hint('<dc_name>')]
    return map(escape_value, opts.difference(keysseen))


def ks_prop_val_mapval_completer(ctxt, cass):
    optname = ctxt.get_binding('propname')[-1]
    if optname != 'replication':
        return ()
    currentkey = dequote_value(ctxt.get_binding('propmapkey')[-1])
    if currentkey == 'class':
        return map(escape_value, CqlRuleSet.replication_strategies)
    return [Hint('<term>')]


def ks_prop_val_mapender_completer(ctxt, cass):
    optname = ctxt.get_binding('propname')[-1]
    if optname != 'replication':
        return [',']
    keysseen = map(dequote_value, ctxt.get_binding('propmapkey', ()))
    valsseen = map(dequote_value, ctxt.get_binding('propmapval', ()))
    for k, v in zip(keysseen, valsseen):
        if k == 'class':
            repclass = v
            break
    else:
        return [',']
    if repclass in CqlRuleSet.replication_factor_strategies:
        if 'replication_factor' not in keysseen:
            return [',']
    if repclass == 'NetworkTopologyStrategy' and len(keysseen) == 1:
        return [',']
    return ['}']


def cf_prop_name_completer(ctxt, cass):
    return [c[0] for c in (CqlRuleSet.columnfamily_layout_options +
                           CqlRuleSet.columnfamily_layout_map_options)]


def cf_prop_val_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('propname')
    this_opt = exist_opts[-1]
    if this_opt == 'compression':
        return ["{'sstable_compression': '"]
    if this_opt == 'compaction':
        return ["{'class': '"]
    if this_opt == 'caching':
        return ["{'keys': '"]
    if any(this_opt == opt[0] for opt in CqlRuleSet.obsolete_cf_options):
        return ["'<obsolete_option>'"]
    if this_opt in ('read_repair_chance', 'bloom_filter_fp_chance',
                    'dclocal_read_repair_chance'):
        return [Hint('<float_between_0_and_1>')]
    if this_opt in ('min_compaction_threshold', 'max_compaction_threshold',
                    'gc_grace_seconds', 'min_index_interval', 'max_index_interval'):
        return [Hint('<integer>')]
    if this_opt in ('cdc'):
        return [Hint('<true|false>')]
    return [Hint('<option_value>')]


def cf_prop_val_mapkey_completer(ctxt, cass):
    optname = ctxt.get_binding('propname')[-1]
    for cql3option, _, subopts in CqlRuleSet.columnfamily_layout_map_options:
        if optname == cql3option:
            break
    else:
        return ()
    keysseen = map(dequote_value, ctxt.get_binding('propmapkey', ()))
    valsseen = map(dequote_value, ctxt.get_binding('propmapval', ()))
    pairsseen = dict(zip(keysseen, valsseen))
    if optname == 'compression':
        return map(escape_value, set(subopts).difference(keysseen))
    if optname == 'caching':
        return map(escape_value, set(subopts).difference(keysseen))
    if optname == 'compaction':
        opts = set(subopts)
        try:
            csc = pairsseen['class']
        except KeyError:
            return ["'class'"]
        csc = csc.split('.')[-1]
        if csc == 'SizeTieredCompactionStrategy':
            opts.add('min_sstable_size')
            opts.add('min_threshold')
            opts.add('bucket_high')
            opts.add('bucket_low')
        elif csc == 'LeveledCompactionStrategy':
            opts.add('sstable_size_in_mb')
            opts.add('fanout_size')
        elif csc == 'DateTieredCompactionStrategy':
            opts.add('base_time_seconds')
            opts.add('max_sstable_age_days')
            opts.add('min_threshold')
            opts.add('max_window_size_seconds')
            opts.add('timestamp_resolution')
        elif csc == 'TimeWindowCompactionStrategy':
            opts.add('compaction_window_unit')
            opts.add('compaction_window_size')
            opts.add('min_threshold')
            opts.add('max_threshold')
            opts.add('timestamp_resolution')

        return map(escape_value, opts)
    return ()


def cf_prop_val_mapval_completer(ctxt, cass):
    opt = ctxt.get_binding('propname')[-1]
    key = dequote_value(ctxt.get_binding('propmapkey')[-1])
    if opt == 'compaction':
        if key == 'class':
            return map(escape_value, CqlRuleSet.available_compaction_classes)
        return [Hint('<option_value>')]
    elif opt == 'compression':
        if key == 'sstable_compression':
            return map(escape_value, CqlRuleSet.available_compression_classes)
        return [Hint('<option_value>')]
    elif opt == 'caching':
        if key == 'rows_per_partition':
            return ["'ALL'", "'NONE'", Hint('#rows_per_partition')]
        elif key == 'keys':
            return ["'ALL'", "'NONE'"]
    return ()


def cf_prop_val_mapender_completer(ctxt, cass):
    return [',', '}']


@completer_for('tokenDefinition', 'token')
def token_word_completer(ctxt, cass):
    return ['token(']


@completer_for('simpleStorageType', 'typename')
def storagetype_completer(ctxt, cass):
    return simple_cql_types


@completer_for('keyspaceName', 'ksname')
def ks_name_completer(ctxt, cass):
    return map(maybe_escape_name, cass.get_keyspace_names())


@completer_for('nonSystemKeyspaceName', 'ksname')
def non_system_ks_name_completer(ctxt, cass):
    ksnames = [n for n in cass.get_keyspace_names() if n not in SYSTEM_KEYSPACES]
    return map(maybe_escape_name, ksnames)


@completer_for('alterableKeyspaceName', 'ksname')
def alterable_ks_name_completer(ctxt, cass):
    ksnames = [n for n in cass.get_keyspace_names() if n not in NONALTERBALE_KEYSPACES]
    return map(maybe_escape_name, ksnames)


def cf_ks_name_completer(ctxt, cass):
    return [maybe_escape_name(ks) + '.' for ks in cass.get_keyspace_names()]


completer_for('columnFamilyName', 'ksname')(cf_ks_name_completer)
completer_for('materializedViewName', 'ksname')(cf_ks_name_completer)


def cf_ks_dot_completer(ctxt, cass):
    name = dequote_name(ctxt.get_binding('ksname'))
    if name in cass.get_keyspace_names():
        return ['.']
    return []


completer_for('columnFamilyName', 'dot')(cf_ks_dot_completer)
completer_for('materializedViewName', 'dot')(cf_ks_dot_completer)


@completer_for('columnFamilyName', 'cfname')
def cf_name_completer(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    try:
        cfnames = cass.get_columnfamily_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_escape_name, cfnames)


@completer_for('materializedViewName', 'mvname')
def mv_name_completer(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    try:
        mvnames = cass.get_materialized_view_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_escape_name, mvnames)


completer_for('userTypeName', 'ksname')(cf_ks_name_completer)

completer_for('userTypeName', 'dot')(cf_ks_dot_completer)


def ut_name_completer(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    try:
        utnames = cass.get_usertype_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_escape_name, utnames)


completer_for('userTypeName', 'utname')(ut_name_completer)
completer_for('userType', 'utname')(ut_name_completer)


@completer_for('unreservedKeyword', 'nocomplete')
def unreserved_keyword_completer(ctxt, cass):
    # we never want to provide completions through this production;
    # this is always just to allow use of some keywords as column
    # names, CF names, property values, etc.
    return ()


def get_table_meta(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    cf = dequote_name(ctxt.get_binding('cfname'))
    return cass.get_table_meta(ks, cf)


def get_ut_layout(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    ut = dequote_name(ctxt.get_binding('utname'))
    return cass.get_usertype_layout(ks, ut)


def working_on_keyspace(ctxt):
    wat = ctxt.get_binding('wat').upper()
    if wat in ('KEYSPACE', 'SCHEMA'):
        return True
    return False


syntax_rules += r'''
<useStatement> ::= "USE" <keyspaceName>
                 ;
<selectStatement> ::= "SELECT" ( "JSON" )? <selectClause>
                        "FROM" (cf=<columnFamilyName> | mv=<materializedViewName>)
                          ( "WHERE" <whereClause> )?
                          ( "GROUP" "BY" <groupByClause> ( "," <groupByClause> )* )?
                          ( "ORDER" "BY" <orderByClause> ( "," <orderByClause> )* )?
                          ( "PER" "PARTITION" "LIMIT" perPartitionLimit=<wholenumber> )?
                          ( "LIMIT" limit=<wholenumber> )?
                          ( "ALLOW" "FILTERING" )?
                    ;
<whereClause> ::= <relation> ( "AND" <relation> )*
                ;
<relation> ::= [rel_lhs]=<cident> ( "[" <term> "]" )? ( "=" | "<" | ">" | "<=" | ">=" | "CONTAINS" ( "KEY" )? ) <term>
             | token="TOKEN" "(" [rel_tokname]=<cident>
                                 ( "," [rel_tokname]=<cident> )*
                             ")" ("=" | "<" | ">" | "<=" | ">=") <tokenDefinition>
             | [rel_lhs]=<cident> "IN" "(" <term> ( "," <term> )* ")"
             ;
<selectClause> ::= "DISTINCT"? <selector> ("AS" <cident>)? ("," <selector> ("AS" <cident>)?)*
                 | "*"
                 ;
<udtSubfieldSelection> ::= <identifier> "." <identifier>
                         ;
<selector> ::= [colname]=<cident>
             | <udtSubfieldSelection>
             | "WRITETIME" "(" [colname]=<cident> ")"
             | "TTL" "(" [colname]=<cident> ")"
             | "COUNT" "(" star=( "*" | "1" ) ")"
             | "CAST" "(" <selector> "AS" <storageType> ")"
             | <functionName> <selectionFunctionArguments>
             | <term>
             ;
<selectionFunctionArguments> ::= "(" ( <selector> ( "," <selector> )* )? ")"
                          ;
<orderByClause> ::= [ordercol]=<cident> ( "ASC" | "DESC" )?
                  ;
<groupByClause> ::= [groupcol]=<cident>
                  ;
'''


def udf_name_completer(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    try:
        udfnames = cass.get_userfunction_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_escape_name, udfnames)


def uda_name_completer(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    try:
        udanames = cass.get_useraggregate_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_escape_name, udanames)


def udf_uda_name_completer(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    try:
        functionnames = cass.get_userfunction_names(ks) + cass.get_useraggregate_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_escape_name, functionnames)


def ref_udf_name_completer(ctxt, cass):
    try:
        udanames = cass.get_userfunction_names(None)
    except Exception:
        return ()
    return map(maybe_escape_name, udanames)


completer_for('functionAggregateName', 'ksname')(cf_ks_name_completer)
completer_for('functionAggregateName', 'dot')(cf_ks_dot_completer)
completer_for('functionAggregateName', 'functionname')(udf_uda_name_completer)
completer_for('anyFunctionName', 'ksname')(cf_ks_name_completer)
completer_for('anyFunctionName', 'dot')(cf_ks_dot_completer)
completer_for('anyFunctionName', 'udfname')(udf_name_completer)
completer_for('userFunctionName', 'ksname')(cf_ks_name_completer)
completer_for('userFunctionName', 'dot')(cf_ks_dot_completer)
completer_for('userFunctionName', 'udfname')(udf_name_completer)
completer_for('refUserFunctionName', 'udfname')(ref_udf_name_completer)
completer_for('userAggregateName', 'ksname')(cf_ks_name_completer)
completer_for('userAggregateName', 'dot')(cf_ks_dot_completer)
completer_for('userAggregateName', 'udaname')(uda_name_completer)


@completer_for('orderByClause', 'ordercol')
def select_order_column_completer(ctxt, cass):
    prev_order_cols = ctxt.get_binding('ordercol', ())
    keyname = ctxt.get_binding('keyname')
    if keyname is None:
        keyname = ctxt.get_binding('rel_lhs', ())
        if not keyname:
            return [Hint("Can't ORDER BY here: need to specify partition key in WHERE clause")]
    layout = get_table_meta(ctxt, cass)
    order_by_candidates = [col.name for col in layout.clustering_key]
    if len(order_by_candidates) > len(prev_order_cols):
        return [maybe_escape_name(order_by_candidates[len(prev_order_cols)])]
    return [Hint('No more orderable columns here.')]


@completer_for('groupByClause', 'groupcol')
def select_group_column_completer(ctxt, cass):
    prev_group_cols = ctxt.get_binding('groupcol', ())
    layout = get_table_meta(ctxt, cass)
    group_by_candidates = [col.name for col in layout.primary_key]
    if len(group_by_candidates) > len(prev_group_cols):
        return [maybe_escape_name(group_by_candidates[len(prev_group_cols)])]
    return [Hint('No more columns here.')]


@completer_for('relation', 'token')
def relation_token_word_completer(ctxt, cass):
    return ['TOKEN(']


@completer_for('relation', 'rel_tokname')
def relation_token_subject_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    return [key.name for key in layout.partition_key]


@completer_for('relation', 'rel_lhs')
def select_relation_lhs_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    filterable = set()
    already_filtered_on = map(dequote_name, ctxt.get_binding('rel_lhs', ()))
    for num in range(0, len(layout.partition_key)):
        if num == 0 or layout.partition_key[num - 1].name in already_filtered_on:
            filterable.add(layout.partition_key[num].name)
        else:
            break
    for num in range(0, len(layout.clustering_key)):
        if num == 0 or layout.clustering_key[num - 1].name in already_filtered_on:
            filterable.add(layout.clustering_key[num].name)
        else:
            break
    for idx in layout.indexes.itervalues():
        filterable.add(idx.index_options["target"])
    return map(maybe_escape_name, filterable)


explain_completion('selector', 'colname')

syntax_rules += r'''
<insertStatement> ::= "INSERT" "INTO" cf=<columnFamilyName>
                      ( ( "(" [colname]=<cident> ( "," [colname]=<cident> )* ")"
                          "VALUES" "(" [newval]=<term> ( valcomma="," [newval]=<term> )* valcomma=")")
                        | ("JSON" <stringLiteral>))
                      ( "IF" "NOT" "EXISTS")?
                      ( "USING" [insertopt]=<usingOption>
                                ( "AND" [insertopt]=<usingOption> )* )?
                    ;
<usingOption> ::= "TIMESTAMP" <wholenumber>
                | "TTL" <wholenumber>
                ;
'''


def regular_column_names(table_meta):
    if not table_meta or not table_meta.columns:
        return []
    regular_columns = list(set(table_meta.columns.keys()) -
                           set([key.name for key in table_meta.partition_key]) -
                           set([key.name for key in table_meta.clustering_key]))
    return regular_columns


@completer_for('insertStatement', 'colname')
def insert_colname_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    colnames = set(map(dequote_name, ctxt.get_binding('colname', ())))
    keycols = layout.primary_key
    for k in keycols:
        if k.name not in colnames:
            return [maybe_escape_name(k.name)]
    normalcols = set(regular_column_names(layout)) - colnames
    return map(maybe_escape_name, normalcols)


@completer_for('insertStatement', 'newval')
def insert_newval_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    insertcols = map(dequote_name, ctxt.get_binding('colname'))
    valuesdone = ctxt.get_binding('newval', ())
    if len(valuesdone) >= len(insertcols):
        return []
    curcol = insertcols[len(valuesdone)]
    coltype = layout.columns[curcol].cql_type
    if coltype in ('map', 'set'):
        return ['{']
    if coltype == 'list':
        return ['[']
    if coltype == 'boolean':
        return ['true', 'false']

    return [Hint('<value for %s (%s)>' % (maybe_escape_name(curcol),
                                          coltype))]


@completer_for('insertStatement', 'valcomma')
def insert_valcomma_completer(ctxt, cass):
    numcols = len(ctxt.get_binding('colname', ()))
    numvals = len(ctxt.get_binding('newval', ()))
    if numcols > numvals:
        return [',']
    return [')']


@completer_for('insertStatement', 'insertopt')
def insert_option_completer(ctxt, cass):
    opts = set('TIMESTAMP TTL'.split())
    for opt in ctxt.get_binding('insertopt', ()):
        opts.discard(opt.split()[0])
    return opts


syntax_rules += r'''
<updateStatement> ::= "UPDATE" cf=<columnFamilyName>
                        ( "USING" [updateopt]=<usingOption>
                                  ( "AND" [updateopt]=<usingOption> )* )?
                        "SET" <assignment> ( "," <assignment> )*
                        "WHERE" <whereClause>
                        ( "IF" ( "EXISTS" | <conditions> ))?
                    ;
<assignment> ::= updatecol=<cident>
                    (( "=" update_rhs=( <term> | <cident> )
                                ( counterop=( "+" | "-" ) inc=<wholenumber>
                                | listadder="+" listcol=<cident> )? )
                    | ( indexbracket="[" <term> "]" "=" <term> )
                    | ( udt_field_dot="." udt_field=<identifier> "=" <term> ))
               ;
<conditions> ::=  <condition> ( "AND" <condition> )*
               ;
<condition_op_and_rhs> ::= (("=" | "<" | ">" | "<=" | ">=" | "!=") <term>)
                           | ("IN" "(" <term> ( "," <term> )* ")" )
                         ;
<condition> ::= conditioncol=<cident>
                    ( (( indexbracket="[" <term> "]" )
                      |( udt_field_dot="." udt_field=<identifier> )) )?
                    <condition_op_and_rhs>
              ;
'''


@completer_for('updateStatement', 'updateopt')
def update_option_completer(ctxt, cass):
    opts = set('TIMESTAMP TTL'.split())
    for opt in ctxt.get_binding('updateopt', ()):
        opts.discard(opt.split()[0])
    return opts


@completer_for('assignment', 'updatecol')
def update_col_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    return map(maybe_escape_name, regular_column_names(layout))


@completer_for('assignment', 'update_rhs')
def update_countername_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    coltype = layout.columns[curcol].cql_type
    if coltype == 'counter':
        return [maybe_escape_name(curcol)]
    if coltype in ('map', 'set'):
        return ["{"]
    if coltype == 'list':
        return ["["]
    return [Hint('<term (%s)>' % coltype)]


@completer_for('assignment', 'counterop')
def update_counterop_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    return ['+', '-'] if layout.columns[curcol].cql_type == 'counter' else []


@completer_for('assignment', 'inc')
def update_counter_inc_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    if layout.columns[curcol].cql_type == 'counter':
        return [Hint('<wholenumber>')]
    return []


@completer_for('assignment', 'listadder')
def update_listadder_completer(ctxt, cass):
    rhs = ctxt.get_binding('update_rhs')
    if rhs.startswith('['):
        return ['+']
    return []


@completer_for('assignment', 'listcol')
def update_listcol_completer(ctxt, cass):
    rhs = ctxt.get_binding('update_rhs')
    if rhs.startswith('['):
        colname = dequote_name(ctxt.get_binding('updatecol'))
        return [maybe_escape_name(colname)]
    return []


@completer_for('assignment', 'indexbracket')
def update_indexbracket_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    coltype = layout.columns[curcol].cql_type
    if coltype in ('map', 'list'):
        return ['[']
    return []


@completer_for('assignment', 'udt_field_dot')
def update_udt_field_dot_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    return ["."] if _is_usertype(layout, curcol) else []


@completer_for('assignment', 'udt_field')
def assignment_udt_field_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    return _usertype_fields(ctxt, cass, layout, curcol)


def _is_usertype(layout, curcol):
    coltype = layout.columns[curcol].cql_type
    return coltype not in simple_cql_types and coltype not in ('map', 'set', 'list')


def _usertype_fields(ctxt, cass, layout, curcol):
    if not _is_usertype(layout, curcol):
        return []

    coltype = layout.columns[curcol].cql_type
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    user_type = cass.get_usertype_layout(ks, coltype)
    return [field_name for (field_name, field_type) in user_type]


@completer_for('condition', 'indexbracket')
def condition_indexbracket_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('conditioncol', ''))
    coltype = layout.columns[curcol].cql_type
    if coltype in ('map', 'list'):
        return ['[']
    return []


@completer_for('condition', 'udt_field_dot')
def condition_udt_field_dot_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('conditioncol', ''))
    return ["."] if _is_usertype(layout, curcol) else []


@completer_for('condition', 'udt_field')
def condition_udt_field_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('conditioncol', ''))
    return _usertype_fields(ctxt, cass, layout, curcol)


syntax_rules += r'''
<deleteStatement> ::= "DELETE" ( <deleteSelector> ( "," <deleteSelector> )* )?
                        "FROM" cf=<columnFamilyName>
                        ( "USING" [delopt]=<deleteOption> )?
                        "WHERE" <whereClause>
                        ( "IF" ( "EXISTS" | <conditions> ) )?
                    ;
<deleteSelector> ::= delcol=<cident>
                     ( ( "[" <term> "]" )
                     | ( "." <identifier> ) )?
                   ;
<deleteOption> ::= "TIMESTAMP" <wholenumber>
                 ;
'''


@completer_for('deleteStatement', 'delopt')
def delete_opt_completer(ctxt, cass):
    opts = set('TIMESTAMP'.split())
    for opt in ctxt.get_binding('delopt', ()):
        opts.discard(opt.split()[0])
    return opts


@completer_for('deleteSelector', 'delcol')
def delete_delcol_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    return map(maybe_escape_name, regular_column_names(layout))


syntax_rules += r'''
<batchStatement> ::= "BEGIN" ( "UNLOGGED" | "COUNTER" )? "BATCH"
                        ( "USING" [batchopt]=<usingOption>
                                  ( "AND" [batchopt]=<usingOption> )* )?
                        [batchstmt]=<batchStatementMember> ";"?
                            ( [batchstmt]=<batchStatementMember> ";"? )*
                     "APPLY" "BATCH"
                   ;
<batchStatementMember> ::= <insertStatement>
                         | <updateStatement>
                         | <deleteStatement>
                         ;
'''


@completer_for('batchStatement', 'batchopt')
def batch_opt_completer(ctxt, cass):
    opts = set('TIMESTAMP'.split())
    for opt in ctxt.get_binding('batchopt', ()):
        opts.discard(opt.split()[0])
    return opts


syntax_rules += r'''
<truncateStatement> ::= "TRUNCATE" ("COLUMNFAMILY" | "TABLE")? cf=<columnFamilyName>
                      ;
'''

syntax_rules += r'''
<createKeyspaceStatement> ::= "CREATE" wat=( "KEYSPACE" | "SCHEMA" ) ("IF" "NOT" "EXISTS")?  ksname=<cfOrKsName>
                                "WITH" <property> ( "AND" <property> )*
                            ;
'''


@completer_for('createKeyspaceStatement', 'wat')
def create_ks_wat_completer(ctxt, cass):
    # would prefer to get rid of the "schema" nomenclature in cql3
    if ctxt.get_binding('partial', '') == '':
        return ['KEYSPACE']
    return ['KEYSPACE', 'SCHEMA']


syntax_rules += r'''
<createColumnFamilyStatement> ::= "CREATE" wat=( "COLUMNFAMILY" | "TABLE" ) ("IF" "NOT" "EXISTS")?
                                    ( ks=<nonSystemKeyspaceName> dot="." )? cf=<cfOrKsName>
                                    "(" ( <singleKeyCfSpec> | <compositeKeyCfSpec> ) ")"
                                   ( "WITH" <cfamProperty> ( "AND" <cfamProperty> )* )?
                                ;

<cfamProperty> ::= <property>
                 | "COMPACT" "STORAGE" "CDC"
                 | "CLUSTERING" "ORDER" "BY" "(" <cfamOrdering>
                                                 ( "," <cfamOrdering> )* ")"
                 ;

<cfamOrdering> ::= [ordercol]=<cident> ( "ASC" | "DESC" )
                 ;

<singleKeyCfSpec> ::= [newcolname]=<cident> <storageType> "PRIMARY" "KEY"
                      ( "," [newcolname]=<cident> <storageType> )*
                    ;

<compositeKeyCfSpec> ::= [newcolname]=<cident> <storageType>
                         "," [newcolname]=<cident> <storageType> ( "static" )?
                         ( "," [newcolname]=<cident> <storageType> ( "static" )? )*
                         "," "PRIMARY" k="KEY" p="(" ( partkey=<pkDef> | [pkey]=<cident> )
                                                     ( c="," [pkey]=<cident> )* ")"
                       ;

<pkDef> ::= "(" [ptkey]=<cident> "," [ptkey]=<cident>
                               ( "," [ptkey]=<cident> )* ")"
          ;
'''


@completer_for('cfamOrdering', 'ordercol')
def create_cf_clustering_order_colname_completer(ctxt, cass):
    colnames = map(dequote_name, ctxt.get_binding('newcolname', ()))
    # Definitely some of these aren't valid for ordering, but I'm not sure
    # precisely which are. This is good enough for now
    return colnames


@completer_for('createColumnFamilyStatement', 'wat')
def create_cf_wat_completer(ctxt, cass):
    # would prefer to get rid of the "columnfamily" nomenclature in cql3
    if ctxt.get_binding('partial', '') == '':
        return ['TABLE']
    return ['TABLE', 'COLUMNFAMILY']


explain_completion('createColumnFamilyStatement', 'cf', '<new_table_name>')
explain_completion('compositeKeyCfSpec', 'newcolname', '<new_column_name>')


@completer_for('createColumnFamilyStatement', 'dot')
def create_cf_ks_dot_completer(ctxt, cass):
    ks = dequote_name(ctxt.get_binding('ks'))
    if ks in cass.get_keyspace_names():
        return ['.']
    return []


@completer_for('pkDef', 'ptkey')
def create_cf_pkdef_declaration_completer(ctxt, cass):
    cols_declared = ctxt.get_binding('newcolname')
    pieces_already = ctxt.get_binding('ptkey', ())
    pieces_already = map(dequote_name, pieces_already)
    while cols_declared[0] in pieces_already:
        cols_declared = cols_declared[1:]
        if len(cols_declared) < 2:
            return ()
    return [maybe_escape_name(cols_declared[0])]


@completer_for('compositeKeyCfSpec', 'pkey')
def create_cf_composite_key_declaration_completer(ctxt, cass):
    cols_declared = ctxt.get_binding('newcolname')
    pieces_already = ctxt.get_binding('ptkey', ()) + ctxt.get_binding('pkey', ())
    pieces_already = map(dequote_name, pieces_already)
    while cols_declared[0] in pieces_already:
        cols_declared = cols_declared[1:]
        if len(cols_declared) < 2:
            return ()
    return [maybe_escape_name(cols_declared[0])]


@completer_for('compositeKeyCfSpec', 'k')
def create_cf_composite_primary_key_keyword_completer(ctxt, cass):
    return ['KEY (']


@completer_for('compositeKeyCfSpec', 'p')
def create_cf_composite_primary_key_paren_completer(ctxt, cass):
    return ['(']


@completer_for('compositeKeyCfSpec', 'c')
def create_cf_composite_primary_key_comma_completer(ctxt, cass):
    cols_declared = ctxt.get_binding('newcolname')
    pieces_already = ctxt.get_binding('pkey', ())
    if len(pieces_already) >= len(cols_declared) - 1:
        return ()
    return [',']


syntax_rules += r'''

<idxName> ::= <identifier>
            | <quotedName>
            | <unreservedKeyword>;

<createIndexStatement> ::= "CREATE" "CUSTOM"? "INDEX" ("IF" "NOT" "EXISTS")? indexname=<idxName>? "ON"
                               cf=<columnFamilyName> "(" (
                                   col=<cident> |
                                   "keys(" col=<cident> ")" |
                                   "full(" col=<cident> ")"
                               ) ")"
                               ( "USING" <stringLiteral> ( "WITH" "OPTIONS" "=" <mapLiteral> )? )?
                         ;

<createMaterializedViewStatement> ::= "CREATE" "MATERIALIZED" "VIEW" ("IF" "NOT" "EXISTS")? <materializedViewName>?
                                      "AS" <selectStatement>
                                      "PRIMARY" "KEY" <pkDef>
                                    ;

<createUserTypeStatement> ::= "CREATE" "TYPE" ( ks=<nonSystemKeyspaceName> dot="." )? typename=<cfOrKsName> "(" newcol=<cident> <storageType>
                                ( "," [newcolname]=<cident> <storageType> )*
                            ")"
                         ;

<createFunctionStatement> ::= "CREATE" ("OR" "REPLACE")? "FUNCTION"
                            ("IF" "NOT" "EXISTS")?
                            <userFunctionName>
                            ( "(" ( newcol=<cident> <storageType>
                              ( "," [newcolname]=<cident> <storageType> )* )?
                            ")" )?
                            ("RETURNS" "NULL" | "CALLED") "ON" "NULL" "INPUT"
                            "RETURNS" <storageType>
                            "LANGUAGE" <cident> "AS" <stringLiteral>
                         ;

<createAggregateStatement> ::= "CREATE" ("OR" "REPLACE")? "AGGREGATE"
                            ("IF" "NOT" "EXISTS")?
                            <userAggregateName>
                            ( "("
                                 ( <storageType> ( "," <storageType> )* )?
                              ")" )?
                            "SFUNC" <refUserFunctionName>
                            "STYPE" <storageType>
                            ( "FINALFUNC" <refUserFunctionName> )?
                            ( "INITCOND" <term> )?
                         ;

'''

explain_completion('createIndexStatement', 'indexname', '<new_index_name>')
explain_completion('createUserTypeStatement', 'typename', '<new_type_name>')
explain_completion('createUserTypeStatement', 'newcol', '<new_field_name>')


@completer_for('createIndexStatement', 'col')
def create_index_col_completer(ctxt, cass):
    """ Return the columns for which an index doesn't exist yet. """
    layout = get_table_meta(ctxt, cass)
    idx_targets = [idx.index_options["target"] for idx in layout.indexes.itervalues()]
    colnames = [cd.name for cd in layout.columns.values() if cd.name not in idx_targets]
    return map(maybe_escape_name, colnames)


syntax_rules += r'''
<dropKeyspaceStatement> ::= "DROP" "KEYSPACE" ("IF" "EXISTS")? ksname=<nonSystemKeyspaceName>
                          ;

<dropColumnFamilyStatement> ::= "DROP" ( "COLUMNFAMILY" | "TABLE" ) ("IF" "EXISTS")? cf=<columnFamilyName>
                              ;

<indexName> ::= ( ksname=<idxOrKsName> dot="." )? idxname=<idxOrKsName> ;

<idxOrKsName> ::= <identifier>
               | <quotedName>
               | <unreservedKeyword>;

<dropIndexStatement> ::= "DROP" "INDEX" ("IF" "EXISTS")? idx=<indexName>
                       ;

<dropMaterializedViewStatement> ::= "DROP" "MATERIALIZED" "VIEW" ("IF" "EXISTS")? mv=<materializedViewName>
                                  ;

<dropUserTypeStatement> ::= "DROP" "TYPE" ut=<userTypeName>
                          ;

<dropFunctionStatement> ::= "DROP" "FUNCTION" ( "IF" "EXISTS" )? <userFunctionName>
                          ;

<dropAggregateStatement> ::= "DROP" "AGGREGATE" ( "IF" "EXISTS" )? <userAggregateName>
                          ;

'''


@completer_for('indexName', 'ksname')
def idx_ks_name_completer(ctxt, cass):
    return [maybe_escape_name(ks) + '.' for ks in cass.get_keyspace_names()]


@completer_for('indexName', 'dot')
def idx_ks_dot_completer(ctxt, cass):
    name = dequote_name(ctxt.get_binding('ksname'))
    if name in cass.get_keyspace_names():
        return ['.']
    return []


@completer_for('indexName', 'idxname')
def idx_ks_idx_name_completer(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    try:
        idxnames = cass.get_index_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_escape_name, idxnames)


syntax_rules += r'''
<alterTableStatement> ::= "ALTER" wat=( "COLUMNFAMILY" | "TABLE" ) cf=<columnFamilyName>
                               <alterInstructions>
                        ;
<alterInstructions> ::= "ADD" newcol=<cident> <storageType> ("static")?
                      | "DROP" existcol=<cident>
                      | "WITH" <cfamProperty> ( "AND" <cfamProperty> )*
                      | "RENAME" existcol=<cident> "TO" newcol=<cident>
                         ( "AND" existcol=<cident> "TO" newcol=<cident> )*
                      ;

<alterUserTypeStatement> ::= "ALTER" "TYPE" ut=<userTypeName>
                               <alterTypeInstructions>
                             ;
<alterTypeInstructions> ::= "ADD" newcol=<cident> <storageType>
                           | "RENAME" existcol=<cident> "TO" newcol=<cident>
                              ( "AND" existcol=<cident> "TO" newcol=<cident> )*
                           ;
'''


@completer_for('alterInstructions', 'existcol')
def alter_table_col_completer(ctxt, cass):
    layout = get_table_meta(ctxt, cass)
    cols = [str(md) for md in layout.columns]
    return map(maybe_escape_name, cols)


@completer_for('alterTypeInstructions', 'existcol')
def alter_type_field_completer(ctxt, cass):
    layout = get_ut_layout(ctxt, cass)
    fields = [tuple[0] for tuple in layout]
    return map(maybe_escape_name, fields)


explain_completion('alterInstructions', 'newcol', '<new_column_name>')
explain_completion('alterTypeInstructions', 'newcol', '<new_field_name>')


syntax_rules += r'''
<alterKeyspaceStatement> ::= "ALTER" wat=( "KEYSPACE" | "SCHEMA" ) ks=<alterableKeyspaceName>
                                 "WITH" <property> ( "AND" <property> )*
                           ;
'''

syntax_rules += r'''
<username> ::= name=( <identifier> | <stringLiteral> )
             ;

<createUserStatement> ::= "CREATE" "USER" ( "IF" "NOT" "EXISTS" )? <username>
                              ( "WITH" "PASSWORD" <stringLiteral> )?
                              ( "SUPERUSER" | "NOSUPERUSER" )?
                        ;

<alterUserStatement> ::= "ALTER" "USER" <username>
                              ( "WITH" "PASSWORD" <stringLiteral> )?
                              ( "SUPERUSER" | "NOSUPERUSER" )?
                       ;

<dropUserStatement> ::= "DROP" "USER" ( "IF" "EXISTS" )? <username>
                      ;

<listUsersStatement> ::= "LIST" "USERS"
                       ;
'''

syntax_rules += r'''
<rolename> ::= <identifier>
             | <quotedName>
             | <unreservedKeyword>
             ;

<createRoleStatement> ::= "CREATE" "ROLE" <rolename>
                              ( "WITH" <roleProperty> ("AND" <roleProperty>)*)?
                        ;

<alterRoleStatement> ::= "ALTER" "ROLE" <rolename>
                              ( "WITH" <roleProperty> ("AND" <roleProperty>)*)?
                       ;

<roleProperty> ::= "PASSWORD" "=" <stringLiteral>
                 | "OPTIONS" "=" <mapLiteral>
                 | "SUPERUSER" "=" <boolean>
                 | "LOGIN" "=" <boolean>
                 ;

<dropRoleStatement> ::= "DROP" "ROLE" <rolename>
                      ;

<grantRoleStatement> ::= "GRANT" <rolename> "TO" <rolename>
                       ;

<revokeRoleStatement> ::= "REVOKE" <rolename> "FROM" <rolename>
                        ;

<listRolesStatement> ::= "LIST" "ROLES"
                              ( "OF" <rolename> )? "NORECURSIVE"?
                       ;
'''

syntax_rules += r'''
<grantStatement> ::= "GRANT" <permissionExpr> "ON" <resource> "TO" <rolename>
                   ;

<revokeStatement> ::= "REVOKE" <permissionExpr> "ON" <resource> "FROM" <rolename>
                    ;

<listPermissionsStatement> ::= "LIST" <permissionExpr>
                                    ( "ON" <resource> )? ( "OF" <rolename> )? "NORECURSIVE"?
                             ;

<permission> ::= "AUTHORIZE"
               | "CREATE"
               | "ALTER"
               | "DROP"
               | "SELECT"
               | "MODIFY"
               | "DESCRIBE"
               | "EXECUTE"
               ;

<permissionExpr> ::= ( <permission> "PERMISSION"? )
                   | ( "ALL" "PERMISSIONS"? )
                   ;

<resource> ::= <dataResource>
             | <roleResource>
             | <functionResource>
             | <jmxResource>
             ;

<dataResource> ::= ( "ALL" "KEYSPACES" )
                 | ( "KEYSPACE" <keyspaceName> )
                 | ( "TABLE"? <columnFamilyName> )
                 ;

<roleResource> ::= ("ALL" "ROLES")
                 | ("ROLE" <rolename>)
                 ;

<functionResource> ::= ( "ALL" "FUNCTIONS" ("IN KEYSPACE" <keyspaceName>)? )
                     | ( "FUNCTION" <functionAggregateName>
                           ( "(" ( newcol=<cident> <storageType>
                             ( "," [newcolname]=<cident> <storageType> )* )?
                           ")" )
                       )
                     ;

<jmxResource> ::= ( "ALL" "MBEANS")
                | ( ( "MBEAN" | "MBEANS" ) <stringLiteral> )
                ;

'''


@completer_for('username', 'name')
def username_name_completer(ctxt, cass):
    def maybe_quote(name):
        if CqlRuleSet.is_valid_cql3_name(name):
            return name
        return "'%s'" % name

    # disable completion for CREATE USER.
    if ctxt.matched[0][1].upper() == 'CREATE':
        return [Hint('<username>')]

    session = cass.session
    return [maybe_quote(row.values()[0].replace("'", "''")) for row in session.execute("LIST USERS")]


@completer_for('rolename', 'role')
def rolename_completer(ctxt, cass):
    def maybe_quote(name):
        if CqlRuleSet.is_valid_cql3_name(name):
            return name
        return "'%s'" % name

    # disable completion for CREATE ROLE.
    if ctxt.matched[0][1].upper() == 'CREATE':
        return [Hint('<rolename>')]

    session = cass.session
    return [maybe_quote(row[0].replace("'", "''")) for row in session.execute("LIST ROLES")]


syntax_rules += r'''
<createTriggerStatement> ::= "CREATE" "TRIGGER" ( "IF" "NOT" "EXISTS" )? <cident>
                               "ON" cf=<columnFamilyName> "USING" class=<stringLiteral>
                           ;
<dropTriggerStatement> ::= "DROP" "TRIGGER" ( "IF" "EXISTS" )? triggername=<cident>
                             "ON" cf=<columnFamilyName>
                         ;
'''
explain_completion('createTriggerStatement', 'class', '\'fully qualified class name\'')


def get_trigger_names(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    return cass.get_trigger_names(ks)


@completer_for('dropTriggerStatement', 'triggername')
def drop_trigger_completer(ctxt, cass):
    names = get_trigger_names(ctxt, cass)
    return map(maybe_escape_name, names)


# END SYNTAX/COMPLETION RULE DEFINITIONS

CqlRuleSet.append_rules(syntax_rules)
