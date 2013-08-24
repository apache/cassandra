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

import re
from warnings import warn
from .cqlhandling import CqlParsingRuleSet, Hint
from cql.cqltypes import (cql_types, lookup_casstype, CompositeType, UTF8Type,
                          ColumnToCollectionType, CounterColumnType, DateType)
from . import helptopics

simple_cql_types = set(cql_types)
simple_cql_types.difference_update(('set', 'map', 'list'))

cqldocs = helptopics.CQL3HelpTopics()

try:
    import json
except ImportError:
    import simplejson as json

# temporarily have this here until a newer cassandra-dbapi2 is bundled with C*
class TimestampType(DateType):
    pass

class UnexpectedTableStructure(UserWarning):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return 'Unexpected table structure; may not translate correctly to CQL. ' + self.msg

SYSTEM_KEYSPACES = ('system', 'system_traces', 'system_auth')
NONALTERBALE_KEYSPACES = ('system', 'system_traces')

class Cql3ParsingRuleSet(CqlParsingRuleSet):
    keywords = set((
        'select', 'from', 'where', 'and', 'key', 'insert', 'update', 'with',
        'limit', 'using', 'use', 'count', 'set',
        'begin', 'apply', 'batch', 'truncate', 'delete', 'in', 'create',
        'keyspace', 'schema', 'columnfamily', 'table', 'index', 'on', 'drop',
        'primary', 'into', 'values', 'timestamp', 'ttl', 'alter', 'add', 'type',
        'compact', 'storage', 'order', 'by', 'asc', 'desc', 'clustering',
        'token', 'writetime', 'map', 'list', 'to', 'custom', 'if', 'not'
    ))

    unreserved_keywords = set((
        'key', 'clustering', 'ttl', 'compact', 'storage', 'type', 'values', 'custom', 'exists'
    ))

    columnfamily_layout_options = (
        ('bloom_filter_fp_chance', None),
        ('caching', None),
        ('comment', None),
        ('dclocal_read_repair_chance', 'local_read_repair_chance'),
        ('gc_grace_seconds', None),
        ('index_interval', None),
        ('read_repair_chance', None),
        ('replicate_on_write', None),
        ('populate_io_cache_on_flush', None),
        ('default_time_to_live', None),
        ('speculative_retry', None),
        ('memtable_flush_period_in_ms', None),
    )

    columnfamily_layout_map_options = (
        # (CQL3 option name, schema_columnfamilies column name (or None if same),
        #  list of known map keys)
        ('compaction', 'compaction_strategy_options',
            ('class', 'min_threshold', 'max_threshold')),
        ('compression', 'compression_parameters',
            ('sstable_compression', 'chunk_length_kb', 'crc_check_chance')),
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
            return 'NULL' # this totally won't work
        if isinstance(value, bool):
            value = str(value).lower()
        elif isinstance(value, float):
            return '%f' % value
        elif isinstance(value, int):
            return str(value)
        return "'%s'" % value.replace("'", "''")

    @staticmethod
    def escape_name(name):
        return '"%s"' % name.replace('"', '""')

    valid_cql3_word_re = re.compile(r'^[a-z][0-9a-z_]*$')

    @classmethod
    def is_valid_cql3_name(cls, s):
        if s is None:
            return False
        if s.lower() in cls.keywords - cls.unreserved_keywords:
            return False
        return cls.valid_cql3_word_re.match(s) is not None

    @classmethod
    def maybe_escape_name(cls, name):
        if cls.is_valid_cql3_name(name):
            return name
        return cls.escape_name(name)

    @staticmethod
    def dequote_name(name):
        name = name.strip()
        if name == '':
            return name
        if name[0] == '"' and name[-1] == '"':
            name = name[1:-1].replace('""', '"')
        return name

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
shorthands = ('completer_for', 'explain_completion',
              'dequote_value', 'dequote_name',
              'escape_value', 'escape_name',
              'maybe_escape_name')

for shorthand in shorthands:
    globals()[shorthand] = getattr(CqlRuleSet, shorthand)


# BEGIN SYNTAX/COMPLETION RULE DEFINITIONS

syntax_rules = r'''
<Start> ::= <CQL_Statement>*
          ;

<CQL_Statement> ::= [statements]=<statementBody> ";"
                  ;

# the order of these terminal productions is significant:
<endline> ::= /\n/ ;

JUNK ::= /([ \t\r\f\v]+|(--|[/][/])[^\n\r]*([\n\r]|$)|[/][*].*?[*][/])/ ;

<stringLiteral> ::= /'([^']|'')*'/ ;
<quotedName> ::=    /"([^"]|"")*"/ ;
<float> ::=         /-?[0-9]+\.[0-9]+/ ;
<wholenumber> ::=   /[0-9]+/ ;
<uuid> ::=          /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/ ;
<identifier> ::=    /[a-z][a-z0-9_]*/ ;
<colon> ::=         ":" ;
<star> ::=          "*" ;
<endtoken> ::=      ";" ;
<op> ::=            /[-+=,().]/ ;
<cmp> ::=           /[<>]=?/ ;
<brackets> ::=      /[][{}]/ ;

<integer> ::= "-"? <wholenumber> ;
<boolean> ::= "true"
            | "false"
            ;

<unclosedString>  ::= /'([^']|'')*/ ;
<unclosedName>    ::= /"([^"]|"")*/ ;
<unclosedComment> ::= /[/][*].*$/ ;

<term> ::= <stringLiteral>
         | <integer>
         | <float>
         | <uuid>
         | <boolean>
         ;

<tokenDefinition> ::= token="TOKEN" "(" <term> ( "," <term> )* ")"
                    | <stringLiteral>
                    ;
<value> ::= <term>
          | <collectionLiteral>
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
                          | <dropKeyspaceStatement>
                          | <dropColumnFamilyStatement>
                          | <dropIndexStatement>
                          | <alterTableStatement>
                          | <alterKeyspaceStatement>
                          ;

<authenticationStatement> ::= <createUserStatement>
                            | <alterUserStatement>
                            | <dropUserStatement>
                            | <listUsersStatement>
                            ;

<authorizationStatement> ::= <grantStatement>
                           | <revokeStatement>
                           | <listPermissionsStatement>
                           ;

# timestamp is included here, since it's also a keyword
<simpleStorageType> ::= typename=( <identifier> | <stringLiteral> | <K_TIMESTAMP> ) ;

<storageType> ::= <simpleStorageType> | <collectionType> ;

<collectionType> ::= "map" "<" <simpleStorageType> "," <simpleStorageType> ">"
                   | "list" "<" <simpleStorageType> ">"
                   | "set" "<" <simpleStorageType> ">"
                   ;

<columnFamilyName> ::= ( ksname=<cfOrKsName> dot="." )? cfname=<cfOrKsName> ;

<keyspaceName> ::= ksname=<cfOrKsName> ;

<nonSystemKeyspaceName> ::= ksname=<cfOrKsName> ;

<alterableKeyspaceName> ::= ksname=<cfOrKsName> ;

<cfOrKsName> ::= <identifier>
               | <quotedName>
               | <unreservedKeyword>;

<unreservedKeyword> ::= nocomplete=
                        ( <K_KEY>
                        | <K_CLUSTERING>
                        # | <K_COUNT>  -- to get count(*) completion, treat count as reserved
                        | <K_TTL>
                        | <K_COMPACT>
                        | <K_STORAGE>
                        | <K_TYPE>
                        | <K_VALUES> )
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
    return [Hint('<value>')]

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
    if any(this_opt == opt[0] for opt in CqlRuleSet.obsolete_cf_options):
        return ["'<obsolete_option>'"]
    if this_opt in ('read_repair_chance', 'bloom_filter_fp_chance',
                    'dclocal_read_repair_chance'):
        return [Hint('<float_between_0_and_1>')]
    if this_opt in ('replicate_on_write', 'populate_io_cache_on_flush'):
        return ["'yes'", "'no'"]
    if this_opt in ('min_compaction_threshold', 'max_compaction_threshold',
                    'gc_grace_seconds', 'index_interval'):
        return [Hint('<integer>')]
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
    if optname == 'compaction':
        opts = set(subopts)
        try:
            csc = pairsseen['class']
        except KeyError:
            return ["'class'"]
        csc = csc.split('.')[-1]
        if csc == 'SizeTieredCompactionStrategy':
            opts.add('min_sstable_size')
        elif csc == 'LeveledCompactionStrategy':
            opts.add('sstable_size_in_mb')
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
def ks_name_completer(ctxt, cass):
    ksnames = [n for n in cass.get_keyspace_names() if n not in SYSTEM_KEYSPACES]
    return map(maybe_escape_name, ksnames)

@completer_for('alterableKeyspaceName', 'ksname')
def ks_name_completer(ctxt, cass):
    ksnames = [n for n in cass.get_keyspace_names() if n not in NONALTERBALE_KEYSPACES]
    return map(maybe_escape_name, ksnames)

@completer_for('columnFamilyName', 'ksname')
def cf_ks_name_completer(ctxt, cass):
    return [maybe_escape_name(ks) + '.' for ks in cass.get_keyspace_names()]

@completer_for('columnFamilyName', 'dot')
def cf_ks_dot_completer(ctxt, cass):
    name = dequote_name(ctxt.get_binding('ksname'))
    if name in cass.get_keyspace_names():
        return ['.']
    return []

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

@completer_for('unreservedKeyword', 'nocomplete')
def unreserved_keyword_completer(ctxt, cass):
    # we never want to provide completions through this production;
    # this is always just to allow use of some keywords as column
    # names, CF names, property values, etc.
    return ()

def get_cf_layout(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    if ks is not None:
        ks = dequote_name(ks)
    cf = dequote_name(ctxt.get_binding('cfname'))
    return cass.get_columnfamily_layout(ks, cf)

def working_on_keyspace(ctxt):
    wat = ctxt.get_binding('wat').upper()
    if wat in ('KEYSPACE', 'SCHEMA'):
        return True
    return False

syntax_rules += r'''
<useStatement> ::= "USE" <keyspaceName>
                 ;
<selectStatement> ::= "SELECT" <selectClause>
                        "FROM" cf=<columnFamilyName>
                          ("WHERE" <whereClause>)?
                          ("ORDER" "BY" <orderByClause> ( "," <orderByClause> )* )?
                          ("LIMIT" limit=<wholenumber>)?
                    ;
<whereClause> ::= <relation> ("AND" <relation>)*
                ;
<relation> ::= [rel_lhs]=<cident> ("=" | "<" | ">" | "<=" | ">=") <term>
             | token="TOKEN" "(" [rel_tokname]=<cident>
                                 ( "," [rel_tokname]=<cident> )*
                             ")" ("=" | "<" | ">" | "<=" | ">=") <tokenDefinition>
             | [rel_lhs]=<cident> "IN" "(" <term> ( "," <term> )* ")"
             ;
<selectClause> ::= <selector> ("AS" <cident>)? ("," <selector> ("AS" <cident>)?)*
                 | "*"
                 | "COUNT" "(" star=( "*" | "1" ) ")" ("AS" <cident>)?
                 ;
<selector> ::= [colname]=<cident>
             | "WRITETIME" "(" [colname]=<cident> ")"
             | "TTL" "(" [colname]=<cident> ")"
             ;
<orderByClause> ::= [ordercol]=<cident> ( "ASC" | "DESC" )?
                  ;
'''

@completer_for('orderByClause', 'ordercol')
def select_order_column_completer(ctxt, cass):
    prev_order_cols = ctxt.get_binding('ordercol', ())
    keyname = ctxt.get_binding('keyname')
    if keyname is None:
        keyname = ctxt.get_binding('rel_lhs', ())
        if not keyname:
            return [Hint("Can't ORDER BY here: need to specify partition key in WHERE clause")]
    layout = get_cf_layout(ctxt, cass)
    order_by_candidates = layout.clustering_key_columns[:]
    if len(order_by_candidates) > len(prev_order_cols):
        return [maybe_escape_name(order_by_candidates[len(prev_order_cols)])]
    return [Hint('No more orderable columns here.')]

@completer_for('relation', 'token')
def relation_token_word_completer(ctxt, cass):
    return ['TOKEN(']

@completer_for('relation', 'rel_tokname')
def relation_token_subject_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    return [layout.partition_key_columns[0]]

@completer_for('relation', 'rel_lhs')
def select_relation_lhs_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    filterable = set((layout.partition_key_columns[0], layout.clustering_key_columns[0]))
    already_filtered_on = map(dequote_name, ctxt.get_binding('rel_lhs'))
    for num in range(1, len(layout.partition_key_columns)):
        if layout.partition_key_columns[num - 1] in already_filtered_on:
            filterable.add(layout.partition_key_columns[num])
        else:
            break
    for num in range(1, len(layout.clustering_key_columns)):
        if layout.clustering_key_columns[num - 1] in already_filtered_on:
            filterable.add(layout.clustering_key_columns[num])
        else:
            break
    for cd in layout.columns:
        if cd.index_name is not None:
            filterable.add(cd.name)
    return map(maybe_escape_name, filterable)

@completer_for('selectClause', 'star')
def select_count_star_completer(ctxt, cass):
    return ['*']

explain_completion('selector', 'colname')

syntax_rules += r'''
<insertStatement> ::= "INSERT" "INTO" cf=<columnFamilyName>
                               "(" [colname]=<cident> "," [colname]=<cident>
                                   ( "," [colname]=<cident> )* ")"
                      "VALUES" "(" [newval]=<value> valcomma="," [newval]=<value>
                                   ( valcomma="," [newval]=<value> )* valcomma=")"
                      ( "USING" [insertopt]=<usingOption>
                                ( "AND" [insertopt]=<usingOption> )* )?
                    ;
<usingOption> ::= "TIMESTAMP" <wholenumber>
                | "TTL" <wholenumber>
                ;
'''

@completer_for('insertStatement', 'colname')
def insert_colname_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    colnames = set(map(dequote_name, ctxt.get_binding('colname', ())))
    keycols = layout.primary_key_columns
    for k in keycols:
        if k not in colnames:
            return [maybe_escape_name(k)]
    normalcols = set(layout.regular_columns) - colnames
    return map(maybe_escape_name, normalcols)

@completer_for('insertStatement', 'newval')
def insert_newval_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    insertcols = map(dequote_name, ctxt.get_binding('colname'))
    valuesdone = ctxt.get_binding('newval', ())
    if len(valuesdone) >= len(insertcols):
        return []
    curcol = insertcols[len(valuesdone)]
    cqltype = layout.get_column(curcol).cqltype
    coltype = cqltype.typename
    if coltype in ('map', 'set'):
        return ['{']
    if coltype == 'list':
        return ['[']
    if coltype == 'boolean':
        return ['true', 'false']
    return [Hint('<value for %s (%s)>' % (maybe_escape_name(curcol),
                                          cqltype.cql_parameterized_type()))]

@completer_for('insertStatement', 'valcomma')
def insert_valcomma_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
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
                    ;
<assignment> ::= updatecol=<cident>
                    ( "=" update_rhs=( <value> | <cident> )
                                ( counterop=( "+" | "-" ) inc=<wholenumber>
                                | listadder="+" listcol=<cident> )
                    | indexbracket="[" <term> "]" "=" <term> )
               ;
'''

@completer_for('updateStatement', 'updateopt')
def insert_option_completer(ctxt, cass):
    opts = set('TIMESTAMP TTL'.split())
    for opt in ctxt.get_binding('updateopt', ()):
        opts.discard(opt.split()[0])
    return opts

@completer_for('assignment', 'updatecol')
def update_col_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    return map(maybe_escape_name, layout.regular_columns)

@completer_for('assignment', 'update_rhs')
def update_countername_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    cqltype = layout.get_column(curcol).cqltype
    coltype = cqltype.typename
    if coltype == 'counter':
        return maybe_escape_name(curcol)
    if coltype in ('map', 'set'):
        return ["{"]
    if coltype == 'list':
        return ["["]
    return [Hint('<term (%s)>' % cqltype.cql_parameterized_type())]

@completer_for('assignment', 'counterop')
def update_counterop_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    return ['+', '-'] if layout.is_counter_col(curcol) else []

@completer_for('assignment', 'inc')
def update_counter_inc_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    if layout.is_counter_col(curcol):
        return Hint('<wholenumber>')
    return []

@completer_for('assignment', 'listadder')
def update_listadder_completer(ctxt, cass):
    rhs = ctxt.get_binding('update_rhs')
    if rhs.startswith('['):
        return ['+']

@completer_for('assignment', 'listcol')
def update_listcol_completer(ctxt, cass):
    rhs = ctxt.get_binding('update_rhs')
    if rhs.startswith('['):
        colname = dequote_name(ctxt.get_binding('updatecol'))
        return [maybe_escape_name(colname)]
    return []

@completer_for('assignment', 'indexbracket')
def update_indexbracket_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    coltype = layout.get_column(curcol).cqltype.typename
    if coltype in ('map', 'list'):
        return ['[']
    return []

syntax_rules += r'''
<deleteStatement> ::= "DELETE" ( <deleteSelector> ( "," <deleteSelector> )* )?
                        "FROM" cf=<columnFamilyName>
                        ( "USING" [delopt]=<deleteOption> )?
                        "WHERE" <whereClause>
                    ;
<deleteSelector> ::= delcol=<cident> ( memberbracket="[" memberselector=<term> "]" )?
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
    layout = get_cf_layout(ctxt, cass)
    return map(maybe_escape_name, layout.regular_columns)

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
<truncateStatement> ::= "TRUNCATE" cf=<columnFamilyName>
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

@completer_for('property', 'propname')
def keyspace_properties_option_name_completer(ctxt, cass):
    optsseen = ctxt.get_binding('propname', ())
    if 'replication' not in optsseen:
        return ['replication']
    return ["durable_writes"]

@completer_for('propertyValue', 'propsimpleval')
def property_value_completer(ctxt, cass):
    optname = ctxt.get_binding('propname')[-1]
    if optname == 'durable_writes':
        return ["'true'", "'false'"]
    if optname == 'replication':
        return ["{'class': '"]
    return ()

@completer_for('propertyValue', 'propmapkey')
def keyspace_properties_map_key_completer(ctxt, cass):
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

@completer_for('propertyValue', 'propmapval')
def keyspace_properties_map_value_completer(ctxt, cass):
    optname = ctxt.get_binding('propname')[-1]
    if optname != 'replication':
        return ()
    currentkey = dequote_value(ctxt.get_binding('propmapkey')[-1])
    if currentkey == 'class':
        return map(escape_value, CqlRuleSet.replication_strategies)
    return [Hint('<value>')]

@completer_for('propertyValue', 'ender')
def keyspace_properties_map_ender_completer(ctxt, cass):
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
        opts = set(('replication_factor',))
        if 'replication_factor' not in keysseen:
            return [',']
    if repclass == 'NetworkTopologyStrategy' and len(keysseen) == 1:
        return [',']
    return ['}']

syntax_rules += r'''
<createColumnFamilyStatement> ::= "CREATE" wat=( "COLUMNFAMILY" | "TABLE" ) ("IF" "NOT" "EXISTS")?
                                    ( ks=<nonSystemKeyspaceName> dot="." )? cf=<cfOrKsName>
                                    "(" ( <singleKeyCfSpec> | <compositeKeyCfSpec> ) ")"
                                   ( "WITH" <cfamProperty> ( "AND" <cfamProperty> )* )?
                                ;

<cfamProperty> ::= <property>
                 | "COMPACT" "STORAGE"
                 | "CLUSTERING" "ORDER" "BY" "(" <cfamOrdering>
                                                 ( "," <cfamOrdering> )* ")"
                 ;

<cfamOrdering> ::= [ordercol]=<cident> ( "ASC" | "DESC" )
                 ;

<singleKeyCfSpec> ::= [newcolname]=<cident> <simpleStorageType> "PRIMARY" "KEY"
                      ( "," [newcolname]=<cident> <storageType> )*
                    ;

<compositeKeyCfSpec> ::= [newcolname]=<cident> <simpleStorageType>
                         "," [newcolname]=<cident> <storageType>
                         ( "," [newcolname]=<cident> <storageType> )*
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
<createIndexStatement> ::= "CREATE" "CUSTOM"? "INDEX" ("IF" "NOT" "EXISTS")? indexname=<identifier>? "ON"
                               cf=<columnFamilyName> "(" col=<cident> ")"
                               ( "USING" <stringLiteral> )?
                         ;
'''

explain_completion('createIndexStatement', 'indexname', '<new_index_name>')

@completer_for('createIndexStatement', 'col')
def create_index_col_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    colnames = [cd.name for cd in layout.columns if cd.index_name is None]
    return map(maybe_escape_name, colnames)

syntax_rules += r'''
<dropKeyspaceStatement> ::= "DROP" "KEYSPACE" ("IF" "EXISTS")? ksname=<nonSystemKeyspaceName>
                          ;

<dropColumnFamilyStatement> ::= "DROP" ( "COLUMNFAMILY" | "TABLE" ) ("IF" "EXISTS")? cf=<columnFamilyName>
                              ;

<dropIndexStatement> ::= "DROP" "INDEX" ("IF" "EXISTS")? indexname=<identifier>
                       ;
'''

@completer_for('dropIndexStatement', 'indexname')
def drop_index_completer(ctxt, cass):
    return map(maybe_escape_name, cass.get_index_names())

syntax_rules += r'''
<alterTableStatement> ::= "ALTER" ( "COLUMNFAMILY" | "TABLE" ) cf=<columnFamilyName>
                               <alterInstructions>
                        ;
<alterInstructions> ::= "ALTER" existcol=<cident> "TYPE" <storageType>
                      | "ADD" newcol=<cident> <storageType>
                      | "DROP" existcol=<cident>
                      | "WITH" <cfamProperty> ( "AND" <cfamProperty> )*
                      | "RENAME" existcol=<cident> "TO" newcol=<cident>
                         ( "AND" existcol=<cident> "TO" newcol=<cident> )*
                      ;
'''

@completer_for('alterInstructions', 'existcol')
def alter_table_col_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    cols = [md.name for md in layout.columns]
    return map(maybe_escape_name, cols)

explain_completion('alterInstructions', 'newcol', '<new_column_name>')

syntax_rules += r'''
<alterKeyspaceStatement> ::= "ALTER" ( "KEYSPACE" | "SCHEMA" ) ks=<alterableKeyspaceName>
                                 "WITH" <property> ( "AND" <property> )*
                           ;
'''

syntax_rules += r'''
<username> ::= name=( <identifier> | <stringLiteral> )
             ;

<createUserStatement> ::= "CREATE" "USER" <username>
                              ( "WITH" "PASSWORD" <stringLiteral> )?
                              ( "SUPERUSER" | "NOSUPERUSER" )?
                        ;

<alterUserStatement> ::= "ALTER" "USER" <username>
                              ( "WITH" "PASSWORD" <stringLiteral> )?
                              ( "SUPERUSER" | "NOSUPERUSER" )?
                       ;

<dropUserStatement> ::= "DROP" "USER" <username>
                      ;

<listUsersStatement> ::= "LIST" "USERS"
                       ;
'''

syntax_rules += r'''
<grantStatement> ::= "GRANT" <permissionExpr> "ON" <resource> "TO" <username>
                   ;

<revokeStatement> ::= "REVOKE" <permissionExpr> "ON" <resource> "FROM" <username>
                    ;

<listPermissionsStatement> ::= "LIST" <permissionExpr>
                                    ( "ON" <resource> )? ( "OF" <username> )? "NORECURSIVE"?
                             ;

<permission> ::= "AUTHORIZE"
               | "CREATE"
               | "ALTER"
               | "DROP"
               | "SELECT"
               | "MODIFY"
               ;

<permissionExpr> ::= ( <permission> "PERMISSION"? )
                   | ( "ALL" "PERMISSIONS"? )
                   ;

<resource> ::= <dataResource>
             ;

<dataResource> ::= ( "ALL" "KEYSPACES" )
                 | ( "KEYSPACE" <keyspaceName> )
                 | ( "TABLE"? <columnFamilyName> )
                 ;
'''

@completer_for('username', 'name')
def username_name_completer(ctxt, cass):
    def maybe_quote(name):
        if CqlRuleSet.is_valid_cql3_name(name):
            return name
        return "'%s'" % name

    # disable completion for CREATE USER.
    if ctxt.matched[0][0] == 'K_CREATE':
        return [Hint('<username>')]

    cursor = cass.conn.cursor()
    cursor.execute("LIST USERS")
    return [maybe_quote(row[0].replace("'", "''")) for row in cursor.fetchall()]

# END SYNTAX/COMPLETION RULE DEFINITIONS

CqlRuleSet.append_rules(syntax_rules)

class CqlColumnDef:
    index_name = None
    index_type = None
    component_type = 'regular'
    component_index = None
    index_options = {}

    def __init__(self, name, cqltype):
        self.name = name
        self.cqltype = cqltype
        assert name is not None

    @classmethod
    def from_layout(cls, layout):
        c = cls(layout[u'column_name'], lookup_casstype(layout[u'validator']))
        c.component_type = layout[u'type']
        idx = layout[u'component_index'] # can be None
        if idx:
            c.component_index = int(idx)
        c.index_name = layout[u'index_name']
        c.index_type = layout[u'index_type']
        if c.index_type == 'CUSTOM':
            c.index_options = json.loads(layout[u'index_options'])
        return c

    def __str__(self):
        indexstr = ' (index %s)' % self.index_name if self.index_name is not None else ''
        return '<CqlColumnDef %r %r%s>' % (self.name, self.cqltype, indexstr)
    __repr__ = __str__

class CqlTableDef:
    """Names of all columns which are grouped into the partition key"""
    partition_key_columns = ()

    """Names of all columns which are part of the primary key, but not grouped
       into the partition key"""
    clustering_key_columns = ()

    """Names of all columns which are part of the primary key, whether or not
       they are grouped into the partition key"""
    primary_key_columns = ()

    """Names of all columns which aren't part of the primary key"""
    regular_columns = ()

    """CqlColumnDef objects for all columns. Use .get_column() to access one
       by name."""
    columns = ()

    def __init__(self, name):
        self.name = name

    @classmethod
    def from_layout(cls, layout, coldefs):
        """
        This constructor accepts a dictionary of column-value pairs from a row
        of system.schema_columnfamilies, and a sequence of similar dictionaries
        from corresponding rows in system.schema_columns.
        """
        cf = cls(name=layout[u'columnfamily_name'])
        cf.keyspace = layout[u'keyspace_name']
        for attr, val in layout.items():
            setattr(cf, attr.encode('ascii'), val)
        cf.comparator = lookup_casstype(cf.comparator)
        for attr in ('compaction_strategy_options', 'compression_parameters'):
            setattr(cf, attr, json.loads(getattr(cf, attr)))

        # deal with columns
        columns = map(CqlColumnDef.from_layout, coldefs)

        partition_key_cols = filter(lambda c: c.component_type == u'partition_key', columns)
        partition_key_cols.sort(key=lambda c: c.component_index)
        cf.partition_key_columns = map(lambda c: c.name, partition_key_cols)

        clustering_key_cols = filter(lambda c: c.component_type == u'clustering_key', columns)
        clustering_key_cols.sort(key=lambda c: c.component_index)
        cf.clustering_key_columns = map(lambda c: c.name, clustering_key_cols)

        cf.primary_key_columns = cf.partition_key_columns + cf.clustering_key_columns

        regular_cols = list(set(columns) - set(partition_key_cols) - set(clustering_key_cols))
        regular_cols.sort(key=lambda c: c.name)
        cf.regular_columns = map(lambda c: c.name, regular_cols)

        cf.columns = partition_key_cols + clustering_key_cols + regular_cols
        return cf

    # not perfect, but good enough; please read CFDefinition constructor comments
    # returns False if we are dealing with a CQL3 table, True otherwise.
    # 'compact' here means 'needs WITH COMPACT STORAGE option for CREATE TABLE in CQL3'.
    def is_compact_storage(self):
        if not issubclass(self.comparator, CompositeType):
            return True
        for subtype in self.comparator.subtypes:
            if issubclass(subtype, ColumnToCollectionType):
                return False
        if len(self.clustering_key_columns) == len(self.comparator.subtypes) - 1:
            if self.comparator.subtypes[-1] is UTF8Type:
                return False
        return True

    def is_counter_col(self, colname):
        try:
            return bool(self.get_column(colname).cqltype is CounterColumnType)
        except KeyError:
            return False

    def get_column(self, colname):
        col_info = [cm for cm in self.columns if cm.name == colname]
        if not col_info:
            raise KeyError("column %r not found" % (colname,))
        return col_info[0]

    def __str__(self):
        return '<%s %s.%s>' % (self.__class__.__name__, self.keyspace, self.name)
    __repr__ = __str__
