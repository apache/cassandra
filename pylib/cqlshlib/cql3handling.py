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
                          ColumnToCollectionType, CounterColumnType)
from . import helptopics

simple_cql_types = set(cql_types)
simple_cql_types.difference_update(('set', 'map', 'list'))

cqldocs = helptopics.CQL3HelpTopics()

try:
    import json
except ImportError:
    import simplejson as json

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
        'token', 'writetime', 'map', 'list', 'to'
    ))

    unreserved_keywords = set((
        'key', 'clustering', 'ttl', 'compact', 'storage', 'type', 'values'
    ))

    columnfamily_options = (
        # (CQL option name, Thrift option name (or None if same))
        ('comment', None),
        ('compaction_strategy_class', 'compaction_strategy'),
        ('comparator', 'comparator_type'),
        ('default_validation', 'default_validation_class'),
        ('gc_grace_seconds', None),
        ('read_repair_chance', None),
        ('replicate_on_write', None),
        ('populate_io_cache_on_flush', None),
    )

    old_columnfamily_layout_options = (
        # (CQL3 option name, schema_columnfamilies column name (or None if same))
        ('bloom_filter_fp_chance', None),
        ('caching', None),
        ('comment', None),
        ('compaction_strategy_class', None),
        ('dclocal_read_repair_chance', 'local_read_repair_chance'),
        ('gc_grace_seconds', None),
        ('read_repair_chance', None),
        ('replicate_on_write', None),
        ('populate_io_cache_on_flush', None),
    )

    new_columnfamily_layout_options = (
        ('bloom_filter_fp_chance', None),
        ('caching', None),
        ('comment', None),
        ('dclocal_read_repair_chance', 'local_read_repair_chance'),
        ('gc_grace_seconds', None),
        ('read_repair_chance', None),
        ('replicate_on_write', None),
        ('populate_io_cache_on_flush', None),
    )

    old_columnfamily_layout_map_options = (
        # (CQL3 option prefix, schema_columnfamilies column name (or None if same),
        #  list of known suboptions)
        ('compaction_strategy_options', None,
            ('min_compaction_threshold', 'max_compaction_threshold')),
        ('compression_parameters', None,
            ('sstable_compression', 'chunk_length_kb', 'crc_check_chance')),
    )

    new_columnfamily_layout_map_options = (
        # (CQL3 option name, schema_columnfamilies column name (or None if same),
        #  list of known map keys)
        ('compaction', 'compaction_strategy_options',
            ('class', 'min_threshold', 'max_threshold')),
        ('compression', 'compression_parameters',
            ('sstable_compression', 'chunk_length_kb', 'crc_check_chance')),
    )

    new_obsolete_cf_options = (
        'compaction_parameters',
        'compaction_strategy_class',
        'compaction_strategy_options',
        'compression_parameters',
        'max_compaction_threshold',
        'min_compaction_threshold',
    )

    @staticmethod
    def token_dequote(tok):
        if tok[0] == 'unclosedName':
            # strip one quote
            return tok[1][1:].replace('""', '"')
        # cql2 version knows how to do everything else
        return CqlParsingRuleSet.token_dequote(tok)

    @classmethod
    def cql3_dequote_value(cls, value):
        return cls.cql2_dequote_value(value)

    @staticmethod
    def cql3_dequote_name(name):
        name = name.strip()
        if name == '':
            return name
        if name[0] == '"' and name[-1] == '"':
            name = name[1:-1].replace('""', '"')
        return name

    @classmethod
    def cql3_escape_value(cls, value):
        return cls.cql2_escape_value(value)

    @staticmethod
    def cql3_escape_name(name):
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
    def cql3_maybe_escape_name(cls, name):
        if cls.is_valid_cql3_name(name):
            return name
        return cls.cql3_escape_name(name)

    @classmethod
    def dequote_any(cls, t):
        if t[0] == '"':
            return cls.cql3_dequote_name(t)
        return CqlParsingRuleSet.dequote_any(t)

    dequote_value = cql3_dequote_value
    dequote_name = cql3_dequote_name
    escape_value = cql3_escape_value
    escape_name = cql3_escape_name
    maybe_escape_name = cql3_maybe_escape_name

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
<unclosedComment> ::= /[/][*][^\n]*$/ ;

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

# <property> will be defined once cqlsh determines whether we're using
# 3.0.0-beta1 or later. :/

<newPropSpec> ::= [propname]=<cident> propeq="=" [propval]=<propertyValue>
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

<oldPropSpec> ::= [propname]=<optionName> propeq="=" [optval]=<optionVal>
                ;
<optionName> ::= optname=<cident> ( optsep=":" subopt=( <cident> | <wholenumber> ) )?
               ;
<optionVal> ::= <identifier>
              | <stringLiteral>
              | <integer>
              | <float>
              ;
'''

def use_pre_3_0_0_syntax():
    # cassandra-1.1 support
    CqlRuleSet.append_rules('''
        <property> ::= <oldPropSpec> ;
    ''')
    CqlRuleSet.columnfamily_layout_map_options = \
            CqlRuleSet.old_columnfamily_layout_map_options
    CqlRuleSet.columnfamily_layout_options = \
            CqlRuleSet.old_columnfamily_layout_options

def use_post_3_0_0_syntax():
    CqlRuleSet.append_rules('''
        <property> ::= <newPropSpec> ;
    ''')
    CqlRuleSet.columnfamily_layout_map_options = \
            CqlRuleSet.new_columnfamily_layout_map_options
    CqlRuleSet.columnfamily_layout_options = \
            CqlRuleSet.new_columnfamily_layout_options
    CqlRuleSet.obsolete_cf_options += CqlRuleSet.new_obsolete_cf_options

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

completer_for('oldPropSpec', 'propeq')(prop_equals_completer)
completer_for('newPropSpec', 'propeq')(prop_equals_completer)

@completer_for('newPropSpec', 'propname')
def new_prop_name_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_new_prop_name_completer(ctxt, cass)
    else:
        return cf_new_prop_name_completer(ctxt, cass)

@completer_for('propertyValue', 'propsimpleval')
def new_prop_val_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_new_prop_val_completer(ctxt, cass)
    else:
        return cf_new_prop_val_completer(ctxt, cass)

@completer_for('propertyValue', 'propmapkey')
def new_prop_val_mapkey_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_new_prop_val_mapkey_completer(ctxt, cass)
    else:
        return cf_new_prop_val_mapkey_completer(ctxt, cass)

@completer_for('propertyValue', 'propmapval')
def new_prop_val_mapval_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_new_prop_val_mapval_completer(ctxt, cass)
    else:
        return cf_new_prop_val_mapval_completer(ctxt, cass)

@completer_for('propertyValue', 'ender')
def new_prop_val_mapender_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_new_prop_val_mapender_completer(ctxt, cass)
    else:
        return cf_new_prop_val_mapender_completer(ctxt, cass)

def ks_new_prop_name_completer(ctxt, cass):
    optsseen = ctxt.get_binding('propname', ())
    if 'replication' not in optsseen:
        return ['replication']
    return ["durable_writes"]

def ks_new_prop_val_completer(ctxt, cass):
    optname = ctxt.get_binding('propname')[-1]
    if optname == 'durable_writes':
        return ["'true'", "'false'"]
    if optname == 'replication':
        return ["{'class': '"]
    return ()

def ks_new_prop_val_mapkey_completer(ctxt, cass):
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

def ks_new_prop_val_mapval_completer(ctxt, cass):
    optname = ctxt.get_binding('propname')[-1]
    if optname != 'replication':
        return ()
    currentkey = dequote_value(ctxt.get_binding('propmapkey')[-1])
    if currentkey == 'class':
        return map(escape_value, CqlRuleSet.replication_strategies)
    return [Hint('<value>')]

def ks_new_prop_val_mapender_completer(ctxt, cass):
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

def cf_new_prop_name_completer(ctxt, cass):
    return [c[0] for c in (CqlRuleSet.columnfamily_layout_options +
                           CqlRuleSet.columnfamily_layout_map_options)]

def cf_new_prop_val_completer(ctxt, cass):
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
                    'gc_grace_seconds'):
        return [Hint('<integer>')]
    if this_opt == 'default_read_consistency':
        return [cl for cl in CqlRuleSet.consistency_levels if cl != 'ANY']
    if this_opt == 'default_write_consistency':
        return CqlRuleSet.consistency_levels
    return [Hint('<option_value>')]

def cf_new_prop_val_mapkey_completer(ctxt, cass):
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

def cf_new_prop_val_mapval_completer(ctxt, cass):
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

def cf_new_prop_val_mapender_completer(ctxt, cass):
    return [',', '}']

@completer_for('optionName', 'optname')
def old_prop_name_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_old_prop_name_completer(ctxt, cass)
    else:
        return cf_old_prop_name_completer(ctxt, cass)

@completer_for('oldPropSpec', 'optval')
def old_prop_val_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_old_prop_val_completer(ctxt, cass)
    else:
        return cf_old_prop_val_completer(ctxt, cass)

@completer_for('optionName', 'optsep')
def old_prop_separator_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_old_prop_separator_completer(ctxt, cass)
    else:
        return cf_old_prop_separator_completer(ctxt, cass)

@completer_for('optionName', 'subopt')
def old_prop_suboption_completer(ctxt, cass):
    if working_on_keyspace(ctxt):
        return ks_old_prop_suboption_completer(ctxt, cass)
    else:
        return cf_old_prop_suboption_completer(ctxt, cass)

def ks_old_prop_name_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('optname', ())
    try:
        stratopt = exist_opts.index('strategy_class')
    except ValueError:
        return ['strategy_class =']
    vals = ctxt.get_binding('optval')
    stratclass = dequote_value(vals[stratopt])
    if stratclass in CqlRuleSet.replication_factor_strategies:
        return ['strategy_options:replication_factor =']
    return [Hint('<strategy_option_name>')]

def ks_old_prop_val_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('optname', (None,))
    if exist_opts[-1] == 'strategy_class':
        return map(escape_value, CqlRuleSet.replication_strategies)
    return [Hint('<option_value>')]

def ks_old_prop_separator_completer(ctxt, cass):
    curopt = ctxt.get_binding('optname')[-1]
    if curopt == 'strategy_options':
        return [':']
    return ()

def ks_old_prop_suboption_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('optname')
    if exist_opts[-1] != 'strategy_options':
        return ()
    try:
        stratopt = exist_opts.index('strategy_class')
    except ValueError:
        return ()
    vals = ctxt.get_binding('optval')
    stratclass = dequote_value(vals[stratopt])
    if stratclass in CqlRuleSet.replication_factor_strategies:
        return ['replication_factor =']
    return [Hint('<dc_name>')]

def cf_old_prop_name_completer(ctxt, cass):
    return list(CqlRuleSet.columnfamily_layout_options) + \
           [c[0] + ':' for c in CqlRuleSet.columnfamily_layout_map_options]

def cf_old_prop_val_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('propname')
    this_opt = exist_opts[-1]
    if this_opt == 'compression_parameters:sstable_compression':
        return map(escape_value, CqlRuleSet.available_compression_classes)
    if this_opt == 'compaction_strategy_class':
        return map(escape_value, CqlRuleSet.available_compaction_classes)
    if any(this_opt == opt[0] for opt in CqlRuleSet.obsolete_cf_options):
        return ["'<obsolete_option>'"]
    if this_opt in ('comparator', 'default_validation'):
        return simple_cql_types
    if this_opt in ('read_repair_chance', 'bloom_filter_fp_chance'):
        return [Hint('<float_between_0_and_1>')]
    if this_opt in ('replicate_on_write', 'populate_io_cache_on_flush'):
        return [Hint('<yes_or_no>')]
    if this_opt in ('min_compaction_threshold', 'max_compaction_threshold', 'gc_grace_seconds'):
        return [Hint('<integer>')]
    return [Hint('<option_value>')]

def cf_old_prop_separator_completer(ctxt, cass):
    opt = ctxt.get_binding('optname')
    if any(opt == c[0] for c in CqlRuleSet.columnfamily_layout_map_options):
        return [':']
    return ()

def cf_old_prop_suboption_completer(ctxt, cass):
    opt = ctxt.get_binding('optname')
    if opt == 'compaction_strategy_options':
        # try to determine the strategy class in use
        prevopts = ctxt.get_binding('propname', ())
        prevvals = ctxt.get_binding('optval', ())
        for prevopt, prevval in zip(prevopts, prevvals):
            if prevopt == 'compaction_strategy_class':
                csc = dequote_value(prevval)
                break
        else:
            layout = get_cf_layout(ctxt, cass)
            try:
                csc = layout.compaction_strategy
            except Exception:
                csc = ''
        csc = csc.split('.')[-1]
        if csc == 'SizeTieredCompactionStrategy':
            return ['min_sstable_size']
        elif csc == 'LeveledCompactionStrategy':
            return ['sstable_size_in_mb']
    for optname, _, subopts in CqlRuleSet.columnfamily_layout_map_options:
        if opt == optname:
            return subopts
    return ()

@completer_for('tokenDefinition', 'token')
def token_word_completer(ctxt, cass):
    return ['TOKEN(']

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
<selectClause> ::= <selector> ("," <selector>)*
                 | "*"
                 | "COUNT" "(" star=( "*" | "1" ) ")"
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
    order_by_candidates = layout.column_aliases[:]
    if len(order_by_candidates) > len(prev_order_cols):
        return [maybe_escape_name(order_by_candidates[len(prev_order_cols)])]
    return [Hint('No more orderable columns here.')]

@completer_for('relation', 'token')
def relation_token_word_completer(ctxt, cass):
    return ['TOKEN(']

@completer_for('relation', 'rel_tokname')
def relation_token_subject_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    return [layout.partition_key_components[0]]

@completer_for('relation', 'rel_lhs')
def select_relation_lhs_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    filterable = set((layout.partition_key_components[0], layout.column_aliases[0]))
    already_filtered_on = map(dequote_name, ctxt.get_binding('rel_lhs'))
    for num in range(1, len(layout.partition_key_components)):
        if layout.partition_key_components[num - 1] in already_filtered_on:
            filterable.add(layout.partition_key_components[num])
        else:
            break
    for num in range(1, len(layout.column_aliases)):
        if layout.column_aliases[num - 1] in already_filtered_on:
            filterable.add(layout.column_aliases[num])
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
    keycols = layout.primary_key_components
    for k in keycols:
        if k not in colnames:
            return [maybe_escape_name(k)]
    normalcols = set([c.name for c in layout.columns]) - set(keycols) - colnames
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
    normals = set([cm.name for cm in layout.columns]) \
            - set(layout.primary_key_components)
    return map(maybe_escape_name, normals)

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
                        ( "USING" [delopt]=<deleteOption> ( "AND" [delopt]=<deleteOption> )* )?
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
        cols = set([c.name for c in layout.columns
                    if c not in layout.primary_key_components])
        return map(maybe_escape_name, cols)

syntax_rules += r'''
<batchStatement> ::= "BEGIN" ( "UNLOGGED" | "COUNTER" )? "BATCH"
                        ( "USING" [batchopt]=<usingOption>
                                  ( "AND" [batchopt]=<usingOption> )* )?
                        [batchstmt]=<batchStatementMember> ";"
                            ( [batchstmt]=<batchStatementMember> ";" )*
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
<createKeyspaceStatement> ::= "CREATE" wat=( "KEYSPACE" | "SCHEMA" ) ksname=<cfOrKsName>
                                "WITH" <property> ( "AND" <property> )*
                            ;
'''

@completer_for('createKeyspaceStatement', 'wat')
def create_ks_wat_completer(ctxt, cass):
    # would prefer to get rid of the "schema" nomenclature in cql3
    if ctxt.get_binding('partial', '') == '':
        return ['KEYSPACE']
    return ['KEYSPACE', 'SCHEMA']

@completer_for('oldPropSpec', 'optname')
def create_ks_opt_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('optname', ())
    try:
        stratopt = exist_opts.index('strategy_class')
    except ValueError:
        return ['strategy_class =']
    vals = ctxt.get_binding('optval')
    stratclass = dequote_value(vals[stratopt])
    if stratclass in CqlRuleSet.replication_factor_strategies:
        return ['strategy_options:replication_factor =']
    return [Hint('<strategy_option_name>')]

@completer_for('oldPropSpec', 'optval')
def create_ks_optval_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('optname', (None,))
    if exist_opts[-1] == 'strategy_class':
        return map(escape_value, CqlRuleSet.replication_strategies)
    return [Hint('<option_value>')]

@completer_for('newPropSpec', 'propname')
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
<createColumnFamilyStatement> ::= "CREATE" wat=( "COLUMNFAMILY" | "TABLE" )
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
<createIndexStatement> ::= "CREATE" "INDEX" indexname=<identifier>? "ON"
                               cf=<columnFamilyName> "(" col=<cident> ")"
                         ;
'''

explain_completion('createIndexStatement', 'indexname', '<new_index_name>')

@completer_for('createIndexStatement', 'col')
def create_index_col_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    colnames = [cd.name for cd in layout.columns if cd.index_name is None]
    return map(maybe_escape_name, colnames)

syntax_rules += r'''
<dropKeyspaceStatement> ::= "DROP" "KEYSPACE" ksname=<nonSystemKeyspaceName>
                          ;

<dropColumnFamilyStatement> ::= "DROP" ( "COLUMNFAMILY" | "TABLE" ) cf=<columnFamilyName>
                              ;

<dropIndexStatement> ::= "DROP" "INDEX" indexname=<identifier>
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
                                 "WITH" <newPropSpec> ( "AND" <newPropSpec> )*
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



# current assumption is that all valid CQL tables match the rules in the
# following table.
#
#                        non-empty     non-empty      multiple    composite
#                       value_alias  column_aliases  key_aliases  comparator
# ---------------------+----------------------------------------------------
# A: single-column PK, |
# compact storage      |   either         no            no           no
# ---------------------+----------------------------------------------------
# B: single-column PK, |
# dynamic storage      |    no            no            no           yes
# ---------------------+----------------------------------------------------
# C: compound PK,      |
# plain part. key,     |    yes[1]        yes           no          either
# compact storage      |
# ---------------------+----------------------------------------------------
# D: compound PK,      |
# plain part. key,     |    no            yes           no           yes
# dynamic storage      |
# ---------------------+----------------------------------------------------
# E: compound PK,      |
# multipart part. key, |
# all key components   |   either         no            yes          no
# go in part. key,     |
# compact storage      |
# ---------------------+----------------------------------------------------
# F: compound PK,      |
# multipart part. key, |
# all key components   |    no            no            yes          yes
# go in part. key,     |
# dynamic storage      |
# ---------------------+----------------------------------------------------
# G: compound PK,      |
# multipart part. key, |
# some key components  |    yes[1]        yes           yes         either
# not in part. key,    |
# compact storage      |
# ---------------------+----------------------------------------------------
# H: compound PK,      |
# multipart part. key, |
# some key components  |    no            yes           yes          yes
# not in part. key,    |
# dynamic storage      |
# ---------------------+----------------------------------------------------
#
# [1] the value_alias may be blank, but not null.

# for compact storage:
#
# if no column aliases:
#     comparator will be UTF8Type
# elif one column alias:
#     comparator will be type of that column
# else:
#     comparator will be composite of types of all column_aliases
#
# for dynamic storage:
#
# comparator is composite of types of column_aliases, followed by UTF8Type,
# followed by one CTCT if there are collections.

class CqlColumnDef:
    index_name = None

    def __init__(self, name, cqltype):
        self.name = name
        self.cqltype = cqltype
        assert name is not None

    @classmethod
    def from_layout(cls, layout):
        try:
            colname = layout[u'column_name']
        except KeyError:
            colname = layout[u'column']
        c = cls(colname, lookup_casstype(layout[u'validator']))
        c.index_name = layout[u'index_name']
        return c

    def __str__(self):
        indexstr = ' (index %s)' % self.index_name if self.index_name is not None else ''
        return '<CqlColumnDef %r %r%s>' % (self.name, self.cqltype, indexstr)
    __repr__ = __str__

class CqlTableDef:
    json_attrs = ('column_aliases', 'compaction_strategy_options', 'compression_parameters',
                  'key_aliases')
    colname_type = UTF8Type
    column_class = CqlColumnDef

    """True if this CF has compact storage (isn't a CQL3 table)"""
    compact_storage = False

    """Names of all columns which are part of the primary key, whether or not
       they are grouped into the partition key"""
    primary_key_components = ()

    """Names of all columns which are grouped into the partition key"""
    partition_key_components = ()

    """Names of all columns which are part of the primary key, but not grouped
       into the partition key"""
    column_aliases = ()

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
        try:
            cfname = layout[u'columnfamily_name']
            ksname = layout[u'keyspace_name']
        except KeyError:
            cfname = layout[u'columnfamily']
            ksname = layout[u'keyspace']
        cf = cls(name=cfname)
        for attr, val in layout.items():
            setattr(cf, attr.encode('ascii'), val)
        cf.keyspace = ksname
        for attr in cls.json_attrs:
            try:
                val = getattr(cf, attr)
                # cfs created in 1.1 may not have key_aliases defined
                if attr == 'key_aliases' and val is None:
                    val = '[]'
                setattr(cf, attr, json.loads(val))
            except AttributeError:
                pass
        cf.partition_key_validator = lookup_casstype(cf.key_validator)
        cf.comparator = lookup_casstype(cf.comparator)
        cf.default_validator = lookup_casstype(cf.default_validator)
        cf.coldefs = coldefs
        cf.compact_storage = cf.is_compact_storage()
        cf.key_aliases = cf.get_key_aliases()
        cf.partition_key_components = cf.key_aliases
        cf.column_aliases = cf.get_column_aliases()
        cf.primary_key_components = cf.key_aliases + list(cf.column_aliases)
        cf.columns = cf.get_columns()
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
        if len(self.column_aliases) == len(self.comparator.subtypes) - 1:
            if self.comparator.subtypes[-1] is UTF8Type:
                return False
        return True

    def get_key_aliases(self):
        if not issubclass(self.partition_key_validator, CompositeType):
            return self.key_aliases or (self.key_alias and [self.key_alias]) or [u'key']
        expected = len(self.partition_key_validator.subtypes)
        # key, key2, key3, ..., keyN
        aliases = [u'key'] + [ u'key' + str(i) for i in range(2, expected + 1) ]
        # append the missing (non-renamed) aliases (if any)
        return self.key_aliases + aliases[len(self.key_aliases):]

    def get_column_aliases(self):
        # CQL3 table
        if not self.compact_storage:
            return self.column_aliases
        if not issubclass(self.comparator, CompositeType):
            # static cf
            if self.coldefs:
                return []
            else:
                return self.column_aliases or [u'column1']
        expected = len(self.comparator.subtypes)
        # column1, column2, column3, ..., columnN
        aliases = [ u'column' + str(i) for i in range(1, expected + 1) ]
        # append the missing (non-renamed) aliases (if any)
        return self.column_aliases + aliases[len(self.column_aliases):]

    def get_columns(self):
        if self.compact_storage:
            return self.get_columns_compact()
        else:
            return self.get_columns_cql3()

    # dense composite or dynamic cf or static cf (technically not compact).
    def get_columns_compact(self):
        if issubclass(self.partition_key_validator, CompositeType):
            partkey_types = self.partition_key_validator.subtypes
        else:
            partkey_types = [self.partition_key_validator]
        partkey_cols = map(self.column_class, self.partition_key_components, partkey_types)

        if len(self.column_aliases) == 0:
            if self.comparator is not UTF8Type:
                warn(UnexpectedTableStructure("Compact storage CF %s has no column aliases,"
                                              " but comparator is not UTF8Type." % (self.name,)))
            colalias_types = []
        elif issubclass(self.comparator, CompositeType):
            colalias_types = self.comparator.subtypes
        else:
            colalias_types = [self.comparator]
        if len(colalias_types) != len(self.column_aliases):
            warn(UnexpectedTableStructure("Compact storage CF comparator-types %r is not"
                                          " the same length as its column_aliases %r"
                                          % (colalias_types, self.column_aliases)))
        colalias_cols = map(self.column_class, self.column_aliases, colalias_types)

        if self.value_alias is not None:
            if self.coldefs:
                warn(UnexpectedTableStructure("Compact storage CF has both a value_alias"
                                              " (%r) and entries in system.schema_columns"
                                              % (self.value_alias,)))
            if self.value_alias == '':
                value_cols = []
            else:
                value_cols = [self.column_class(self.value_alias, self.default_validator)]
        elif self.value_alias is None and not self.coldefs:
            value_cols = [self.column_class("value", self.default_validator)]
        else:
            value_cols = map(self.column_class.from_layout, self.coldefs)
            value_cols.sort(key=lambda c: c.name)

        return partkey_cols + colalias_cols + value_cols

    # sparse composite (CQL3 table).
    def get_columns_cql3(self):
        if issubclass(self.partition_key_validator, CompositeType):
            partkey_types = self.partition_key_validator.subtypes
        else:
            partkey_types = [self.partition_key_validator]
        partkey_cols = map(self.column_class, self.partition_key_components, partkey_types)

        for subtype in self.comparator.subtypes[:-1]:
            if issubclass(subtype, ColumnToCollectionType):
                warn(UnexpectedTableStructure("ColumnToCollectionType found, but not in "
                                              "last position inside composite comparator"))
        coltypes = list(self.comparator.subtypes)
        if issubclass(coltypes[-1], ColumnToCollectionType):
            # all this information should be available in schema_columns
            coltypes.pop(-1)
        if len(coltypes) != len(self.column_aliases) + 1 or coltypes[-1] is not UTF8Type:
            warn(UnexpectedTableStructure("CQL3 CF does not have UTF8Type"
                                          " added to comparator"))
        colalias_cols = map(self.column_class, self.column_aliases, coltypes[:-1])

        if self.value_alias is not None:
            warn(UnexpectedTableStructure("CQL3 CF has a value_alias (%r)"
                                          % (self.value_alias,)))
        value_cols = map(self.column_class.from_layout, self.coldefs)
        value_cols.sort(key=lambda c: c.name)

        return partkey_cols + colalias_cols + value_cols

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
