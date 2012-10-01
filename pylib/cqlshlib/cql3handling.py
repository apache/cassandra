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
                          ColumnToCollectionType)

try:
    import json
except ImportError:
    import simplejson as json

class UnexpectedTableStructure(UserWarning):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return 'Unexpected table structure; may not translate correctly to CQL. ' + self.msg

class Cql3ParsingRuleSet(CqlParsingRuleSet):
    keywords = set((
        'select', 'from', 'where', 'and', 'key', 'insert', 'update', 'with',
        'limit', 'using', 'consistency', 'one', 'quorum', 'all', 'any',
        'local_quorum', 'each_quorum', 'two', 'three', 'use', 'count', 'set',
        'begin', 'apply', 'batch', 'truncate', 'delete', 'in', 'create',
        'keyspace', 'schema', 'columnfamily', 'table', 'index', 'on', 'drop',
        'primary', 'into', 'values', 'timestamp', 'ttl', 'alter', 'add', 'type',
        'compact', 'storage', 'order', 'by', 'asc', 'desc', 'clustering', 'token'
    ))

    columnfamily_options = (
        # (CQL option name, Thrift option name (or None if same))
        ('comment', None),
        ('comparator', 'comparator_type'),
        ('read_repair_chance', None),
        ('gc_grace_seconds', None),
        ('default_validation', 'default_validation_class'),
        ('replicate_on_write', None),
        ('compaction_strategy_class', 'compaction_strategy'),
    )

    columnfamily_layout_options = (
        'comment',
        'bloom_filter_fp_chance',
        'caching',
        'read_repair_chance',
        # 'local_read_repair_chance',   -- not yet a valid cql option
        'gc_grace_seconds',
        'replicate_on_write',
        'compaction_strategy_class',
    )

    columnfamily_layout_map_options = (
        ('compaction_strategy_options',
            ()),
        ('compression_parameters',
            ('sstable_compression', 'chunk_length_kb', 'crc_check_chance')),
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
        if name[0] == '"':
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
        if s is None or s.lower() in cls.keywords:
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
<integer> ::=       /-?[0-9]+/ ;
<uuid> ::=          /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/ ;
<identifier> ::=    /[a-z][a-z0-9_]*/ ;
<colon> ::=         ":" ;
<star> ::=          "*" ;
<endtoken> ::=      ";" ;
<op> ::=            /[-+=,().]/ ;
<cmp> ::=           /[<>]=?/ ;
<brackets> ::=      /[][{}]/ ;

<unclosedString>  ::= /'([^']|'')*/ ;
<unclosedName>    ::= /"([^"]|"")*/ ;
<unclosedComment> ::= /[/][*][^\n]*$/ ;

<term> ::= <stringLiteral>
         | <integer>
         | <float>
         | <uuid>
         ;
<extendedTerm> ::= token="TOKEN" "(" <term> ")"
                 | <term>
                 ;
<cident> ::= <quotedName>
           | <identifier>
           | <unreservedKeyword>
           ;
<colname> ::= <cident> ;   # just an alias

<statementBody> ::= <useStatement>
                  | <selectStatement>
                  | <dataChangeStatement>
                  | <schemaChangeStatement>
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
                          ;

<consistencylevel> ::= cl=( <K_ONE>
                          | <K_QUORUM>
                          | <K_ALL>
                          | <K_ANY>
                          | <K_LOCAL_QUORUM>
                          | <K_EACH_QUORUM>
                          | <K_TWO>
                          | <K_THREE> )
                          ;

<storageType> ::= typename=( <identifier> | <stringLiteral> ) ;

<columnFamilyName> ::= ( ksname=<cfOrKsName> "." )? cfname=<cfOrKsName> ;

<keyspaceName> ::= ksname=<cfOrKsName> ;

<cfOrKsName> ::= <identifier>
               | <quotedName>
               | <unreservedKeyword>;

<unreservedKeyword> ::= nocomplete=
                        ( <K_KEY>
                        | <K_CONSISTENCY>
                        | <K_CLUSTERING>
                        # | <K_COUNT>  -- to get count(*) completion, treat count as reserved
                        | <K_TTL>
                        | <K_COMPACT>
                        | <K_STORAGE>
                        | <K_TYPE>
                        | <K_VALUES>
                        | <consistencylevel> )
                      ;
'''

@completer_for('consistencylevel', 'cl')
def consistencylevel_cl_completer(ctxt, cass):
    return CqlRuleSet.consistency_levels

@completer_for('extendedTerm', 'token')
def token_word_completer(ctxt, cass):
    return ['TOKEN(']

@completer_for('storageType', 'typename')
def storagetype_completer(ctxt, cass):
    return cql_types

@completer_for('keyspaceName', 'ksname')
def ks_name_completer(ctxt, cass):
    return map(maybe_escape_name, cass.get_keyspace_names())

@completer_for('columnFamilyName', 'ksname')
def cf_ks_name_completer(ctxt, cass):
    return [maybe_escape_name(ks) + '.' for ks in cass.get_keyspace_names()]

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
    ks = dequote_name(ctxt.get_binding('ksname', None))
    cf = dequote_name(ctxt.get_binding('cfname'))
    return cass.get_columnfamily_layout(ks, cf)

syntax_rules += r'''
<useStatement> ::= "USE" <keyspaceName>
                 ;
<selectStatement> ::= "SELECT" <selectClause>
                        "FROM" cf=<columnFamilyName>
                          ("USING" "CONSISTENCY" selcl=<consistencylevel>)?
                          ("WHERE" <whereClause>)?
                          ("ORDER" "BY" <orderByClause> ( "," <orderByClause> )* )?
                          ("LIMIT" <wholenumber>)?
                    ;
<whereClause> ::= <relation> ("AND" <relation>)*
                ;
<relation> ::= [rel_lhs]=<cident> ("=" | "<" | ">" | "<=" | ">=") <term>
             | token="TOKEN" "(" rel_tokname=<cident> ")" ("=" | "<" | ">" | "<=" | ">=") <extendedTerm>
             | [rel_lhs]=<cident> "IN" "(" <term> ( "," <term> )* ")"
             ;
<selectClause> ::= colname=<cident> ("," colname=<cident>)*
                 | "*"
                 | "COUNT" "(" star=( "*" | "1" ) ")"
                 ;
<orderByClause> ::= [ordercol]=<cident> ( "ASC" | "DESC" )?
                  ;
'''

@completer_for('selectStatement', 'selcl')
def select_statement_consistencylevel(ctxt, cass):
    return [cl for cl in CqlRuleSet.consistency_levels if cl != 'ANY']

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

explain_completion('selectClause', 'colname')

syntax_rules += r'''
<insertStatement> ::= "INSERT" "INTO" cf=<columnFamilyName>
                               "(" keyname=<cident> ","
                                   [colname]=<cident> ( "," [colname]=<cident> )* ")"
                      "VALUES" "(" <term> "," <term> ( "," <term> )* ")"
                      ( "USING" [insertopt]=<usingOption>
                                ( "AND" [insertopt]=<usingOption> )* )?
                    ;
<usingOption> ::= "CONSISTENCY" <consistencylevel>
                | "TIMESTAMP" <wholenumber>
                | "TTL" <wholenumber>
                ;
'''

@completer_for('insertStatement', 'keyname')
def insert_keyname_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    return [layout.primary_key_components[0]]

explain_completion('insertStatement', 'colname')

@completer_for('insertStatement', 'insertopt')
def insert_option_completer(ctxt, cass):
    opts = set('CONSISTENCY TIMESTAMP TTL'.split())
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
<assignment> ::= updatecol=<cident> "=" update_rhs=<cident>
                                         ( counterop=( "+" | "-" ) <wholenumber> )?
               ;
'''

@completer_for('updateStatement', 'updateopt')
def insert_option_completer(ctxt, cass):
    opts = set('CONSISTENCY TIMESTAMP TTL'.split())
    for opt in ctxt.get_binding('updateopt', ()):
        opts.discard(opt.split()[0])
    return opts

@completer_for('assignment', 'updatecol')
def update_col_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    return map(maybe_escape_name, [cm.name for cm in layout.columns])

@completer_for('assignment', 'update_rhs')
def update_countername_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    return [maybe_escape_name(curcol)] if layout.is_counter_col(curcol) else [Hint('<term>')]

@completer_for('assignment', 'counterop')
def update_counterop_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    return ['+', '-'] if layout.is_counter_col(curcol) else []

syntax_rules += r'''
<deleteStatement> ::= "DELETE" ( [delcol]=<cident> ( "," [delcol]=<cident> )* )?
                        "FROM" cf=<columnFamilyName>
                        ( "USING" [delopt]=<deleteOption> ( "AND" [delopt]=<deleteOption> )* )?
                        "WHERE" <whereClause>
                    ;
<deleteOption> ::= "CONSISTENCY" <consistencylevel>
                 | "TIMESTAMP" <wholenumber>
                 ;
'''

@completer_for('deleteStatement', 'delopt')
def delete_opt_completer(ctxt, cass):
    opts = set('CONSISTENCY TIMESTAMP'.split())
    for opt in ctxt.get_binding('delopt', ()):
        opts.discard(opt.split()[0])
    return opts

explain_completion('deleteStatement', 'delcol', '<column_to_delete>')

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
    opts = set('CONSISTENCY TIMESTAMP'.split())
    for opt in ctxt.get_binding('batchopt', ()):
        opts.discard(opt.split()[0])
    return opts

syntax_rules += r'''
<truncateStatement> ::= "TRUNCATE" cf=<columnFamilyName>
                      ;
'''

syntax_rules += r'''
<createKeyspaceStatement> ::= "CREATE" "KEYSPACE" ksname=<cfOrKsName>
                                 "WITH" [optname]=<optionName> "=" [optval]=<optionVal>
                                 ( "AND" [optname]=<optionName> "=" [optval]=<optionVal> )*
                            ;
<optionName> ::= <identifier> ( ":" ( <identifier> | <wholenumber> ) )?
               ;
<optionVal> ::= <stringLiteral>
              | <identifier>
              | <integer>
              ;
'''

explain_completion('createKeyspaceStatement', 'ksname', '<new_keyspace_name>')

@completer_for('createKeyspaceStatement', 'optname')
def create_ks_opt_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('optname', ())
    try:
        stratopt = exist_opts.index('strategy_class')
    except ValueError:
        return ['strategy_class =']
    vals = ctxt.get_binding('optval')
    stratclass = dequote_value(vals[stratopt])
    if stratclass in ('SimpleStrategy',
                      'org.apache.cassandra.locator.SimpleStrategy',
                      'OldNetworkTopologyStrategy',
                      'org.apache.cassandra.locator.OldNetworkTopologyStrategy'):
        return ['strategy_options:replication_factor =']
    return [Hint('<strategy_option_name>')]

@completer_for('createKeyspaceStatement', 'optval')
def create_ks_optval_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('optname', (None,))
    if exist_opts[-1] == 'strategy_class':
        return map(escape_value, CqlRuleSet.replication_strategies)
    return [Hint('<option_value>')]

syntax_rules += r'''
<createColumnFamilyStatement> ::= "CREATE" ( "COLUMNFAMILY" | "TABLE" )
                                    ( ks=<keyspaceName> "." )? cf=<cfOrKsName>
                                    "(" ( <singleKeyCfSpec> | <compositeKeyCfSpec> ) ")"
                                   ( "WITH" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal>
                                     ( "AND" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal> )* )?
                                ;

<singleKeyCfSpec> ::= keyalias=<cident> <storageType> "PRIMARY" "KEY"
                      ( "," colname=<cident> <storageType> )*
                    ;

<compositeKeyCfSpec> ::= [newcolname]=<cident> <storageType>
                         "," [newcolname]=<cident> <storageType>
                         ( "," [newcolname]=<cident> <storageType> )*
                         "," "PRIMARY" k="KEY" p="(" [pkey]=<cident>
                                                     ( c="," [pkey]=<cident> )* ")"
                       ;

<cfOptionName> ::= cfoptname=<identifier> ( cfoptsep=":" cfsubopt=( <identifier> | <wholenumber> ) )?
                 ;

<cfOptionVal> ::= <identifier>
                | <stringLiteral>
                | <integer>
                | <float>
                ;
'''

explain_completion('createColumnFamilyStatement', 'cf', '<new_table_name>')
explain_completion('singleKeyCfSpec', 'keyalias', '<new_key_name>')
explain_completion('singleKeyCfSpec', 'colname', '<new_column_name>')
explain_completion('compositeKeyCfSpec', 'newcolname', '<new_column_name>')

@completer_for('compositeKeyCfSpec', 'pkey')
def create_cf_composite_key_declaration(ctxt, cass):
    cols_declared = ctxt.get_binding('newcolname')
    pieces_already = ctxt.get_binding('pkey', ())
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

@completer_for('cfOptionName', 'cfoptname')
def create_cf_option_completer(ctxt, cass):
    return list(CqlRuleSet.columnfamily_layout_options) + \
           [c[0] + ':' for c in CqlRuleSet.columnfamily_map_options]

@completer_for('cfOptionName', 'cfoptsep')
def create_cf_suboption_separator(ctxt, cass):
    opt = ctxt.get_binding('cfoptname')
    if any(opt == c[0] for c in CqlRuleSet.columnfamily_map_options):
        return [':']
    return ()

@completer_for('cfOptionName', 'cfsubopt')
def create_cf_suboption_completer(ctxt, cass):
    opt = ctxt.get_binding('cfoptname')
    if opt == 'compaction_strategy_options':
        # try to determine the strategy class in use
        prevopts = ctxt.get_binding('cfopt', ())
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
    for optname, _, subopts in CqlRuleSet.columnfamily_map_options:
        if opt == optname:
            return subopts
    return ()

def create_cf_option_val_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('cfopt')
    this_opt = exist_opts[-1]
    if this_opt == 'compression_parameters:sstable_compression':
        return map(escape_value, CqlRuleSet.available_compression_classes)
    if this_opt == 'compaction_strategy_class':
        return map(escape_value, CqlRuleSet.available_compaction_classes)
    if any(this_opt == opt[0] for opt in CqlRuleSet.obsolete_cf_options):
        return ["'<obsolete_option>'"]
    if this_opt in ('comparator', 'default_validation'):
        return cql_types
    if this_opt in ('read_repair_chance', 'bloom_filter_fp_chance'):
        return [Hint('<float_between_0_and_1>')]
    if this_opt == 'replicate_on_write':
        return [Hint('<yes_or_no>')]
    if this_opt in ('min_compaction_threshold', 'max_compaction_threshold', 'gc_grace_seconds'):
        return [Hint('<integer>')]
    return [Hint('<option_value>')]

completer_for('createColumnFamilyStatement', 'optval') \
    (create_cf_option_val_completer)

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
<dropKeyspaceStatement> ::= "DROP" "KEYSPACE" ksname=<keyspaceName>
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
                      | "WITH" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal>
                        ( "AND" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal> )*
                      ;
'''

@completer_for('alterInstructions', 'existcol')
def alter_table_col_completer(ctxt, cass):
    layout = get_cf_layout(ctxt, cass)
    cols = [md.name for md in layout.columns]
    return map(maybe_escape_name, cols)

explain_completion('alterInstructions', 'newcol', '<new_column_name>')

completer_for('alterInstructions', 'optval') \
    (create_cf_option_val_completer)

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

    """True if this CF has compact storage"""
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
        except KeyError:
            cfname = layout[u'columnfamily']
        cf = cls(name=cfname)
        for attr, val in layout.items():
            setattr(cf, attr.encode('ascii'), val)
        for attr in cls.json_attrs:
            try:
                setattr(cf, attr, json.loads(getattr(cf, attr)))
            except AttributeError:
                pass
        if not cf.key_aliases:
            if cf.key_alias:
                cf.key_aliases = [cf.key_alias.decode('ascii')]
            else:
                cf.key_aliases = [u'KEY']
        cf.partition_key_components = cf.key_aliases
        cf.primary_key_components = cf.key_aliases + list(cf.column_aliases)
        cf.partition_key_validator = lookup_casstype(cf.key_validator)
        cf.default_validator = lookup_casstype(cf.default_validator)
        cf.comparator = lookup_casstype(cf.comparator)
        cf.coldefs = coldefs
        cf.parse_types()
        return cf

    def is_compact_storage(self):
        if not issubclass(self.comparator, CompositeType):
            return True
        for subtype in self.comparator.subtypes:
            if issubclass(subtype, ColumnToCollectionType):
                return False
        return bool(len(self.comparator.subtypes) == len(self.column_aliases))

    def parse_types(self):
        self.compact_storage = self.is_compact_storage()
        if self.compact_storage:
            self.columns = self.parse_types_compact()
        else:
            self.columns = self.parse_types_dynamic()

    def parse_types_compact(self):
        if issubclass(self.partition_key_validator, CompositeType):
            partkey_types = self.partition_key_validator.subtypes
        else:
            partkey_types = [self.partition_key_validator]
        if len(partkey_types) != len(self.key_aliases):
            warn(UnexpectedTableStructure("Compact storage CF key-validator-types %r is not"
                                          " the same length as its key_aliases %r"
                                          % (partkey_types, self.key_aliases)))
        if len(self.column_aliases) == 0:
            if self.comparator is not UTF8Type:
                warn(UnexpectedTableStructure("Compact storage CF %s has no column aliases,"
                                              " but comparator is not UTF8Type."))
            colalias_types = []
        elif issubclass(self.comparator, CompositeType):
            colalias_types = self.comparator.subtypes
        else:
            colalias_types = [self.comparator]
        if len(colalias_types) != len(self.column_aliases):
            warn(UnexpectedTableStructure("Compact storage CF comparator-types %r is not"
                                          " the same length as its column_aliases %r"
                                          % (colalias_types, self.column_aliases)))

        partkey_cols = map(self.column_class, self.partition_key_components, partkey_types)
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
        else:
            value_cols = map(self.column_class.from_layout, self.coldefs)
            value_cols.sort(key=lambda c: c.name)

        return partkey_cols + colalias_cols + value_cols

    def parse_types_dynamic(self):
        if not issubclass(self.comparator, CompositeType):
            warn(UnexpectedTableStructure("Dynamic storage CF comparator is %r, not"
                                          " composite" % (self.comparator,)))
        if issubclass(self.partition_key_validator, CompositeType):
            partkey_types = self.partition_key_validator.subtypes
        else:
            partkey_types = [self.partition_key_validator]
        if len(partkey_types) != len(self.key_aliases):
            warn(UnexpectedTableStructure("Dynamic storage CF key-validator-types %r "
                                          "is not the same length as its key_aliases %r"
                                          % (partkey_types, self.key_aliases)))
        for subtype in self.comparator.subtypes[:-1]:
            if issubclass(subtype, ColumnToCollectionType):
                warn(UnexpectedTableStructure("ColumnToCollectionType found, but not in "
                                              "last position inside composite comparator"))
        coltypes = list(self.comparator.subtypes)
        if issubclass(coltypes[-1], ColumnToCollectionType):
            # all this information should be available in schema_columns
            coltypes.pop(-1)
        if len(coltypes) != len(self.column_aliases) + 1 or coltypes[-1] is not UTF8Type:
            warn(UnexpectedTableStructure("Dynamic storage CF does not have UTF8Type"
                                          " added to comparator"))
        colalias_types = coltypes[:-1]
        partkey_cols = map(self.column_class, self.partition_key_components, partkey_types)
        colalias_cols = map(self.column_class, self.column_aliases, colalias_types)

        if self.value_alias is not None:
            warn(UnexpectedTableStructure("Dynamic storage CF has a value_alias (%r)"
                                          % (self.value_alias,)))
        value_cols = map(self.column_class.from_layout, self.coldefs)
        value_cols.sort(key=lambda c: c.name)

        return partkey_cols + colalias_cols + value_cols

    def is_counter_col(self, colname):
        try:
            return bool(self.get_column(colname).cqltype == 'counter')
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
