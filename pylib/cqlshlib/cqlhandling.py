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

# code for dealing with CQL's syntax, rules, interpretation
# i.e., stuff that's not necessarily cqlsh-specific

import re
from . import pylexotron
from itertools import izip

Hint = pylexotron.Hint

columnfamily_options = (
    # (CQL option name, Thrift option name (or None if same))
    ('comment', None),
    ('comparator', 'comparator_type'),
    ('row_cache_provider', None),
    ('key_cache_size', None),
    ('row_cache_size', None),
    ('read_repair_chance', None),
    ('gc_grace_seconds', None),
    ('default_validation', 'default_validation_class'),
    ('min_compaction_threshold', None),
    ('max_compaction_threshold', None),
    ('row_cache_save_period_in_seconds', None),
    ('key_cache_save_period_in_seconds', None),
    ('replicate_on_write', None)
)

cql_type_to_apache_class = {
    'ascii': 'AsciiType',
    'bigint': 'LongType',
    'blob': 'BytesType',
    'boolean': 'BooleanType',
    'counter': 'CounterColumnType',
    'decimal': 'DecimalType',
    'double': 'DoubleType',
    'float': 'FloatType',
    'int': 'Int32Type',
    'text': 'UTF8Type',
    'timestamp': 'DateType',
    'uuid': 'UUIDType',
    'varchar': 'UTF8Type',
    'varint': 'IntegerType'
}

apache_class_to_cql_type = dict((v,k) for (k,v) in cql_type_to_apache_class.items())

cql_types = sorted(cql_type_to_apache_class.keys())

def find_validator_class(cqlname):
    return cql_type_to_apache_class[cqlname]

replication_strategies = (
    'SimpleStrategy',
    'OldNetworkTopologyStrategy',
    'NetworkTopologyStrategy'
)

consistency_levels = (
    'ANY',
    'ONE',
    'QUORUM',
    'ALL',
    'LOCAL_QUORUM',
    'EACH_QUORUM'
)

# if a term matches this, it shouldn't need to be quoted to be valid cql
valid_cql_word_re = re.compile(r"^(?:[a-z][a-z0-9_]*|-?[0-9][0-9.]*)$", re.I)

def is_valid_cql_word(s):
    return valid_cql_word_re.match(s) is not None

def tokenize_cql(cql_text):
    return CqlLexotron.scan(cql_text)[0]

def cql_detokenize(toklist):
    return ' '.join([t[1] for t in toklist])

# note: commands_end_with_newline may be extended by an importing module.
commands_end_with_newline = set()

def token_dequote(tok):
    if tok[0] == 'stringLiteral':
        # strip quotes
        return tok[1][1:-1].replace("''", "'")
    if tok[0] == 'unclosedString':
        # strip one quote
        return tok[1][1:].replace("''", "'")
    if tok[0] == 'unclosedComment':
        return ''
    return tok[1]

def cql_dequote(cqlword):
    cqlword = cqlword.strip()
    if cqlword == '':
        return cqlword
    if cqlword[0] == "'":
        cqlword = cqlword[1:-1].replace("''", "'")
    return cqlword

def token_is_word(tok):
    return tok[0] == 'identifier'

def cql_escape(value):
    if value is None:
        return 'NULL' # this totally won't work
    if isinstance(value, float):
        return '%f' % value
    if isinstance(value, int):
        return str(value)
    return "'%s'" % value.replace("'", "''")

def maybe_cql_escape(value):
    if is_valid_cql_word(value):
        return value
    return cql_escape(value)

def cql_typename(classname):
    fq_classname = 'org.apache.cassandra.db.marshal.'
    if classname.startswith(fq_classname):
        classname = classname[len(fq_classname):]
    try:
        return apache_class_to_cql_type[classname]
    except KeyError:
        return cql_escape(classname)

special_completers = []

def completer_for(rulename, symname):
    def registrator(f):
        def completerwrapper(ctxt):
            cass = ctxt.get_binding('cassandra_conn', None)
            if cass is None:
                return ()
            return f(ctxt, cass)
        completerwrapper.func_name = 'completerwrapper_on_' + f.func_name
        special_completers.append((rulename, symname, completerwrapper))
        return completerwrapper
    return registrator

def explain_completion(rulename, symname, explanation=None):
    if explanation is None:
        explanation = '<%s>' % (symname,)
    @completer_for(rulename, symname)
    def explainer(ctxt, cass):
        return [Hint(explanation)]
    return explainer

def is_counter_col(cfdef, colname):
    col_info = [cm for cm in cfdef.column_metadata if cm.name == colname]
    return bool(col_info and cql_typename(col_info[0].validation_class) == 'counter')



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
<float> ::=         /-?[0-9]+\.[0-9]+/ ;
<integer> ::=       /-?[0-9]+/ ;
<uuid> ::=          /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/ ;
<identifier> ::=    /[a-z][a-z0-9_:]*/ ;
<star> ::=          "*" ;
<range> ::=         ".." ;
<endtoken> ::=      ";" ;
<op> ::=            /[-+=,().]/ ;
<cmp> ::=           /[<>]=?/ ;

<unclosedString>  ::= /'([^']|'')*/ ;
<unclosedComment> ::= /[/][*][^\n]*$/ ;

<symbol> ::= <star>
           | <range>
           | <op>
           | <cmp>
           ;
<name> ::= <identifier>
         | <stringLiteral>
         | <integer>
         ;
<term> ::= <stringLiteral>
         | <integer>
         | <float>
         | <uuid>
         ;
<colname> ::= <term>
            | <identifier>
            ;

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

<consistencylevel> ::= cl=<identifier> ;

<storageType> ::= typename=( <identifier> | <stringLiteral> );
'''

@completer_for('consistencylevel', 'cl')
def cl_completer(ctxt, cass):
    return consistency_levels

@completer_for('storageType', 'typename')
def storagetype_completer(ctxt, cass):
    return cql_types

syntax_rules += r'''
<useStatement> ::= "USE" ksname=<name>
                 ;
'''

@completer_for('useStatement', 'ksname')
def use_ks_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_keyspace_names())

syntax_rules += r'''
<selectStatement> ::= "SELECT" <whatToSelect>
                        "FROM" ( selectks=<name> "." )? selectsource=<name>
                          ("USING" "CONSISTENCY" <consistencylevel>)?
                          ("WHERE" <selectWhereClause>)?
                          ("LIMIT" <integer>)?
                    ;
<selectWhereClause> ::= <relation> ("AND" <relation>)*
                      | keyname=<colname> "IN" "(" <term> ("," <term>)* ")"
                      ;
<relation> ::= [rel_lhs]=<colname> ("=" | "<" | ">" | "<=" | ">=") <colname>
             ;
<whatToSelect> ::= colname=<colname> ("," colname=<colname>)*
                 | ("FIRST" <integer>)? "REVERSED"? (rangestart=<colname> ".." rangeend=<colname>
                                                     | "*")
                 | "COUNT" countparens="(" "*" ")"
                 ;
'''

@completer_for('selectStatement', 'selectsource')
def select_source_completer(ctxt, cass):
    ks = ctxt.get_binding('selectks', None)
    if ks is not None:
        ks = cql_dequote(ks)
    try:
        cfnames = cass.get_columnfamily_names(ks)
    except Exception:
        if ks is None:
            return ()
        raise
    return map(maybe_cql_escape, cfnames)

@completer_for('selectStatement', 'selectks')
def select_keyspace_completer(ctxt, cass):
    return [maybe_cql_escape(ks) + '.' for ks in cass.get_keyspace_names()]

@completer_for('selectWhereClause', 'keyname')
def select_where_keyname_completer(ctxt, cass):
    ksname = ctxt.get_binding('selectks')
    if ksname is not None:
        ksname = cql_dequote(ksname)
    selectsource = cql_dequote(ctxt.get_binding('selectsource'))
    cfdef = cass.get_columnfamily(selectsource, ksname=ksname)
    return [cfdef.key_alias if cfdef.key_alias is not None else 'KEY']

@completer_for('relation', 'rel_lhs')
def select_relation_lhs_completer(ctxt, cass):
    ksname = ctxt.get_binding('selectks')
    if ksname is not None:
        ksname = cql_dequote(ksname)
    selectsource = cql_dequote(ctxt.get_binding('selectsource'))
    return map(maybe_cql_escape, cass.filterable_column_names(selectsource, ksname=ksname))

@completer_for('whatToSelect', 'countparens')
def select_count_parens_completer(ctxt, cass):
    return ['(*)']

explain_completion('whatToSelect', 'colname')
explain_completion('whatToSelect', 'rangestart', '<range_start>')
explain_completion('whatToSelect', 'rangeend', '<range_end>')

syntax_rules += r'''
<insertStatement> ::= "INSERT" "INTO" insertcf=<name>
                               "(" keyname=<colname> ","
                                   [colname]=<colname> ( "," [colname]=<colname> )* ")"
                      "VALUES" "(" <term> "," <term> ( "," <term> )* ")"
                      ( "USING" [insertopt]=<usingOption>
                                ( "AND" [insertopt]=<usingOption> )* )?
                    ;
<usingOption> ::= "CONSISTENCY" <consistencylevel>
                | "TIMESTAMP" <integer>
                | "TTL" <integer>
                ;
'''

@completer_for('insertStatement', 'insertcf')
def insert_cf_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_columnfamily_names())

@completer_for('insertStatement', 'keyname')
def insert_keyname_completer(ctxt, cass):
    insertcf = ctxt.get_binding('insertcf')
    cfdef = cass.get_columnfamily(cql_dequote(insertcf))
    return [cfdef.key_alias if cfdef.key_alias is not None else 'KEY']

explain_completion('insertStatement', 'colname')

@completer_for('insertStatement', 'insertopt')
def insert_option_completer(ctxt, cass):
    opts = set('CONSISTENCY TIMESTAMP TTL'.split())
    for opt in ctxt.get_binding('insertopt', ()):
        opts.discard(opt.split()[0])
    return opts

syntax_rules += r'''
<updateStatement> ::= "UPDATE" cf=<name>
                        ( "USING" [updateopt]=<usingOption>
                                  ( "AND" [updateopt]=<usingOption> )* )?
                        "SET" <assignment> ( "," <assignment> )*
                        "WHERE" <updateWhereClause>
                    ;
<assignment> ::= updatecol=<colname> "=" update_rhs=<colname>
                                         ( counterop=( "+" | "-"? ) <integer> )?
               ;
<updateWhereClause> ::= updatefiltercol=<colname> "=" <term>
                      | updatefilterkey=<colname> filter_in="IN" "(" <term> ( "," <term> )* ")"
                      ;
'''

@completer_for('updateStatement', 'cf')
def update_cf_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_columnfamily_names())

@completer_for('updateStatement', 'updateopt')
def insert_option_completer(ctxt, cass):
    opts = set('CONSISTENCY TIMESTAMP TTL'.split())
    for opt in ctxt.get_binding('updateopt', ()):
        opts.discard(opt.split()[0])
    return opts

@completer_for('assignment', 'updatecol')
def update_col_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    colnames = map(maybe_cql_escape, [cm.name for cm in cfdef.column_metadata])
    return colnames + [Hint('<colname>')]

@completer_for('assignment', 'update_rhs')
def update_countername_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    curcol = cql_dequote(ctxt.get_binding('updatecol', ''))
    return [maybe_cql_escape(curcol)] if is_counter_col(cfdef, curcol) else [Hint('<term>')]

@completer_for('assignment', 'counterop')
def update_counterop_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    curcol = cql_dequote(ctxt.get_binding('updatecol', ''))
    return ['+', '-'] if is_counter_col(cfdef, curcol) else []

@completer_for('updateWhereClause', 'updatefiltercol')
def update_filtercol_completer(ctxt, cass):
    cfname = cql_dequote(ctxt.get_binding('cf'))
    return map(maybe_cql_escape, cass.filterable_column_names(cfname))

@completer_for('updateWhereClause', 'updatefilterkey')
def update_filterkey_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    return [cfdef.key_alias if cfdef.key_alias is not None else 'KEY']

@completer_for('updateWhereClause', 'filter_in')
def update_filter_in_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    fk = ctxt.get_binding('updatefilterkey')
    return ['IN'] if fk in ('KEY', cfdef.key_alias) else []

syntax_rules += r'''
<deleteStatement> ::= "DELETE" ( [delcol]=<colname> ( "," [delcol]=<colname> )* )?
                        "FROM" cf=<name>
                        ( "USING" [delopt]=<deleteOption> ( "AND" [delopt]=<deleteOption> )* )?
                        "WHERE" <updateWhereClause>
                    ;
<deleteOption> ::= "CONSISTENCY" <consistencylevel>
                 | "TIMESTAMP" <integer>
                 ;
'''

@completer_for('deleteStatement', 'cf')
def delete_cf_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_columnfamily_names())

@completer_for('deleteStatement', 'delopt')
def delete_opt_completer(ctxt, cass):
    opts = set('CONSISTENCY TIMESTAMP'.split())
    for opt in ctxt.get_binding('delopt', ()):
        opts.discard(opt.split()[0])
    return opts

explain_completion('deleteStatement', 'delcol', '<column_to_delete>')

syntax_rules += r'''
<batchStatement> ::= "BEGIN" "BATCH"
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
<truncateStatement> ::= "TRUNCATE" cf=<name>
                      ;
'''

@completer_for('truncateStatement', 'cf')
def truncate_cf_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_columnfamily_names())

syntax_rules += r'''
<createKeyspaceStatement> ::= "CREATE" "KEYSPACE" ksname=<name>
                                 "WITH" [optname]=<optionName> "=" [optval]=<optionVal>
                                 ( "AND" [optname]=<optionName> "=" [optval]=<optionVal> )*
                            ;
<optionName> ::= <identifier> ( ":" ( <identifier> | <integer> ) )?
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
    stratclass = cql_dequote(vals[stratopt])
    if stratclass in ('SimpleStrategy', 'OldNetworkTopologyStrategy'):
        return ['strategy_options:replication_factor =']
    return [Hint('<strategy_option_name>')]

@completer_for('createKeyspaceStatement', 'optval')
def create_ks_optval_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('optname', (None,))
    if exist_opts[-1] == 'strategy_class':
        return map(cql_escape, replication_strategies)
    return [Hint('<option_value>')]

syntax_rules += r'''
<createColumnFamilyStatement> ::= "CREATE" "COLUMNFAMILY" cf=<name>
                                    "(" keyalias=<colname> <storageType> "PRIMARY" "KEY"
                                        ( "," colname=<colname> <storageType> )* ")"
                                   ( "WITH" [cfopt]=<identifier> "=" [optval]=<cfOptionVal>
                                     ( "AND" [cfopt]=<identifier> "=" [optval]=<cfOptionVal> )* )?
                                ;
<cfOptionVal> ::= <storageType>
                | <identifier>
                | <stringLiteral>
                | <integer>
                | <float>
                ;
'''

explain_completion('createColumnFamilyStatement', 'keyalias', '<new_key_alias>')
explain_completion('createColumnFamilyStatement', 'cf', '<new_columnfamily_name>')
explain_completion('createColumnFamilyStatement', 'colname', '<new_column_name>')
explain_completion('createColumnFamilyStatement', 'optval', '<option_value>')

@completer_for('createColumnFamilyStatement', 'cfopt')
def create_cf_option_completer(ctxt, cass):
    return [c[0] for c in columnfamily_options]

syntax_rules += r'''
<createIndexStatement> ::= "CREATE" "INDEX" indexname=<identifier>? "ON"
                               cf=<name> "(" col=<colname> ")"
                         ;
'''

explain_completion('createIndexStatement', 'indexname', '<new_index_name>')

@completer_for('createIndexStatement', 'cf')
def create_index_cf_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_columnfamily_names())

@completer_for('createIndexStatement', 'col')
def create_index_col_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    colnames = [md.name for md in cfdef.column_metadata if md.index_name is None]
    return map(maybe_cql_escape, colnames)

syntax_rules += r'''
<dropKeyspaceStatement> ::= "DROP" "KEYSPACE" ksname=<name>
                          ;
'''

@completer_for('dropKeyspaceStatement', 'ksname')
def drop_ks_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_keyspace_names())

syntax_rules += r'''
<dropColumnFamilyStatement> ::= "DROP" "COLUMNFAMILY" cf=<name>
                              ;
'''

@completer_for('dropColumnFamilyStatement', 'cf')
def drop_cf_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_columnfamily_names())

syntax_rules += r'''
<dropIndexStatement> ::= "DROP" "INDEX" indexname=<name>
                       ;
'''

@completer_for('dropIndexStatement', 'cf')
def drop_index_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_index_names())

syntax_rules += r'''
<alterTableStatement> ::= "ALTER" "COLUMNFAMILY" cf=<name> <alterInstructions>
                        ;
<alterInstructions> ::= "ALTER" existcol=<name> "TYPE" <storageType>
                      | "ADD" newcol=<name> <storageType>
                      | "DROP" existcol=<name>
                      ;
'''

@completer_for('alterTableStatement', 'cf')
def alter_table_cf_completer(ctxt, cass):
    return map(maybe_cql_escape, cass.get_columnfamily_names())

@completer_for('alterInstructions', 'existcol')
def alter_table_col_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(cql_dequote(ctxt.get_binding('cf')))
    return map(maybe_cql_escape, [md.name for md in cfdef.column_metadata])

explain_completion('alterInstructions', 'newcol', '<new_column_name>')

# END SYNTAX/COMPLETION RULE DEFINITIONS



CqlRuleSet = pylexotron.ParsingRuleSet.from_rule_defs(syntax_rules)
for rulename, symname, compf in special_completers:
    CqlRuleSet.register_completer(compf, rulename, symname)

def cql_add_completer(rulename, symname):
    registrator = completer_for(rulename, symname)
    def more_registration(f):
        f = registrator(f)
        CqlRuleSet.register_completer(f, rulename, symname)
        return f
    return more_registration

def cql_parse(text, startsymbol='Start'):
    tokens = CqlRuleSet.lex(text)
    tokens = cql_massage_tokens(tokens)
    return cql_parse_tokens(tokens, startsymbol)

def cql_parse_tokens(toklist, startsymbol='Start'):
    return CqlRuleSet.parse(startsymbol, toklist)

def cql_whole_parse_tokens(toklist, startsymbol='Start'):
    return CqlRuleSet.whole_match(startsymbol, toklist)

def cql_massage_tokens(toklist):
    curstmt = []
    output = []

    term_on_nl = False

    for t in toklist:
        if t[0] == 'endline':
            if term_on_nl:
                t = ('endtoken', '\n')
            else:
                # don't put any 'endline' tokens in output
                continue
        curstmt.append(t)
        if t[0] == 'endtoken':
            term_on_nl = False
            output.extend(curstmt)
            curstmt = []
        else:
            if len(curstmt) == 1:
                # first token in statement; command word
                cmd = t[1].lower()
                term_on_nl = bool(cmd in commands_end_with_newline)

    output.extend(curstmt)
    return output

def split_list(items, pred):
    thisresult = []
    results = [thisresult]
    for i in items:
        thisresult.append(i)
        if pred(i):
            thisresult = []
            results.append(thisresult)
    return results

def cql_split_statements(text):
    tokens = CqlRuleSet.lex(text)
    tokens = cql_massage_tokens(tokens)
    stmts = split_list(tokens, lambda t: t[0] == 'endtoken')
    output = []
    in_batch = False
    for stmt in stmts:
        if in_batch:
            output[-1].extend(stmt)
        else:
            output.append(stmt)
        if len(stmt) > 1 \
        and stmt[0][0] == 'identifier' and stmt[1][0] == 'identifier' \
        and stmt[1][1].lower() == 'batch':
            if stmt[0][1].lower() == 'begin':
                in_batch = True
            elif stmt[0][1].lower() == 'apply':
                in_batch = False
    return output, in_batch

def want_space_between(tok, following):
    if tok[0] == 'op' and tok[1] in (',', ')', '='):
        return True
    if tok[0] == 'stringLiteral' and following[0] != ';':
        return True
    if tok[0] == 'star' and following[0] != ')':
        return True
    if tok[0] == 'endtoken':
        return True
    if tok[1][-1].isalnum() and following[0] != ',':
        return True
    return False

def find_common_prefix(strs):
    common = []
    for cgroup in izip(*strs):
        if all(x == cgroup[0] for x in cgroup[1:]):
            common.append(cgroup[0])
        else:
            break
    return ''.join(common)

def list_bifilter(pred, iterable):
    yes_s = []
    no_s = []
    for i in iterable:
        (yes_s if pred(i) else no_s).append(i)
    return yes_s, no_s

def cql_complete_single(text, partial, init_bindings={}, ignore_case=True, startsymbol='Start'):
    tokens = (cql_split_statements(text)[0] or [[]])[-1]
    bindings = init_bindings.copy()

    # handle some different completion scenarios- in particular, completing
    # inside a string literal
    prefix = None
    if tokens and tokens[-1][0] == 'unclosedString':
        prefix = token_dequote(tokens[-1])
        tokens = tokens[:-1]
        partial = prefix + partial
    if tokens and tokens[-1][0] == 'unclosedComment':
        return []

    # find completions for the position
    completions = CqlRuleSet.complete(startsymbol, tokens, init_bindings)

    hints, strcompletes = list_bifilter(pylexotron.is_hint, completions)

    # it's possible to get a newline token from completion; of course, we
    # don't want to actually have that be a candidate, we just want to hint
    if '\n' in strcompletes:
        strcompletes.remove('\n')
        if partial == '':
            hints.append(Hint('<enter>'))

    # find matches with the partial word under completion
    if ignore_case:
        partial = partial.lower()
        f = lambda s: s and cql_dequote(s).lower().startswith(partial)
    else:
        f = lambda s: s and cql_dequote(s).startswith(partial)
    candidates = filter(f, strcompletes)

    if prefix is not None:
        # dequote, re-escape, strip quotes: gets us the right quoted text
        # for completion. the opening quote is already there on the command
        # line and not part of the word under completion, and readline
        # fills in the closing quote for us.
        candidates = [cql_escape(cql_dequote(c))[len(prefix)+1:-1] for c in candidates]

    # prefix a space when desirable for pleasant cql formatting
    if tokens:
        newcandidates = []
        for c in candidates:
            if want_space_between(tokens[-1], c) \
            and prefix is None \
            and not text[-1].isspace() \
            and not c[0].isspace():
                c = ' ' + c
            newcandidates.append(c)
        candidates = newcandidates

    return candidates, hints

def cql_complete(text, partial, cassandra_conn=None, ignore_case=True, debug=False,
                 startsymbol='Start'):
    init_bindings = {'cassandra_conn': cassandra_conn}
    if debug:
        init_bindings['*DEBUG*'] = True

    completions, hints = cql_complete_single(text, partial, init_bindings, startsymbol=startsymbol)

    if hints:
        hints = [h.text for h in hints]
        hints.append('')

    if len(completions) == 1 and len(hints) == 0:
        c = completions[0]
        if not c.isspace():
            new_c = cql_complete_multiple(text, c, init_bindings, startsymbol=startsymbol)
            completions = [new_c]

    return hints + completions

def cql_complete_multiple(text, first, init_bindings, startsymbol='Start'):
    try:
        completions, hints = cql_complete_single(text + first, '', init_bindings,
                                                 startsymbol=startsymbol)
    except Exception:
        return first
    if hints:
        if not first[-1].isspace():
            first += ' '
        return first
    if len(completions) == 1 and completions[0] != '':
        first += completions[0]
    else:
        common_prefix = find_common_prefix(completions)
        if common_prefix != '':
            first += common_prefix
        else:
            return first
    return cql_complete_multiple(text, first, init_bindings, startsymbol=startsymbol)
