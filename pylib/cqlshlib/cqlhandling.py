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
import traceback
from . import pylexotron, util

Hint = pylexotron.Hint

class CqlParsingRuleSet(pylexotron.ParsingRuleSet):
    keywords = set((
        'select', 'from', 'where', 'and', 'key', 'insert', 'update', 'with',
        'limit', 'using', 'consistency', 'one', 'quorum', 'all', 'any',
        'local_quorum', 'each_quorum', 'two', 'three', 'use', 'count', 'set',
        'begin', 'apply', 'batch', 'truncate', 'delete', 'in', 'create',
        'keyspace', 'schema', 'columnfamily', 'table', 'index', 'on', 'drop',
        'primary', 'into', 'values', 'timestamp', 'ttl', 'alter', 'add', 'type',
        'first', 'reversed'
    ))

    columnfamily_options = (
        # (CQL option name, Thrift option name (or None if same))
        ('comment', None),
        ('comparator', 'comparator_type'),
        ('read_repair_chance', None),
        ('gc_grace_seconds', None),
        ('default_validation', 'default_validation_class'),
        ('min_compaction_threshold', None),
        ('max_compaction_threshold', None),
        ('replicate_on_write', None),
        ('compaction_strategy_class', 'compaction_strategy'),
    )

    obsolete_cf_options = (
        ('key_cache_size', None),
        ('row_cache_size', None),
        ('row_cache_save_period_in_seconds', None),
        ('key_cache_save_period_in_seconds', None),
        ('memtable_throughput_in_mb', None),
        ('memtable_operations_in_millions', None),
        ('memtable_flush_after_mins', None),
        ('row_cache_provider', None),
    )

    all_columnfamily_options = columnfamily_options + obsolete_cf_options

    columnfamily_map_options = (
        ('compaction_strategy_options', None,
            ()),
        ('compression_parameters', 'compression_options',
            ('sstable_compression', 'chunk_length_kb', 'crc_check_chance')),
    )

    available_compression_classes = (
        'DeflateCompressor',
        'SnappyCompressor',
    )

    available_compaction_classes = (
        'LeveledCompactionStrategy',
        'SizeTieredCompactionStrategy'
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

    replication_strategies = (
        'SimpleStrategy',
        'OldNetworkTopologyStrategy',
        'NetworkTopologyStrategy'
    )

    consistency_levels = (
        'ANY',
        'ONE',
        'TWO',
        'THREE',
        'QUORUM',
        'ALL',
        'LOCAL_QUORUM',
        'EACH_QUORUM'
    )

    # if a term matches this, it shouldn't need to be quoted to be valid cql
    valid_cql_word_re = re.compile(r"^(?:[a-z][a-z0-9_]*|-?[0-9][0-9.]*)$", re.I)

    def __init__(self, *args, **kwargs):
        pylexotron.ParsingRuleSet.__init__(self, *args, **kwargs)

        # note: commands_end_with_newline may be extended by callers.
        self.commands_end_with_newline = set()
        self.set_keywords_as_syntax()

    def completer_for(self, rulename, symname):
        def registrator(f):
            def completerwrapper(ctxt):
                cass = ctxt.get_binding('cassandra_conn', None)
                if cass is None:
                    return ()
                return f(ctxt, cass)
            completerwrapper.func_name = 'completerwrapper_on_' + f.func_name
            self.register_completer(completerwrapper, rulename, symname)
            return completerwrapper
        return registrator

    def explain_completion(self, rulename, symname, explanation=None):
        if explanation is None:
            explanation = '<%s>' % (symname,)
        @self.completer_for(rulename, symname)
        def explainer(ctxt, cass):
            return [Hint(explanation)]
        return explainer

    def set_keywords_as_syntax(self):
        syntax = []
        for k in self.keywords:
            syntax.append('<K_%s> ::= "%s" ;' % (k.upper(), k))
        self.append_rules('\n'.join(syntax))

    def cql_massage_tokens(self, toklist):
        curstmt = []
        output = []

        term_on_nl = False

        for t in toklist:
            if t[0] == 'endline':
                if term_on_nl:
                    t = ('endtoken',) + t[1:]
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
                    term_on_nl = bool(cmd in self.commands_end_with_newline)

        output.extend(curstmt)
        return output

    def cql_parse(self, text, startsymbol='Start'):
        tokens = self.lex(text)
        tokens = self.cql_massage_tokens(tokens)
        return self.parse(startsymbol, tokens, init_bindings={'*SRC*': text})

    def cql_whole_parse_tokens(self, toklist, srcstr=None, startsymbol='Start'):
        return self.whole_match(startsymbol, toklist, srcstr=srcstr)

    def cql_split_statements(self, text):
        tokens = self.lex(text)
        tokens = self.cql_massage_tokens(tokens)
        stmts = util.split_list(tokens, lambda t: t[0] == 'endtoken')
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

    def cql_complete_single(self, text, partial, init_bindings={}, ignore_case=True,
                            startsymbol='Start'):
        tokens = (self.cql_split_statements(text)[0] or [[]])[-1]
        bindings = init_bindings.copy()

        # handle some different completion scenarios- in particular, completing
        # inside a string literal
        prefix = None
        dequoter = util.identity
        lasttype = None
        if tokens:
            lasttype = tokens[-1][0]
            if lasttype == 'unclosedString':
                prefix = self.token_dequote(tokens[-1])
                tokens = tokens[:-1]
                partial = prefix + partial
                dequoter = self.dequote_value
                requoter = self.escape_value
            elif lasttype == 'unclosedName':
                prefix = self.token_dequote(tokens[-1])
                tokens = tokens[:-1]
                partial = prefix + partial
                dequoter = self.dequote_name
                requoter = self.escape_name
            elif lasttype == 'unclosedComment':
                return []
        bindings['partial'] = partial
        bindings['*LASTTYPE*'] = lasttype
        bindings['*SRC*'] = text

        # find completions for the position
        completions = self.complete(startsymbol, tokens, bindings)

        hints, strcompletes = util.list_bifilter(pylexotron.is_hint, completions)

        # it's possible to get a newline token from completion; of course, we
        # don't want to actually have that be a candidate, we just want to hint
        if '\n' in strcompletes:
            strcompletes.remove('\n')
            if partial == '':
                hints.append(Hint('<enter>'))

        # find matches with the partial word under completion
        if ignore_case:
            partial = partial.lower()
            f = lambda s: s and dequoter(s).lower().startswith(partial)
        else:
            f = lambda s: s and dequoter(s).startswith(partial)
        candidates = filter(f, strcompletes)

        if prefix is not None:
            # dequote, re-escape, strip quotes: gets us the right quoted text
            # for completion. the opening quote is already there on the command
            # line and not part of the word under completion, and readline
            # fills in the closing quote for us.
            candidates = [requoter(dequoter(c))[len(prefix)+1:-1] for c in candidates]

            # the above process can result in an empty string; this doesn't help for
            # completions
            candidates = filter(None, candidates)

        # prefix a space when desirable for pleasant cql formatting
        if tokens:
            newcandidates = []
            for c in candidates:
                if self.want_space_between(tokens[-1], c) \
                and prefix is None \
                and not text[-1].isspace() \
                and not c[0].isspace():
                    c = ' ' + c
                newcandidates.append(c)
            candidates = newcandidates

        # append a space for single, complete identifiers
        if len(candidates) == 1 and candidates[0][-1].isalnum():
            candidates[0] += ' '
        return candidates, hints

    @staticmethod
    def want_space_between(tok, following):
        if following in (',', ')', ':'):
            return False
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

    def cql_complete(self, text, partial, cassandra_conn=None, ignore_case=True, debug=False,
                     startsymbol='Start'):
        init_bindings = {'cassandra_conn': cassandra_conn}
        if debug:
            init_bindings['*DEBUG*'] = True
            print "cql_complete(%r, partial=%r)" % (text, partial)

        completions, hints = self.cql_complete_single(text, partial, init_bindings,
                                                      startsymbol=startsymbol)

        if hints:
            hints = [h.text for h in hints]
            hints.append('')

        if len(completions) == 1 and len(hints) == 0:
            c = completions[0]
            if debug:
                print "** Got one completion: %r. Checking for further matches...\n" % (c,)
            if not c.isspace():
                new_c = self.cql_complete_multiple(text, c, init_bindings, startsymbol=startsymbol)
                completions = [new_c]
            if debug:
                print "** New list of completions: %r" % (completions,)

        return hints + completions

    def cql_complete_multiple(self, text, first, init_bindings, startsymbol='Start'):
        debug = init_bindings.get('*DEBUG*', False)
        try:
            completions, hints = self.cql_complete_single(text + first, '', init_bindings,
                                                          startsymbol=startsymbol)
        except Exception:
            if debug:
                print "** completion expansion had a problem:"
                traceback.print_exc()
            return first
        if hints:
            if not first[-1].isspace():
                first += ' '
            if debug:
                print "** completion expansion found hints: %r" % (hints,)
            return first
        if len(completions) == 1 and completions[0] != '':
            if debug:
                print "** Got another completion: %r." % (completions[0],)
            if completions[0][0] in (',', ')', ':') and first[-1] == ' ':
                first = first[:-1]
            first += completions[0]
        else:
            common_prefix = util.find_common_prefix(completions)
            if common_prefix == '':
                return first
            if common_prefix[0] in (',', ')', ':') and first[-1] == ' ':
                first = first[:-1]
            if debug:
                print "** Got a partial completion: %r." % (common_prefix,)
            first += common_prefix
        if debug:
            print "** New total completion: %r. Checking for further matches...\n" % (first,)
        return self.cql_complete_multiple(text, first, init_bindings, startsymbol=startsymbol)

    @classmethod
    def cql_typename(cls, classname):
        fq_classname = 'org.apache.cassandra.db.marshal.'
        if classname.startswith(fq_classname):
            classname = classname[len(fq_classname):]
        try:
            return cls.apache_class_to_cql_type[classname]
        except KeyError:
            return cls.escape_value(classname)

    @classmethod
    def find_validator_class(cls, cqlname):
        return cls.cql_type_to_apache_class[cqlname]

    @classmethod
    def is_valid_cql_word(cls, s):
        return cls.valid_cql_word_re.match(s) is not None and s.lower() not in cls.keywords

    @staticmethod
    def cql_extract_orig(toklist, srcstr):
        # low end of span for first token, to high end of span for last token
        return srcstr[toklist[0][2][0]:toklist[-1][2][1]]

    @staticmethod
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

    @staticmethod
    def token_is_word(tok):
        return tok[0] == 'identifier'

    @classmethod
    def cql2_maybe_escape_name(cls, name):
        if cls.is_valid_cql_word(name):
            return name
        return cls.cql2_escape_name(name)

    # XXX: this doesn't really belong here.
    @classmethod
    def is_counter_col(cls, cfdef, colname):
        col_info = [cm for cm in cfdef.column_metadata if cm.name == colname]
        return bool(col_info and cls.cql_typename(col_info[0].validation_class) == 'counter')

    @staticmethod
    def cql2_dequote_value(cqlword):
        cqlword = cqlword.strip()
        if cqlword == '':
            return cqlword
        if cqlword[0] == "'":
            cqlword = cqlword[1:-1].replace("''", "'")
        return cqlword

    @staticmethod
    def cql2_escape_value(value):
        if value is None:
            return 'NULL' # this totally won't work
        if isinstance(value, bool):
            value = str(value).lower()
        elif isinstance(value, float):
            return '%f' % value
        elif isinstance(value, int):
            return str(value)
        return "'%s'" % value.replace("'", "''")

    # use _name for keyspace, cf, and column names, and _value otherwise.
    # also use the cql2_ prefix when dealing with cql2, or leave it off to
    # get whatever behavior is default for this CqlParsingRuleSet.
    cql2_dequote_name = dequote_name = dequote_value = cql2_dequote_value
    cql2_escape_name = escape_name = escape_value = cql2_escape_value
    maybe_escape_name = cql2_maybe_escape_name
    dequote_any = cql2_dequote_value

CqlRuleSet = CqlParsingRuleSet()

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
<float> ::=         /-?[0-9]+\.[0-9]+/ ;
<integer> ::=       /-?[0-9]+/ ;
<uuid> ::=          /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/ ;
<identifier> ::=    /[a-z][a-z0-9_]*/ ;
<colon> ::=         ":" ;
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
            | nocomplete=<K_KEY>
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

<storageType> ::= typename=( <identifier> | <stringLiteral> ) ;

<keyspaceName> ::= ksname=<name> ;

<columnFamilyName> ::= ( ksname=<name> "." )? cfname=<name> ;
'''

@completer_for('colname', 'nocomplete')
def nocomplete(ctxt, cass):
    return ()

@completer_for('consistencylevel', 'cl')
def cl_completer(ctxt, cass):
    return CqlRuleSet.consistency_levels

@completer_for('storageType', 'typename')
def storagetype_completer(ctxt, cass):
    return CqlRuleSet.cql_types

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

def get_cfdef(ctxt, cass):
    ks = ctxt.get_binding('ksname', None)
    cf = ctxt.get_binding('cfname')
    return cass.get_columnfamily(cf, ksname=ks)

syntax_rules += r'''
<useStatement> ::= "USE" ksname=<keyspaceName>
                 ;
<selectStatement> ::= "SELECT" <whatToSelect>
                        "FROM" cf=<columnFamilyName>
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

@completer_for('selectWhereClause', 'keyname')
def select_where_keyname_completer(ctxt, cass):
    cfdef = get_cfdef(ctxt, cass)
    return [cfdef.key_alias if cfdef.key_alias is not None else 'KEY']

@completer_for('relation', 'rel_lhs')
def select_relation_lhs_completer(ctxt, cass):
    cfdef = get_cfdef(ctxt, cass)
    return map(maybe_escape_name, cass.filterable_column_names(cfdef))

@completer_for('whatToSelect', 'countparens')
def select_count_parens_completer(ctxt, cass):
    return ['(*)']

explain_completion('whatToSelect', 'colname')
explain_completion('whatToSelect', 'rangestart', '<range_start>')
explain_completion('whatToSelect', 'rangeend', '<range_end>')

syntax_rules += r'''
<insertStatement> ::= "INSERT" "INTO" cf=<columnFamilyName>
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

@completer_for('insertStatement', 'keyname')
def insert_keyname_completer(ctxt, cass):
    cfdef = get_cfdef(ctxt, cass)
    return [cfdef.key_alias if cfdef.key_alias is not None else 'KEY']

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
                        "WHERE" <updateWhereClause>
                    ;
<assignment> ::= updatecol=<colname> "=" update_rhs=<colname>
                                         ( counterop=( "+" | "-"? ) <integer> )?
               ;
<updateWhereClause> ::= updatefiltercol=<colname> "=" <term>
                      | updatefilterkey=<colname> filter_in="IN" "(" <term> ( "," <term> )* ")"
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
    cfdef = get_cfdef(ctxt, cass)
    colnames = map(maybe_escape_name, [cm.name for cm in cfdef.column_metadata])
    return colnames + [Hint('<colname>')]

@completer_for('assignment', 'update_rhs')
def update_countername_completer(ctxt, cass):
    cfdef = get_cfdef(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    return [maybe_escape_name(curcol)] if CqlRuleSet.is_counter_col(cfdef, curcol) else [Hint('<term>')]

@completer_for('assignment', 'counterop')
def update_counterop_completer(ctxt, cass):
    cfdef = get_cfdef(ctxt, cass)
    curcol = dequote_name(ctxt.get_binding('updatecol', ''))
    return ['+', '-'] if CqlRuleSet.is_counter_col(cfdef, curcol) else []

@completer_for('updateWhereClause', 'updatefiltercol')
def update_filtercol_completer(ctxt, cass):
    cfdef = get_cfdef(ctxt, cass)
    return map(maybe_escape_name, cass.filterable_column_names(cfdef))

@completer_for('updateWhereClause', 'updatefilterkey')
def update_filterkey_completer(ctxt, cass):
    cfdef = get_cfdef(ctxt, cass)
    return [cfdef.key_alias if cfdef.key_alias is not None else 'KEY']

@completer_for('updateWhereClause', 'filter_in')
def update_filter_in_completer(ctxt, cass):
    cfdef = get_cfdef(ctxt, cass)
    fk = ctxt.get_binding('updatefilterkey')
    return ['IN'] if fk in ('KEY', cfdef.key_alias) else []

syntax_rules += r'''
<deleteStatement> ::= "DELETE" ( [delcol]=<colname> ( "," [delcol]=<colname> )* )?
                        "FROM" cf=<columnFamilyName>
                        ( "USING" [delopt]=<deleteOption> ( "AND" [delopt]=<deleteOption> )* )?
                        "WHERE" <updateWhereClause>
                    ;
<deleteOption> ::= "CONSISTENCY" <consistencylevel>
                 | "TIMESTAMP" <integer>
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
<truncateStatement> ::= "TRUNCATE" cf=<columnFamilyName>
                      ;
'''

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
    stratclass = dequote_value(vals[stratopt])
    if stratclass in ('SimpleStrategy', 'OldNetworkTopologyStrategy'):
        return ['strategy_options:replication_factor =']
    return [Hint('<strategy_option_name>')]

@completer_for('createKeyspaceStatement', 'optval')
def create_ks_optval_completer(ctxt, cass):
    exist_opts = ctxt.get_binding('optname', (None,))
    if exist_opts[-1] == 'strategy_class':
        return map(escape_value, CqlRuleSet.replication_strategies)
    return [Hint('<option_value>')]

syntax_rules += r'''
<createColumnFamilyStatement> ::= "CREATE" ( "COLUMNFAMILY" | "TABLE" ) cf=<name>
                                    "(" keyalias=<colname> <storageType> "PRIMARY" "KEY"
                                        ( "," colname=<colname> <storageType> )* ")"
                                   ( "WITH" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal>
                                     ( "AND" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal> )* )?
                                ;

<cfOptionName> ::= cfoptname=<identifier> ( cfoptsep=":" cfsubopt=( <identifier> | <integer> ) )?
                 ;

<cfOptionVal> ::= <identifier>
                | <stringLiteral>
                | <integer>
                | <float>
                ;
'''

explain_completion('createColumnFamilyStatement', 'keyalias', '<new_key_name>')
explain_completion('createColumnFamilyStatement', 'cf', '<new_table_name>')
explain_completion('createColumnFamilyStatement', 'colname', '<new_column_name>')

@completer_for('cfOptionName', 'cfoptname')
def create_cf_option_completer(ctxt, cass):
    return [c[0] for c in CqlRuleSet.columnfamily_options] + \
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
            cf = ctxt.get_binding('cf')
            try:
                csc = cass.get_columnfamily(cf).compaction_strategy
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
        return CqlRuleSet.cql_types
    if this_opt == 'read_repair_chance':
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
                               cf=<name> "(" col=<colname> ")"
                         ;
'''

explain_completion('createIndexStatement', 'indexname', '<new_index_name>')

@completer_for('createIndexStatement', 'cf')
def create_index_cf_completer(ctxt, cass):
    return map(maybe_escape_name, cass.get_columnfamily_names())

@completer_for('createIndexStatement', 'col')
def create_index_col_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(dequote_name(ctxt.get_binding('cf')))
    colnames = [md.name for md in cfdef.column_metadata if md.index_name is None]
    return map(maybe_escape_name, colnames)

syntax_rules += r'''
<dropKeyspaceStatement> ::= "DROP" "KEYSPACE" ksname=<keyspaceName>
                          ;
'''

@completer_for('dropKeyspaceStatement', 'ksname')
def drop_ks_completer(ctxt, cass):
    return map(maybe_escape_name, cass.get_keyspace_names())

syntax_rules += r'''
<dropColumnFamilyStatement> ::= "DROP" ( "COLUMNFAMILY" | "TABLE" ) cf=<name>
                              ;
'''

@completer_for('dropColumnFamilyStatement', 'cf')
def drop_cf_completer(ctxt, cass):
    return map(maybe_escape_name, cass.get_columnfamily_names())

syntax_rules += r'''
<dropIndexStatement> ::= "DROP" "INDEX" indexname=<name>
                       ;
'''

@completer_for('dropIndexStatement', 'indexname')
def drop_index_completer(ctxt, cass):
    return map(maybe_escape_name, cass.get_index_names())

syntax_rules += r'''
<alterTableStatement> ::= "ALTER" ( "COLUMNFAMILY" | "TABLE" ) cf=<name> <alterInstructions>
                        ;
<alterInstructions> ::= "ALTER" existcol=<name> "TYPE" <storageType>
                      | "ADD" newcol=<name> <storageType>
                      | "DROP" existcol=<name>
                      | "WITH" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal>
                        ( "AND" [cfopt]=<cfOptionName> "=" [optval]=<cfOptionVal> )*
                      ;
'''

@completer_for('alterTableStatement', 'cf')
def alter_table_cf_completer(ctxt, cass):
    return map(maybe_escape_name, cass.get_columnfamily_names())

@completer_for('alterInstructions', 'existcol')
def alter_table_col_completer(ctxt, cass):
    cfdef = cass.get_columnfamily(dequote_name(ctxt.get_binding('cf')))
    cols = [md.name for md in cfdef.column_metadata]
    if cfdef.key_alias is not None:
        cols.append(cfdef.key_alias)
    return map(maybe_escape_name, cols)

explain_completion('alterInstructions', 'newcol', '<new_column_name>')

completer_for('alterInstructions', 'optval') \
    (create_cf_option_val_completer)

# END SYNTAX/COMPLETION RULE DEFINITIONS

CqlRuleSet.append_rules(syntax_rules)
