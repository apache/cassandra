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

import traceback

import cassandra
from cqlshlib import pylexotron, util

Hint = pylexotron.Hint

cql_keywords_reserved = {'add', 'allow', 'alter', 'and', 'apply', 'asc', 'authorize', 'batch', 'begin', 'by',
                         'columnfamily', 'create', 'delete', 'desc', 'describe', 'drop', 'entries', 'execute', 'from',
                         'full', 'grant', 'if', 'in', 'index', 'infinity', 'insert', 'into', 'is', 'keyspace', 'limit',
                         'materialized', 'modify', 'nan', 'norecursive', 'not', 'null', 'of', 'on', 'or', 'order',
                         'primary', 'rename', 'revoke', 'schema', 'select', 'set', 'table', 'to', 'token', 'truncate',
                         'unlogged', 'update', 'use', 'using', 'view', 'where', 'with'}
"""
Set of reserved keywords in CQL.

Derived from .../cassandra/src/java/org/apache/cassandra/cql3/ReservedKeywords.java
"""


class CqlParsingRuleSet(pylexotron.ParsingRuleSet):

    available_compression_classes = (
        'DeflateCompressor',
        'SnappyCompressor',
        'LZ4Compressor',
        'ZstdCompressor',
    )

    available_compaction_classes = (
        'LeveledCompactionStrategy',
        'SizeTieredCompactionStrategy',
        'TimeWindowCompactionStrategy',
        'UnifiedCompactionStrategy'
    )

    replication_strategies = (
        'SimpleStrategy',
        'NetworkTopologyStrategy'
    )

    def __init__(self, *args, **kwargs):
        pylexotron.ParsingRuleSet.__init__(self)

        # note: commands_end_with_newline may be extended by callers.
        self.commands_end_with_newline = set()
        self.set_reserved_keywords()

    def set_reserved_keywords(self):
        """
        We cannot let reserved cql keywords be simple 'identifier' since this caused
        problems with completion, see CASSANDRA-10415
        """
        cassandra.metadata.cql_keywords_reserved = cql_keywords_reserved
        syntax = '<reserved_identifier> ::= /(' + '|'.join(r'\b{}\b'.format(k) for k in cql_keywords_reserved) + ')/ ;'
        self.append_rules(syntax)

    def completer_for(self, rulename, symname):
        def registrator(f):
            def completerwrapper(ctxt):
                cass = ctxt.get_binding('cassandra_conn', None)
                if cass is None:
                    return ()
                return f(ctxt, cass)
            completerwrapper.__name__ = 'completerwrapper_on_' + f.__name__
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
        in_pg_string = len([st for st in tokens if len(st) > 0 and st[0] == 'unclosedPgString']) == 1
        for stmt in stmts:
            if in_batch:
                output[-1].extend(stmt)
            else:
                output.append(stmt)
            if len(stmt) > 2:
                if stmt[-3][1].upper() == 'APPLY':
                    in_batch = False
                elif stmt[0][1].upper() == 'BEGIN':
                    in_batch = True
        return output, in_batch or in_pg_string

    def cql_complete_single(self, text, partial, init_bindings=None, ignore_case=True,
                            startsymbol='Start'):
        tokens = (self.cql_split_statements(text)[0] or [[]])[-1]
        bindings = {} if init_bindings is None else init_bindings.copy()

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
        candidates = list(filter(f, strcompletes))

        if prefix is not None:
            # dequote, re-escape, strip quotes: gets us the right quoted text
            # for completion. the opening quote is already there on the command
            # line and not part of the word under completion, and readline
            # fills in the closing quote for us.
            candidates = [requoter(dequoter(c))[len(prefix) + 1:-1] for c in candidates]

            # the above process can result in an empty string; this doesn't help for
            # completions
            candidates = [_f for _f in candidates if _f]

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
        if len(candidates) == 1 and candidates[0][-1].isalnum()  \
                and lasttype != 'unclosedString' \
                and lasttype != 'unclosedName':
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
            print("cql_complete(%r, partial=%r)" % (text, partial))

        completions, hints = self.cql_complete_single(text, partial, init_bindings,
                                                      startsymbol=startsymbol)

        if hints:
            hints = [h.text for h in hints]
            hints.append('')

        if len(completions) == 1 and len(hints) == 0:
            c = completions[0]
            if debug:
                print("** Got one completion: %r. Checking for further matches...\n" % (c,))
            if not c.isspace():
                new_c = self.cql_complete_multiple(text, c, init_bindings, startsymbol=startsymbol)
                completions = [new_c]
            if debug:
                print("** New list of completions: %r" % (completions,))

        return hints + completions

    def cql_complete_multiple(self, text, first, init_bindings, startsymbol='Start'):
        debug = init_bindings.get('*DEBUG*', False)
        try:
            completions, hints = self.cql_complete_single(text + first, '', init_bindings,
                                                          startsymbol=startsymbol)
        except Exception:
            if debug:
                print("** completion expansion had a problem:")
                traceback.print_exc()
            return first
        if hints:
            if not first[-1].isspace():
                first += ' '
            if debug:
                print("** completion expansion found hints: %r" % (hints,))
            return first
        if len(completions) == 1 and completions[0] != '':
            if debug:
                print("** Got another completion: %r." % (completions[0],))
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
                print("** Got a partial completion: %r." % (common_prefix,))
            return first + common_prefix
        if debug:
            print("** New total completion: %r. Checking for further matches...\n" % (first,))
        return self.cql_complete_multiple(text, first, init_bindings, startsymbol=startsymbol)

    @staticmethod
    def cql_extract_orig(toklist, srcstr):
        # low end of span for first token, to high end of span for last token
        return srcstr[toklist[0][2][0]:toklist[-1][2][1]]

    @staticmethod
    def token_dequote(tok):
        if tok[0] == 'unclosedName':
            # strip one quote
            return tok[1][1:].replace('""', '"')
        if tok[0] == 'quotedStringLiteral':
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
