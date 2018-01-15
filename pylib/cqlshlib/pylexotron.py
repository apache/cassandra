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
from .saferscanner import SaferScanner


class LexingError(Exception):

    @classmethod
    def from_text(cls, rulestr, unmatched, msg='Lexing error'):
        bad_char = len(rulestr) - len(unmatched)
        linenum = rulestr[:bad_char].count('\n') + 1
        charnum = len(rulestr[:bad_char].rsplit('\n', 1)[-1]) + 1
        snippet_start = max(0, min(len(rulestr), bad_char - 10))
        snippet_end = max(0, min(len(rulestr), bad_char + 10))
        msg += " (Error at: '...%s...')" % (rulestr[snippet_start:snippet_end],)
        raise cls(linenum, charnum, msg)

    def __init__(self, linenum, charnum, msg='Lexing error'):
        self.linenum = linenum
        self.charnum = charnum
        self.msg = msg
        self.args = (linenum, charnum, msg)

    def __str__(self):
        return '%s at line %d, char %d' % (self.msg, self.linenum, self.charnum)


class Hint:

    def __init__(self, text):
        self.text = text

    def __hash__(self):
        return hash((id(self.__class__), self.text))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.text == self.text

    def __repr__(self):
        return '%s(%r)' % (self.__class__, self.text)


def is_hint(x):
    return isinstance(x, Hint)


class ParseContext:
    """
    These are meant to be immutable, although it would be something of a
    pain to enforce that in python.
    """

    def __init__(self, ruleset, bindings, matched, remainder, productionname):
        self.ruleset = ruleset
        self.bindings = bindings
        self.matched = matched
        self.remainder = remainder
        self.productionname = productionname

    def get_production_by_name(self, name):
        return self.ruleset[name]

    def get_completer(self, symname):
        return self.ruleset[(self.productionname, symname)]

    def get_binding(self, name, default=None):
        return self.bindings.get(name, default)

    def with_binding(self, name, val):
        newbinds = self.bindings.copy()
        newbinds[name] = val
        return self.__class__(self.ruleset, newbinds, self.matched,
                              self.remainder, self.productionname)

    def with_match(self, num):
        return self.__class__(self.ruleset, self.bindings,
                              self.matched + self.remainder[:num],
                              self.remainder[num:], self.productionname)

    def with_production_named(self, newname):
        return self.__class__(self.ruleset, self.bindings, self.matched,
                              self.remainder, newname)

    def extract_orig(self, tokens=None):
        if tokens is None:
            tokens = self.matched
        if not tokens:
            return ''
        orig = self.bindings.get('*SRC*', None)
        if orig is None:
            # pretty much just guess
            return ' '.join([t[1] for t in tokens])
        # low end of span for first token, to high end of span for last token
        orig_text = orig[tokens[0][2][0]:tokens[-1][2][1]]

        # Convert all unicode tokens to ascii, where possible.  This
        # helps avoid problems with performing unicode-incompatible
        # operations on tokens (like .lower()).  See CASSANDRA-9083
        # for one example of this.
        try:
            orig_text = orig_text.encode('ascii')
        except UnicodeEncodeError:
            pass
        return orig_text

    def __repr__(self):
        return '<%s matched=%r remainder=%r prodname=%r bindings=%r>' \
               % (self.__class__.__name__, self.matched, self.remainder, self.productionname, self.bindings)


class matcher:

    def __init__(self, arg):
        self.arg = arg

    def match(self, ctxt, completions):
        raise NotImplementedError

    def match_with_results(self, ctxt, completions):
        matched_before = len(ctxt.matched)
        newctxts = self.match(ctxt, completions)
        return [(newctxt, newctxt.matched[matched_before:]) for newctxt in newctxts]

    @staticmethod
    def try_registered_completion(ctxt, symname, completions):
        debugging = ctxt.get_binding('*DEBUG*', False)
        if ctxt.remainder or completions is None:
            return False
        try:
            completer = ctxt.get_completer(symname)
        except KeyError:
            return False
        if debugging:
            print "Trying completer %r with %r" % (completer, ctxt)
        try:
            new_compls = completer(ctxt)
        except Exception:
            if debugging:
                import traceback
                traceback.print_exc()
            return False
        if debugging:
            print "got %r" % (new_compls,)
        completions.update(new_compls)
        return True

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.arg)


class choice(matcher):

    def match(self, ctxt, completions):
        foundctxts = []
        for a in self.arg:
            subctxts = a.match(ctxt, completions)
            foundctxts.extend(subctxts)
        return foundctxts


class one_or_none(matcher):

    def match(self, ctxt, completions):
        return [ctxt] + list(self.arg.match(ctxt, completions))


class repeat(matcher):

    def match(self, ctxt, completions):
        found = [ctxt]
        ctxts = [ctxt]
        while True:
            new_ctxts = []
            for c in ctxts:
                new_ctxts.extend(self.arg.match(c, completions))
            if not new_ctxts:
                return found
            found.extend(new_ctxts)
            ctxts = new_ctxts


class rule_reference(matcher):

    def match(self, ctxt, completions):
        prevname = ctxt.productionname
        try:
            rule = ctxt.get_production_by_name(self.arg)
        except KeyError:
            raise ValueError("Can't look up production rule named %r" % (self.arg,))
        output = rule.match(ctxt.with_production_named(self.arg), completions)
        return [c.with_production_named(prevname) for c in output]


class rule_series(matcher):

    def match(self, ctxt, completions):
        ctxts = [ctxt]
        for patpiece in self.arg:
            new_ctxts = []
            for c in ctxts:
                new_ctxts.extend(patpiece.match(c, completions))
            if not new_ctxts:
                return ()
            ctxts = new_ctxts
        return ctxts


class named_symbol(matcher):

    def __init__(self, name, arg):
        matcher.__init__(self, arg)
        self.name = name

    def match(self, ctxt, completions):
        pass_in_compls = completions
        if self.try_registered_completion(ctxt, self.name, completions):
            # don't collect other completions under this; use a dummy
            pass_in_compls = set()
        results = self.arg.match_with_results(ctxt, pass_in_compls)
        return [c.with_binding(self.name, ctxt.extract_orig(matchtoks)) for (c, matchtoks) in results]

    def __repr__(self):
        return '%s(%r, %r)' % (self.__class__.__name__, self.name, self.arg)


class named_collector(named_symbol):

    def match(self, ctxt, completions):
        pass_in_compls = completions
        if self.try_registered_completion(ctxt, self.name, completions):
            # don't collect other completions under this; use a dummy
            pass_in_compls = set()
        output = []
        for ctxt, matchtoks in self.arg.match_with_results(ctxt, pass_in_compls):
            oldval = ctxt.get_binding(self.name, ())
            output.append(ctxt.with_binding(self.name, oldval + (ctxt.extract_orig(matchtoks),)))
        return output


class terminal_matcher(matcher):

    def pattern(self):
        raise NotImplementedError


class regex_rule(terminal_matcher):

    def __init__(self, pat):
        terminal_matcher.__init__(self, pat)
        self.regex = pat
        self.re = re.compile(pat + '$', re.I | re.S)

    def match(self, ctxt, completions):
        if ctxt.remainder:
            if self.re.match(ctxt.remainder[0][1]):
                return [ctxt.with_match(1)]
        elif completions is not None:
            completions.add(Hint('<%s>' % ctxt.productionname))
        return []

    def pattern(self):
        return self.regex


class text_match(terminal_matcher):
    alpha_re = re.compile(r'[a-zA-Z]')

    def __init__(self, text):
        try:
            terminal_matcher.__init__(self, eval(text))
        except SyntaxError:
            print "bad syntax %r" % (text,)

    def match(self, ctxt, completions):
        if ctxt.remainder:
            if self.arg.lower() == ctxt.remainder[0][1].lower():
                return [ctxt.with_match(1)]
        elif completions is not None:
            completions.add(self.arg)
        return []

    def pattern(self):
        # can't use (?i) here- Scanner component regex flags won't be applied
        def ignorecaseify(matchobj):
            c = matchobj.group(0)
            return '[%s%s]' % (c.upper(), c.lower())
        return self.alpha_re.sub(ignorecaseify, re.escape(self.arg))


class case_match(text_match):

    def match(self, ctxt, completions):
        if ctxt.remainder:
            if self.arg == ctxt.remainder[0][1]:
                return [ctxt.with_match(1)]
        elif completions is not None:
            completions.add(self.arg)
        return []

    def pattern(self):
        return re.escape(self.arg)


class word_match(text_match):

    def pattern(self):
        return r'\b' + text_match.pattern(self) + r'\b'


class case_word_match(case_match):

    def pattern(self):
        return r'\b' + case_match.pattern(self) + r'\b'


class terminal_type_matcher(matcher):

    def __init__(self, tokentype, submatcher):
        matcher.__init__(self, tokentype)
        self.tokentype = tokentype
        self.submatcher = submatcher

    def match(self, ctxt, completions):
        if ctxt.remainder:
            if ctxt.remainder[0][0] == self.tokentype:
                return [ctxt.with_match(1)]
        elif completions is not None:
            self.submatcher.match(ctxt, completions)
        return []

    def __repr__(self):
        return '%s(%r, %r)' % (self.__class__.__name__, self.tokentype, self.submatcher)


class ParsingRuleSet:
    RuleSpecScanner = SaferScanner([
        (r'::=', lambda s, t: t),
        (r'\[[a-z0-9_]+\]=', lambda s, t: ('named_collector', t[1:-2])),
        (r'[a-z0-9_]+=', lambda s, t: ('named_symbol', t[:-1])),
        (r'/(\[\^?.[^]]*\]|[^/]|\\.)*/', lambda s, t: ('regex', t[1:-1].replace(r'\/', '/'))),
        (r'"([^"]|\\.)*"', lambda s, t: ('litstring', t)),
        (r'<[^>]*>', lambda s, t: ('reference', t[1:-1])),
        (r'\bJUNK\b', lambda s, t: ('junk', t)),
        (r'[@()|?*;]', lambda s, t: t),
        (r'\s+', None),
        (r'#[^\n]*', None),
    ], re.I | re.S)

    def __init__(self):
        self.ruleset = {}
        self.scanner = None
        self.terminals = []

    @classmethod
    def from_rule_defs(cls, rule_defs):
        prs = cls()
        prs.ruleset, prs.terminals = cls.parse_rules(rule_defs)
        return prs

    @classmethod
    def parse_rules(cls, rulestr):
        tokens, unmatched = cls.RuleSpecScanner.scan(rulestr)
        if unmatched:
            raise LexingError.from_text(rulestr, unmatched, msg="Syntax rules unparseable")
        rules = {}
        terminals = []
        tokeniter = iter(tokens)
        for t in tokeniter:
            if isinstance(t, tuple) and t[0] in ('reference', 'junk'):
                assign = tokeniter.next()
                if assign != '::=':
                    raise ValueError('Unexpected token %r; expected "::="' % (assign,))
                name = t[1]
                production = cls.read_rule_tokens_until(';', tokeniter)
                if isinstance(production, terminal_matcher):
                    terminals.append((name, production))
                    production = terminal_type_matcher(name, production)
                rules[name] = production
            else:
                raise ValueError('Unexpected token %r; expected name' % (t,))
        return rules, terminals

    @staticmethod
    def mkrule(pieces):
        if isinstance(pieces, (tuple, list)):
            if len(pieces) == 1:
                return pieces[0]
            return rule_series(pieces)
        return pieces

    @classmethod
    def read_rule_tokens_until(cls, endtoks, tokeniter):
        if isinstance(endtoks, basestring):
            endtoks = (endtoks,)
        counttarget = None
        if isinstance(endtoks, int):
            counttarget = endtoks
            endtoks = ()
        countsofar = 0
        myrules = []
        mybranches = [myrules]
        for t in tokeniter:
            countsofar += 1
            if t in endtoks:
                if len(mybranches) == 1:
                    return cls.mkrule(mybranches[0])
                return choice(map(cls.mkrule, mybranches))
            if isinstance(t, tuple):
                if t[0] == 'reference':
                    t = rule_reference(t[1])
                elif t[0] == 'litstring':
                    if t[1][1].isalnum() or t[1][1] == '_':
                        t = word_match(t[1])
                    else:
                        t = text_match(t[1])
                elif t[0] == 'regex':
                    t = regex_rule(t[1])
                elif t[0] == 'named_collector':
                    t = named_collector(t[1], cls.read_rule_tokens_until(1, tokeniter))
                elif t[0] == 'named_symbol':
                    t = named_symbol(t[1], cls.read_rule_tokens_until(1, tokeniter))
            elif t == '(':
                t = cls.read_rule_tokens_until(')', tokeniter)
            elif t == '?':
                t = one_or_none(myrules.pop(-1))
            elif t == '*':
                t = repeat(myrules.pop(-1))
            elif t == '@':
                x = tokeniter.next()
                if not isinstance(x, tuple) or x[0] != 'litstring':
                    raise ValueError("Unexpected token %r following '@'" % (x,))
                t = case_match(x[1])
            elif t == '|':
                myrules = []
                mybranches.append(myrules)
                continue
            else:
                raise ValueError('Unparseable rule token %r after %r' % (t, myrules[-1]))
            myrules.append(t)
            if countsofar == counttarget:
                if len(mybranches) == 1:
                    return cls.mkrule(mybranches[0])
                return choice(map(cls.mkrule, mybranches))
        raise ValueError('Unexpected end of rule tokens')

    def append_rules(self, rulestr):
        rules, terminals = self.parse_rules(rulestr)
        self.ruleset.update(rules)
        self.terminals.extend(terminals)
        if terminals:
            self.scanner = None  # recreate it if/when necessary

    def register_completer(self, func, rulename, symname):
        self.ruleset[(rulename, symname)] = func

    def make_lexer(self):
        def make_handler(name):
            if name == 'JUNK':
                return None
            return lambda s, t: (name, t, s.match.span())
        regexes = [(p.pattern(), make_handler(name)) for (name, p) in self.terminals]
        return SaferScanner(regexes, re.I | re.S).scan

    def lex(self, text):
        if self.scanner is None:
            self.scanner = self.make_lexer()
        tokens, unmatched = self.scanner(text)
        if unmatched:
            raise LexingError.from_text(text, unmatched, 'text could not be lexed')
        return tokens

    def parse(self, startsymbol, tokens, init_bindings=None):
        if init_bindings is None:
            init_bindings = {}
        ctxt = ParseContext(self.ruleset, init_bindings, (), tuple(tokens), startsymbol)
        pattern = self.ruleset[startsymbol]
        return pattern.match(ctxt, None)

    def whole_match(self, startsymbol, tokens, srcstr=None):
        bindings = {}
        if srcstr is not None:
            bindings['*SRC*'] = srcstr
        for c in self.parse(startsymbol, tokens, init_bindings=bindings):
            if not c.remainder:
                return c

    def lex_and_parse(self, text, startsymbol='Start'):
        return self.parse(startsymbol, self.lex(text), init_bindings={'*SRC*': text})

    def lex_and_whole_match(self, text, startsymbol='Start'):
        tokens = self.lex(text)
        return self.whole_match(startsymbol, tokens, srcstr=text)

    def complete(self, startsymbol, tokens, init_bindings=None):
        if init_bindings is None:
            init_bindings = {}
        ctxt = ParseContext(self.ruleset, init_bindings, (), tuple(tokens), startsymbol)
        pattern = self.ruleset[startsymbol]
        if init_bindings.get('*DEBUG*', False):
            completions = Debugotron(stream=sys.stderr)
        else:
            completions = set()
        pattern.match(ctxt, completions)
        return completions


import sys


class Debugotron(set):
    depth = 10

    def __init__(self, initializer=(), stream=sys.stdout):
        set.__init__(self, initializer)
        self.stream = stream

    def add(self, item):
        self._note_addition(item)
        set.add(self, item)

    def _note_addition(self, foo):
        self.stream.write("\nitem %r added by:\n" % (foo,))
        frame = sys._getframe().f_back.f_back
        for i in range(self.depth):
            name = frame.f_code.co_name
            filename = frame.f_code.co_filename
            lineno = frame.f_lineno
            if 'self' in frame.f_locals:
                clsobj = frame.f_locals['self']
                line = '%s.%s() (%s:%d)' % (clsobj, name, filename, lineno)
            else:
                line = '%s (%s:%d)' % (name, filename, lineno)
            self.stream.write('  - %s\n' % (line,))
            if i == 0 and 'ctxt' in frame.f_locals:
                self.stream.write('    - %s\n' % (frame.f_locals['ctxt'],))
            frame = frame.f_back

    def update(self, items):
        if items:
            self._note_addition(items)
        set.update(self, items)
