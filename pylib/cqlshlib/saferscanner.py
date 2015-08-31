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

# SaferScanner is just like re.Scanner, but it neuters any grouping in the lexicon
# regular expressions and throws an error on group references, named groups, or
# regex in-pattern flags. Any of those can break correct operation of Scanner.

import re
from sre_constants import BRANCH, SUBPATTERN, GROUPREF, GROUPREF_IGNORE, GROUPREF_EXISTS


class SaferScanner(re.Scanner):

    def __init__(self, lexicon, flags=0):
        self.lexicon = lexicon
        p = []
        s = re.sre_parse.Pattern()
        s.flags = flags
        for phrase, action in lexicon:
            p.append(re.sre_parse.SubPattern(s, [
                (SUBPATTERN, (len(p) + 1, self.subpat(phrase, flags))),
            ]))
        s.groups = len(p) + 1
        p = re.sre_parse.SubPattern(s, [(BRANCH, (None, p))])
        self.p = p
        self.scanner = re.sre_compile.compile(p)

    @classmethod
    def subpat(cls, phrase, flags):
        return cls.scrub_sub(re.sre_parse.parse(phrase, flags), flags)

    @classmethod
    def scrub_sub(cls, sub, flags):
        scrubbedsub = []
        seqtypes = (type(()), type([]))
        for op, arg in sub.data:
            if type(arg) in seqtypes:
                arg = [cls.scrub_sub(a, flags) if isinstance(a, re.sre_parse.SubPattern) else a
                       for a in arg]
            if op in (BRANCH, SUBPATTERN):
                arg = [None] + arg[1:]
            if op in (GROUPREF, GROUPREF_IGNORE, GROUPREF_EXISTS):
                raise ValueError("Group references not allowed in SaferScanner lexicon")
            scrubbedsub.append((op, arg))
        if sub.pattern.groupdict:
            raise ValueError("Named captures not allowed in SaferScanner lexicon")
        if sub.pattern.flags ^ flags:
            raise ValueError("RE flag setting not allowed in SaferScanner lexicon (%s)" % (bin(sub.pattern.flags),))
        return re.sre_parse.SubPattern(sub.pattern, scrubbedsub)
