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

from __future__ import unicode_literals

import re
import six

LIGHT = 0o10


ansi_CSI = '\x1b['
ansi_seq = re.compile(re.escape(ansi_CSI) + r'(?P<params>[\x20-\x3f]*)(?P<final>[\x40-\x7e])')
ansi_cmd_SGR = 'm'  # set graphics rendition

color_defs = (
    (000, 'k', 'black'),
    (0o01, 'r', 'dark red'),
    (0o02, 'g', 'dark green'),
    (0o03, 'w', 'brown', 'dark yellow'),
    (0o04, 'b', 'dark blue'),
    (0o05, 'm', 'dark magenta', 'dark purple'),
    (0o06, 'c', 'dark cyan'),
    (0o07, 'n', 'light grey', 'light gray', 'neutral', 'dark white'),
    (0o10, 'B', 'dark grey', 'dark gray', 'light black'),
    (0o11, 'R', 'red', 'light red'),
    (0o12, 'G', 'green', 'light green'),
    (0o13, 'Y', 'yellow', 'light yellow'),
    (0o14, 'B', 'blue', 'light blue'),
    (0o15, 'M', 'magenta', 'purple', 'light magenta', 'light purple'),
    (0o16, 'C', 'cyan', 'light cyan'),
    (0o17, 'W', 'white', 'light white'),
)

colors_by_num = {}
colors_by_letter = {}
colors_by_name = {}
letters_by_num = {}

for colordef in color_defs:
    colorcode = colordef[0]
    colorletter = colordef[1]
    colors_by_num[colorcode] = nameset = set(colordef[2:])
    colors_by_letter[colorletter] = colorcode
    letters_by_num[colorcode] = colorletter
    for c in list(nameset):
        # equivalent names without spaces
        nameset.add(c.replace(' ', ''))
    for c in list(nameset):
        # with "bright" being an alias for "light"
        nameset.add(c.replace('light', 'bright'))
    for c in nameset:
        colors_by_name[c] = colorcode

class ColoredChar(object):
    def __init__(self, c, colorcode):
        self.c = c
        self._colorcode = colorcode

    def colorcode(self):
        return self._colorcode

    def plain(self):
        return self.c

    def __getattr__(self, name):
        return getattr(self.c, name)

    def ansi_color(self):
        clr = str(30 + (0o7 & self._colorcode))
        if self._colorcode & 0o10:
            clr = '1;' + clr
        return clr

    def __str__(self):
        return "<%s '%r'>" % (self.__class__.__name__, self.colored_repr())
    __repr__ = __str__

    def colored_version(self):
        return '%s0;%sm%s%s0m' % (ansi_CSI, self.ansi_color(), self.c, ansi_CSI)

    def colored_repr(self):
        if self.c == "'":
            crepr = r"\'"
        elif self.c == '"':
            crepr = self.c
        else:
            crepr = repr(self.c)[1:-1]
        return '%s0;%sm%s%s0m' % (ansi_CSI, self.ansi_color(), crepr, ansi_CSI)

    def colortag(self):
        return lookup_letter_from_code(self._colorcode)

class ColoredText(object):
    def __init__(self, source=''):
        if isinstance(source, six.text_type):
            plain, colors = self.parse_ansi_colors(source)
            self.chars = list(map(ColoredChar, plain, colors))
        else:
            # expected that source is an iterable of ColoredChars (or duck-typed as such)
            self.chars = tuple(source)

    def splitlines(self):
        lines = [[]]
        for c in self.chars:
            if c.plain() == '\n':
                lines.append([])
            else:
                lines[-1].append(c)
        return [self.__class__(line) for line in lines]

    def plain(self):
        return ''.join([c.plain() for c in self.chars])

    def __getitem__(self, index):
        return self.chars[index]

    @classmethod
    def parse_ansi_colors(cls, source):
        # note: strips all control sequences, even if not SGRs.
        colors = []
        plain = ''
        last = 0
        curclr = 0
        for match in ansi_seq.finditer(source):
            prevsegment = source[last:match.start()]
            plain += prevsegment
            colors.extend([curclr] * len(prevsegment))
            if match.group('final') == ansi_cmd_SGR:
                try:
                    curclr = cls.parse_sgr_param(curclr, match.group('params'))
                except ValueError:
                    pass
            last = match.end()
        prevsegment = source[last:]
        plain += prevsegment
        colors.extend([curclr] * len(prevsegment))
        return ''.join(plain), colors

    @staticmethod
    def parse_sgr_param(curclr, paramstr):
        oldclr = curclr
        args = list(map(int, paramstr.split(';')))
        for a in args:
            if a == 0:
                curclr = lookup_colorcode('neutral')
            elif a == 1:
                curclr |= LIGHT
            elif 30 <= a <= 37:
                curclr = (curclr & LIGHT) | (a - 30)
            else:
                # not supported renditions here; ignore for now
                pass
        return curclr

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, ''.join([c.colored_repr() for c in self.chars]))
    __str__ = __repr__

    def __iter__(self):
        return iter(self.chars)

    def colored_version(self):
        return ''.join([c.colored_version() for c in self.chars])

    def colortags(self):
        return ''.join([c.colortag() for c in self.chars])

def lookup_colorcode(name):
    return colors_by_name[name]

def lookup_colorname(code):
    return colors_by_num.get(code, 'Unknown-color-0%o' % code)

def lookup_colorletter(letter):
    return colors_by_letter[letter]

def lookup_letter_from_code(code):
    letr = letters_by_num.get(code, ' ')
    if letr == 'n':
        letr = ' '
    return letr
