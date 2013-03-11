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

# to configure behavior, define $CQL_TEST_HOST to the destination address
# for Thrift connections, and $CQL_TEST_PORT to the associated port.

from __future__ import with_statement

import re
from itertools import izip
from .basecase import (BaseTestCase, cqlshlog, dedent, at_a_time, cql,
                       TEST_HOST, TEST_PORT)
from .cassconnect import (get_test_keyspace, testrun_cqlsh, testcall_cqlsh,
                          cassandra_cursor, split_cql_commands, quote_name)
from .ansi_colors import (ColoredText, lookup_colorcode, lookup_colorname,
                          lookup_colorletter, ansi_seq)

CONTROL_C = '\x03'
CONTROL_D = '\x04'

class TestCqlshOutput(BaseTestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def assertNoHasColors(self, text, msg=None):
        self.assertNotRegexpMatches(text, ansi_seq, msg='ANSI CSI sequence found in %r' % text)

    def assertHasColors(self, text, msg=None):
        self.assertRegexpMatches(text, ansi_seq, msg=msg)

    def assertColored(self, coloredtext, colorname):
        wanted_colorcode = lookup_colorcode(colorname)
        for num, c in enumerate(coloredtext):
            if not c.isspace():
                ccolor = c.colorcode()
                self.assertEqual(ccolor, wanted_colorcode,
                                 msg='Output text %r (char #%d) is colored %s, not %s'
                                     % (coloredtext, num, lookup_colorname(ccolor), colorname))

    def assertColorFromTags(self, coloredtext, tags):
        for (char, tag) in izip(coloredtext, tags):
            if char.isspace():
                continue
            if tag.isspace():
                tag = 'n'  # neutral
            self.assertEqual(char.colorcode(), lookup_colorletter(tag),
                             msg='Coloring mismatch.\nExpected coloring: %s\n'
                                 'Actually got:      %s\ncolor code:        %s'
                                 % (tags, coloredtext.colored_version(), coloredtext.colortags()))

    def assertCqlverQueriesGiveColoredOutput(self, queries_and_expected_outputs,
                                             cqlver=(), **kwargs):
        if not isinstance(cqlver, (tuple, list)):
            cqlver = (cqlver,)
        for ver in cqlver:
            self.assertQueriesGiveColoredOutput(queries_and_expected_outputs, cqlver=ver, **kwargs)

    def assertQueriesGiveColoredOutput(self, queries_and_expected_outputs,
                                       sort_results=False, **kwargs):
        """
        Allow queries and expected output to be specified in structured tuples,
        along with expected color information.
        """
        with testrun_cqlsh(tty=True, **kwargs) as c:
            for query, expected in queries_and_expected_outputs:
                cqlshlog.debug('Testing %r' % (query,))
                output = c.cmd_and_response(query).lstrip("\r\n")
                c_output = ColoredText(output)
                pairs = at_a_time(dedent(expected).split('\n'), 2)
                outlines = c_output.splitlines()
                if sort_results:
                    if outlines[1].plain().startswith('---'):
                        firstpart = outlines[:2]
                        sortme = outlines[2:]
                    else:
                        firstpart = []
                        sortme = outlines
                    lastpart = []
                    while sortme[-1].plain() == '':
                        lastpart.append(sortme.pop(-1))
                    outlines = firstpart + sorted(sortme, key=lambda c:c.plain()) + lastpart
                for (plain, colorcodes), outputline in zip(pairs, outlines):
                    self.assertEqual(outputline.plain().rstrip(), plain)
                    self.assertColorFromTags(outputline, colorcodes)

    def test_no_color_output(self):
        for termname in ('', 'dumb', 'vt100'):
            cqlshlog.debug('TERM=%r' % termname)
            with testrun_cqlsh(tty=True, env={'TERM': termname}) as c:
                c.send('select * from has_all_types;\n')
                self.assertNoHasColors(c.read_to_next_prompt())
                c.send('select * from has_value_encoding_errors;\n')
                self.assertNoHasColors(c.read_to_next_prompt())
                c.send('select count(*) from has_all_types;\n')
                self.assertNoHasColors(c.read_to_next_prompt())
                c.send('totally invalid cql;\n')
                self.assertNoHasColors(c.read_to_next_prompt())

    def test_no_prompt_or_colors_output(self):
        # CQL queries and number of lines expected in output:
        queries = (('select * from has_all_types limit 1;', 5),
                   ('select * from has_value_encoding_errors limit 1;', 6))
        for termname in ('', 'dumb', 'vt100', 'xterm'):
            cqlshlog.debug('TERM=%r' % termname)
            for cql, lines_expected in queries:
                output, result = testcall_cqlsh(prompt=None, env={'TERM': termname},
                                                tty=False, input=cql + '\n')
                output = output.splitlines()
                for line in output:
                    self.assertNoHasColors(line)
                    self.assertNotRegexpMatches(line, r'^cqlsh\S*>')
                self.assertEqual(len(output), lines_expected,
                                 msg='output: %r' % '\n'.join(output))
                self.assertEqual(output[0], '')
                self.assertNicelyFormattedTableHeader(output[1])
                self.assertNicelyFormattedTableRule(output[2])
                self.assertNicelyFormattedTableData(output[3])
                self.assertEqual(output[4].strip(), '')

    def test_color_output(self):
        for termname in ('xterm', 'unknown-garbage'):
            cqlshlog.debug('TERM=%r' % termname)
            with testrun_cqlsh(tty=True, env={'TERM': termname}) as c:
                c.send('select * from has_all_types;\n')
                self.assertHasColors(c.read_to_next_prompt())
                c.send('select * from has_value_encoding_errors;\n')
                self.assertHasColors(c.read_to_next_prompt())
                c.send('select count(*) from has_all_types;\n')
                self.assertHasColors(c.read_to_next_prompt())
                c.send('totally invalid cql;\n')
                self.assertHasColors(c.read_to_next_prompt())

    def test_count_output(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ('select count(*) from has_all_types;', """
             count
             MMMMM
            -------

                 4
                 G

            """),

            ('select COUNT(*) FROM empty_table;', """
             count
             MMMMM
            -------

                 0
                 G

            """),

            ('select COUNT(*) FROM empty_composite_table;', """
             count
             MMMMM
            -------

                 0
                 G

            """),

            ('select COUNT(*) FROM twenty_rows_table limit 10;', """
             count
             MMMMM
            -------

                10
                GG

            """),

            ('select COUNT(*) FROM twenty_rows_table limit 1000000;', """
             count
             MMMMM
            -------

                20
                GG

            """),
        ), cqlver=(2, 3))

        # different results cql2/cql3
        q = 'select COUNT(*) FROM twenty_rows_composite_table limit 1000000;'

        self.assertQueriesGiveColoredOutput((
            (q, """
             count
             MMMMM
            -------

                 1
                GG

            """),
        ), cqlver=2)

        self.assertQueriesGiveColoredOutput((
            (q, """
             count
             MMMMM
            -------

                20
                GG

            """),
        ), cqlver=3)

    def test_dynamic_cf_output(self):
        self.assertQueriesGiveColoredOutput((
            ('select * from dynamic_columns;', """
             somekey,1 | 1.2,one point two
            nMMMMMMM G   MMM YYYYYYYYYYYYY
             somekey,2 | 2.3,two point three
             MMMMMMM G   MMM YYYYYYYYYYYYYYY
             somekey,3 | -0.0001,negative ten thousandth | 3.46,three point four six | 99,ninety-nine point oh
             MMMMMMM G   MMMMMMM YYYYYYYYYYYYYYYYYYYYYYY   MMMM YYYYYYYYYYYYYYYYYYYY n MM YYYYYYYYYYYYYYYYYYYY

            """),

            ('select somekey, 2.3 from dynamic_columns;', """
             somekey | 2.3
             MMMMMMM   MMM
            ---------+-----------------

                   1 |            null
                   G              RRRR
                   2 | two point three
                   G   YYYYYYYYYYYYYYY
                   3 |            null
                   G              RRRR

            """),

            ('select first 1 * from dynamic_columns where somekey = 2;', """
             2.3
             MMM
            -----------------

             two point three
             YYYYYYYYYYYYYYY

            """),

            ('select * from dynamic_columns where somekey = 2;', """
             somekey | 2.3
             MMMMMMM   MMM
            ---------+-----------------

                   2 | two point three
                   G   YYYYYYYYYYYYYYY

            """),
        ), cqlver=2, sort_results=True)

    def test_static_cf_output(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ("select a, b from twenty_rows_table where a in ('1', '13', '2');", """
             a  | b
             MM   MM
            ----+----

              1 |  1
             YY   YY
             13 | 13
             YY   YY
              2 |  2
             YY   YY

            """),
        ), cqlver=(2, 3))

        self.assertQueriesGiveColoredOutput((
            ('select * from dynamic_columns;', """
             somekey | column1 | value
             MMMMMMM   MMMMMMM   MMMMM
            ---------+---------+-------------------------

                   1 |     1.2 |           one point two
                  GG   GGGGGGG   YYYYYYYYYYYYYYYYYYYYYYY
                   2 |     2.3 |         two point three
                  GG   GGGGGGG   YYYYYYYYYYYYYYYYYYYYYYY
                   3 |      99 |    ninety-nine point oh
                  GG   GGGGGGG   YYYYYYYYYYYYYYYYYYYYYYY
                   3 |    3.46 |    three point four six
                  GG   GGGGGGG   YYYYYYYYYYYYYYYYYYYYYYY
                   3 | -0.0001 | negative ten thousandth
                  GG   GGGGGGG   YYYYYYYYYYYYYYYYYYYYYYY

            """),
        ), cqlver=3, sort_results=True)

    def test_empty_cf_output(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ('select * from empty_table;', """
            """),
        ), cqlver=(2, 3))

        q = 'select * from has_all_types where num = 999;'
        self.assertQueriesGiveColoredOutput((
            (q, """
             num
             MMM
            -----

             999
             GGG

            """),
        ), cqlver=2)

        # same query should show up as empty in cql 3
        self.assertQueriesGiveColoredOutput((
            (q, """
            """),
        ), cqlver=3)

    def test_columnless_key_output(self):
        q = "select a from twenty_rows_table where a in ('1', '2', '-9192');"
        self.assertQueriesGiveColoredOutput((
            (q, """
             a
             MMMMM
            -------

                 1
             YYYYY
                 2
             YYYYY
             -9192
             YYYYY

            """),
        ), cqlver=2)

        self.assertQueriesGiveColoredOutput((
            (q, """
             a
             M
            ---

             1
             Y
             2
             Y

            """),
        ), cqlver=3)

    def test_numeric_output(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ('''select intcol, bigintcol, varintcol \
                  from has_all_types \
                 where num in (0, 1, 2, 3);''', """
             intcol      | bigintcol            | varintcol
             MMMMMM        MMMMMMMMM              MMMMMMMMM
            -------------+----------------------+-----------------------------

                     -12 |  1234567890123456789 |  10000000000000000000000000
             GGGGGGGGGGG   GGGGGGGGGGGGGGGGGGGG   GGGGGGGGGGGGGGGGGGGGGGGGGGG
              2147483647 |  9223372036854775807 |                           9
             GGGGGGGGGGG   GGGGGGGGGGGGGGGGGGGG   GGGGGGGGGGGGGGGGGGGGGGGGGGG
                       0 |                    0 |                           0
             GGGGGGGGGGG   GGGGGGGGGGGGGGGGGGGG   GGGGGGGGGGGGGGGGGGGGGGGGGGG
             -2147483648 | -9223372036854775808 | -10000000000000000000000000
             GGGGGGGGGGG   GGGGGGGGGGGGGGGGGGGG   GGGGGGGGGGGGGGGGGGGGGGGGGGG

            """),

            ('''select decimalcol, doublecol, floatcol \
                  from has_all_types \
                 where num in (0, 1, 2, 3);''', """
             decimalcol       | doublecol | floatcol
             MMMMMMMMMM         MMMMMMMMM   MMMMMMMM
            ------------------+-----------+----------

                  19952.11882 |         1 |     -2.1
             GGGGGGGGGGGGGGGG     GGGGGGG      GGGGG
                        1E-14 |     1e+07 |    1e+05
             GGGGGGGGGGGGGGGG     GGGGGGG      GGGGG
                          0.0 |         0 |        0
             GGGGGGGGGGGGGGGG     GGGGGGG      GGGGG
             10.0000000000000 |   -1004.1 |    1e+08
             GGGGGGGGGGGGGGGG     GGGGGGG      GGGGG

            """),
        ), cqlver=(2, 3))

        self.assertQueriesGiveColoredOutput((
            ('''select * from dynamic_columns where somekey = 3;''', """
             somekey | -0.0001                 | 3.46                 | 99
             MMMMMMM   MMMMMMM                   MMMM                   MM
            ---------+-------------------------+----------------------+----------------------

                   3 | negative ten thousandth | three point four six | ninety-nine point oh
                   G   YYYYYYYYYYYYYYYYYYYYYYY   YYYYYYYYYYYYYYYYYYYY   YYYYYYYYYYYYYYYYYYYY

            """),
        ), cqlver=2)

    def test_timestamp_output(self):
        self.assertQueriesGiveColoredOutput((
            ('''select timestampcol from has_all_types where num = 0;''', """
             timestampcol
             MMMMMMMMMMMM
            --------------------------

             2012-05-14 12:53:20+0000
             GGGGGGGGGGGGGGGGGGGGGGGG

            """),
        ), env={'TZ': 'Etc/UTC'})

        self.assertQueriesGiveColoredOutput((
            ('''select timestampcol from has_all_types where num = 0;''', """
             timestampcol
             MMMMMMMMMMMM
            --------------------------

             2012-05-14 07:53:20-0500
             GGGGGGGGGGGGGGGGGGGGGGGG

            """),
        ), env={'TZ': 'EST'})

    def test_boolean_output(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ('select num, booleancol from has_all_types where num in (0, 1, 2, 3);', """
             num | booleancol
             MMM   MMMMMMMMMM
            -----+------------

               0 |       True
               G        GGGGG
               1 |       True
               G        GGGGG
               2 |      False
               G        GGGGG
               3 |      False
               G        GGGGG

            """),
        ), cqlver=(2, 3))

    def test_null_output(self):
        # column with metainfo but no values
        self.assertCqlverQueriesGiveColoredOutput((
            ("select k, c, notthere from undefined_values_table where k in ('k1', 'k2');", """
             k  | c  | notthere
             M    M    MMMMMMMM
            ----+----+----------

             k1 | c1 |     null
             YY   YY       RRRR
             k2 | c2 |     null
             YY   YY       RRRR

            """),
        ), cqlver=(2, 3))

        # all-columns, including a metainfo column has no values (cql3)
        self.assertQueriesGiveColoredOutput((
            ("select * from undefined_values_table where k in ('k1', 'k2');", """
             k  | c  | notthere
             M    M    MMMMMMMM
            ----+----+----------

             k1 | c1 |     null
             YY   YY       RRRR
             k2 | c2 |     null
             YY   YY       RRRR

            """),
        ), cqlver=3)

        # all-columns, including a metainfo column has no values (cql2)
        self.assertQueriesGiveColoredOutput((
            ("select * from undefined_values_table where k in ('k1', 'k2');", """
             k  | c
             M    M
            ----+----

             k1 | c1
             YY   YY
             k2 | c2
             YY   YY

            """),
        ), cqlver=2)

        # column not in metainfo, which has no values (invalid query in cql3)
        self.assertQueriesGiveColoredOutput((
            ("select num, fakecol from has_all_types where num in (1, 2);", """
             num
             MMM
            -----

               1
             GGG
               2
             GGG

            """),
        ), cqlver=2)

    def test_string_output_ascii(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ("select * from ascii_with_invalid_and_special_chars where k in (0, 1, 2, 3, 4);", r"""
             k | val
             M   MMM
            ---+-----------------------------------------------

             0 |                                    newline:\n
             G                                      YYYYYYYYmm
             1 |                         return\rand null\x00!
             G                           YYYYYYmmYYYYYYYYmmmmY
             2 | \x00\x01\x02\x03\x04\x05control chars\x06\x07
             G   mmmmmmmmmmmmmmmmmmmmmmmmYYYYYYYYYYYYYmmmmmmmm
             3 |                       \xfe\xffbyte order mark 
             G                         mmmmmmmmYYYYYYYYYYYYYYY
             4 |                      fake special chars\x00\n
             G                        YYYYYYYYYYYYYYYYYYYYYYYY

            """),
        ), cqlver=(2, 3))

    def test_string_output_utf8(self):
        # many of these won't line up visually here, to keep the source code
        # here ascii-only. note that some of the special Unicode characters
        # here will render as double-width or zero-width in unicode-aware
        # terminals, but the color-checking machinery here will still treat
        # it as one character, so those won't seem to line up visually either.

        self.assertCqlverQueriesGiveColoredOutput((
            ("select * from utf8_with_special_chars where k in (0, 1, 2, 3, 4, 5, 6);", u"""
             k | val
             M   MMM
            ---+-------------------------------

             0 |                 Normal string
             G                   YYYYYYYYYYYYY
             1 |         Text with\\nnewlines\\n
             G           YYYYYYYYYmmYYYYYYYYmm
             2 |  Text with embedded \\x01 char
             G    YYYYYYYYYYYYYYYYYYYmmmmYYYYY
             3 | \u24c8\u24c5\u24ba\u24b8\u24be\u24b6\u24c1\u2008\u249e\u24a3\u249c\u24ad\u24ae and normal ones
             G   YYYYYYYYYYYYYYYYYYYYYYYYYYYYY
             4 |          double wides: \u2f91\u2fa4\u2f9a
             G            YYYYYYYYYYYYYYYYY
             5 |               zero width\u200bspace
             G                 YYYYYYYYYYYYYYYY
             6 |      fake special chars\\x00\\n
             G        YYYYYYYYYYYYYYYYYYYYYYYY

            """.encode('utf-8')),
        ), cqlver=(2, 3), env={'LANG': 'en_US.UTF-8'})

    def test_blob_output(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ("select num, blobcol from has_all_types where num in (0, 1, 2, 3);", r"""
             num | blobcol
             MMM   MMMMMMM
            -----+----------------------

               0 | 0x000102030405fffefd
               G   mmmmmmmmmmmmmmmmmmmm
               1 | 0xffffffffffffffffff
               G   mmmmmmmmmmmmmmmmmmmm
               2 |                   0x
               G   mmmmmmmmmmmmmmmmmmmm
               3 |                 0x80
               G   mmmmmmmmmmmmmmmmmmmm

            """),
        ), cqlver=(2, 3))

    def test_colname_decoding_errors(self):
        # not clear how to achieve this situation in the first place. the
        # validator works pretty well, and we can't change the comparator
        # after insertion.
        #
        # guess we could monkey-patch cqlsh or python-cql source to
        # explicitly generate an exception on the deserialization of type X..
        pass

    def test_colval_decoding_errors(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ("select * from has_value_encoding_errors;", r"""
             pkey | utf8col
             MMMM   MMMMMMM
            ------+--------------------

                A | '\x00\xff\x00\xff'
                Y   RRRRRRRRRRRRRRRRRR


            Failed to decode value '\x00\xff\x00\xff' (for column 'utf8col') as text: 'utf8' codec can't decode byte 0xff in position 1: invalid start byte
            RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
            """),
        ), cqlver=(2, 3))

    def test_key_decoding_errors(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ("select * from has_key_encoding_errors;", r"""
             pkey               | col
             MMMM                 MMM
            --------------------+----------

             '\x00\xff\x02\x8f' | whatever
             RRRRRRRRRRRRRRRRRR   YYYYYYYY


            Failed to decode value '\x00\xff\x02\x8f' (for column 'pkey') as text: 'utf8' codec can't decode byte 0xff in position 1: invalid start byte
            RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
            """),
        ), cqlver=(2, 3))

    def test_prompt(self):
        with testrun_cqlsh(tty=True, keyspace=None, cqlver=2) as c:
            self.assertEqual(c.output_header.splitlines()[-1], 'cqlsh> ')

            c.send('\n')
            output = c.read_to_next_prompt().replace('\r\n', '\n')
            self.assertEqual(output, '\ncqlsh> ')

            cmd = "USE '%s';\n" % get_test_keyspace().replace("'", "''")
            c.send(cmd)
            output = c.read_to_next_prompt().replace('\r\n', '\n')
            self.assertEqual(output, '%scqlsh:%s> ' % (cmd, get_test_keyspace()))

            c.send('use system;\n')
            output = c.read_to_next_prompt().replace('\r\n', '\n')
            self.assertEqual(output, 'use system;\ncqlsh:system> ')

            c.send('use NONEXISTENTKEYSPACE;\n')
            outputlines = c.read_to_next_prompt().splitlines()
            self.assertEqual(outputlines[0], 'use NONEXISTENTKEYSPACE;')
            self.assertEqual(outputlines[3], 'cqlsh:system> ')
            midline = ColoredText(outputlines[1])
            self.assertEqual(midline.plain(),
                             "Bad Request: Keyspace 'NONEXISTENTKEYSPACE' does not exist")
            self.assertColorFromTags(midline,
                             "RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")

    def check_describe_keyspace(self, fullcqlver):
        with testrun_cqlsh(tty=True, cqlver=fullcqlver) as c:
            ks = get_test_keyspace()
            qks = quote_name(fullcqlver, ks)
            for cmd in ('describe keyspace', 'desc keyspace'):
                for givename in ('system', '', qks):
                    for semicolon in ('', ';'):
                        fullcmd = cmd + (' ' if givename else '') + givename + semicolon
                        desc = c.cmd_and_response(fullcmd)
                        self.check_describe_keyspace_output(desc, givename or qks, fullcqlver)

            # try to actually execute that last keyspace description, with a
            # new keyspace name
            new_ks_name = 'COPY_OF_' + ks
            copy_desc = desc.replace(ks, new_ks_name)
            statements = split_cql_commands(copy_desc, cqlver=fullcqlver)
            do_drop = True

            with cassandra_cursor(cql_version=fullcqlver) as curs:
                try:
                    for stmt in statements:
                        cqlshlog.debug('TEST EXEC: %s' % stmt)
                        try:
                            curs.execute(stmt)
                        except cql.ProgrammingError, e:
                            # expected sometimes under cql3, since some existing
                            # tables made under cql2 are not recreatable with cql3
                            for errmsg in ('No definition found that is not part of the PRIMARY KEY',
                                           "mismatched input '<' expecting EOF"):
                                if errmsg in str(e):
                                    do_drop = False
                                    break
                            else:
                                raise
                    # maybe we should do some checks that the two keyspaces
                    # match up as much as we expect at this point?
                finally:
                    curs.execute('use system')
                    if do_drop:
                        curs.execute('drop keyspace %s' % quote_name(fullcqlver, new_ks_name))

    def check_describe_keyspace_output(self, output, qksname, fullcqlver):
        expected_bits = [r'(?im)^CREATE KEYSPACE %s WITH\b' % re.escape(qksname),
                         r'(?im)^USE \S+;$',
                         r';\s*$']
        if fullcqlver == '3.0.0':
            expected_bits.append(r'\breplication = {\n  \'class\':')
        else:
            expected_bits.append(r'\bstrategy_class =')
        for expr in expected_bits:
            self.assertRegexpMatches(output, expr)

    def test_describe_keyspace_output(self):
        for v in ('2.0.0', '3.0.0'):
            self.check_describe_keyspace(v)

    def test_describe_columnfamily_output(self):
        # we can change these to regular expressions if/when it makes sense
        # to do so; these will likely be subject to lots of adjustments.

        table_desc2 = dedent("""

            CREATE TABLE has_all_types (
              num int PRIMARY KEY,
              intcol int,
              timestampcol timestamp,
              floatcol float,
              uuidcol uuid,
              bigintcol bigint,
              doublecol double,
              booleancol boolean,
              decimalcol decimal,
              asciicol ascii,
              blobcol blob,
              varcharcol text,
              textcol text,
              varintcol varint
            ) WITH
              comment='' AND
              comparator=text AND
              read_repair_chance=0.100000 AND
              gc_grace_seconds=864000 AND
              default_validation=text AND
              min_compaction_threshold=4 AND
              max_compaction_threshold=32 AND
              replicate_on_write='true' AND
              compaction_strategy_class='SizeTieredCompactionStrategy' AND
              compression_parameters:sstable_compression='LZ4Compressor';

        """)

        # note columns are now comparator-ordered instead of original-order.
        table_desc3 = dedent("""

            CREATE TABLE has_all_types (
              num int PRIMARY KEY,
              asciicol ascii,
              bigintcol bigint,
              blobcol blob,
              booleancol boolean,
              decimalcol decimal,
              doublecol double,
              floatcol float,
              intcol int,
              textcol text,
              timestampcol timestamp,
              uuidcol uuid,
              varcharcol text,
              varintcol varint
            ) WITH COMPACT STORAGE AND
              bloom_filter_fp_chance=0.010000 AND
              caching='KEYS_ONLY' AND
              comment='' AND
              dclocal_read_repair_chance=0.000000 AND
              gc_grace_seconds=864000 AND
              read_repair_chance=0.100000 AND
              replicate_on_write='true' AND
              populate_io_cache_on_flush='false' AND
              compaction={'class': 'SizeTieredCompactionStrategy'} AND
              compression={'sstable_compression': 'LZ4Compressor'};

        """)

        for v in ('2.0.0', '3.0.0'):
            with testrun_cqlsh(tty=True, cqlver=v) as c:
                for cmdword in ('describe table', 'desc columnfamily'):
                    for semicolon in (';', ''):
                        output = c.cmd_and_response('%s has_all_types%s' % (cmdword, semicolon))
                        self.assertNoHasColors(output)
                        self.assertEqual(output, {'2.0.0': table_desc2, '3.0.0': table_desc3}[v])

    def test_describe_columnfamilies_output(self):
        output_re = r'''
            \n
            Keyspace [ ] (?P<ksname> \S+ ) \n
            -----------* \n
            (?P<cfnames> .*? )
            \n
        '''

        ks = get_test_keyspace()

        with testrun_cqlsh(tty=True, keyspace=None, cqlver=3) as c:

            # when not in a keyspace
            for cmdword in ('DESCRIBE COLUMNFAMILIES', 'desc tables'):
                for semicolon in (';', ''):
                    ksnames = []
                    output = c.cmd_and_response(cmdword + semicolon)
                    self.assertNoHasColors(output)
                    self.assertRegexpMatches(output, '(?xs) ^ ( %s )+ $' % output_re)

                    for section in re.finditer('(?xs)' + output_re, output):
                        ksname = section.group('ksname')
                        ksnames.append(ksname)
                        cfnames = section.group('cfnames')
                        self.assertNotIn('\n\n', cfnames)
                        if ksname == ks:
                            self.assertIn('ascii_with_invalid_and_special_chars', cfnames)

                    self.assertIn('system', ksnames)
                    self.assertIn(quote_name('3.0.0', ks), ksnames)

            # when in a keyspace
            c.send('USE %s;\n' % quote_name('3.0.0', ks))
            c.read_to_next_prompt()

            for cmdword in ('DESCRIBE COLUMNFAMILIES', 'desc tables'):
                for semicolon in (';', ''):
                    output = c.cmd_and_response(cmdword + semicolon)
                    self.assertNoHasColors(output)
                    self.assertEqual(output[0], '\n')
                    self.assertEqual(output[-1], '\n')
                    self.assertNotIn('Keyspace %s' % quote_name('3.0.0', ks), output)
                    self.assertIn('has_value_encoding_errors', output)
                    self.assertIn('undefined_values_table', output)

    def test_describe_cluster_output(self):
        output_re = r'''(?x)
            ^
            \n
            Cluster: [ ] (?P<clustername> .* ) \n
            Partitioner: [ ] (?P<partitionername> .* ) \n
            Snitch: [ ] (?P<snitchname> .* ) \n
            \n
        '''

        ringinfo_re = r'''
            Range[ ]ownership: \n
            (
              [ ] .*? [ ][ ] \[ ( \d+ \. ){3} \d+ \] \n
            )+
            \n
        '''

        with testrun_cqlsh(tty=True, keyspace=None, cqlver=3) as c:

            # not in a keyspace
            for semicolon in ('', ';'):
                output = c.cmd_and_response('describe cluster' + semicolon)
                self.assertNoHasColors(output)
                self.assertRegexpMatches(output, output_re + '$')

            c.send('USE %s;\n' % quote_name('3.0.0', get_test_keyspace()))
            c.read_to_next_prompt()

            for semicolon in ('', ';'):
                output = c.cmd_and_response('describe cluster' + semicolon)
                self.assertNoHasColors(output)
                self.assertRegexpMatches(output, output_re + ringinfo_re + '$')

    def test_describe_schema_output(self):
        with testrun_cqlsh(tty=True) as c:
            for semicolon in ('', ';'):
                output = c.cmd_and_response('desc schema' + semicolon)
                self.assertNoHasColors(output)
                self.assertRegexpMatches(output, '^\nCREATE KEYSPACE')
                self.assertIn("\nCREATE KEYSPACE system WITH replication = {\n  'class': 'LocalStrategy'\n};\n",
                              output)
                self.assertRegexpMatches(output, ';\s*$')

    def test_show_output(self):
        with testrun_cqlsh(tty=True) as c:
            output = c.cmd_and_response('show version;')
            self.assertRegexpMatches(output,
                    '^\[cqlsh \S+ \| Cassandra \S+ \| CQL spec \S+ \| Thrift protocol \S+\]$')

            output = c.cmd_and_response('show host;')
            self.assertHasColors(output)
            self.assertRegexpMatches(output, '^Connected to .* at %s:%d\.$'
                                             % (re.escape(TEST_HOST), TEST_PORT))

    def test_show_assumptions_output(self):
        expected_output = '\nUSE %s;\n\n' % quote_name('', get_test_keyspace())

        with testrun_cqlsh(tty=True) as c:
            output = c.cmd_and_response('show assumptions')
            self.assertEqual(output, 'No overrides.\n')

            c.cmd_and_response('assume dynamic_values VALUES aRe uuid;')
            expected_output += 'ASSUME dynamic_values VALUES ARE uuid;\n'
            output = c.cmd_and_response('show assumptions')
            self.assertEqual(output, expected_output + '\n')

            c.cmd_and_response('Assume has_all_types names arE float;')
            expected_output += 'ASSUME has_all_types NAMES ARE float;\n'
            output = c.cmd_and_response('show assumptions')
            self.assertEqual(output, expected_output + '\n')

            c.cmd_and_response('assume twenty_rows_table ( b ) values are decimal;')
            expected_output += 'ASSUME twenty_rows_table(b) VALUES ARE decimal;\n'
            output = c.cmd_and_response('show assumptions')
            self.assertEqual(output, expected_output + '\n')

    def test_eof_prints_newline(self):
        with testrun_cqlsh(tty=True) as c:
            c.send(CONTROL_D)
            out = c.read_lines(1)[0].replace('\r', '')
            self.assertEqual(out, '\n')
            with self.assertRaises(BaseException) as cm:
                c.read_lines(1)
            self.assertIn(type(cm.exception), (EOFError, OSError))

    def test_exit_prints_no_newline(self):
        for semicolon in ('', ';'):
            with testrun_cqlsh(tty=True) as c:
                cmd = 'exit%s\n' % semicolon
                c.send(cmd)
                out = c.read_lines(1)[0].replace('\r', '')
                self.assertEqual(out, cmd)
                with self.assertRaises(BaseException) as cm:
                    c.read_lines(1)
                self.assertIn(type(cm.exception), (EOFError, OSError))

    def test_help_types(self):
        with testrun_cqlsh(tty=True) as c:
            output = c.cmd_and_response('help types')


    def test_help(self):
        pass

    def test_printing_parse_error(self):
        pass

    def test_printing_lex_error(self):
        pass

    def test_multiline_statements(self):
        pass

    def test_cancel_statement(self):
        pass

    def test_printing_integrity_error(self):
        pass

    def test_printing_cql_error(self):
        pass

    def test_empty_line(self):
        pass
