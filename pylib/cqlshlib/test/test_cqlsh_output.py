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
from .basecase import (BaseTestCase, cqlshlog, dedent, at_a_time, cqlsh,
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
                                             cqlver=(cqlsh.DEFAULT_CQLVER,), **kwargs):
        if not isinstance(cqlver, (tuple, list)):
            cqlver = (cqlver,)
        for ver in cqlver:
            self.assertQueriesGiveColoredOutput(queries_and_expected_outputs, cqlver=ver, **kwargs)

    def assertQueriesGiveColoredOutput(self, queries_and_expected_outputs, **kwargs):
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
                for (plain, colorcodes), outputline in zip(pairs, outlines):
                    self.assertEqual(outputline.plain().rstrip(), plain)
                    self.assertColorFromTags(outputline, colorcodes)

    def test_no_color_output(self):
        for termname in ('', 'dumb', 'vt100'):
            cqlshlog.debug('TERM=%r' % termname)
            with testrun_cqlsh(tty=True, env={'TERM': termname}) as c:
                c.send('select * from has_all_types;\n')
                self.assertNoHasColors(c.read_to_next_prompt())
                c.send('select count(*) from has_all_types;\n')
                self.assertNoHasColors(c.read_to_next_prompt())
                c.send('totally invalid cql;\n')
                self.assertNoHasColors(c.read_to_next_prompt())

    def test_no_prompt_or_colors_output(self):
        for termname in ('', 'dumb', 'vt100', 'xterm'):
            cqlshlog.debug('TERM=%r' % termname)
            query = 'select * from has_all_types limit 1;'
            output, result = testcall_cqlsh(prompt=None, env={'TERM': termname},
                                            tty=False, input=query + '\n')
            output = output.splitlines()
            for line in output:
                self.assertNoHasColors(line)
                self.assertNotRegexpMatches(line, r'^cqlsh\S*>')
            self.assertTrue(6 <= len(output) <= 8,
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

                 5
                 G


            (1 rows)
            nnnnnnnn
            """),

            ('select COUNT(*) FROM empty_table;', """
             count
             MMMMM
            -------

                 0
                 G


            (1 rows)
            nnnnnnnn
            """),

            ('select COUNT(*) FROM empty_composite_table;', """
             count
             MMMMM
            -------

                 0
                 G


            (1 rows)
            nnnnnnnn
            """),

            ('select COUNT(*) FROM twenty_rows_table limit 10;', """
             count
             MMMMM
            -------

                10
                GG


            (1 rows)
            nnnnnnnn
            """),

            ('select COUNT(*) FROM twenty_rows_table limit 1000000;', """
             count
             MMMMM
            -------

                20
                GG


            (1 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

        q = 'select COUNT(*) FROM twenty_rows_composite_table limit 1000000;'
        self.assertQueriesGiveColoredOutput((
            (q, """
             count
             MMMMM
            -------

                20
                GG


            (1 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

    def test_static_cf_output(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ("select a, b from twenty_rows_table where a in ('1', '13', '2');", """
             a  | b
             RR   MM
            ----+----

              1 |  1
             YY   YY
             13 | 13
             YY   YY
              2 |  2
             YY   YY


            (3 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

        self.assertQueriesGiveColoredOutput((
            ('select * from dynamic_columns;', """
             somekey | column1 | value
             RRRRRRR   CCCCCCC   MMMMM
            ---------+---------+-------------------------

                   1 |     1.2 |           one point two
                  GG   GGGGGGG   YYYYYYYYYYYYYYYYYYYYYYY
                   2 |     2.3 |         two point three
                  GG   GGGGGGG   YYYYYYYYYYYYYYYYYYYYYYY
                   3 | -0.0001 | negative ten thousandth
                  GG   GGGGGGG   YYYYYYYYYYYYYYYYYYYYYYY
                   3 |    3.46 |    three point four six
                  GG   GGGGGGG   YYYYYYYYYYYYYYYYYYYYYYY
                   3 |      99 |    ninety-nine point oh
                  GG   GGGGGGG   YYYYYYYYYYYYYYYYYYYYYYY


            (5 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

    def test_empty_cf_output(self):
        # we print the header after CASSANDRA-6910
        self.assertCqlverQueriesGiveColoredOutput((
            ('select * from empty_table;', """
             lonelykey | lonelycol
             RRRRRRRRR   MMMMMMMMM
            -----------+-----------


            (0 rows)
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

        q = 'select * from has_all_types where num = 999;'

        # same query should show up as empty in cql 3
        self.assertQueriesGiveColoredOutput((
            (q, """
             num | asciicol | bigintcol | blobcol | booleancol | decimalcol | doublecol | floatcol | intcol | textcol | timestampcol | uuidcol | varcharcol | varintcol
             RRR   MMMMMMMM   MMMMMMMMM   MMMMMMM   MMMMMMMMMM   MMMMMMMMMM   MMMMMMMMM   MMMMMMMM   MMMMMM   MMMMMMM   MMMMMMMMMMMM   MMMMMMM   MMMMMMMMMM   MMMMMMMMM
            -----+----------+-----------+---------+------------+------------+-----------+----------+--------+---------+--------------+---------+------------+-----------


            (0 rows)
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

    def test_columnless_key_output(self):
        q = "select a from twenty_rows_table where a in ('1', '2', '-9192');"

        self.assertQueriesGiveColoredOutput((
            (q, """
             a
             R
            ---

             1
             Y
             2
             Y


            (2 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

    def test_numeric_output(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ('''select intcol, bigintcol, varintcol \
                  from has_all_types \
                 where num in (0, 1, 2, 3, 4);''', """
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
                         |                      |
             nnnnnnnnnnn   nnnnnnnnnnnnnnnnnnnn   nnnnnnnnnnnnnnnnnnnnnnnnnnn


            (5 rows)
            nnnnnnnn
            """),

            ('''select decimalcol, doublecol, floatcol \
                  from has_all_types \
                 where num in (0, 1, 2, 3, 4);''', """
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
                              |           |
             nnnnnnnnnnnnnnnn     nnnnnnn      nnnnn


            (5 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

    def test_timestamp_output(self):
        self.assertQueriesGiveColoredOutput((
            ('''select timestampcol from has_all_types where num = 0;''', """
             timestampcol
             MMMMMMMMMMMM
            --------------------------

             2012-05-14 12:53:20+0000
             GGGGGGGGGGGGGGGGGGGGGGGG


            (1 rows)
            nnnnnnnn
            """),
        ), env={'TZ': 'Etc/UTC'})

        self.assertQueriesGiveColoredOutput((
            ('''select timestampcol from has_all_types where num = 0;''', """
             timestampcol
             MMMMMMMMMMMM
            --------------------------

             2012-05-14 07:53:20-0500
             GGGGGGGGGGGGGGGGGGGGGGGG


            (1 rows)
            nnnnnnnn
            """),
        ), env={'TZ': 'EST'})

    def test_boolean_output(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ('select num, booleancol from has_all_types where num in (0, 1, 2, 3);', """
             num | booleancol
             RRR   MMMMMMMMMM
            -----+------------

               0 |       True
               G        GGGGG
               1 |       True
               G        GGGGG
               2 |      False
               G        GGGGG
               3 |      False
               G        GGGGG


            (4 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

    def test_null_output(self):
        # column with metainfo but no values
        self.assertCqlverQueriesGiveColoredOutput((
            ("select k, c, notthere from undefined_values_table where k in ('k1', 'k2');", """
             k  | c  | notthere
             R    M    MMMMMMMM
            ----+----+----------

             k1 | c1 |     null
             YY   YY       RRRR
             k2 | c2 |     null
             YY   YY       RRRR


            (2 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

        # all-columns, including a metainfo column has no values (cql3)
        self.assertQueriesGiveColoredOutput((
            ("select * from undefined_values_table where k in ('k1', 'k2');", """
             k  | c  | notthere
             R    M    MMMMMMMM
            ----+----+----------

             k1 | c1 |     null
             YY   YY       RRRR
             k2 | c2 |     null
             YY   YY       RRRR


            (2 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

    def test_string_output_ascii(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ("select * from ascii_with_special_chars where k in (0, 1, 2, 3);", r"""
             k | val
             R   MMM
            ---+-----------------------------------------------

             0 |                                    newline:\n
             G                                      YYYYYYYYmm
             1 |                         return\rand null\x00!
             G                           YYYYYYmmYYYYYYYYmmmmY
             2 | \x00\x01\x02\x03\x04\x05control chars\x06\x07
             G   mmmmmmmmmmmmmmmmmmmmmmmmYYYYYYYYYYYYYmmmmmmmm
             3 |                      fake special chars\x00\n
             G                        YYYYYYYYYYYYYYYYYYYYYYYY


            (4 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

    def test_string_output_utf8(self):
        # many of these won't line up visually here, to keep the source code
        # here ascii-only. note that some of the special Unicode characters
        # here will render as double-width or zero-width in unicode-aware
        # terminals, but the color-checking machinery here will still treat
        # it as one character, so those won't seem to line up visually either.

        self.assertCqlverQueriesGiveColoredOutput((
            ("select * from utf8_with_special_chars where k in (0, 1, 2, 3, 4, 5, 6);", u"""
             k | val
             R   MMM
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


            (7 rows)
            nnnnnnnn
            """.encode('utf-8')),
        ), cqlver=cqlsh.DEFAULT_CQLVER, env={'LANG': 'en_US.UTF-8'})

    def test_blob_output(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ("select num, blobcol from has_all_types where num in (0, 1, 2, 3);", r"""
             num | blobcol
             RRR   MMMMMMM
            -----+----------------------

               0 | 0x000102030405fffefd
               G   mmmmmmmmmmmmmmmmmmmm
               1 | 0xffffffffffffffffff
               G   mmmmmmmmmmmmmmmmmmmm
               2 |                   0x
               G   mmmmmmmmmmmmmmmmmmmm
               3 |                 0x80
               G   mmmmmmmmmmmmmmmmmmmm


            (4 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

    def test_prompt(self):
        with testrun_cqlsh(tty=True, keyspace=None, cqlver=cqlsh.DEFAULT_CQLVER) as c:
            self.assertEqual(c.output_header.splitlines()[-1], 'cqlsh> ')

            c.send('\n')
            output = c.read_to_next_prompt().replace('\r\n', '\n')
            self.assertEqual(output, '\ncqlsh> ')

            cmd = "USE \"%s\";\n" % get_test_keyspace().replace('"', '""')
            c.send(cmd)
            output = c.read_to_next_prompt().replace('\r\n', '\n')
            self.assertEqual(output, '%scqlsh:%s> ' % (cmd, get_test_keyspace()))

            c.send('use system;\n')
            output = c.read_to_next_prompt().replace('\r\n', '\n')
            self.assertEqual(output, 'use system;\ncqlsh:system> ')

            c.send('use NONEXISTENTKEYSPACE;\n')
            outputlines = c.read_to_next_prompt().splitlines()

            self.assertEqual(outputlines[0], 'use NONEXISTENTKEYSPACE;')
            self.assertEqual(outputlines[2], 'cqlsh:system> ')
            midline = ColoredText(outputlines[1])
            self.assertEqual(midline.plain(),
                             'InvalidRequest: code=2200 [Invalid query] message="Keyspace \'nonexistentkeyspace\' does not exist"')
            self.assertColorFromTags(midline,
                             "RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")

    def test_describe_keyspace_output(self):
        fullcqlver = cqlsh.DEFAULT_CQLVER
        with testrun_cqlsh(tty=True, cqlver=fullcqlver) as c:
            ks = get_test_keyspace()
            qks = quote_name(ks)
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
            statements = split_cql_commands(copy_desc)
            do_drop = True

            with cassandra_cursor(cql_version=fullcqlver) as curs:
                try:
                    for stmt in statements:
                        cqlshlog.debug('TEST EXEC: %s' % stmt)
                        curs.execute(stmt)
                finally:
                    curs.execute('use system')
                    if do_drop:
                        curs.execute('drop keyspace %s' % quote_name(new_ks_name))

    def check_describe_keyspace_output(self, output, qksname, fullcqlver):
        expected_bits = [r'(?im)^CREATE KEYSPACE %s WITH\b' % re.escape(qksname),
                         r';\s*$',
                         r'\breplication = {\'class\':']
        for expr in expected_bits:
            self.assertRegexpMatches(output, expr)

    def test_describe_columnfamily_output(self):
        # we can change these to regular expressions if/when it makes sense
        # to do so; these will likely be subject to lots of adjustments.

        # note columns are now comparator-ordered instead of original-order.
        table_desc3 = dedent("""

            CREATE TABLE %s.has_all_types (
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
            ) WITH bloom_filter_fp_chance = 0.01
                AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
                AND comment = ''
                AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
                AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                AND dclocal_read_repair_chance = 0.1
                AND default_time_to_live = 0
                AND gc_grace_seconds = 864000
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99.0PERCENTILE';

        """ % quote_name(get_test_keyspace()))

        with testrun_cqlsh(tty=True, cqlver=cqlsh.DEFAULT_CQLVER) as c:
            for cmdword in ('describe table', 'desc columnfamily'):
                for semicolon in (';', ''):
                    output = c.cmd_and_response('%s has_all_types%s' % (cmdword, semicolon))
                    self.assertNoHasColors(output)
                    self.assertEqual(output, table_desc3)

    def test_describe_columnfamilies_output(self):
        output_re = r'''
            \n
            Keyspace [ ] (?P<ksname> \S+ ) \n
            -----------* \n
            (?P<cfnames> .*? )
            \n
        '''

        ks = get_test_keyspace()

        with testrun_cqlsh(tty=True, keyspace=None, cqlver=cqlsh.DEFAULT_CQLVER) as c:

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
                            self.assertIn('ascii_with_special_chars', cfnames)

                    self.assertIn('system', ksnames)
                    self.assertIn(quote_name(ks), ksnames)

            # when in a keyspace
            c.send('USE %s;\n' % quote_name(ks))
            c.read_to_next_prompt()

            for cmdword in ('DESCRIBE COLUMNFAMILIES', 'desc tables'):
                for semicolon in (';', ''):
                    output = c.cmd_and_response(cmdword + semicolon)
                    self.assertNoHasColors(output)
                    self.assertEqual(output[0], '\n')
                    self.assertEqual(output[-1], '\n')
                    self.assertNotIn('Keyspace %s' % quote_name(ks), output)
                    self.assertIn('undefined_values_table', output)

    def test_describe_cluster_output(self):
        output_re = r'''(?x)
            ^
            \n
            Cluster: [ ] (?P<clustername> .* ) \n
            Partitioner: [ ] (?P<partitionername> .* ) \n
            \n
        '''

        ringinfo_re = r'''
            Range[ ]ownership: \n
            (
              [ ] .*? [ ][ ] \[ ( \d+ \. ){3} \d+ \] \n
            )+
            \n
        '''

        with testrun_cqlsh(tty=True, keyspace=None, cqlver=cqlsh.DEFAULT_CQLVER) as c:

            # not in a keyspace
            for semicolon in ('', ';'):
                output = c.cmd_and_response('describe cluster' + semicolon)
                self.assertNoHasColors(output)
                self.assertRegexpMatches(output, output_re + '$')

            c.send('USE %s;\n' % quote_name(get_test_keyspace()))
            c.read_to_next_prompt()

            for semicolon in ('', ';'):
                output = c.cmd_and_response('describe cluster' + semicolon)
                self.assertNoHasColors(output)
                self.assertRegexpMatches(output, output_re + ringinfo_re + '$')

    def test_describe_schema_output(self):
        with testrun_cqlsh(tty=True) as c:
            for semicolon in ('', ';'):
                output = c.cmd_and_response('desc full schema' + semicolon)
                self.assertNoHasColors(output)
                self.assertRegexpMatches(output, '^\nCREATE KEYSPACE')
                self.assertIn("\nCREATE KEYSPACE system WITH replication = {'class': 'LocalStrategy'}  AND durable_writes = true;\n",
                              output)
                self.assertRegexpMatches(output, ';\s*$')

    def test_show_output(self):
        with testrun_cqlsh(tty=True) as c:
            output = c.cmd_and_response('show version;')
            self.assertRegexpMatches(output,
                    '^\[cqlsh \S+ \| Cassandra \S+ \| CQL spec \S+ \| Native protocol \S+\]$')

            output = c.cmd_and_response('show host;')
            self.assertHasColors(output)
            self.assertRegexpMatches(output, '^Connected to .* at %s:%d\.$'
                                             % (re.escape(TEST_HOST), TEST_PORT))

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
            c.cmd_and_response('help types')

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

    def test_user_types_output(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ("select addresses from users;", r"""
             addresses
             MMMMMMMMM
            --------------------------------------------------------------------------------------------------------------------------------------------

                                          {{city: 'Chelyabinsk', address: '3rd street', zip: null}, {city: 'Chigirinsk', address: null, zip: '676722'}}
                                          BBYYYYBBYYYYYYYYYYYYYBBYYYYYYYBBYYYYYYYYYYYYBBYYYBBRRRRBBBBYYYYBBYYYYYYYYYYYYBBYYYYYYYBBRRRRBBYYYBBYYYYYYYYBB
             {{city: 'Austin', address: '902 East 5th St. #202', zip: '78702'}, {city: 'Sunnyvale', address: '292 Gibraltar Drive #107', zip: '94089'}}
             BBYYYYBBYYYYYYYYBBYYYYYYYBBYYYYYYYYYYYYYYYYYYYYYYYBBYYYBBYYYYYYYBBBBYYYYBBYYYYYYYYYYYBBYYYYYYYBBYYYYYYYYYYYYYYYYYYYYYYYYYYBBYYYBBYYYYYYYBB


            (2 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)
        self.assertCqlverQueriesGiveColoredOutput((
            ("select phone_numbers from users;", r"""
             phone_numbers
             MMMMMMMMMMMMM
            -------------------------------------------------------------------------------------

                                  {{country: null, number: '03'}, {country: '+7', number: null}}
                                  BBYYYYYYYBBRRRRBBYYYYYYBBYYYYBBBBYYYYYYYBBYYYYBBYYYYYYBBRRRRBB
             {{country: '+1', number: '512-537-7809'}, {country: '+44', number: '208 622 3021'}}
             BBYYYYYYYBBYYYYBBYYYYYYBBYYYYYYYYYYYYYYBBBBYYYYYYYBBYYYYYBBYYYYYYBBYYYYYYYYYYYYYYBB


            (2 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)

    def test_user_types_with_collections(self):
        self.assertCqlverQueriesGiveColoredOutput((
            ("select info from songs;", r"""
             info
             MMMM
            -------------------------------------------------------------------------------------------------------------------------------------------------------------------

             {founded: 188694000, members: {'Adrian Smith', 'Bruce Dickinson', 'Dave Murray', 'Janick Gers', 'Nicko McBrain', 'Steve Harris'}, description: 'Pure evil metal'}
             BYYYYYYYBBGGGGGGGGGBBYYYYYYYBBBYYYYYYYYYYYYYYBBYYYYYYYYYYYYYYYYYBBYYYYYYYYYYYYYBBYYYYYYYYYYYYYBBYYYYYYYYYYYYYYYBBYYYYYYYYYYYYYYBBBYYYYYYYYYYYBBYYYYYYYYYYYYYYYYYB


            (1 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)
        self.assertCqlverQueriesGiveColoredOutput((
            ("select tags from songs;", r"""
             tags
             MMMM
            -------------------------------------------------

             {tags: {'genre': 'metal', 'origin': 'england'}}
             BYYYYBBBYYYYYYYBBYYYYYYYBBYYYYYYYYBBYYYYYYYYYBB


            (1 rows)
            nnnnnnnn
            """),
        ), cqlver=cqlsh.DEFAULT_CQLVER)
