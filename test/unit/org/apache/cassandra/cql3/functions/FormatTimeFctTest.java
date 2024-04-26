/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.functions;

import org.junit.Test;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static java.util.List.of;
import static org.apache.cassandra.cql3.CQL3Type.Native.ASCII;
import static org.apache.cassandra.cql3.CQL3Type.Native.BIGINT;
import static org.apache.cassandra.cql3.CQL3Type.Native.INT;
import static org.apache.cassandra.cql3.CQL3Type.Native.SMALLINT;
import static org.apache.cassandra.cql3.CQL3Type.Native.TEXT;
import static org.apache.cassandra.cql3.CQL3Type.Native.TINYINT;
import static org.apache.cassandra.cql3.CQL3Type.Native.VARINT;
import static org.apache.cassandra.cql3.functions.FormatFcts.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.quicktheories.WithQuickTheories;

public class FormatTimeFctTest extends AbstractFormatFctTest implements WithQuickTheories
{
    @Test
    public void testOneValueArgument()
    {
        createTable(of(INT), new Object[][]{ { 1, 7200001 }, // 2h + 1ms
                                             { 2, 7199999 }, // 2h - 1ms
                                             { 3, 0 } }); // 0 B
        assertRows(execute("select format_time(col1) from %s where pk = 1"), row("2 h"));
        assertRows(execute("select format_time(col1) from %s where pk = 2"), row("2 h"));
        assertRows(execute("select format_time(col1) from %s where pk = 3"), row("0 ms"));
    }

    @Test
    public void testOneValueArgumentDecimal()
    {
        createTable(of(INT), new Object[][]{ { 1, 9000000 }, // 2.5h
                                             { 2, 7704000 }, // 2.14h
                                             { 3, 7848000 } }); // 2.18h
        assertRows(execute("select format_time(col1) from %s where pk = 1"), row("2.5 h"));
        assertRows(execute("select format_time(col1) from %s where pk = 2"), row("2.14 h"));
        assertRows(execute("select format_time(col1) from %s where pk = 3"), row("2.18 h"));
    }

    @Test
    public void testValueAndUnitArguments()
    {
        createTable(of(INT), new Object[][]{ { 1, 1073741826 },
                                             { 2, 0 }});
        assertRows(execute("select format_time(col1, 's') from %s where pk = 1"), row("1073741.83 s"));
        assertRows(execute("select format_time(col1, 'm') from %s where pk = 1"), row("17895.7 m"));
        assertRows(execute("select format_time(col1, 'h') from %s where pk = 1"), row("298.26 h"));
        assertRows(execute("select format_time(col1, 'd') from %s where pk = 1"), row("12.43 d"));

        assertRows(execute("select format_time(col1, 's') from %s where pk = 2"), row("0 s"));
        assertRows(execute("select format_time(col1, 'm') from %s where pk = 2"), row("0 m"));
        assertRows(execute("select format_time(col1, 'h') from %s where pk = 2"), row("0 h"));
        assertRows(execute("select format_time(col1, 'd') from %s where pk = 2"), row("0 d"));
    }

    @Test
    public void testValueWithSourceAndTargetArgument()
    {
        createTable(of(INT), new Object[][]{ { 1, 1073741826 },
                                             { 2, 1 },
                                             { 3, 0 } });
        assertRows(execute("select format_time(col1, 'ns', 'us') from %s where pk = 1"), row("1073741.83 us"));
        assertRows(execute("select format_time(col1, 'ns', 'ms') from %s where pk = 1"), row("1073.74 ms"));
        assertRows(execute("select format_time(col1, 'ns', 's') from %s where pk = 1"), row("1.07 s"));
        assertRows(execute("select format_time(col1, 'ns', 'm') from %s where pk = 1"), row("0.02 m"));

        assertRows(execute("select format_time(col1, 'us', 'ns') from %s where pk = 1"), row("1073741826000 ns"));
        assertRows(execute("select format_time(col1, 'us', 'ms') from %s where pk = 1"), row("1073741.83 ms"));
        assertRows(execute("select format_time(col1, 'us', 's') from %s where pk = 1"), row("1073.74 s"));
        assertRows(execute("select format_time(col1, 'us', 'm') from %s where pk = 1"), row("17.9 m"));
        assertRows(execute("select format_time(col1, 'us', 'h') from %s where pk = 1"), row("0.3 h"));
        assertRows(execute("select format_time(col1, 'us', 'd') from %s where pk = 1"), row("0.01 d"));

        assertRows(execute("select format_time(col1, 'ms', 'ms') from %s where pk = 1"), row("1073741826 ms"));
        assertRows(execute("select format_time(col1, 'ms', 's') from %s where pk = 1"), row("1073741.83 s"));
        assertRows(execute("select format_time(col1, 'ms', 'm') from %s where pk = 1"), row("17895.7 m"));
        assertRows(execute("select format_time(col1, 'ms', 'h') from %s where pk = 1"), row("298.26 h"));
        assertRows(execute("select format_time(col1, 'ms', 'd') from %s where pk = 1"), row("12.43 d"));

        assertRows(execute("select format_time(col1, 'd', 'd') from %s where pk = 2"), row("1 d"));
        assertRows(execute("select format_time(col1, 'd', 'h') from %s where pk = 2"), row("24 h"));
        assertRows(execute("select format_time(col1, 'd', 'm') from %s where pk = 2"), row("1440 m"));
        assertRows(execute("select format_time(col1, 'd', 's') from %s where pk = 2"), row("86400 s"));

        assertRows(execute("select format_time(col1, 'd', 'd') from %s where pk = 3"), row("0 d"));
        assertRows(execute("select format_time(col1, 'd', 'h') from %s where pk = 3"), row("0 h"));
        assertRows(execute("select format_time(col1, 'd', 'm') from %s where pk = 3"), row("0 m"));
        assertRows(execute("select format_time(col1, 'd', 's') from %s where pk = 3"), row("0 s"));
        assertRows(execute("select format_time(col1, 'd', 'ms') from %s where pk = 3"), row("0 ms"));
        assertRows(execute("select format_time(col1, 'd', 'us') from %s where pk = 3"), row("0 us"));
    }

    @Test
    public void testNoOverflow()
    {
        createTable(of(BIGINT, INT, SMALLINT, TINYINT),
                    new Object[][]{ { 1,
                                      Long.MAX_VALUE - 1,
                                      Integer.MAX_VALUE - 1,
                                      Short.MAX_VALUE - 1,
                                      Byte.MAX_VALUE - 1 },
                                    { 2,
                                      Long.MAX_VALUE,
                                      Integer.MAX_VALUE,
                                      Short.MAX_VALUE,
                                      Byte.MAX_VALUE } });

        // Won't overlfow because the value is one less than the Double.MAX_VALUE
        assertRows(execute("select format_time(col1, 'd', 'ns') from %s where pk = 1"), row("796899343984252600000000000000000 ns"));
        assertRows(execute("select format_time(col2, 'd', 'ns') from %s where pk = 1"), row("185542587014400000000000 ns"));
        assertRows(execute("select format_time(col3, 'd', 'ns') from %s where pk = 1"), row("2830982400000000000 ns"));
        assertRows(execute("select format_time(col4, 'd', 'ns') from %s where pk = 1"), row("10886400000000000 ns"));

        assertRows(execute("select format_time(col1, 'd', 'ns') from %s where pk = 2"), row("796899343984252600000000000000000 ns"));
        assertRows(execute("select format_time(col2, 'd', 'ns') from %s where pk = 2"), row("185542587100800000000000 ns"));
        assertRows(execute("select format_time(col3, 'd', 'ns') from %s where pk = 2"), row("2831068800000000000 ns"));
        assertRows(execute("select format_time(col4, 'd', 'ns') from %s where pk = 2"), row("10972800000000000 ns"));
    }

    @Test
    public void testAllSupportedColumnTypes()
    {
        createTable(of(INT, TINYINT, SMALLINT, BIGINT, VARINT, ASCII, TEXT),
                    new Object[][]{ { 1,
                                      Integer.MAX_VALUE,
                                      Byte.MAX_VALUE,
                                      Short.MAX_VALUE,
                                      Long.MAX_VALUE,
                                      Integer.MAX_VALUE,
                                      '\'' + Integer.valueOf(Integer.MAX_VALUE).toString() + '\'',
                                      '\'' + Integer.valueOf(Integer.MAX_VALUE).toString() + '\'',
                                      } });

        assertRows(execute("select format_time(col1) from %s where pk = 1"), row("24.86 d"));
        assertRows(execute("select format_time(col2) from %s where pk = 1"), row("127 ms"));
        assertRows(execute("select format_time(col3) from %s where pk = 1"), row("32.77 s"));
        assertRows(execute("select format_time(col4) from %s where pk = 1"), row("106751991167.3 d"));
        assertRows(execute("select format_time(col5) from %s where pk = 1"), row("24.86 d"));
        assertRows(execute("select format_time(col6) from %s where pk = 1"), row("24.86 d"));
        assertRows(execute("select format_time(col7) from %s where pk = 1"), row("24.86 d"));
    }

    @Test
    public void testNegativeValueIsInvalid()
    {
        createDefaultTable(new Object[][]{ { "1", "-1", "-2" } });
        assertThatThrownBy(() -> execute("select format_time(col1) from %s where pk = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("value must be non-negative");
    }

    @Test
    public void testUnparsableTextIsInvalid()
    {
        createTable(of(TEXT), new Object[][]{ { 1, "'abc'" }, { 2, "'-1'" } });

        assertThatThrownBy(() -> execute("select format_time(col1) from %s where pk = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("unable to convert string 'abc' to a value of type long");

        assertThatThrownBy(() -> execute("select format_time(col1) from %s where pk = 2"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("value must be non-negative");
    }

    @Test
    public void testInvalidUnits()
    {
        createDefaultTable(new Object[][]{ { "1", "1", "2" } });
        for (String functionCall : new String[] {
        "format_time(col1, 'abc')",
        "format_time(col1, 'd', 'abc')",
        "format_time(col1, 'abc', 'd')",
        "format_time(col1, 'abc', 'abc')"
        })
        {
            assertThatThrownBy(() -> execute("select " + functionCall + " from %s where pk = 1"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("Unsupported time unit: abc. Supported units are: ns, us, ms, s, m, h, d");
        }
    }

    @Test
    public void testInvalidArgumentsSize()
    {
        createDefaultTable(new Object[][]{ { "1", "1", "2" } });
        // test arguemnt size = 0
        assertThatThrownBy(() -> execute("select format_time() from %s where pk = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid number of arguments for function system.format_time([int|tinyint|smallint|bigint|varint|ascii|text], [ascii], [ascii])");

        // Test argument size > 3
        assertThatThrownBy(() -> execute("select format_time(col1, 'ms', 's', 'h') from %s where pk = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid number of arguments for function system.format_time([int|tinyint|smallint|bigint|varint|ascii|text], [ascii], [ascii])");
    }

    @Test
    public void testHandlingNullValues()
    {
        createTable(of(TEXT, ASCII, INT),
                    new Object[][]{ { 1, null, null, null } });

        assertRows(execute("select format_time(col1), format_time(col2), format_time(col3) from %s where pk = 1"),
                   row(null, null, null));

        assertRows(execute("select format_time(col1, 's') from %s where pk = 1"), row((Object) null));
        assertRows(execute("select format_time(col1, 's', 'd') from %s where pk = 1"), row((Object) null));
    }

    @Test
    public void testHandlingNullArguments()
    {
        createTable(of(TEXT, ASCII, INT),
                    new Object[][]{ { 1, null, null, null },
                                    { 2, "'1'", "'2'", 3 } });

        assertRows(execute("select format_time(col1, null) from %s where pk = 1"), row((Object) null));

        for (String functionCall : new String[] {
        "format_time(col3, null)",
        "format_time(col3, null, null)",
        "format_time(col3, null, 'd')",
        "format_time(col3, 'd', null)"
        })
        {
            assertThatThrownBy(() -> execute("select " + functionCall + " from %s where pk = 2"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("none of the arguments may be null");
        }
    }

    @Test
    public void testFuzzRandomGenerators()
    {
        createTable("CREATE TABLE %s (pk int primary key, col1 int)");
        qt().withExamples(1024).forAll(integers().allPositive()).checkAssert(
        (randInt) -> {
            execute("INSERT INTO %s (pk, col1) VALUES (?, ?)", 1, randInt);
            assertRows(execute("select format_time(col1, 's', 'm') from %s where pk = 1"), row(format((double) randInt * (1 / 60.0)) + " m"));
            assertRows(execute("select format_time(col1, 's', 'h') from %s where pk = 1"), row(format((double) randInt * (1 / 3600.0)) + " h"));
            assertRows(execute("select format_time(col1, 's', 'd') from %s where pk = 1"), row(format((double) randInt * (1 / 86400.0)) + " d"));
            assertRows(execute("select format_time(col1, 'ms', 'm') from %s where pk = 1"), row(format((double) randInt * (1 / (60 * 1000.0))) + " m"));
            assertRows(execute("select format_time(col1, 'ms', 'h') from %s where pk = 1"), row(format((double) randInt * (1 / (3600 * 1000.0))) + " h"));
            assertRows(execute("select format_time(col1, 'ms', 'd') from %s where pk = 1"), row(format((double) randInt * (1 / (86400 * 1000.0))) + " d"));
        });
    }
}
