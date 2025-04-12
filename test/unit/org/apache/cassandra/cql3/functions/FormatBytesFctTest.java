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
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class FormatBytesFctTest extends AbstractFormatFctTest
{
    @Test
    public void testOneValueArgumentExact()
    {
        createTable(of(INT), new Object[][]{ { 1, 1073741825 },
                                             { 2, 1073741823 },
                                             { 3, 0 } }); // 0 B
        assertRows(execute("select format_bytes(col1) from %s where pk = 1"), row("1 GiB"));
        assertRows(execute("select format_bytes(col1) from %s where pk = 2"), row("1024 MiB"));
        assertRows(execute("select format_bytes(col1) from %s where pk = 3"), row("0 B"));
    }

    @Test
    public void testOneValueArgumentDecimalRoundup()
    {
        createTable(of(INT), new Object[][]{ { 1, 1563401650 },
                                             { 2, 1072441589 },
                                             { 3, 102775 },
                                             { 4, 102 } });
        assertRows(execute("select format_bytes(col1) from %s where pk = 1"), row("1.46 GiB")); // 1.4560
        assertRows(execute("select format_bytes(col1) from %s where pk = 2"), row("1022.76 MiB")); // 1022.7599
        assertRows(execute("select format_bytes(col1) from %s where pk = 3"), row("100.37 KiB")); // 100.3662
        assertRows(execute("select format_bytes(col1) from %s where pk = 4"), row("102 B"));
    }

    @Test
    public void testOneValueArgumentDecimalRoundDown()
    {
        createTable(of(INT), new Object[][]{ { 1, 1557999386 },
                                             { 2, 1072433201 },
                                             { 3, 102769 },
                                             { 4, 102 } });
        assertRows(execute("select format_bytes(col1) from %s where pk = 1"), row("1.45 GiB")); // 1.451
        assertRows(execute("select format_bytes(col1) from %s where pk = 2"), row("1022.75 MiB")); // 1022.752
        assertRows(execute("select format_bytes(col1) from %s where pk = 3"), row("100.36 KiB")); // 100.3613
        assertRows(execute("select format_bytes(col1) from %s where pk = 4"), row("102 B"));
    }

    @Test
    public void testValueAndUnitArgumentsExact()
    {
        createTable(of(INT), new Object[][]{ { 1, 1073741825 },
                                             { 2, 0 } });
        assertRows(execute("select format_bytes(col1, 'B') from %s where pk = 1"), row("1073741825 B"));
        assertRows(execute("select format_bytes(col1, 'KiB') from %s where pk = 1"), row("1048576 KiB"));
        assertRows(execute("select format_bytes(col1, 'MiB') from %s where pk = 1"), row("1024 MiB"));
        assertRows(execute("select format_bytes(col1, 'GiB') from %s where pk = 1"), row("1 GiB"));

        assertRows(execute("select format_bytes(col1, 'B') from %s where pk = 2"), row("0 B"));
        assertRows(execute("select format_bytes(col1, 'KiB') from %s where pk = 2"), row("0 KiB"));
        assertRows(execute("select format_bytes(col1, 'MiB') from %s where pk = 2"), row("0 MiB"));
        assertRows(execute("select format_bytes(col1, 'GiB') from %s where pk = 2"), row("0 GiB"));
    }

    @Test
    public void testValueAndUnitArgumentsDecimal()
    {
        createTable(of(INT), new Object[][]{ { 1, 1563401650 },
                                             { 2, 1557999336 } });
        assertRows(execute("select format_bytes(col1, 'B') from %s where pk = 1"), row("1563401650 B"));
        assertRows(execute("select format_bytes(col1, 'KiB') from %s where pk = 1"), row("1526759.42 KiB"));
        assertRows(execute("select format_bytes(col1, 'MiB') from %s where pk = 1"), row("1490.98 MiB"));
        assertRows(execute("select format_bytes(col1, 'GiB') from %s where pk = 1"), row("1.46 GiB"));

        assertRows(execute("select format_bytes(col1, 'B') from %s where pk = 2"), row("1557999336 B"));
        assertRows(execute("select format_bytes(col1, 'KiB') from %s where pk = 2"), row("1521483.73 KiB"));
        assertRows(execute("select format_bytes(col1, 'MiB') from %s where pk = 2"), row("1485.82 MiB"));
        assertRows(execute("select format_bytes(col1, 'GiB') from %s where pk = 2"), row("1.45 GiB"));
    }

    @Test
    public void testValueWithSourceAndTargetArgumentExact()
    {
        createTable(of(INT), new Object[][]{ { 1, 1073741825 },
                                             { 2, 1 },
                                             { 3, 0 } });
        assertRows(execute("select format_bytes(col1, 'B',   'B') from %s where pk = 1"), row("1073741825 B"));
        assertRows(execute("select format_bytes(col1, 'B', 'KiB') from %s where pk = 1"), row("1048576 KiB"));
        assertRows(execute("select format_bytes(col1, 'B', 'MiB') from %s where pk = 1"), row("1024 MiB"));
        assertRows(execute("select format_bytes(col1, 'B', 'GiB') from %s where pk = 1"), row("1 GiB"));

        assertRows(execute("select format_bytes(col1, 'GiB', 'GiB') from %s where pk = 2"), row("1 GiB"));
        assertRows(execute("select format_bytes(col1, 'GiB', 'MiB') from %s where pk = 2"), row("1024 MiB"));
        assertRows(execute("select format_bytes(col1, 'GiB', 'KiB') from %s where pk = 2"), row("1048576 KiB"));
        assertRows(execute("select format_bytes(col1, 'GiB',   'B') from %s where pk = 2"), row("1073741824 B"));

        assertRows(execute("select format_bytes(col1, 'GiB', 'GiB') from %s where pk = 3"), row("0 GiB"));
        assertRows(execute("select format_bytes(col1, 'GiB', 'MiB') from %s where pk = 3"), row("0 MiB"));
        assertRows(execute("select format_bytes(col1, 'GiB', 'KiB') from %s where pk = 3"), row("0 KiB"));
        assertRows(execute("select format_bytes(col1, 'GiB',   'B') from %s where pk = 3"), row("0 B"));
    }

    @Test
    public void testValueWithSourceAndTargetArgumentDecimal()
    {
        createTable(of(INT), new Object[][]{ { 1, 1563401650 },
                                             { 2, 1557999336 },});
        assertRows(execute("select format_bytes(col1, 'B',   'B') from %s where pk = 1"), row("1563401650 B"));
        assertRows(execute("select format_bytes(col1, 'B', 'KiB') from %s where pk = 1"), row("1526759.42 KiB"));
        assertRows(execute("select format_bytes(col1, 'B', 'MiB') from %s where pk = 1"), row("1490.98 MiB"));
        assertRows(execute("select format_bytes(col1, 'B', 'GiB') from %s where pk = 1"), row("1.46 GiB"));

        assertRows(execute("select format_bytes(col1, 'B', 'B') from %s where pk = 2"), row("1557999336 B"));
        assertRows(execute("select format_bytes(col1, 'B', 'KiB') from %s where pk = 2"), row("1521483.73 KiB"));
        assertRows(execute("select format_bytes(col1, 'B', 'MiB') from %s where pk = 2"), row("1485.82 MiB"));
        assertRows(execute("select format_bytes(col1, 'B', 'GiB') from %s where pk = 2"), row("1.45 GiB"));
    }

    @Test
    public void testFuzzNumberGenerators()
    {
        createTable("CREATE TABLE %s (pk int primary key, col1 int)");

        qt().withExamples(1024).forAll(integers().allPositive()).checkAssert(
        (randInt) -> {
            execute("INSERT INTO %s (pk, col1) VALUES (?, ?)", 1, randInt);

            assertRows(execute("select format_bytes(col1, 'MiB') from %s where pk = 1"), row(format(randInt / 1024.0 / 1024.0) + " MiB"));
            assertRows(execute("select format_bytes(col1, 'KiB', 'GiB') from %s where pk = 1"), row(format(randInt / 1024.0 / 1024.0) + " GiB"));
            assertRows(execute("select format_bytes(col1, 'B', 'GiB') from %s where pk = 1"), row(format(randInt / 1024.0 / 1024.0 / 1024.0 ) + " GiB"));
        });
    }

    @Test
    public void testOverflow()
    {
        createTable(of(BIGINT, INT, SMALLINT, TINYINT),
                    new Object[][]{ { 1,
                                      1073741825L * 1024 + 1,
                                      Integer.MAX_VALUE - 1,
                                      Short.MAX_VALUE - 1,
                                      Byte.MAX_VALUE - 1 },
                                    { 2,
                                      1073741825L * 1024 + 1,
                                      Integer.MAX_VALUE,
                                      Short.MAX_VALUE,
                                      Byte.MAX_VALUE } });

        // this will stop at Long.MAX_VALUE
        assertRows(execute("select format_bytes(col1, 'GiB', 'B') from %s where pk = 1"), row("9223372036854776000 B"));
        assertRows(execute("select format_bytes(col2, 'GiB', 'B') from %s where pk = 1"), row("2305843007066210300 B"));
        assertRows(execute("select format_bytes(col3, 'GiB', 'B') from %s where pk = 1"), row("35182224605184 B"));
        assertRows(execute("select format_bytes(col4, 'GiB', 'B') from %s where pk = 1"), row("135291469824 B"));

        assertRows(execute("select format_bytes(col2, 'GiB', 'B') from %s where pk = 2"), row("2305843008139952130 B"));
        assertRows(execute("select format_bytes(col3, 'GiB', 'B') from %s where pk = 2"), row("35183298347008 B"));
        assertRows(execute("select format_bytes(col4, 'GiB', 'B') from %s where pk = 2"), row("136365211648 B"));
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

        assertRows(execute("select format_bytes(col1) from %s where pk = 1"), row("2 GiB"));
        assertRows(execute("select format_bytes(col2) from %s where pk = 1"), row("127 B"));
        assertRows(execute("select format_bytes(col3) from %s where pk = 1"), row("32 KiB"));
        assertRows(execute("select format_bytes(col4) from %s where pk = 1"), row("8589934592 GiB"));
        assertRows(execute("select format_bytes(col5) from %s where pk = 1"), row("2 GiB"));
        assertRows(execute("select format_bytes(col6) from %s where pk = 1"), row("2 GiB"));
        assertRows(execute("select format_bytes(col7) from %s where pk = 1"), row("2 GiB"));
    }

    @Test
    public void testNegativeValueIsInvalid()
    {
        createDefaultTable(new Object[][]{ { "1", "-1", "-2" } });
        assertThatThrownBy(() -> execute("select format_bytes(col1) from %s where pk = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("value must be non-negative");
    }

    @Test
    public void testUnparsableTextIsInvalid()
    {
        createTable(of(TEXT), new Object[][]{ { 1, "'abc'" }, { 2, "'-1'" } });

        assertThatThrownBy(() -> execute("select format_bytes(col1) from %s where pk = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("unable to convert string 'abc' to a value of type long");

        assertThatThrownBy(() -> execute("select format_bytes(col1) from %s where pk = 2"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("value must be non-negative");
    }

    @Test
    public void testInvalidUnits()
    {
        createDefaultTable(new Object[][]{ { "1", "1", "2" } });
        for (String functionCall : new String[] {
        "format_bytes(col1, 'abc')",
        "format_bytes(col1, 'B', 'abc')",
        "format_bytes(col1, 'abc', 'B')",
        "format_bytes(col1, 'abc', 'abc')"
        })
        {
            assertThatThrownBy(() -> execute("select " + functionCall + " from %s where pk = 1"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("Unsupported data storage unit: abc. Supported units are: B, KiB, MiB, GiB");
        }
    }

    @Test
    public void testInvalidArgumentsSize()
    {
        createDefaultTable(new Object[][]{ { "1", "1", "2" } });
        
        // Test arguemnt size = 0
        assertThatThrownBy(() -> execute("select format_bytes() from %s where pk = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid number of arguments for function system.format_bytes([int|tinyint|smallint|bigint|varint|ascii|text], [ascii], [ascii])");

        // Test argument size > 3
        assertThatThrownBy(() -> execute("select format_bytes(col1, 'B', 'KiB', 'GiB') from %s where pk = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid number of arguments for function system.format_bytes([int|tinyint|smallint|bigint|varint|ascii|text], [ascii], [ascii])");
    }

    @Test
    public void testHandlingNullValues()
    {
        createTable(of(TEXT, ASCII, INT),
                    new Object[][]{ { 1, null, null, null } });

        assertRows(execute("select format_bytes(col1), format_bytes(col2), format_bytes(col3) from %s where pk = 1"),
                   row(null, null, null));

        assertRows(execute("select format_bytes(col1, 'B') from %s where pk = 1"), row((Object) null));
        assertRows(execute("select format_bytes(col1, 'B', 'KiB') from %s where pk = 1"), row((Object) null));
    }

    @Test
    public void testHandlingNullArguments()
    {
        createTable(of(TEXT, ASCII, INT),
                    new Object[][]{ { 1, null, null, null },
                                    { 2, "'1'", "'2'", 3 } });

        assertRows(execute("select format_bytes(col1, null) from %s where pk = 1"), row((Object) null));

        for (String functionCall : new String[] {
        "format_bytes(col3, null)",
        "format_bytes(col3, null, null)",
        "format_bytes(col3, null, 'KiB')",
        "format_bytes(col3, 'KiB', null)"
        })
        {
            assertThatThrownBy(() -> execute("select " + functionCall + " from %s where pk = 2"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageContaining("none of the arguments may be null");
        }
    }

    @Test
    public void testSizeSmallerThan1KibiByte()
    {
        createDefaultTable(new Object[][]{ { "1", "900", "2000" } });
        assertRows(execute("select format_bytes(col1) from %s where pk = 1"), row("900 B"));
    }
}
