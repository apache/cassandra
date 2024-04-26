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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static java.util.List.of;
import static org.apache.cassandra.cql3.CQL3Type.Native.ASCII;
import static org.apache.cassandra.cql3.CQL3Type.Native.BIGINT;
import static org.apache.cassandra.cql3.CQL3Type.Native.INT;
import static org.apache.cassandra.cql3.CQL3Type.Native.SMALLINT;
import static org.apache.cassandra.cql3.CQL3Type.Native.TEXT;
import static org.apache.cassandra.cql3.CQL3Type.Native.TINYINT;
import static org.apache.cassandra.cql3.CQL3Type.Native.VARINT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ToHumanSizeFctTest extends CQLTester
{
    @Test
    public void testOneValueArgument()
    {
        createTable(of(INT), new Object[][]{ { 1, 1073741825 },
                                             { 2, 1073741823 },
                                             { 3, 0 } }); // 0 B
        assertRows(execute("select to_human_size(col1) from %s where pk = 1"), row("1 GiB"));
        assertRows(execute("select to_human_size(col1) from %s where pk = 2"), row("1023 MiB"));
        assertRows(execute("select to_human_size(col1) from %s where pk = 3"), row("0 B"));
    }

    @Test
    public void testValueAndUnitArguments()
    {
        createTable(of(INT), new Object[][]{ { 1, 1073741825 },
                                             { 2, 0 } });
        assertRows(execute("select to_human_size(col1, 'B') from %s where pk = 1"), row("1073741825 B"));
        assertRows(execute("select to_human_size(col1, 'KiB') from %s where pk = 1"), row("1048576 KiB"));
        assertRows(execute("select to_human_size(col1, 'MiB') from %s where pk = 1"), row("1024 MiB"));
        assertRows(execute("select to_human_size(col1, 'GiB') from %s where pk = 1"), row("1 GiB"));

        assertRows(execute("select to_human_size(col1, 'B') from %s where pk = 2"), row("0 B"));
        assertRows(execute("select to_human_size(col1, 'KiB') from %s where pk = 2"), row("0 KiB"));
        assertRows(execute("select to_human_size(col1, 'MiB') from %s where pk = 2"), row("0 MiB"));
        assertRows(execute("select to_human_size(col1, 'GiB') from %s where pk = 2"), row("0 GiB"));
    }

    @Test
    public void testValueWithSourceAndTargetArgument()
    {
        createTable(of(INT), new Object[][]{ { 1, 1073741825 },
                                             { 2, 1 },
                                             { 3, 0 } });
        assertRows(execute("select to_human_size(col1, 'B',   'B') from %s where pk = 1"), row("1073741825 B"));
        assertRows(execute("select to_human_size(col1, 'B', 'KiB') from %s where pk = 1"), row("1048576 KiB"));
        assertRows(execute("select to_human_size(col1, 'B', 'MiB') from %s where pk = 1"), row("1024 MiB"));
        assertRows(execute("select to_human_size(col1, 'B', 'GiB') from %s where pk = 1"), row("1 GiB"));

        assertRows(execute("select to_human_size(col1, 'GiB', 'GiB') from %s where pk = 2"), row("1 GiB"));
        assertRows(execute("select to_human_size(col1, 'GiB', 'MiB') from %s where pk = 2"), row("1024 MiB"));
        assertRows(execute("select to_human_size(col1, 'GiB', 'KiB') from %s where pk = 2"), row("1048576 KiB"));
        assertRows(execute("select to_human_size(col1, 'GiB',   'B') from %s where pk = 2"), row("1073741824 B"));

        assertRows(execute("select to_human_size(col1, 'GiB', 'GiB') from %s where pk = 3"), row("0 GiB"));
        assertRows(execute("select to_human_size(col1, 'GiB', 'MiB') from %s where pk = 3"), row("0 MiB"));
        assertRows(execute("select to_human_size(col1, 'GiB', 'KiB') from %s where pk = 3"), row("0 KiB"));
        assertRows(execute("select to_human_size(col1, 'GiB',   'B') from %s where pk = 3"), row("0 B"));
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
        assertRows(execute("select to_human_size(col1, 'GiB', 'B') from %s where pk = 1"), row("9223372036854775807 B"));
        assertRows(execute("select to_human_size(col2, 'GiB', 'B') from %s where pk = 1"), row("2305843007066210304 B"));
        assertRows(execute("select to_human_size(col3, 'GiB', 'B') from %s where pk = 1"), row("35182224605184 B"));
        assertRows(execute("select to_human_size(col4, 'GiB', 'B') from %s where pk = 1"), row("135291469824 B"));

        assertRows(execute("select to_human_size(col2, 'GiB', 'B') from %s where pk = 2"), row("2305843008139952128 B"));
        assertRows(execute("select to_human_size(col3, 'GiB', 'B') from %s where pk = 2"), row("35183298347008 B"));
        assertRows(execute("select to_human_size(col4, 'GiB', 'B') from %s where pk = 2"), row("136365211648 B"));
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

        assertRows(execute("select to_human_size(col1) from %s where pk = 1"), row("1 GiB"));
        assertRows(execute("select to_human_size(col2) from %s where pk = 1"), row("127 B"));
        assertRows(execute("select to_human_size(col3) from %s where pk = 1"), row("31 KiB"));
        assertRows(execute("select to_human_size(col4) from %s where pk = 1"), row("8589934591 GiB"));
        assertRows(execute("select to_human_size(col5) from %s where pk = 1"), row("1 GiB"));
        assertRows(execute("select to_human_size(col6) from %s where pk = 1"), row("1 GiB"));
        assertRows(execute("select to_human_size(col7) from %s where pk = 1"), row("1 GiB"));
    }

    @Test
    public void testNegativeValueIsInvalid()
    {
        createDefaultTable(new Object[][]{ { "1", "-1", "-2" } });
        assertThatThrownBy(() -> execute("select to_human_size(col1) from %s where pk = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("value must be non-negative");
    }

    @Test
    public void testUnparsableTextIsInvalid()
    {
        createTable(of(TEXT), new Object[][]{ { 1, "'abc'" }, { 2, "'-1'" } });

        assertThatThrownBy(() -> execute("select to_human_size(col1) from %s where pk = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("unable to convert string 'abc' to a value of type long");

        assertThatThrownBy(() -> execute("select to_human_size(col1) from %s where pk = 2"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("value must be non-negative");
    }

    @Test
    public void testInvalidUnits()
    {
        createDefaultTable(new Object[][]{ { "1", "1", "2" } });
        for (String functionCall : new String[] {
        "to_human_size(col1, 'abc')",
        "to_human_size(col1, 'B', 'abc')",
        "to_human_size(col1, 'abc', 'B')",
        "to_human_size(col1, 'abc', 'abc')"
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
        assertThatThrownBy(() -> execute("select to_human_size(col1, 'arg1', 'arg2', 'arg3') from %s where pk = 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Invalid number of arguments for function system.to_human_size([int|tinyint|smallint|bigint|varint|ascii|text], [ascii], [ascii])");
    }

    @Test
    public void testHandlingNullValues()
    {
        createTable(of(TEXT, ASCII, INT),
                    new Object[][]{ { 1, null, null, null } });

        assertRows(execute("select to_human_size(col1), to_human_size(col2), to_human_size(col3) from %s where pk = 1"),
                   row(null, null, null));

        assertRows(execute("select to_human_size(col1, 'B') from %s where pk = 1"), row((Object) null));
        assertRows(execute("select to_human_size(col1, 'B', 'KiB') from %s where pk = 1"), row((Object) null));
    }

    @Test
    public void testHandlingNullArguments()
    {
        createTable(of(TEXT, ASCII, INT),
                    new Object[][]{ { 1, null, null, null },
                                    { 2, "'1'", "'2'", 3 } });

        assertRows(execute("select to_human_size(col1, null) from %s where pk = 1"), row((Object) null));

        for (String functionCall : new String[] {
        "to_human_size(col3, null)",
        "to_human_size(col3, null, null)",
        "to_human_size(col3, null, 'KiB')",
        "to_human_size(col3, 'KiB', null)"
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
        assertRows(execute("select to_human_size(col1) from %s where pk = 1"), row("900 B"));
    }

    private void createTable(List<CQL3Type.Native> columnTypes, Object[][] rows)
    {
        String[][] columns = new String[columnTypes.size() + 1][2];

        columns[0][0] = "pk";
        columns[0][1] = "int";

        for (int i = 1; i <= columnTypes.size(); i++)
        {
            columns[i][0] = "col" + i;
            columns[i][1] = columnTypes.get(i - 1).name().toLowerCase();
        }

        createTable(columns, rows);
    }

    private void createDefaultTable(Object[][] rows)
    {
        createTable(new String[][]{ { "pk", "int" }, { "col1", "int" }, { "col2", "int" } }, rows);
    }

    private void createTable(String[][] columns, Object[][] rows)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.length; i++)
        {
            sb.append(columns[i][0]);
            sb.append(' ');
            sb.append(columns[i][1]);

            if (i == 0)
                sb.append(" primary key");

            if (i + 1 != columns.length)
                sb.append(", ");
        }
        String columnsDefinition = sb.toString();
        createTable(KEYSPACE, "CREATE TABLE %s (" + columnsDefinition + ')');

        String cols = Arrays.stream(columns).map(s -> s[0]).collect(Collectors.joining(", "));

        for (Object[] row : rows)
        {
            String vals = Arrays.stream(row).map(v -> {
                if (v == null)
                    return "null";
                return v.toString();
            }).collect(Collectors.joining(", "));
            execute("INSERT INTO %s (" + cols + ") values (" + vals + ')');
        }
    }
}
