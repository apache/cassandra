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

import org.junit.Ignore;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLTester;

@Ignore
public abstract class AbstractFormatFctTest extends CQLTester
{
    protected void createTable(List<CQL3Type.Native> columnTypes, Object[][] rows)
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

    protected void createDefaultTable(Object[][] rows)
    {
        createTable(new String[][]{ { "pk", "int" }, { "col1", "int" }, { "col2", "int" } }, rows);
    }

    protected void createTable(String[][] columns, Object[][] rows)
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