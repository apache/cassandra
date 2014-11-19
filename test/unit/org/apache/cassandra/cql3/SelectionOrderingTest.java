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
package org.apache.cassandra.cql3;

import org.junit.Test;

public class SelectionOrderingTest extends CQLTester
{

    @Test
    public void testNormalSelectionOrderSingleClustering() throws Throwable
    {
        for (String descOption : new String[]{"", " WITH CLUSTERING ORDER BY (b DESC)"})
        {
            createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))" + descOption);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 2, 2);

            assertRows(execute("SELECT * FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0, 0, 0),
                    row(0, 1, 1),
                    row(0, 2, 2)
            );

            assertRows(execute("SELECT * FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(0, 2, 2),
                    row(0, 1, 1),
                    row(0, 0, 0)
            );

            // order by the only column in the selection
            assertRows(execute("SELECT b FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0), row(1), row(2));

            assertRows(execute("SELECT b FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(2), row(1), row(0));

            // order by a column not in the selection
            assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0), row(1), row(2));

            assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(2), row(1), row(0));
        }
    }

    @Test
    public void testFunctionSelectionOrderSingleClustering() throws Throwable
    {
        for (String descOption : new String[]{"", " WITH CLUSTERING ORDER BY (b DESC)"})
        {
            createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))" + descOption);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 2, 2);

            // order by the only column in the selection
            assertRows(execute("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0), row(1), row(2));

            assertRows(execute("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(2), row(1), row(0));

            // order by a column not in the selection
            assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0), row(1), row(2));

            assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(2), row(1), row(0));

            assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY c ASC", 0);
            assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY c DESC", 0);
        }
    }

    @Test
    public void testFieldSelectionOrderSingleClustering() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a int)");

        for (String descOption : new String[]{"", " WITH CLUSTERING ORDER BY (b DESC)"})
        {
            createTable("CREATE TABLE %s (a int, b int, c frozen<" + type + "   >, PRIMARY KEY (a, b))" + descOption);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, {a: ?})", 0, 0, 0);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, {a: ?})", 0, 1, 1);
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, {a: ?})", 0, 2, 2);

            // order by a column not in the selection
            assertRows(execute("SELECT c.a FROM %s WHERE a=? ORDER BY b ASC", 0),
                    row(0), row(1), row(2));

            assertRows(execute("SELECT c.a FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(2), row(1), row(0));

            assertRows(execute("SELECT blobAsInt(intAsBlob(c.a)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                    row(2), row(1), row(0));
            dropTable("DROP TABLE %s");
        }
    }

    @Test
    public void testNormalSelectionOrderMultipleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 2, 2);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 3);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 4);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 2, 5);

        assertRows(execute("SELECT * FROM %s WHERE a=? ORDER BY b ASC", 0),
                row(0, 0, 0, 0),
                row(0, 0, 1, 1),
                row(0, 0, 2, 2),
                row(0, 1, 0, 3),
                row(0, 1, 1, 4),
                row(0, 1, 2, 5)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? ORDER BY b DESC", 0),
                row(0, 1, 2, 5),
                row(0, 1, 1, 4),
                row(0, 1, 0, 3),
                row(0, 0, 2, 2),
                row(0, 0, 1, 1),
                row(0, 0, 0, 0)
        );

        assertRows(execute("SELECT * FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                row(0, 1, 2, 5),
                row(0, 1, 1, 4),
                row(0, 1, 0, 3),
                row(0, 0, 2, 2),
                row(0, 0, 1, 1),
                row(0, 0, 0, 0)
        );

        assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY c ASC", 0);
        assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY c DESC", 0);
        assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY b ASC, c DESC", 0);
        assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY b DESC, c ASC", 0);
        assertInvalid("SELECT * FROM %s WHERE a=? ORDER BY d ASC", 0);

        // select and order by b
        assertRows(execute("SELECT b FROM %s WHERE a=? ORDER BY b ASC", 0),
                row(0), row(0), row(0), row(1), row(1), row(1));
        assertRows(execute("SELECT b FROM %s WHERE a=? ORDER BY b DESC", 0),
                row(1), row(1), row(1), row(0), row(0), row(0));

        // select c, order by b
        assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b ASC", 0),
                row(0), row(1), row(2), row(0), row(1), row(2));
        assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b DESC", 0),
                row(2), row(1), row(0), row(2), row(1), row(0));

        // select c, order by b, c
        assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                row(0), row(1), row(2), row(0), row(1), row(2));
        assertRows(execute("SELECT c FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                row(2), row(1), row(0), row(2), row(1), row(0));

        // select d, order by b, c
        assertRows(execute("SELECT d FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                row(0), row(1), row(2), row(3), row(4), row(5));
        assertRows(execute("SELECT d FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                row(5), row(4), row(3), row(2), row(1), row(0));
    }

    @Test
    public void testFunctionSelectionOrderMultipleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 2, 2);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 3);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 4);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 2, 5);

        assertInvalid("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY c ASC", 0);
        assertInvalid("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY c DESC", 0);
        assertInvalid("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC, c DESC", 0);
        assertInvalid("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC, c ASC", 0);
        assertInvalid("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY d ASC", 0);

        // select and order by b
        assertRows(execute("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                row(0), row(0), row(0), row(1), row(1), row(1));
        assertRows(execute("SELECT blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                row(1), row(1), row(1), row(0), row(0), row(0));

        assertRows(execute("SELECT b, blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                row(0, 0), row(0, 0), row(0, 0), row(1, 1), row(1, 1), row(1, 1));
        assertRows(execute("SELECT b, blobAsInt(intAsBlob(b)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                row(1, 1), row(1, 1), row(1, 1), row(0, 0), row(0, 0), row(0, 0));

        // select c, order by b
        assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b ASC", 0),
                row(0), row(1), row(2), row(0), row(1), row(2));
        assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b DESC", 0),
                row(2), row(1), row(0), row(2), row(1), row(0));

        // select c, order by b, c
        assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                row(0), row(1), row(2), row(0), row(1), row(2));
        assertRows(execute("SELECT blobAsInt(intAsBlob(c)) FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                row(2), row(1), row(0), row(2), row(1), row(0));

        // select d, order by b, c
        assertRows(execute("SELECT blobAsInt(intAsBlob(d)) FROM %s WHERE a=? ORDER BY b ASC, c ASC", 0),
                row(0), row(1), row(2), row(3), row(4), row(5));
        assertRows(execute("SELECT blobAsInt(intAsBlob(d)) FROM %s WHERE a=? ORDER BY b DESC, c DESC", 0),
                row(5), row(4), row(3), row(2), row(1), row(0));

    }
}
