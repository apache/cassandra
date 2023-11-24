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
package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.index.sai.SAITester;

public class CompositePartitionKeyIndexTest extends SAITester
{
    @Test
    public void testCompositePartitionIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 text, val int, PRIMARY KEY((pk1, pk2)))");
        createIndex("CREATE INDEX ON %s(pk1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(pk2) USING 'sai'");

        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, '1', 1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (2, '2', 2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (3, '3', 3)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (4, '4', 4)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (5, '5', 5)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (6, '6', 6)");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 = 2"),
                                    expectedRow(2));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 > 1"),
                                    expectedRow(2),
                                    expectedRow(3),
                                    expectedRow(4),
                                    expectedRow(5),
                                    expectedRow(6));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 >= 3"),
                                    expectedRow(3),
                                    expectedRow(4),
                                    expectedRow(5),
                                    expectedRow(6));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 < 3"),
                                    expectedRow(1),
                                    expectedRow(2));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 <= 3"),
                                    expectedRow(1),
                                    expectedRow(2),
                                    expectedRow(3));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk2 = '2'"),
                                    expectedRow(2));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 > 1 AND pk2 = '2'"),
                                    expectedRow(2));

            assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 = -1 AND pk2 = '2'"));

            assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, "SELECT * FROM %s WHERE pk1 = -1 AND val = 2");
        });
    }

    @Test
    public void testFilterWithIndexForContains() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v set<int>, PRIMARY KEY ((k1, k2)))");
        createIndex("CREATE INDEX ON %s(k2) USING 'sai'");

        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 0, set(1, 2, 3));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 0, 1, set(2, 3, 4));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 0, set(3, 4, 5));
        execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", 1, 1, set(4, 5, 6));

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE k2 = ?", 1),
                       row(0, 1, set(2, 3, 4)),
                       row(1, 1, set(4, 5, 6))
            );

            assertRows(execute("SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 6),
                       row(1, 1, set(4, 5, 6))
            );

            assertEmpty(execute("SELECT * FROM %s WHERE k2 = ? AND v CONTAINS ? ALLOW FILTERING", 1, 7));
        });
    }

    private Object[] expectedRow(int index)
    {
        return row(index, Integer.toString(index), index);
    }
}
