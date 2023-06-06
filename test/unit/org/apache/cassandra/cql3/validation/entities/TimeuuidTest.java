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

package org.apache.cassandra.cql3.validation.entities;

import java.util.Date;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.TimeUUID;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

public class TimeuuidTest extends CQLTester
{
    /**
     * Migrated from cql_tests.py:TestCQL.timeuuid_test()
     */
    @Test
    public void testTimeuuid() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, t timeuuid, PRIMARY KEY(k, t))");

        assertInvalidThrow(SyntaxException.class, "INSERT INTO %s (k, t) VALUES (0, 2012-11-07 18:18:22-0800)");

        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (k, t) VALUES (0, now())");

        Object[][] rows = getRows(execute("SELECT * FROM %s"));
        assertEquals(4, rows.length);

        assertRowCount(execute("SELECT * FROM %s WHERE k = 0 AND t >= ?", rows[0][1]), 4);

        assertEmpty(execute("SELECT * FROM %s WHERE k = 0 AND t < ?", rows[0][1]));

        assertRowCount(execute("SELECT * FROM %s WHERE k = 0 AND t > ? AND t <= ?", rows[0][1], rows[2][1]), 2);

        assertRowCount(execute("SELECT * FROM %s WHERE k = 0 AND t = ?", rows[0][1]), 1);

        assertInvalidMessage("k cannot be passed as argument 0 of function",
                             "SELECT min_timeuuid(k) FROM %s WHERE k = 0 AND t = ?", rows[0][1]);

        for (int i = 0; i < 4; i++)
        {
            long timestamp = ((TimeUUID) rows[i][1]).unix(MILLISECONDS);
            assertRows(execute("SELECT to_timestamp(t), to_unix_timestamp(t) FROM %s WHERE k = 0 AND t = ?", rows[i][1]),
                       row(new Date(timestamp), timestamp));
        }

        assertEmpty(execute("SELECT t FROM %s WHERE k = 0 AND t > max_timeuuid(1234567) AND t < min_timeuuid('2012-11-07 18:18:22-0800')"));
        assertEmpty(execute("SELECT t FROM %s WHERE k = 0 AND t > max_timeuuid(1564830182000) AND t < min_timeuuid('2012-11-07 18:18:22-0800')"));
    }

    /**
     * Test for 5386,
     * migrated from cql_tests.py:TestCQL.function_and_reverse_type_test()
     */
    @Test
    public void testDescClusteringOnTimeuuid() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c timeuuid, v int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)");

        execute("INSERT INTO %s (k, c, v) VALUES (0, now(), 0)");
    }
}
