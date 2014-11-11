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

public class TupleTypeTest extends CQLTester
{
    @Test
    public void testTuplePutAndGet() throws Throwable
    {
        String[] valueTypes = {"frozen<tuple<int, text, double>>", "tuple<int, text, double>"};
        for (String valueType : valueTypes)
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, t " + valueType + ")");

            execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(3, "foo", 3.4));
            execute("INSERT INTO %s (k, t) VALUES (?, ?)", 1, tuple(8, "bar", 0.2));
            assertAllRows(
                row(0, tuple(3, "foo", 3.4)),
                row(1, tuple(8, "bar", 0.2))
            );

            // nulls
            execute("INSERT INTO %s (k, t) VALUES (?, ?)", 2, tuple(5, null, 3.4));
            assertRows(execute("SELECT * FROM %s WHERE k=?", 2),
                row(2, tuple(5, null, 3.4))
            );

            // incomplete tuple
            execute("INSERT INTO %s (k, t) VALUES (?, ?)", 3, tuple(5, "bar"));
            assertRows(execute("SELECT * FROM %s WHERE k=?", 3),
                row(3, tuple(5, "bar"))
            );
        }
    }

    @Test
    public void testNestedTuple() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t frozen<tuple<int, tuple<text, double>>>)");

        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(3, tuple("foo", 3.4)));
        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 1, tuple(8, tuple("bar", 0.2)));
        assertAllRows(
            row(0, tuple(3, tuple("foo", 3.4))),
            row(1, tuple(8, tuple("bar", 0.2)))
        );
    }

    @Test
    public void testTupleInPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (t frozen<tuple<int, text>> PRIMARY KEY)");

        execute("INSERT INTO %s (t) VALUES (?)", tuple(3, "foo"));
        assertAllRows(row(tuple(3, "foo")));
    }

    @Test
    public void testTupleInClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, t frozen<tuple<int, text>>, PRIMARY KEY (k, t))");

        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(5, "bar"));
        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(3, "foo"));
        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(6, "bar"));
        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(5, "foo"));

        assertAllRows(
            row(0, tuple(3, "foo")),
            row(0, tuple(5, "bar")),
            row(0, tuple(5, "foo")),
            row(0, tuple(6, "bar"))
        );
    }

    @Test
    public void testInvalidQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t frozen<tuple<int, text, double>>)");

        assertInvalidSyntax("INSERT INTO %s (k, t) VALUES (0, ())");
        assertInvalid("INSERT INTO %s (k, t) VALUES (0, (2, 'foo', 3.1, 'bar'))");
    }
}
