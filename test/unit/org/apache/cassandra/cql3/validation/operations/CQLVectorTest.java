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

package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

public class CQLVectorTest extends CQLTester.InMemory
{
    @Test
    public void select()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk vector<int, 2> primary key)");

        execute("INSERT INTO %s (pk) VALUES ([1, 2])");

        assertRows(execute("SELECT * FROM %s WHERE pk = [1, 2]"), row(list(1, 2)));
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", vector(1, 2)), row(list(1, 2)));
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, 1 + 1]"), row(list(1, 2)));
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, ?]", 2), row(list(1, 2)));
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, (int) ?]", 2), row(list(1, 2)));
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, 1 + (int) ?]", 1), row(list(1, 2)));

        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 2])"), row(list(1, 2)));
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 2], [1, 2])"), row(list(1, 2)));
        assertRows(execute("SELECT * FROM %s WHERE pk IN (?)", vector(1, 2)), row(list(1, 2)));
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 1 + 1])"), row(list(1, 2)));
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, ?])", 2), row(list(1, 2)));
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, (int) ?])", 2), row(list(1, 2)));
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 1 + (int) ?])", 1), row(list(1, 2)));
    }

    @Test
    public void insert()
    {
        Runnable test = () -> {
            assertRows(execute("SELECT * FROM %s"), row(list(1, 2)));
            execute("TRUNCATE %s");
            assertRows(execute("SELECT * FROM %s"));
        };

        createTable(KEYSPACE, "CREATE TABLE %s (pk vector<int, 2> primary key)");

        execute("INSERT INTO %s (pk) VALUES ([1, 2])");
        test.run();

        execute("INSERT INTO %s (pk) VALUES (?)", vector(1, 2));
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, 1 + 1])");
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, (int) ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, 1 + (int) ?])", 1);
        test.run();
    }

    @Test
    public void insertNonPK()
    {
        Runnable test = () -> {
            assertRows(execute("SELECT * FROM %s"), row(0, list(1, 2)));
            execute("TRUNCATE %s");
            assertRows(execute("SELECT * FROM %s"));
        };

        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, 2])");
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, 2));
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, 1 + 1])");
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, (int) ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, 1 + (int) ?])", 1);
        test.run();
    }

    @Test
    public void update()
    {
        Runnable test = () -> {
            assertRows(execute("SELECT * FROM %s"), row(0, list(1, 2)));
            execute("TRUNCATE %s");
            assertRows(execute("SELECT * FROM %s"));
        };

        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        execute("UPDATE %s set VALUE = [1, 2] WHERE pk = 0");
        test.run();

        execute("UPDATE %s set VALUE = ? WHERE pk = 0", vector(1, 2));
        test.run();

        execute("UPDATE %s set VALUE = [1, 1 + 1] WHERE pk = 0");
        test.run();

        execute("UPDATE %s set VALUE = [1, ?] WHERE pk = 0", 2);
        test.run();

        execute("UPDATE %s set VALUE = [1, (int) ?] WHERE pk = 0", 2);
        test.run();

        execute("UPDATE %s set VALUE = [1, 1 + (int) ?] WHERE pk = 0", 1);
        test.run();
    }
}
