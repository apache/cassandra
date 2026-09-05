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

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

public class PrefixSearchCQLTest extends SAITester
{
    private void createPrefixTable()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, name text)");
        createIndex("CREATE INDEX ON %s(name) USING 'sai' WITH OPTIONS = {'enable_literal_prefix_sai': 'true'}");
    }

    @Test
    public void testPrefixFromMemtable() throws Throwable
    {
        createPrefixTable();
        execute("INSERT INTO %s (id, name) VALUES (1, 'apple')");
        execute("INSERT INTO %s (id, name) VALUES (2, 'application')");
        execute("INSERT INTO %s (id, name) VALUES (3, 'app')");
        execute("INSERT INTO %s (id, name) VALUES (4, 'car')");

        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'app%%'"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'appl%%'"), 2);
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'car%%'"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'xyz%%'"), 0);
    }

    @Test
    public void testPrefixIncludesExactTermFromMemtable() throws Throwable
    {
        createPrefixTable();
        execute("INSERT INTO %s (id, name) VALUES (1, 'foo')");
        execute("INSERT INTO %s (id, name) VALUES (2, 'foobar')");

        // 'foo%' matches both 'foo' (exact) and 'foobar'
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'foo%%'"), 2);
    }

    @Test
    public void testExactMatchStillWorksOnPrefixIndexMemtable() throws Throwable
    {
        createPrefixTable();
        execute("INSERT INTO %s (id, name) VALUES (1, 'apple')");
        execute("INSERT INTO %s (id, name) VALUES (2, 'car')");

        assertRows(execute("SELECT id FROM %s WHERE name = 'apple'"), row(1));
        assertRows(execute("SELECT id FROM %s WHERE name = 'car'"), row(2));
        assertRowCount(execute("SELECT * FROM %s WHERE name = 'ap'"), 0);
    }

    @Test
    public void testPrefixFromSSTable() throws Throwable
    {
        createPrefixTable();
        execute("INSERT INTO %s (id, name) VALUES (1, 'apple')");
        execute("INSERT INTO %s (id, name) VALUES (2, 'application')");
        execute("INSERT INTO %s (id, name) VALUES (3, 'app')");
        execute("INSERT INTO %s (id, name) VALUES (4, 'car')");

        flush();

        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'app%%'"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'appl%%'"), 2);
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'car%%'"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'xyz%%'"), 0);
    }

    @Test
    public void testPrefixIncludesExactTermFromSSTable() throws Throwable
    {
        createPrefixTable();
        execute("INSERT INTO %s (id, name) VALUES (1, 'foo')");
        execute("INSERT INTO %s (id, name) VALUES (2, 'foobar')");

        flush();

        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'foo%%'"), 2);
    }

    @Test
    public void testExactMatchStillWorksOnPrefixIndexSSTable() throws Throwable
    {
        createPrefixTable();
        execute("INSERT INTO %s (id, name) VALUES (1, 'apple')");
        execute("INSERT INTO %s (id, name) VALUES (2, 'car')");

        flush();

        assertRows(execute("SELECT id FROM %s WHERE name = 'apple'"), row(1));
        assertRows(execute("SELECT id FROM %s WHERE name = 'car'"), row(2));
    }

    @Test
    public void testPrefixAcrossMemtableAndSSTable() throws Throwable
    {
        createPrefixTable();
        execute("INSERT INTO %s (id, name) VALUES (1, 'apple')");
        execute("INSERT INTO %s (id, name) VALUES (2, 'apricot')");
        flush();
        execute("INSERT INTO %s (id, name) VALUES (3, 'application')");
        execute("INSERT INTO %s (id, name) VALUES (4, 'banana')");

        // 'ap%' spans the flushed SSTable (apple, apricot) and the live memtable (application)
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'ap%%'"), 3);
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'appl%%'"), 2);
    }

    @Test
    public void testLargePrefixUsesAggregatedSection() throws Throwable
    {
        createPrefixTable();
        // 100 distinct terms sharing the 3-char prefix "abc" (an eligible depth for postings_skip=3 with
        // >= minimum_postings_leaves=64 rows), so an aggregated prefix section is written and the read path
        // can jump straight to it instead of scanning every term.
        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (id, name) VALUES (?, ?)", i, String.format("abc%04d", i));
        execute("INSERT INTO %s (id, name) VALUES (1000, 'zzz')");

        flush();

        // 'abc%' lands on the aggregated prefix node (fast path); 'ab%' is a shorter prefix that falls back to the
        // range scan. Both must return all 100 matching rows.
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'abc%%'"), 100);
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'ab%%'"), 100);
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'abc0001%%'"), 1);
        assertRowCount(execute("SELECT * FROM %s WHERE name LIKE 'zzz%%'"), 1);
    }

    @Test
    public void testInvalidOptionOnNonStringColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, score int)");
        assertInvalidThrow(InvalidRequestException.class,
                           "CREATE INDEX ON %s(score) USING 'sai' WITH OPTIONS = {'enable_literal_prefix_sai': 'true'}");
    }
}
