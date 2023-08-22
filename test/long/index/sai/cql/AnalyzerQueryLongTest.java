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

package index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThat;

public class AnalyzerQueryLongTest extends CQLTester
{
    @Test
    public void manyWritesTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, not_analyzed int, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': '[{\"tokenizer\": \"standard\"}, {\"filter\": \"lowercase\"}]' }");
        waitForIndex(KEYSPACE, currentTable(), "val");
        var iterations = 15000;
        for (int i = 0; i < iterations; i++)
        {
            var x = i % 100;
            if (i % 100 == 0)
            {
                execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (?, ?, ?)", i, x, "this will be tokenized");
            }
            else if (i % 2 == 0)
            {
                execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (?, ?, ?)", i, x, "this is different");
            }
            else
            {
                execute("INSERT INTO %s (pk, not_analyzed, val) VALUES (?, ?, ?)", i, x, "basic test");
            }
        }
        var result = execute("SELECT * FROM %s WHERE val : 'tokenized'");
        assertThat(result).hasSize(iterations / 100);
        result = execute("SELECT * FROM %s WHERE val : 'this'");
        assertThat(result).hasSize(iterations / 2);
        result = execute("SELECT * FROM %s WHERE val : 'test'");
        assertThat(result).hasSize(iterations / 2);
    }

    @Test
    public void manyWritesAndUpsertsTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': '[{\"tokenizer\": \"standard\"}, {\"filter\": \"lowercase\"}]' }");
        waitForIndex(KEYSPACE, currentTable(), "val");
        var iterations = 15000;
        for (int i = 0; i < iterations; i++)
        {
            if (i % 999 == 0) {
                // flush on irregular cadence so that final queries are executed based on paritially flushed data
                flush();
            }
            if (i % 2 == 0)
            {
                // Upsert the same entry many times
                execute("INSERT INTO %s (pk, val) VALUES (0, 'text to be analyzed')");
            }
            else
            {
                execute("INSERT INTO %s (pk, val) VALUES (?, 'different text to be analyzed')", i);
            }
        }
        var result = execute("SELECT * FROM %s WHERE val : 'different'");
        assertThat(result).hasSize(iterations / 2);
        result = execute("SELECT * FROM %s WHERE val : 'text'");
        assertThat(result).hasSize(iterations / 2 + 1);
    }
    @Test
    public void manyWritesUpsertsAndDeletesForSamePKTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': '[{\"tokenizer\": \"standard\"}, {\"filter\": \"lowercase\"}]' }");
        waitForIndex(KEYSPACE, currentTable(), "val");
        var iterations = 15000;
        for (int i = 0; i < iterations; i++)
        {
            if (i % 999 == 0) {
                // flush on irregular cadence so that final queries are executed based on paritially flushed data
                flush();
            }
            // 3 works well because 14999 is the last value for i, and it is not divisible by 3
            // Further, it by using 3, we first insert, then upsert, then delet, and repeat.
            if (i % 3 == 0)
            {
                // Upsert the same entry many times
                execute("DELETE FROM %s WHERE pk = 0");
            }
            else if (i % 2 == 0)
            {
                execute("INSERT INTO %s (pk, val) VALUES (0, 'text to be analyzed')");
            }
            else
            {
                execute("INSERT INTO %s (pk, val) VALUES (0, 'completely different value')");
            }
        }
        // 'completely different value' wins
        var result = execute("SELECT * FROM %s WHERE val : 'text'");
        assertThat(result).hasSize(0);
        result = execute("SELECT * FROM %s WHERE val : 'value'");
        assertThat(result).hasSize(1);
    }
}
