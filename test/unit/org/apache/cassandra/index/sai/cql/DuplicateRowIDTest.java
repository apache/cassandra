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

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Row;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

public class DuplicateRowIDTest extends SAITester
{
    @BeforeClass
    public static void setupCluster()
    {
        requireNetwork();
    }

    @Test
    public void shouldTolerateDuplicatedRowIDsAfterMemtableUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT)");
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        for (int i = 0; i < 2048; ++i)
        {
            execute("INSERT INTO %s (id1, v1) VALUES (?, ?)", Integer.toString(i % 10), i);
        }

        // tolerate query duplicates from memtable
        List<Row> rows = executeNet("SELECT * FROM %s WHERE v1 > 0").all();
        assertEquals(10, rows.size());

        for (int i = 0; i < 2048; ++i)
        {
            execute("INSERT INTO %s (id1, v1) VALUES (?, ?)", Integer.toString(i % 10), i);
        }

        // tolerate duplicates from memtable and sstable
        rows = executeNet("SELECT * FROM %s WHERE v1 > 0").all();
        assertEquals(10, rows.size());
    }
}
