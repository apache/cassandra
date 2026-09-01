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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class ClusteringKeyIndexTest extends SAITester
{
    @Before
    public void createTableAndIndex()
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 text, val int, PRIMARY KEY((pk1), pk2)) WITH CLUSTERING ORDER BY (pk2 DESC)");
        createIndex("CREATE CUSTOM INDEX pk2_idx ON %s(pk2) USING 'StorageAttachedIndex'");

        disableCompaction();
    }

    @After
    public void resetSegmentSize()
    {
        SegmentBuilder.updateLastValidSegmentRowId(-1);
    }

    private void insertData1() throws Throwable
    {
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (1, '1', 1)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (2, '2', 2)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (3, '3', 3)");
    }

    private void insertData2() throws Throwable
    {
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (4, '4', 4)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (5, '5', 5)");
        execute("INSERT INTO %s (pk1, pk2, val) VALUES (6, '6', 6)");
    }

    @Test
    public void queryFromMemtable() throws Throwable
    {
        insertData1();
        insertData2();
        runQueries();
    }


    @Test
    public void reversedBigintClusteringMultiSegment()
    {
        createTable("CREATE TABLE %s (pk int, ck bigint, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)");
        createIndex("CREATE INDEX ON %s(ck) USING 'sai'");
        disableCompaction(keyspace());

        for (long ck = 10; ck < 16; ck++)
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 1, ck);
        flush();

        for (long ck = 16; ck < 22; ck++)
            execute("INSERT INTO %s (pk, ck) VALUES (?, ?)", 1, ck);
        flush();

        // Force compaction with a small segment size cap, producing multiple segments.
        SegmentBuilder.updateLastValidSegmentRowId(3);
        compact();

        for (long ck = 10; ck < 22; ck++)
        {
            int cnt = getRows(execute("SELECT ck FROM %s WHERE ck = ?", ck)).length;
            assertEquals("Missing row for ck=" + ck, 1, cnt);
        }
    }

    private Object[] expectedRow(int index)
    {
        return row(index, Integer.toString(index), index);
    }

    private void runQueries() throws Throwable
    {
        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 = 2"), expectedRow(2));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk2 = '2'"), expectedRow(2));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE pk1 = -1 AND pk2 = '2'"));

        assertThatThrownBy(()->execute("SELECT * FROM %s WHERE pk1 = -1 AND val = 2")).hasMessageContaining("use ALLOW FILTERING");

    }
}
