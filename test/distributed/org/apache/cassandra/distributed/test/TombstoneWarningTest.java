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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.LogResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TombstoneWarningTest extends TestBaseImpl
{
    private static final int COMPACTION_TOMBSTONE_WARN = 75;
    private static final ICluster<IInvokableInstance> cluster;

    static
    {
        try
        {
            Cluster.Builder builder = Cluster.build(3);
            builder.withConfig(c -> c.set("compaction_tombstone_warning_threshold", COMPACTION_TOMBSTONE_WARN));
            cluster = builder.createWithoutStarting();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    @BeforeClass
    public static void setupClass()
    {
        cluster.startup();
    }

    @Before
    public void setup()
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(cluster);
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
    }

    @Test
    public void regularTombstonesLogTest()
    {
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                cluster.coordinator(1).execute(withKeyspace("update %s.tbl set v = null where pk = ? and ck = ?"), ConsistencyLevel.ALL, i, j);
        assertTombstoneLogs(99 - COMPACTION_TOMBSTONE_WARN , false);
    }

    @Test
    public void rowTombstonesLogTest()
    {
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                cluster.coordinator(1).execute(withKeyspace("delete from %s.tbl where pk = ? and ck = ?"), ConsistencyLevel.ALL, i, j);
        assertTombstoneLogs(99 - COMPACTION_TOMBSTONE_WARN , false);
    }

    @Test
    public void rangeTombstonesLogTest()
    {
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                cluster.coordinator(1).execute(withKeyspace("delete from %s.tbl where pk = ? and ck >= ? and ck <= ?"), ConsistencyLevel.ALL, i, j, j);
        assertTombstoneLogs(99 - (COMPACTION_TOMBSTONE_WARN / 2), true);
    }

    @Test
    public void ttlTest() throws InterruptedException
    {
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v) values (?, ?, ?) using ttl 1000"), ConsistencyLevel.ALL, i, j, j);
        assertTombstoneLogs(0, true);
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                cluster.coordinator(1).execute(withKeyspace("update %s.tbl using ttl 1 set v = 33 where pk = ? and ck = ?"), ConsistencyLevel.ALL, i, j);
        Thread.sleep(1500);
        assertTombstoneLogs(99 - COMPACTION_TOMBSTONE_WARN, false);
    }

    @Test
    public void noTombstonesLogTest()
    {
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v) values (?, ?, ?)"), ConsistencyLevel.ALL, i, j, j);
        assertTombstoneLogs(0, false);
    }

    private void assertTombstoneLogs(long expectedCount, boolean isRangeTombstones)
    {
        long mark = cluster.get(1).logs().mark();
        cluster.get(1).flush(KEYSPACE);
        String pattern = ".*Guardrail partition_tombstones violated: " +
                         "Partition distributed_test_keyspace\\.tbl:(?<key>\\d+).* has (?<tscount>\\d+) tombstones.*";
        LogResult<List<String>> res = cluster.get(1).logs().grep(mark, pattern);
        assertEquals(expectedCount, res.getResult().size());
        Pattern p = Pattern.compile(pattern);
        for (String r : res.getResult())
        {
            Matcher m = p.matcher(r);
            assertTrue(m.matches());
            long tombstoneCount = Integer.parseInt(m.group("tscount"));
            assertTrue(tombstoneCount > COMPACTION_TOMBSTONE_WARN);
            assertEquals(r, Integer.parseInt(m.group("key")) * (isRangeTombstones ? 2 : 1), tombstoneCount);
        }

        mark = cluster.get(1).logs().mark();
        cluster.get(1).forceCompact(KEYSPACE, "tbl");
        res = cluster.get(1).logs().grep(mark, pattern);
        assertEquals(expectedCount, res.getResult().size());
    }
}
