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
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

/**
 * Tests CASSANDRA-18105.
 * <p>
 * Truncation will insert an entry to system.local into truncated_at map for given table. Because id of an index
 * is same as id of the base table, when dropping an index, it in fact drops a table with same id as the base table
 * which will remove an entry for truncated_at column. Hence, after restart of the node, querying the base table
 * will resurrect the data because commitlog was fully replayed, not taking into consideration (now removed) entry
 * in truncated_at column.
 * <p>
 * The fix consists of not removing an entry in truncated_at column in system.local
 * if the table being removed is an index.
 */
public class IndexDroppingTest extends TestBaseImpl
{

    private static Cluster CLUSTER;

    @BeforeClass
    public static void init() throws IOException
    {
        CLUSTER = Cluster.build(1).withConfig(conf -> conf.with(NETWORK).set("materialized_views_enabled", "true")).start();
        CLUSTER.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> CompactionManager.instance.disableAutoCompaction());
    }

    @AfterClass
    public static void shutdown()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testIndexDropping() throws Throwable
    {
        CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".tb1 (c3 TEXT, c4 TEXT, c2 INT, c1 TEXT, PRIMARY KEY (c1, c2, c3 ))");
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tb1 (c3, c1, c2) VALUES ('val1','val2',1)", ONE);
        CLUSTER.schemaChange("CREATE INDEX tb ON " + KEYSPACE + ".tb1 (c3)");

        waitForIndex(KEYSPACE, "tb1", "tb");

        CLUSTER.schemaChange("TRUNCATE TABLE " + KEYSPACE + " .tb1");
        CLUSTER.schemaChange("DROP INDEX " + KEYSPACE + ".tb");

        assertEmptyTable("tb1");
        restart();
        assertEmptyTable("tb1");
    }

    private void restart() throws Throwable
    {
        CLUSTER.get(1).shutdown(true).get();
        CLUSTER.get(1).startup();
        CLUSTER.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () ->
                                                                              CompactionManager.instance.disableAutoCompaction());
    }

    private void assertEmptyTable(String table)
    {
        assertEquals(0, CLUSTER.coordinator(1).execute("SELECT * FROM " + KEYSPACE + " ." + table, ONE).length);
    }

    private static void waitForIndex(String keyspace, String table, String indexName)
    {
        Instant startTime = Instant.now();
        Instant maxTime = startTime.plus(10, MINUTES);

        boolean builtIndex = false;
        while (!builtIndex)
        {
            if (maxTime.isBefore(Instant.now()))
                return;

            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            builtIndex = CLUSTER.get(1).applyOnInstance((IIsolatedExecutor.SerializableTriFunction<String, String, String, Boolean>) (ks, tbl, idx) -> {
                ColumnFamilyStore cf = ColumnFamilyStore.getIfExists(ks, tbl);
                if (cf == null)
                    return false;
                return cf.indexManager.getBuiltIndexNames().contains(idx);
            }, keyspace, table, indexName);
        }
    }
}
