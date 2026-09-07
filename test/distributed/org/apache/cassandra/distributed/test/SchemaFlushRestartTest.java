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
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.SCHEMA_FLUSH_COALESCE_MS;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.junit.Assert.assertEquals;

/**
 * Crash-restart correctness for the coalesced/asynchronous {@code system_schema} flush
 * (see SchemaKeyspace#scheduleFlush()). Creates a burst of tables under a long coalesce window so the
 * flush task backing them has certainly not run yet, kills the node without a drain (so no synchronous
 * flush of any kind happens either), then restarts and confirms every table survives: durability here
 * comes from the commitlog (system_schema has durable_writes=true) and the TCM log, not from the
 * system_schema flush, so this must hold regardless of how long that flush is deferred.
 */
public class SchemaFlushRestartTest extends TestBaseImpl
{
    private static final int TABLE_COUNT = 30;

    @Test
    public void schemaSurvivesRestartWithoutDrainUnderCoalescedFlush() throws IOException
    {
        try (WithProperties properties = new WithProperties();
             Cluster cluster = init(Cluster.build(1).start()))
        {
            // Set a long coalesce window (60s) before any DDL below, so none of the CREATE TABLEs below
            // get a chance to actually trigger a system_schema flush before the node is killed.
            properties.set(SCHEMA_FLUSH_COALESCE_MS, "60000");

            List<String> tableNames = new ArrayList<>(TABLE_COUNT);
            for (int i = 0; i < TABLE_COUNT; i++)
            {
                String table = "tbl" + i;
                tableNames.add(table);
                cluster.schemaChange(withKeyspace("CREATE TABLE %s." + table + " (k int PRIMARY KEY, v int)"));
            }

            IInvokableInstance instance = cluster.get(1);

            // Crash without drain: StorageService.drain() is never called, so there is no synchronous
            // flush of any kind (system_schema included) here. Combined with the 60s coalesce window
            // above, the coalesced flush task for these 30 DDLs is guaranteed not to have run either.
            stopUnchecked(instance);
            instance.startup();

            Object[][] rows = cluster.coordinator(1).execute(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
                ConsistencyLevel.ONE, KEYSPACE);
            assertEquals(TABLE_COUNT, rows.length);

            for (String table : tableNames)
            {
                Object[][] result = cluster.coordinator(1).execute(
                    withKeyspace("SELECT * FROM %s." + table), ConsistencyLevel.ONE);
                assertEquals(0, result.length);
            }
        }
    }
}
