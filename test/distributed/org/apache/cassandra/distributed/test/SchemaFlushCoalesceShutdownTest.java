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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.SchemaKeyspace;

import static org.junit.Assert.assertFalse;

/**
 * Tests that coalesced system_schema flushes complete before shutdown when a node stops with a pending
 * coalesced flush. Without the fix, the scheduled flush is cancelled or fires against stopped executors,
 * leaving unflushed memtables that cause memory exhaustion in test suites with many in-jvm clusters.
 */
public class SchemaFlushCoalesceShutdownTest extends TestBaseImpl
{
    /**
     * Creates a table under a long coalescing window, then shuts down immediately. The pending flush
     * must be executed synchronously at shutdown. Verified by checking that a new system_schema.tables
     * sstable exists after shutdown. Without the fix, the pending flush is dropped and no new sstable
     * appears.
     */
    @Test
    public void shutdownFlushesCoalescedSchemaChanges() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                            .withConfig(c -> c.set("schema_flush_coalescing_window", "60s"))
                                            .start()))
        {
            IInvokableInstance instance = cluster.get(1);

            // Get the data directory path for system_schema.tables
            String dataDir = ((String[]) instance.config().get("data_file_directories"))[0];
            String tableId = instance.callOnInstance(() ->
                SchemaKeyspace.metadata().getTableOrViewNullable("tables").id.toHexString());
            Path tablesDir = Paths.get(dataDir, "system_schema", "tables-" + tableId);

            // Record existing tables sstables before the DDL
            Set<String> sstablesBefore = listDataFiles(tablesDir);

            // Create a table - schedules a coalesced flush with 60s delay (won't fire before shutdown)
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)"));

            // Shutdown immediately. With the fix, shutdownCoalescedFlush() cancels the scheduled task
            // and flushes synchronously. Without it, the pending flush is dropped.
            instance.shutdown().get();

            // Check that a NEW system_schema.tables sstable was created (proving the flush happened)
            Set<String> sstablesAfter = listDataFiles(tablesDir);
            sstablesAfter.removeAll(sstablesBefore);

            assertFalse("Shutdown must flush pending coalesced schema changes; expected new system_schema.tables sstable, found none",
                        sstablesAfter.isEmpty());
        }
    }

    /**
     * Multiple rapid DDL operations coalesce into a single pending flush. Shutdown must flush all changes.
     */
    @Test
    public void shutdownFlushesMultipleCoalescedChanges() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                            .withConfig(c -> c.set("schema_flush_coalescing_window", "60s"))
                                            .start()))
        {
            IInvokableInstance instance = cluster.get(1);

            String dataDir = ((String[]) instance.config().get("data_file_directories"))[0];
            String tableId = instance.callOnInstance(() ->
                SchemaKeyspace.metadata().getTableOrViewNullable("tables").id.toHexString());
            Path tablesDir = Paths.get(dataDir, "system_schema", "tables-" + tableId);

            Set<String> sstablesBefore = listDataFiles(tablesDir);

            // Burst of DDL - all coalesce into a single pending flush
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl1 (k int PRIMARY KEY, v int)"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl2 (k int PRIMARY KEY, v int)"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl3 (k int PRIMARY KEY, v int)"));

            instance.shutdown().get();

            Set<String> sstablesAfter = listDataFiles(tablesDir);
            sstablesAfter.removeAll(sstablesBefore);

            assertFalse("Shutdown must flush all coalesced schema changes; expected new sstable, found none",
                        sstablesAfter.isEmpty());
        }
    }

    private Set<String> listDataFiles(Path dir) throws IOException
    {
        if (!Files.exists(dir))
            return new HashSet<>();

        try (Stream<Path> files = Files.list(dir))
        {
            return files.filter(p -> p.getFileName().toString().endsWith("-Data.db"))
                        .map(p -> p.getFileName().toString())
                        .collect(Collectors.toSet());
        }
    }
}
