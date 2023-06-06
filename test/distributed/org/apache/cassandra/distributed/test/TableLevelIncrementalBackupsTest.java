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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;

import static org.apache.cassandra.Util.getBackups;
import static org.apache.cassandra.distributed.Cluster.build;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.test.ExecUtil.rethrow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableLevelIncrementalBackupsTest extends TestBaseImpl  
{
    @Test
    public void testIncrementalBackupEnabledCreateTable() throws Exception
    {
        try (Cluster c = getCluster(true))
        {
            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table (a text primary key, b int) WITH incremental_backups = true"));
            disableCompaction(c, KEYSPACE, "test_table");
            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table (a, b) VALUES ('a', 1)"), ALL);
            flush(c, KEYSPACE);
            assertBackupSSTablesCount(c, 1, true, KEYSPACE, "test_table");

            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table (a, b) VALUES ('a', 1)"), ALL);
            flush(c, KEYSPACE);
            assertBackupSSTablesCount(c, 2, true, KEYSPACE, "test_table");
            c.schemaChange(withKeyspace("DROP TABLE %s.test_table"));

            
            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table2 (a text primary key, b int) WITH incremental_backups = false"));
            disableCompaction(c, KEYSPACE, "test_table2");
            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table2 (a, b) VALUES ('a', 1)"), ALL);
            flush(c, KEYSPACE);
            assertBackupSSTablesCount(c, 0, false, KEYSPACE, "test_table2");
            c.schemaChange(withKeyspace("DROP TABLE %s.test_table2"));
        }
    }

    @Test
    public void testIncrementalBackupEnabledAlterTable() throws Exception
    {
        try (Cluster c = getCluster(true))
        {
            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table (a text primary key, b int) WITH incremental_backups = false"));
            disableCompaction(c, KEYSPACE, "test_table");
            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table (a, b) VALUES ('a', 1)"), ALL);
            flush(c, KEYSPACE);
            assertBackupSSTablesCount(c, 0, false, KEYSPACE, "test_table");

            c.schemaChange(withKeyspace("ALTER TABLE %s.test_table  WITH incremental_backups = true"));
            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table (a, b) VALUES ('a', 1)"), ALL);
            flush(c, KEYSPACE);
            assertBackupSSTablesCount(c, 1, true, KEYSPACE, "test_table");
            c.schemaChange(withKeyspace("DROP TABLE %s.test_table"));


            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table2 (a text primary key, b int) WITH incremental_backups = true"));
            disableCompaction(c, KEYSPACE, "test_table2");
            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table2 (a, b) VALUES ('a', 1)"), ALL);
            flush(c, KEYSPACE);
            assertBackupSSTablesCount(c, 1, true, KEYSPACE, "test_table2");

            c.schemaChange(withKeyspace("ALTER TABLE %s.test_table2  WITH incremental_backups = false"));
            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table2 (a, b) VALUES ('a', 1)"), ALL);
            flush(c, KEYSPACE);
            assertBackupSSTablesCount(c, 1, false, KEYSPACE, "test_table2");
            c.schemaChange(withKeyspace("DROP TABLE %s.test_table2"));
        }
    }

    @Test
    public void testIncrementalBackupWhenCreateTableByDefault() throws Exception
    {
        try (Cluster c = getCluster(true))
        {
            //incremental_backups is set to true by default
            c.schemaChange(withKeyspace("CREATE TABLE %s.test_table (a text primary key, b int)"));
            disableCompaction(c, KEYSPACE, "test_table");
            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table (a, b) VALUES ('a', 1)"), ALL);
            flush(c, KEYSPACE);
            assertBackupSSTablesCount(c, 1, true, KEYSPACE, "test_table");

            c.schemaChange(withKeyspace("ALTER TABLE %s.test_table  WITH incremental_backups = false"));
            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table (a, b) VALUES ('a', 1)"), ALL);
            flush(c, KEYSPACE);
            assertBackupSSTablesCount(c, 1, false, KEYSPACE, "test_table");

            c.schemaChange(withKeyspace("ALTER TABLE %s.test_table  WITH incremental_backups = true"));
            c.coordinator(1).execute(withKeyspace("INSERT INTO %s.test_table (a, b) VALUES ('a', 1)"), ALL);
            flush(c, KEYSPACE);
            assertBackupSSTablesCount(c, 2, true, KEYSPACE, "test_table");
            
            c.schemaChange(withKeyspace("DROP TABLE %s.test_table"));
        }
    }

    private Cluster getCluster(boolean incrementalBackups) throws IOException
    {
        return init(build(2).withDataDirCount(1).withConfig(c -> c.with(Feature.GOSSIP)
                .set("incremental_backups", incrementalBackups)).start());
    }

    private void flush(Cluster cluster, String keyspace) 
    {
        for (int i = 1; i < cluster.size() + 1; i++)
            cluster.get(i).flush(keyspace);
    }

    private void disableCompaction(Cluster cluster, String keyspace, String table)
    {
        for (int i = 1; i < cluster.size() + 1; i++)
            cluster.get(i).nodetool("disableautocompaction", keyspace, table);
    }

    private static  void assertBackupSSTablesCount(Cluster cluster, int expectedSeqGenIds, boolean enable, String ks, String... tableNames)
    {
        for (int i = 1; i < cluster.size() + 1; i++)
        {
            cluster.get(i).runOnInstance(rethrow(() -> Arrays.stream(tableNames).forEach(tableName ->  assertTableMetaIncrementalBackupEnable(ks, tableName, enable))));
            cluster.get(i).runOnInstance(rethrow(() -> Arrays.stream(tableNames).forEach(tableName -> assertSSTablesCount(getBackups(ks, tableName), tableName, expectedSeqGenIds))));
        }
    }

    private static void assertSSTablesCount(Set<Descriptor> descs, String tableName, int expectedSeqGenIds)
    {
        List<String> seqSSTables = descs.stream()
                                        .filter(desc -> desc.id instanceof SequenceBasedSSTableId)
                                        .map(descriptor -> descriptor.baseFile().toString())
                                        .sorted()
                                        .collect(Collectors.toList());
        assertThat(seqSSTables).describedAs("SSTables of %s with sequence based id", tableName).hasSize(expectedSeqGenIds);
    }

    private static void assertTableMetaIncrementalBackupEnable(String ks, String tableName, boolean enable)
    {
        ColumnFamilyStore columnFamilyStore = ColumnFamilyStore.getIfExists(ks, tableName);
        if (enable)
            assertTrue(columnFamilyStore.isTableIncrementalBackupsEnabled());
        else
            assertFalse(columnFamilyStore.isTableIncrementalBackupsEnabled());
    }
}
