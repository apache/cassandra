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

package org.apache.cassandra.service.snapshot;

import java.util.Date;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;

@Ignore
public class SnapshotManager2Test
{
    static final String KEYSPACE = "KEYSPACE";

    static int NUM_SSTABLES = 30;
    static int NUM_KEYSPACES = 1;
    static int NUM_TABLES_PER_KEYSPACE = 10;
    static int NUM_SNAPSHOTS_PER_TABLE = 10;

    static int NUM_RUNS = 100;

    static SnapshotManager snapshotManager;

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();

        for (int i = 0; i < NUM_KEYSPACES; i++)
        {
            String keyspaceName = KEYSPACE + '_' + i;

            // Create Schema
            TableMetadata[] tables = new TableMetadata[NUM_TABLES_PER_KEYSPACE];
            for (int j = 0; j < NUM_TABLES_PER_KEYSPACE; j++)
            {
                tables[j] = SchemaLoader.standardCFMD(keyspaceName, tableName(i + '_' + j)).build();
            }

            SchemaLoader.createKeyspace(keyspaceName,
                                        KeyspaceParams.simple(1),
                                        tables);

            for (int j = 0; j < NUM_TABLES_PER_KEYSPACE; j++)
            {
                String tableName = tableName(i + '_' + j);

                ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
                cfs.disableAutoCompaction();
                for (int k = 0; k < NUM_SSTABLES; k++)
                {
                    System.out.printf("Creating sstable %d of table %s%n", k, tableName);
                    new RowUpdateBuilder(cfs.metadata(), 0, "key1")
                    .clustering("Column1")
                    .add("val", "asdf")
                    .build()
                    .applyUnsafe();
                    Util.flush(cfs);
                }
                for (int k = 0; k < NUM_SNAPSHOTS_PER_TABLE; k++)
                {
                    System.out.printf("Creating snapshot %d of table %s.%s%n", k, cfs.keyspace.getName(), cfs.name);
                    SnapshotManager.instance.takeSnapshot(snapshotName(k), cfs.getKeyspaceTableName());
                }
            }
        }

        snapshotManager = new SnapshotManager(1000L, 60L);
        assertEquals(snapshotManager.getSnapshots(KEYSPACE).size(), 0);
        snapshotManager.addSnapshots(snapshotManager.loadSnapshots());
        assertEquals(snapshotManager.getSnapshots(t -> true).size(), NUM_KEYSPACES * NUM_TABLES_PER_KEYSPACE * NUM_SNAPSHOTS_PER_TABLE);
    }

    private static String tableName(int i)
    {
        return String.format("table%d", i);
    }

    private static String snapshotName(int i)
    {
        return String.format("snap%d", i);
    }

    @Test
    public void testListSnapshotsCachedCheckExists()
    {
        long start = new Date().getTime();
        System.out.println(start);
        for (int i = 0; i < NUM_RUNS; i++)
        {
            snapshotManager.listSnapshots(null);
        }
        long end = new Date().getTime();
        System.out.println(end);
        System.out.println((double) (end - start) / NUM_RUNS);
    }
}