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

package org.apache.cassandra.test.microbench;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.tcm.ClusterMetadataService;

import static org.apache.cassandra.service.snapshot.SnapshotOptions.userSnapshot;
import static org.junit.Assert.assertEquals;

public abstract class AbstractSnapshotManagerBase
{
    static final String KEYSPACE = "KEYSPACE";

    private final Random random = new Random();

    static int NUM_SSTABLES = 10;
    static int NUM_KEYSPACES = 5;
    static int NUM_TABLES_PER_KEYSPACE = 10;
    static int NUM_SNAPSHOTS_PER_TABLE = 10;

    public void teardown()
    {
        SnapshotManager.instance.clearAllSnapshots();
        CQLTester.tearDownClass();
        CommitLog.instance.stopUnsafe(true);
        // TODO CASSANDRA-20119
        ClusterMetadataService.instance().log().close();
        CQLTester.cleanup();
    }

    public void setup(boolean takeSnapshotAfterTablePopulation)
    {
        CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(1000);
        CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.setInt(60);

        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            config.dump_heap_on_uncaught_exception = false;
            return config;
        });

        SchemaLoader.prepareServer();
        SnapshotManager.instance.start(true);

        populateTables(takeSnapshotAfterTablePopulation);
    }

    public void populateTables(boolean takeSnapshotAfterTablePopulation)
    {
        for (int i = 0; i < NUM_KEYSPACES; i++)
        {
            String keyspaceName = KEYSPACE + '_' + i;

            // Create Schema
            TableMetadata[] tables = new TableMetadata[NUM_TABLES_PER_KEYSPACE];
            for (int j = 0; j < NUM_TABLES_PER_KEYSPACE; j++)
            {
                tables[j] = SchemaLoader.standardCFMD(keyspaceName, String.format("table%d", i + '_' + j)).build();
            }

            SchemaLoader.createKeyspace(keyspaceName,
                                        KeyspaceParams.simple(1),
                                        tables);

            for (int j = 0; j < NUM_TABLES_PER_KEYSPACE; j++)
            {
                String tableName = String.format("table%d", i + '_' + j);

                ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(tableName);
                cfs.disableAutoCompaction();
                for (int k = 0; k < NUM_SSTABLES; k++)
                {
                    new RowUpdateBuilder(cfs.metadata(), 0, "key1")
                    .clustering("Column1")
                    .add("val", "asdf")
                    .build()
                    .applyUnsafe();
                    Util.flush(cfs);
                }
                if (takeSnapshotAfterTablePopulation)
                {
                    for (int snapId = 0; snapId < NUM_SNAPSHOTS_PER_TABLE; snapId++)
                        SnapshotManager.instance.takeSnapshot(userSnapshot(String.format("snap_%d_%d_%d", i, j, snapId), cfs.getKeyspaceTableName()));
                }
            }
        }

        if (takeSnapshotAfterTablePopulation)
        {
            List<TableSnapshot> snapshots = SnapshotManager.instance.getSnapshots(t -> true);
            assertEquals(NUM_KEYSPACES * NUM_TABLES_PER_KEYSPACE * NUM_SNAPSHOTS_PER_TABLE, snapshots.size());
        }
    }

    public String getRandomKeyspaceTable()
    {
        List<Keyspace> keyspaces = new ArrayList<>();
        Keyspace.nonSystem().forEach(keyspaces::add);
        Keyspace randomKeyspace = keyspaces.get(random.nextInt(keyspaces.size()));
        List<ColumnFamilyStore> cfss = new ArrayList<>(randomKeyspace.getColumnFamilyStores());
        ColumnFamilyStore cfs = cfss.get(random.nextInt(cfss.size()));

        return cfs.getKeyspaceTableName();
    }

    public String getRandomKeyspace()
    {
        List<Keyspace> keyspaces = new ArrayList<>();
        Keyspace.nonSystem().forEach(keyspaces::add);
        Keyspace randomKeyspace = keyspaces.get(random.nextInt(keyspaces.size()));
        return randomKeyspace.getName();
    }

    public Pair<String, String> getRandomKeyspaceTablePair()
    {
        List<Keyspace> keyspaces = new ArrayList<>();
        Keyspace.nonSystem().forEach(keyspaces::add);
        Keyspace randomKeyspace = keyspaces.get(random.nextInt(keyspaces.size()));
        List<ColumnFamilyStore> cfss = new ArrayList<>(randomKeyspace.getColumnFamilyStores());
        ColumnFamilyStore cfs = cfss.get(random.nextInt(cfss.size()));

        return Pair.of(cfs.getKeyspaceName(), cfs.getTableName());
    }


    public void takeSnapshotOnRandomTable()
    {
        SnapshotManager.instance.takeSnapshot(userSnapshot("snap_" + UUID.randomUUID(), getRandomKeyspaceTable()));
    }
}
