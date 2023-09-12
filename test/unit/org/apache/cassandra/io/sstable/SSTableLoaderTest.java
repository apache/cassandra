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
package org.apache.cassandra.io.sstable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.awaitility.Awaitility;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.byteman.rule.Rule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class SSTableLoaderTest
{
    public static final String KEYSPACE1 = "SSTableLoaderTest";
    public static final String KEYSPACE2 = "SSTableLoaderTest1";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";
    public static final String CF_BACKUPS = Directories.BACKUPS_SUBDIR;
    public static final String CF_SNAPSHOTS = Directories.SNAPSHOT_SUBDIR;

    private static final String schema = "CREATE TABLE %s.%s (key ascii, name ascii, val ascii, val1 ascii, PRIMARY KEY (key, name))";
    private static final String query = "INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)";

    private File tmpdir;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_BACKUPS),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_SNAPSHOTS));

        SchemaLoader.createKeyspace(KEYSPACE2,
                KeyspaceParams.simple(1),
                SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD1),
                SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD2));

        StorageService.instance.initServer();
    }

    @Before
    public void setup() throws Exception
    {
        tmpdir = new File(Files.createTempDir());
    }

    @After
    public void cleanup()
    {
        try
        {
            tmpdir.deleteRecursive();
        }
        catch (FSWriteError e)
        {
            /*
              We force a GC here to force buffer deallocation, and then try deleting the directory again.
              For more information, see: http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
              If this is not the problem, the exception will be rethrown anyway.
             */
            System.gc();
            tmpdir.deleteRecursive();
        }

        try
        {
            for (String[] keyspaceTable : new String[][] { {KEYSPACE1, CF_STANDARD1},
                                                           {KEYSPACE1, CF_STANDARD2},
                                                           {KEYSPACE1, CF_BACKUPS},
                                                           {KEYSPACE2, CF_STANDARD1},
                                                           {KEYSPACE2, CF_STANDARD2}})
                StorageService.instance.truncate(keyspaceTable[0], keyspaceTable[1]);
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Unable to truncate table!", ex);
        }
    }

    private static final class TestClient extends SSTableLoader.Client
    {
        private String keyspace;

        public void init(String keyspace)
        {
            this.keyspace = keyspace;
            for (Replica replica : StorageService.instance.getLocalReplicas(KEYSPACE1))
                addRangeForEndpoint(replica.range(), FBUtilities.getBroadcastAddressAndPort());
        }

        public TableMetadataRef getTableMetadata(String tableName)
        {
            return Schema.instance.getTableMetadataRef(keyspace, tableName);
        }
    }

    @Test
    public void testLoadingSSTable() throws Exception
    {
        File dataDir = dataDir(CF_STANDARD1);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE1, CF_STANDARD1);

        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                       .inDirectory(dataDir)
                                                       .forTable(String.format(schema, KEYSPACE1, CF_STANDARD1))
                                                       .using(String.format(query, KEYSPACE1, CF_STANDARD1))
                                                       .build())
        {
            writer.addRow("key1", "col1", "100");
        }

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        Util.flush(cfs); // wait for sstables to be on disk else we won't be able to stream them

        final CountDownLatch latch = new CountDownLatch(1);
        SSTableLoader loader = new SSTableLoader(dataDir, new TestClient(), new OutputHandler.SystemOutput(false, false));
        loader.stream(Collections.emptySet(), completionStreamListener(latch)).get();

        List<FilteredPartition> partitions = Util.getAll(Util.cmd(cfs).build());

        assertEquals(1, partitions.size());
        assertEquals("key1", AsciiType.instance.getString(partitions.get(0).partitionKey().getKey()));
        assert metadata != null;

        Row row = partitions.get(0).getRow(Clustering.make(ByteBufferUtil.bytes("col1")));
        assert row != null;

        assertEquals(ByteBufferUtil.bytes("100"), row.getCell(metadata.getColumn(ByteBufferUtil.bytes("val"))).buffer());

        // The stream future is signalled when the work is complete but before releasing references. Wait for release
        // before cleanup (CASSANDRA-10118).
        latch.await();

        checkAllRefsAreClosed(loader);
    }

    @Test
    public void testLoadingIncompleteSSTable() throws Exception
    {
        File dataDir = dataDir(CF_STANDARD2);

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(String.format(schema, KEYSPACE1, CF_STANDARD2))
                                                  .using(String.format(query, KEYSPACE1, CF_STANDARD2))
                                                  .withBufferSizeInMiB(1)
                                                  .build();

        int NB_PARTITIONS = 5000; // Enough to write >1MiB and get at least one completed sstable before we've closed the writer

        for (int i = 0; i < NB_PARTITIONS; i++)
        {
            for (int j = 0; j < 100; j++)
                writer.addRow(String.format("key%d", i), String.format("col%d", j), "100");
        }

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2);
        Util.flush(cfs); // wait for sstables to be on disk else we won't be able to stream them

        //make sure we have some tables...
        assertTrue(Objects.requireNonNull(dataDir.tryList()).length > 0);

        final CountDownLatch latch = new CountDownLatch(2);
        //writer is still open so loader should not load anything
        SSTableLoader loader = new SSTableLoader(dataDir, new TestClient(), new OutputHandler.SystemOutput(false, false));
        loader.stream(Collections.emptySet(), completionStreamListener(latch)).get();

        List<FilteredPartition> partitions = Util.getAll(Util.cmd(cfs).build());

        assertTrue(partitions.size() > 0 && partitions.size() < NB_PARTITIONS);

        // now we complete the write and the second loader should load the last sstable as well
        writer.close();

        loader = new SSTableLoader(dataDir, new TestClient(), new OutputHandler.SystemOutput(false, false));
        loader.stream(Collections.emptySet(), completionStreamListener(latch)).get();

        partitions = Util.getAll(Util.cmd(Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2)).build());
        assertEquals(NB_PARTITIONS, partitions.size());

        // The stream future is signalled when the work is complete but before releasing references. Wait for release
        // before cleanup (CASSANDRA-10118).
        latch.await();

        checkAllRefsAreClosed(loader);
    }

    @Test
    public void testLoadingSSTableToDifferentKeyspaceAndTable() throws Exception
    {
        File dataDir = dataDir(CF_STANDARD1);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE1, CF_STANDARD1);

        String schema = "CREATE TABLE %s.%s (key ascii, name ascii, val ascii, val1 ascii, PRIMARY KEY (key, name))";
        String query = "INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)";

        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(dataDir)
                .forTable(String.format(schema, KEYSPACE1, CF_STANDARD1))
                .using(String.format(query, KEYSPACE1, CF_STANDARD1))
                .build())
        {
            writer.addRow("key1", "col1", "100");
        }

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        Util.flush(cfs); // wait for sstables to be on disk else we won't be able to stream them

        for (String table : new String[] { CF_STANDARD2, null })
        {
            final CountDownLatch latch = new CountDownLatch(1);
            SSTableLoader loader = new SSTableLoader(dataDir, new TestClient(), new OutputHandler.SystemOutput(false, false), 1, KEYSPACE2, table);
            loader.stream(Collections.emptySet(), completionStreamListener(latch)).get();

            String targetTable = table == null ? CF_STANDARD1 : table;
            cfs = Keyspace.open(KEYSPACE2).getColumnFamilyStore(targetTable);
            Util.flush(cfs);

            List<FilteredPartition> partitions = Util.getAll(Util.cmd(cfs).build());

            assertEquals(1, partitions.size());
            assertEquals("key1", AsciiType.instance.getString(partitions.get(0).partitionKey().getKey()));
            assert metadata != null;

            Row row = partitions.get(0).getRow(Clustering.make(ByteBufferUtil.bytes("col1")));
            assert row != null;

            assertEquals(ByteBufferUtil.bytes("100"), row.getCell(metadata.getColumn(ByteBufferUtil.bytes("val"))).buffer());

            // The stream future is signalled when the work is complete but before releasing references. Wait for release
            // before cleanup (CASSANDRA-10118).
            latch.await();

            checkAllRefsAreClosed(loader);
        }
    }

    @Test
    public void testLoadingBackupsTable() throws Exception
    {
        testLoadingTable(CF_BACKUPS, false);
    }

    @Test
    public void testLoadingSnapshotsTable() throws Exception
    {
        testLoadingTable(CF_SNAPSHOTS, false);
    }

    @Test
    public void testLoadingLegacyBackupsTable() throws Exception
    {
        testLoadingTable(CF_BACKUPS, true);
    }

    @Test
    public void testLoadingLegacySnapshotsTable() throws Exception
    {
        testLoadingTable(CF_SNAPSHOTS, true);
    }

    private void testLoadingTable(String tableName, boolean isLegacyTable) throws Exception
    {
        File dataDir = dataDir(tableName, isLegacyTable);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE1, tableName);

        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                       .inDirectory(dataDir)
                                                       .forTable(String.format(schema, KEYSPACE1, tableName))
                                                       .using(String.format(query, KEYSPACE1, tableName))
                                                       .build())
        {
            writer.addRow("key", "col1", "100");
        }

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(tableName);
        Util.flush(cfs); // wait for sstables to be on disk else we won't be able to stream them

        final CountDownLatch latch = new CountDownLatch(1);
        SSTableLoader loader = new SSTableLoader(dataDir, new TestClient(), new OutputHandler.SystemOutput(false, false));
        loader.stream(Collections.emptySet(), completionStreamListener(latch)).get();

        List<FilteredPartition> partitions = Util.getAll(Util.cmd(cfs).build());

        assertEquals(1, partitions.size());
        assertEquals("key", AsciiType.instance.getString(partitions.get(0).partitionKey().getKey()));
        assert metadata != null;

        Row row = partitions.get(0).getRow(Clustering.make(ByteBufferUtil.bytes("col1")));
        assert row != null;

        assertEquals(ByteBufferUtil.bytes("100"), row.getCell(metadata.getColumn(ByteBufferUtil.bytes("val"))).buffer());

        // The stream future is signalled when the work is complete but before releasing references. Wait for release
        // before cleanup (CASSANDRA-10118).
        latch.await();

        checkAllRefsAreClosed(loader);
    }

    @Test
    @BMRule(name = "fail_on_deserialize",
    targetClass = "org.apache.cassandra.streaming.StreamSession",
    targetMethod = "messageReceived",
    targetLocation = "AT ENTRY",
    action = "throw new RuntimeException();")
    public void testLoadingSSTableFail() throws Exception
    {
        // This test verifies that all references are closed if the sstable loading fails for some reason on the server
        // side. This is done by throwing an exception in StreamSession.messageReceived() which is called when the
        // server receives a stream message from the client. After completion, we check that all references are closed.
        Rule.disableTriggers();
        File dataDir = dataDir(CF_STANDARD1);
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE1, CF_STANDARD1);

        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                       .inDirectory(dataDir)
                                                       .forTable(String.format(schema, KEYSPACE1, CF_STANDARD1))
                                                       .using(String.format(query, KEYSPACE1, CF_STANDARD1))
                                                       .build())
        {
            writer.addRow("key1", "col1", "100");
        }

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        Util.flush(cfs); // wait for sstables to be on disk else we won't be able to stream them

        final CountDownLatch latch = new CountDownLatch(1);
        Rule.enableTriggers();
        SSTableLoader loader = new SSTableLoader(dataDir, new TestClient(), new OutputHandler.SystemOutput(false, false));
        AsyncFuture<StreamState> result = loader.stream(Collections.emptySet(), completionStreamListener(latch)).await();
        assertThat(result.isSuccess()).isFalse();

        checkAllRefsAreClosed(loader);
    }

    private File dataDir(String cf)
    {
        return dataDir(cf, false);
    }

    private File dataDir(String cf, boolean isLegacyTable)
    {
        // Add -{tableUuid} suffix to table dir if not a legacy table
        File dataDir = new File(tmpdir.absolutePath() + File.pathSeparator() + SSTableLoaderTest.KEYSPACE1 + File.pathSeparator() + cf
                                + (isLegacyTable ? "" : String.format("-%s",TableId.generate().toHexString())));
        assert dataDir.tryCreateDirectories();
        //make sure we have no tables...
        assertEquals(Objects.requireNonNull(dataDir.tryList()).length, 0);
        return dataDir;
    }

    StreamEventHandler completionStreamListener(final CountDownLatch latch)
    {
        return new StreamEventHandler() {
            public void onFailure(Throwable arg0)
            {
                latch.countDown();
            }

            public void onSuccess(StreamState arg0)
            {
                latch.countDown();
            }

            public void handleStreamEvent(StreamEvent event) {}
        };
    }

    private static void checkAllRefsAreClosed(SSTableLoader loader)
    {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> loader.getSSTables().stream().allMatch(sstable -> sstable.selfRef().globalCount() == 0));
    }

}
