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

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.google.common.io.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTableLoaderTest
{
    public static final String KEYSPACE1 = "SSTableLoaderTest";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_STANDARD2 = "Standard2";

    private File tmpdir;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2));

        StorageService.instance.initServer();
    }

    @Before
    public void setup() throws Exception
    {
        tmpdir = Files.createTempDir();
    }

    @After
    public void cleanup()
    {
        try {
            FileUtils.deleteRecursive(tmpdir);
        } catch (FSWriteError e) {
            /**
             * Windows does not allow a mapped file to be deleted, so we probably forgot to clean the buffers somewhere.
             * We force a GC here to force buffer deallocation, and then try deleting the directory again.
             * For more information, see: http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
             * If this is not the problem, the exception will be rethrown anyway.
             */
            System.gc();
            FileUtils.deleteRecursive(tmpdir);
        }
    }

    private static final class TestClient extends SSTableLoader.Client
    {
        private String keyspace;

        public void init(String keyspace)
        {
            this.keyspace = keyspace;
            for (Range<Token> range : StorageService.instance.getLocalRanges(KEYSPACE1))
                addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
        }

        public CFMetaData getTableMetadata(String tableName)
        {
            return Schema.instance.getCFMetaData(keyspace, tableName);
        }
    }

    @Test
    public void testLoadingSSTable() throws Exception
    {
        File dataDir = new File(tmpdir.getAbsolutePath() + File.separator + KEYSPACE1 + File.separator + CF_STANDARD1);
        assert dataDir.mkdirs();
        CFMetaData cfmeta = Schema.instance.getCFMetaData(KEYSPACE1, CF_STANDARD1);

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
        cfs.forceBlockingFlush(); // wait for sstables to be on disk else we won't be able to stream them

        final CountDownLatch latch = new CountDownLatch(1);
        SSTableLoader loader = new SSTableLoader(dataDir, new TestClient(), new OutputHandler.SystemOutput(false, false));
        loader.stream(Collections.emptySet(), completionStreamListener(latch)).get();

        List<FilteredPartition> partitions = Util.getAll(Util.cmd(cfs).build());

        assertEquals(1, partitions.size());
        assertEquals("key1", AsciiType.instance.getString(partitions.get(0).partitionKey().getKey()));
        assertEquals(ByteBufferUtil.bytes("100"), partitions.get(0).getRow(Clustering.make(ByteBufferUtil.bytes("col1")))
                                                                   .getCell(cfmeta.getColumnDefinition(ByteBufferUtil.bytes("val")))
                                                                   .value());

        // The stream future is signalled when the work is complete but before releasing references. Wait for release
        // before cleanup (CASSANDRA-10118).
        latch.await();
    }

    @Test
    public void testLoadingIncompleteSSTable() throws Exception
    {
        File dataDir = new File(tmpdir.getAbsolutePath() + File.separator + KEYSPACE1 + File.separator + CF_STANDARD2);
        assert dataDir.mkdirs();

        //make sure we have no tables...
        assertTrue(dataDir.listFiles().length == 0);

        String schema = "CREATE TABLE %s.%s (key ascii, name ascii, val ascii, val1 ascii, PRIMARY KEY (key, name))";
        String query = "INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(String.format(schema, KEYSPACE1, CF_STANDARD2))
                                                  .using(String.format(query, KEYSPACE1, CF_STANDARD2))
                                                  .withBufferSizeInMB(1)
                                                  .build();

        int NB_PARTITIONS = 5000; // Enough to write >1MB and get at least one completed sstable before we've closed the writer

        for (int i = 0; i < NB_PARTITIONS; i++)
        {
            for (int j = 0; j < 100; j++)
                writer.addRow(String.format("key%d", i), String.format("col%d", j), "100");
        }

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD2);
        cfs.forceBlockingFlush(); // wait for sstables to be on disk else we won't be able to stream them

        //make sure we have some tables...
        assertTrue(dataDir.listFiles().length > 0);

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
}
