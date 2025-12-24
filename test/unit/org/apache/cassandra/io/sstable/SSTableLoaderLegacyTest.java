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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertTrue;

/**
 * Tests SSTableLoader with legacy sstables from Cassandra 3.x
 */
public class SSTableLoaderLegacyTest
{
    public static final String KEYSPACE1 = "sstableloaderlegacytest";
    public static final String LEGACY_VERSION = "me"; // Cassandra 3.11
    public static final String LEGACY_TABLE = "legacy_me_simple";

    private static File LEGACY_SSTABLE_ROOT;
    private File tmpdir;

    @BeforeClass
    public static void defineSchema()
    {
        String scp = CassandraRelevantProperties.TEST_LEGACY_SSTABLE_ROOT.getString();
        if (scp == null || scp.isEmpty())
        {
            throw new RuntimeException("System property for legacy sstable root is not set.");
        }
        LEGACY_SSTABLE_ROOT = new File(scp).toAbsolute();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1));

        // Create table matching the legacy sstable schema
        // legacy_me_simple has schema: pk text PRIMARY KEY, val text
        QueryProcessor.executeInternal(String.format(
            "CREATE TABLE %s.%s (pk text PRIMARY KEY, val text)",
            KEYSPACE1, LEGACY_TABLE));

        StorageService.instance.initServer();
    }

    @Before
    public void setup() throws IOException
    {
        tmpdir = new File(Files.createTempDirectory("sstableloaderlegacytest").toFile());
    }

    @After
    public void cleanup()
    {
        FileUtils.deleteRecursive(tmpdir);
    }

    /**
     * Test that loading legacy 3.11 sstables works automatically.
     * Zero-copy streaming is automatically disabled for legacy sstables that use the old bloom filter format.
     */
    @Test
    public void testLoadLegacy311SSTable() throws Exception
    {
        assertTrue("Zero-copy streaming should be enabled by default",
                   DatabaseDescriptor.streamEntireSSTables());

        File dataDir = setupLegacySSTableDirectory();
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(LEGACY_TABLE);

        final CountDownLatch latch = new CountDownLatch(1);
        SSTableLoader loader = new SSTableLoader(dataDir, new TestClient(),
                                                new OutputHandler.SystemOutput(false, false));

        loader.stream(Collections.emptySet(), completionStreamListener(latch)).get();
        latch.await();

        assertTrue("Data should be loaded from legacy sstable",
                  !Util.getAll(Util.cmd(cfs).build()).isEmpty());
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

    /**
     * Sets up a directory with legacy 3.11 sstables copied from test data.
     */
    private File setupLegacySSTableDirectory() throws IOException
    {
        File dataDir = new File(tmpdir, KEYSPACE1 + "/" + LEGACY_TABLE);
        if (!dataDir.exists())
            dataDir.createDirectoriesIfNotExists();

        File legacyTableDir = new File(LEGACY_SSTABLE_ROOT,
                                       String.format("%s/legacy_tables/%s", LEGACY_VERSION, LEGACY_TABLE));

        if (!legacyTableDir.isDirectory())
        {
            throw new RuntimeException("Legacy sstable directory not found: " + legacyTableDir);
        }

        // Copy all sstable components to the test directory
        File[] sourceFiles = legacyTableDir.tryList();
        if (sourceFiles != null)
        {
            for (File sourceFile : sourceFiles)
            {
                copyFile(sourceFile, new File(dataDir, sourceFile.name()));
            }
        }

        System.out.println("Copied legacy sstables from: " + legacyTableDir);
        System.out.println("To: " + dataDir);
        File[] copiedFiles = dataDir.tryList();
        System.out.println("File count: " + (copiedFiles != null ? copiedFiles.length : 0));

        return dataDir;
    }

    /**
     * Copies a file from source to target.
     */
    private static void copyFile(File sourceFile, File targetFile) throws IOException
    {
        if (sourceFile.isFile())
        {
            Files.copy(sourceFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Creates a stream completion listener.
     */
    private StreamEventHandler completionStreamListener(final CountDownLatch latch)
    {
        return new StreamEventHandler()
        {
            public void onFailure(Throwable arg0)
            {
                latch.countDown();
            }

            public void onSuccess(StreamState arg0)
            {
                latch.countDown();
            }

            public void handleStreamEvent(StreamEvent event)
            {
            }
        };
    }
}
