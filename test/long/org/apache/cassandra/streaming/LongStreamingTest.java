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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Files;
import org.apache.cassandra.io.util.File;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;

public class LongStreamingTest
{
    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();

        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        StorageService.instance.initServer();

        StorageService.instance.setCompactionThroughputMbPerSec(0);
        StorageService.instance.setStreamThroughputMbitPerSec(0);
        StorageService.instance.setInterDCStreamThroughputMbitPerSec(0);
    }

    @Test
    public void testSstableCompressionStreaming() throws InterruptedException, ExecutionException, IOException
    {
        testStream(true);
    }

    @Test
    public void testStreamCompressionStreaming() throws InterruptedException, ExecutionException, IOException
    {
        testStream(false);
    }

    private void testStream(boolean useSstableCompression) throws InvalidRequestException, IOException, ExecutionException, InterruptedException
    {
        String KS = useSstableCompression ? "sstable_compression_ks" : "stream_compression_ks";
        String TABLE = "table1";

        File tempdir = new File(Files.createTempDir());
        File dataDir = new File(tempdir.absolutePath() + File.pathSeparator() + KS + File.pathSeparator() + TABLE);
        assert dataDir.tryCreateDirectories();

        String schema = "CREATE TABLE " + KS + '.'  + TABLE + "  ("
                        + "  k int PRIMARY KEY,"
                        + "  v1 text,"
                        + "  v2 int"
                        + ") with compression = " + (useSstableCompression ? "{'class': 'LZ4Compressor'};" : "{};");
        String insert = "INSERT INTO " + KS + '.'  + TABLE + " (k, v1, v2) VALUES (?, ?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .sorted()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using(insert).build();

        CompressionParams compressionParams = Keyspace.open(KS).getColumnFamilyStore(TABLE).metadata().params.compression;
        Assert.assertEquals(useSstableCompression, compressionParams.isEnabled());


        long start = nanoTime();

        for (int i = 0; i < 10_000_000; i++)
            writer.addRow(i, "test1", 24);

        writer.close();
        System.err.println(String.format("Writer finished after %d seconds....", TimeUnit.NANOSECONDS.toSeconds(nanoTime() - start)));

        File[] dataFiles = dataDir.tryList((dir, name) -> name.endsWith("-Data.db"));
        long dataSizeInBytes = 0l;
        for (File file : dataFiles)
        {
            System.err.println("File : "+file.absolutePath());
            dataSizeInBytes += file.length();
        }

        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            private String ks;
            public void init(String keyspace)
            {
                for (Replica range : StorageService.instance.getLocalReplicas(KS))
                    addRangeForEndpoint(range.range(), FBUtilities.getBroadcastAddressAndPort());

                this.ks = keyspace;
            }

            public TableMetadataRef getTableMetadata(String cfName)
            {
                return Schema.instance.getTableMetadataRef(ks, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        start = nanoTime();
        loader.stream().get();

        long millis = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);
        System.err.println(String.format("Finished Streaming in %.2f seconds: %.2f MiBsec",
                                         millis/1000d,
                                         (dataSizeInBytes / (1 << 20) / (millis / 1000d)) * 8));


        //Stream again
        loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            private String ks;
            public void init(String keyspace)
            {
                for (Replica range : StorageService.instance.getLocalReplicas(KS))
                    addRangeForEndpoint(range.range(), FBUtilities.getBroadcastAddressAndPort());

                this.ks = keyspace;
            }

            public TableMetadataRef getTableMetadata(String cfName)
            {
                return Schema.instance.getTableMetadataRef(ks, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        start = nanoTime();
        loader.stream().get();

        millis = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);
        System.err.println(String.format("Finished Streaming in %.2f seconds: %.2f MiBsec",
                                         millis/1000d,
                                         (dataSizeInBytes / (1 << 20) / (millis / 1000d)) * 8));


        //Compact them both
        start = nanoTime();
        Keyspace.open(KS).getColumnFamilyStore(TABLE).forceMajorCompaction();
        millis = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);

        System.err.println(String.format("Finished Compacting in %.2f seconds: %.2f MiBsec",
                                         millis / 1000d,
                                         (dataSizeInBytes * 2 / (1 << 20) / (millis / 1000d)) * 8));

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM " + KS + '.'  + TABLE + " limit 100;");
        assertEquals(100, rs.size());
    }
}
