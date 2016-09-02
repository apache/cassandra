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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

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
        StorageService.instance.setStreamThroughputMbPerSec(0);
        StorageService.instance.setInterDCStreamThroughputMbPerSec(0);
    }

    @Test
    public void testCompressedStream() throws InvalidRequestException, IOException, ExecutionException, InterruptedException
    {
        String KS = "cql_keyspace";
        String TABLE = "table1";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        String schema = "CREATE TABLE cql_keyspace.table1 ("
                        + "  k int PRIMARY KEY,"
                        + "  v1 text,"
                        + "  v2 int"
                        + ");";// with compression = {};";
        String insert = "INSERT INTO cql_keyspace.table1 (k, v1, v2) VALUES (?, ?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .sorted()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using(insert).build();
        long start = System.nanoTime();

        for (int i = 0; i < 10_000_000; i++)
            writer.addRow(i, "test1", 24);

        writer.close();
        System.err.println(String.format("Writer finished after %d seconds....", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)));

        File[] dataFiles = dataDir.listFiles((dir, name) -> name.endsWith("-Data.db"));
        long dataSize = 0l;
        for (File file : dataFiles)
        {
            System.err.println("File : "+file.getAbsolutePath());
            dataSize += file.length();
        }

        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            private String ks;
            public void init(String keyspace)
            {
                for (Range<Token> range : StorageService.instance.getLocalRanges("cql_keyspace"))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());

                this.ks = keyspace;
            }

            public CFMetaData getTableMetadata(String cfName)
            {
                return Schema.instance.getCFMetaData(ks, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        start = System.nanoTime();
        loader.stream().get();

        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.err.println(String.format("Finished Streaming in %.2f seconds: %.2f Mb/sec",
                                         millis/1000d,
                                         (dataSize / (1 << 20) / (millis / 1000d)) * 8));


        //Stream again
        loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            private String ks;
            public void init(String keyspace)
            {
                for (Range<Token> range : StorageService.instance.getLocalRanges("cql_keyspace"))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());

                this.ks = keyspace;
            }

            public CFMetaData getTableMetadata(String cfName)
            {
                return Schema.instance.getCFMetaData(ks, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        start = System.nanoTime();
        loader.stream().get();

        millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.err.println(String.format("Finished Streaming in %.2f seconds: %.2f Mb/sec",
                                         millis/1000d,
                                         (dataSize / (1 << 20) / (millis / 1000d)) * 8));


        //Compact them both
        start = System.nanoTime();
        Keyspace.open(KS).getColumnFamilyStore(TABLE).forceMajorCompaction();
        millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        System.err.println(String.format("Finished Compacting in %.2f seconds: %.2f Mb/sec",
                                         millis / 1000d,
                                         (dataSize * 2 / (1 << 20) / (millis / 1000d)) * 8));

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace.table1 limit 100;");
        assertEquals(100, rs.size());
    }
}
