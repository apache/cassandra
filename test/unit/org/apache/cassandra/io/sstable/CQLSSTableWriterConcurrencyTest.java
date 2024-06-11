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
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests modifications to the Schema in the {@link CQLSSTableWriter} class while other Schema modifications are
 * occurring concurrently
 */
public class CQLSSTableWriterConcurrencyTest extends CQLTester
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CQLSSTableWriterTest.class);
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Test
    public void testConcurrentSchemaModification() throws InterruptedException, IOException
    {
        String schema = "CREATE TABLE %s ("
                        + "  k int PRIMARY KEY,"
                        + "  v1 text,"
                        + "  v2 int"
                        + ")";

        int nThreads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        CountDownLatch latch = new CountDownLatch(nThreads);
        AtomicInteger errorCount = new AtomicInteger();

        // Prepare all the variables required for the test
        String[] tableNames = new String[nThreads];
        String[] fullQueries = new String[nThreads];
        String[] insertStatements = new String[nThreads];
        File[] dataDirs = new File[nThreads];
        String baseDataDir = tempFolder.newFolder().getAbsolutePath();

        for (int i = 0; i < nThreads; i++)
        {
            tableNames[i] = String.format("table_%02d", i);
            fullQueries[i] = String.format(schema, KEYSPACE + '.' + tableNames[i]);
            LOGGER.info(fullQueries[i]);

            if (i % 2 != 0)
            {
                // dataDir and insert statement are only needed for the CQLSSTableWriter class
                dataDirs[i] = new File(Paths.get(baseDataDir, KEYSPACE, tableNames[i]));
                assert dataDirs[i].tryCreateDirectories();
                insertStatements[i] = String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (?, ?, ?)", KEYSPACE, tableNames[i]);
            }

            final int finalI = i;
            pool.submit(() -> {
                try
                {
                    latch.countDown();
                    latch.await();

                    // Invoke all schema modifications roughly at the same time
                    if (finalI % 2 == 0)
                    {
                        schemaChange(fullQueries[finalI]);
                        // If another thread modified the Schema without the proper synchronization, it's possible
                        // that the table metadata was swapped out and calling the Keyspace#getColumnFamilyStore
                        // method will produce an IllegalArgumentException
                        Schema.instance.getKeyspaceInstance(KEYSPACE).getColumnFamilyStore(tableNames[finalI]);
                    }
                    else
                    {
                        CQLSSTableWriter.builder()
                                        .inDirectory(dataDirs[finalI])
                                        .forTable(fullQueries[finalI])
                                        .withPartitioner(Murmur3Partitioner.instance)
                                        .using(insertStatements[finalI])
                                        .build();
                    }
                }
                catch (Throwable throwable)
                {
                    LOGGER.error("Error while processing element number {}", finalI, throwable);
                    errorCount.incrementAndGet();
                }
            });
        }

        pool.shutdown();
        if (!pool.awaitTermination(1, TimeUnit.MINUTES))
        {
            LOGGER.warn("Unable to close executor pool after 1 minute");
        }
        assertThat(errorCount.get()).isEqualTo(0);
    }
}
