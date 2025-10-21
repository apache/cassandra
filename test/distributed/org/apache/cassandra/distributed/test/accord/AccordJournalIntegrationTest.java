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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class AccordJournalIntegrationTest extends TestBaseImpl
{
    @Test
    public void saveLoadSanityCheck() throws Throwable
    {
        try (WithProperties wp = new WithProperties().set(CassandraRelevantProperties.DTEST_ACCORD_JOURNAL_SANITY_CHECK_ENABLED, "true");
             Cluster cluster = init(Cluster.build(1)
                                           .withoutVNodes()
                                           .start()))
        {
            final String TABLE = createTable(cluster);
            List<Thread> threads = new ArrayList<>();
            int numThreads = 10;
            CountDownLatch latch = CountDownLatch.newCountDownLatch(numThreads);
            AtomicInteger counter = new AtomicInteger();
            for (int i = 0; i < numThreads; i++)
            {
                int finalI = i;
                Thread t = new Thread(() -> {
                    latch.decrement();
                    latch.awaitUninterruptibly();
                    try
                    {
                        for (int j = 0; j < 100; j++)
                        {
                            cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                                           "INSERT INTO " + TABLE + "(k, c, v) VALUES (?, ?, ?);\n" +
                                                           "INSERT INTO " + TABLE + "(k, c, v) VALUES (?, ?, ?);\n" +
                                                           "COMMIT TRANSACTION",
                                                           ConsistencyLevel.ALL,
                                                           1, j, finalI * 100 + j,
                                                           2, j, finalI * 100 + j);
                            counter.incrementAndGet();
                        }
                    }
                    catch (Throwable throwable)
                    {
                        throwable.printStackTrace();
                    }
                });
                t.start();
                threads.add(t);
            }
            for (Thread thread : threads)
                thread.join();

            cluster.coordinator(1).execute("SELECT * FROM " + TABLE + " WHERE k = ?;", ConsistencyLevel.SERIAL, 1);
        }
    }

    @Test
    public void memtableStateReloadingTest() throws Throwable
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withoutVNodes()
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
            final String TABLE = createTable(cluster);

            insertData(cluster, TABLE);

            Object[][] before = cluster.coordinator(1).execute("SELECT * FROM " + TABLE + " WHERE k = ?;", ConsistencyLevel.SERIAL, 1);

            cluster.get(1).runOnInstance(() -> {
                ((AccordService) AccordService.instance()).journal().closeCurrentSegmentForTestingIfNonEmpty();
            });
            ClusterUtils.stopUnchecked(cluster.get(1));
            cluster.get(1).startup();

            Object[][] after = cluster.coordinator(1).execute("SELECT * FROM " + TABLE + " WHERE k = ?;", ConsistencyLevel.SERIAL, 1);
            Assert.assertEquals(before.length, after.length);
            for (int i = 0; i < before.length; i++)
            {
                Assert.assertTrue(Arrays.equals(before[i], after[i]));
            }
        }
    }

    @Test
    public void restartWithEpochChanges() throws IOException
    {
        try (Cluster cluster = Cluster.build(3).withoutVNodes().withConfig(c -> c.with(GOSSIP).with(NETWORK)).start())
        {
            init(cluster);
            final String TABLE = createTable(cluster);
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();

            insertData(cluster, TABLE);

            IInvokableInstance restartNode = cluster.get(1);
            ClusterUtils.stopUnchecked(restartNode);

            // make epoch changes
            for (int i = 0; i < 10; i++)
                cluster.schemaChange("ALTER TABLE " + TABLE + " WITH comment = 'change " + i + "'", true, cluster.get(2));

            restartNode.startup();
            insertData(cluster, TABLE);
        }
    }

    private void insertData(Cluster cluster, String TABLE) {
        for (int j = 0; j < 1_000; j++)
        {
            cluster.coordinator(1).execute("BEGIN TRANSACTION\n" +
                                           "INSERT INTO " + TABLE + "(k, c, v) VALUES (?, ?, ?);\n" +
                                           "COMMIT TRANSACTION",
                                           ConsistencyLevel.ALL,
                                           j, j, 1
            );
        }
    }

    private String createTable(Cluster cluster) {
        final String TABLE = KEYSPACE + ".test_table";
        cluster.schemaChange("CREATE TABLE " + TABLE + " (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'");
        return TABLE;
    }
}