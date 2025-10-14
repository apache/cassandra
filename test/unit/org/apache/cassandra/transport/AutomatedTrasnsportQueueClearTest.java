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
package org.apache.cassandra.transport;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.metrics.ServiceLevelIndicatorMetrics;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.throttler.dynamic.CassandraResourceUtilization;

import static org.apache.cassandra.transport.Dispatcher.NATIVE_TRANSPORT_THREAD_POOL;

public class AutomatedTrasnsportQueueClearTest extends CQLTester
{
    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        DatabaseDescriptor.setNativeTransportMaxThreads(1);
        cassandra = ServerTestUtils.startEmbeddedCassandraService();
        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("drop keyspace if exists transport;");
        session.execute("create keyspace transport WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("CREATE TABLE transport.tbl (\n" +
                        "  id int,\n" +
                        "  a int,\n" +
                        "  b int,\n" +
                        "  c int,\n" +
                        " PRIMARY KEY(id)" +
                        ");");

        // the default should be 0
        Assert.assertEquals(0, DatabaseDescriptor.getInjectArtificialDelayMutationPath().toMilliseconds());
    }

    @AfterClass
    public static void tearDown()
    {
        try
        {
            DatabaseDescriptor.setInjectArtificialDelayMutationPath("0s");
            System.out.println("Shutting down...");
            if (session != null)
                session.close();
            if (cluster != null)
                cluster.close();
            if (cassandra != null)
                cassandra.stop();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @After
    public void reset()
    {
        DatabaseDescriptor.setInjectArtificialDelayMutationPath("0s");
    }

    @Test
    public void testAutomatedQueueCleanup() throws InterruptedException
    {
        // simulate expensive query by injecting the artificial delay
        DatabaseDescriptor.setInjectArtificialDelayMutationPath("1s");
        DatabaseDescriptor.setNativeTransportTimeout(Integer.MAX_VALUE);
        DatabaseDescriptor.setMaxWaitTimeInTransportQueue(Integer.MAX_VALUE);
        
        ExecutorService executor = Executors.newFixedThreadPool(1000);

        for (int i = 0; i < 1000; i++)
        {
            int finalI = i;
            executor.submit(() -> {
                session.execute(String.format("INSERT INTO transport.tbl (id, a, b, c) VALUES (%d, %d, %d, %d);", finalI, finalI, finalI, finalI));
            });
        }

        // let the queue grow
        Thread.sleep(60000);
        long timeoutBeforeMetric = ClientMetrics.instance.timedOutBeforeProcessing.getCount();
        SEPExecutor nativeTransportSEPTP = SharedExecutorPool.SHARED.getExecutor(NATIVE_TRANSPORT_THREAD_POOL);
        int pendingQueue1 = nativeTransportSEPTP.getPendingTaskCount();
        System.out.println("TimedOutBeforeProcessing1: " + ClientMetrics.instance.timedOutBeforeProcessing.getCount() + ", pendingQueue1: " + pendingQueue1);
        Assert.assertEquals(timeoutBeforeMetric, ClientMetrics.instance.timedOutBeforeProcessing.getCount());
        // it is difficult to guess the exact pending requests, but it should be > 100 as we have set the max threads to 1
        Assert.assertTrue(pendingQueue1 > 150);

        // let the queue drain organically
        Thread.sleep(40000);
        int pendingQueue2 = nativeTransportSEPTP.getPendingTaskCount();
        System.out.println("TimedOutBeforeProcessing2: " + ClientMetrics.instance.timedOutBeforeProcessing.getCount() + ", pendingQueue2: " + pendingQueue2);
        Assert.assertEquals(timeoutBeforeMetric, ClientMetrics.instance.timedOutBeforeProcessing.getCount());
        // it is difficult to guess the exact pending requests, but it should be > 100 as we have set the max threads to 1
        Assert.assertTrue(pendingQueue2 < pendingQueue1);
        Assert.assertTrue(pendingQueue2 > 100);

        // force the queue to drain
        StorageService.instance.setNativeTransportTimeoutMillis(1000);
        StorageService.instance.setMaxWaitTimeInTransportQueueMillis(1000);
        Thread.sleep(40000);
        int pendingQueue3 = nativeTransportSEPTP.getPendingTaskCount();
        System.out.println("TimedOutBeforeProcessing3: " + ClientMetrics.instance.timedOutBeforeProcessing.getCount() + ", pendingQueue3: " + pendingQueue3);
        Assert.assertTrue(ClientMetrics.instance.timedOutBeforeProcessing.getCount() >= timeoutBeforeMetric + pendingQueue2);
        Assert.assertEquals(0, pendingQueue3);
        Assert.assertEquals(ClientMetrics.instance.timedOutBeforeProcessing.getCount(), ServiceLevelIndicatorMetrics.overloadedExceptionMetrics.getCount());
    }

    @Test
    public void testAutomatedQueueCleanupThroughNodetool() throws InterruptedException
    {
        // simulate expensive query by injecting the artificial delay
        DatabaseDescriptor.setInjectArtificialDelayMutationPath("1s");
        DatabaseDescriptor.setNativeTransportTimeout(Integer.MAX_VALUE);
        DatabaseDescriptor.setMaxWaitTimeInTransportQueue(Integer.MAX_VALUE);

        ExecutorService executor = Executors.newFixedThreadPool(1000);

        for (int i = 0; i < 1000; i++)
        {
            int finalI = i;
            executor.submit(() -> {
                session.execute(String.format("INSERT INTO transport.tbl (id, a, b, c) VALUES (%d, %d, %d, %d);", finalI, finalI, finalI, finalI));
            });
        }

        // let the queue grow
        Thread.sleep(60000);
        long timeoutBeforeMetric = ClientMetrics.instance.timedOutBeforeProcessing.getCount();

        SEPExecutor nativeTransportSEPTP = SharedExecutorPool.SHARED.getExecutor(NATIVE_TRANSPORT_THREAD_POOL);
        int pendingQueue1 = nativeTransportSEPTP.getPendingTaskCount();
        System.out.println("TimedOutBeforeProcessing1: " + ClientMetrics.instance.timedOutBeforeProcessing.getCount() + ", pendingQueue1: " + pendingQueue1);
        Assert.assertEquals(timeoutBeforeMetric, ClientMetrics.instance.timedOutBeforeProcessing.getCount());
        // it is difficult to guess the exact pending requests, but it should be > 100 as we have set the max threads to 1
        Assert.assertTrue(pendingQueue1 > 150);

        // let the queue drain organically
        Thread.sleep(40000);
        int pendingQueue2 = nativeTransportSEPTP.getPendingTaskCount();
        System.out.println("TimedOutBeforeProcessing2: " + ClientMetrics.instance.timedOutBeforeProcessing.getCount() + ", pendingQueue2: " + pendingQueue2);
        Assert.assertEquals(timeoutBeforeMetric, ClientMetrics.instance.timedOutBeforeProcessing.getCount());
        // it is difficult to guess the exact pending requests, but it should be > 100 as we have set the max threads to 1
        Assert.assertTrue(pendingQueue2 < pendingQueue1);
        Assert.assertTrue(pendingQueue2 > 100);

        // Cleanup the queue through nodetool
        StorageService.instance.internalQueueCleanupEMERGENCYUSEONLY(NATIVE_TRANSPORT_THREAD_POOL);
        int pendingQueue3 = nativeTransportSEPTP.getPendingTaskCount();
        System.out.println("TimedOutBeforeProcessing3: " + ClientMetrics.instance.timedOutBeforeProcessing.getCount() + ", pendingQueue3: " + pendingQueue3);
        Assert.assertEquals(0, pendingQueue3);
    }

    @Test
    public void testInternalQueue()
    {
        DatabaseDescriptor.setInjectArtificialDelayMutationPath("1s");
        session.execute(String.format("INSERT INTO transport.tbl (id, a, b, c) VALUES (%d, %d, %d, %d);", 1, 2, 3, 4)); // insert a row to activate the Mutation thread pool
        session.execute("SELECT * FROM transport.tbl WHERE id = 1"); // read a row to activate the Read thread pool
        Assert.assertTrue(StorageService.instance.internalQueueCleanupEMERGENCYUSEONLY(NATIVE_TRANSPORT_THREAD_POOL));
        Assert.assertEquals(0, SharedExecutorPool.SHARED.getExecutor(NATIVE_TRANSPORT_THREAD_POOL).getPendingTaskCount());
        Assert.assertTrue(StorageService.instance.internalQueueCleanupEMERGENCYUSEONLY(CassandraResourceUtilization.READ_THREAD_POOL));
        Assert.assertEquals(0, SharedExecutorPool.SHARED.getExecutor(CassandraResourceUtilization.READ_THREAD_POOL).getPendingTaskCount());
        Assert.assertTrue(StorageService.instance.internalQueueCleanupEMERGENCYUSEONLY(CassandraResourceUtilization.MUTATION_THREAD_POOL));
        Assert.assertEquals(0, SharedExecutorPool.SHARED.getExecutor(CassandraResourceUtilization.MUTATION_THREAD_POOL).getPendingTaskCount());
        Assert.assertFalse(StorageService.instance.internalQueueCleanupEMERGENCYUSEONLY("Some invalid queue"));
    }
}
