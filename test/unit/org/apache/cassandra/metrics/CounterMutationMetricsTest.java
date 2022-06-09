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
package org.apache.cassandra.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.Mutation.PartitionUpdateCollector;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CounterMutationMetricsTest
{
    private static final String KEYSPACE1 = "CounterMutationMetricsTest";
    private static final String CF1 = "Counter1";
    private static final String CF2 = "Counter2";
    private static long LOCK_TIMEOUT_MILLIS;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF1),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF2));
        LOCK_TIMEOUT_MILLIS = DatabaseDescriptor.getCounterWriteRpcTimeout(TimeUnit.MILLISECONDS);
    }

    @After
    public void after()
    {
        long currentTimeout = DatabaseDescriptor.getCounterWriteRpcTimeout(TimeUnit.MILLISECONDS);
        if (currentTimeout != LOCK_TIMEOUT_MILLIS)
            DatabaseDescriptor.setCounterWriteRpcTimeout(LOCK_TIMEOUT_MILLIS);
    }

    @Test
    public void testLocksMetrics()
    {
        AssertMetrics metrics = new AssertMetrics();

        ColumnFamilyStore cfsOne = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1);
        ColumnFamilyStore cfsTwo = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF2);

        RowUpdateBuilder mutationBuilder1 = new RowUpdateBuilder(cfsOne.metadata(), 5, "key1");
        Mutation mutation1 = mutationBuilder1.clustering("cc")
                                             .add("val", 1L)
                                             .add("val2", -1L)
                                             .build();
        RowUpdateBuilder mutationBuilder2 = new RowUpdateBuilder(cfsTwo.metadata(), 5, "key1");
        Mutation mutation2 = mutationBuilder2.clustering("cc")
                                             .add("val", 2L)
                                             .add("val2", -2L)
                                             .build();

        PartitionUpdateCollector batch = new PartitionUpdateCollector(KEYSPACE1, Util.dk("key1"));
        batch.add(mutation1.getPartitionUpdate(cfsOne.metadata()));
        batch.add(mutation2.getPartitionUpdate(cfsTwo.metadata()));

        new CounterMutation(batch.build(), ConsistencyLevel.ONE).apply();

        metrics.assertLockTimeout(0);
        metrics.assertLocksPerUpdate(1);
    }

    private void obtainLocks(TableMetadata metadata, List<Lock> locks)
    {
        RowUpdateBuilder mutationBuilder = new RowUpdateBuilder(metadata, 5, "key1");
        Mutation mutation = mutationBuilder.clustering("cc")
                                           .add("val", 1L)
                                           .build();
        CounterMutation counterMutation = new CounterMutation(mutation , ConsistencyLevel.ONE);
        counterMutation.grabCounterLocks(Keyspace.open(KEYSPACE1), locks);
    }

    private void tryMutate(TableMetadata metadata)
    {
        RowUpdateBuilder mutateBuilder = new RowUpdateBuilder(metadata, 5, "key1");
        Mutation mutation = mutateBuilder.clustering("cc")
                                         .add("val", 1L)
                                         .add("val2", -1L)
                                         .build();
        boolean failedMutate = false;
        try
        {
            new CounterMutation(mutation, ConsistencyLevel.ONE).apply();
        }
        catch (WriteTimeoutException e)
        {
            failedMutate = true;
        }
        assertTrue("Expected to fail apply mutation", failedMutate);
    }

    @Test
    public void testLockTimeout() throws Throwable
    {
        final long timeoutMillis = 11;
        final long biggerThanTimeoutMillis = 1000;
        AssertMetrics metrics = new AssertMetrics(timeoutMillis);

        TableMetadata metadata = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF1).metadata();

        List<Lock> obtainedLocks = new ArrayList<>();
        try
        {
            obtainLocks(metadata, obtainedLocks);
            metrics.assertLockTimeout(0);

            CompletableFuture.runAsync(() -> tryMutate(metadata))
                             .get(biggerThanTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        finally
        {
            for (Lock lock : obtainedLocks)
                lock.unlock();
        }

        metrics.assertLockTimeout(1);
        metrics.assertLocksPerUpdate(2);
    }

    public class AssertMetrics
    {
        public final long lockTimeoutMilis;
        public final long prevLockTimeoutCount;
        public final long prevMaxLockAcquireTime;

        public AssertMetrics()
        {
            this(LOCK_TIMEOUT_MILLIS);
        }

        public AssertMetrics(long timeoutInMilis)
        {
            clear();
            prevLockTimeoutCount = CounterMutation.lockTimeout.getCount();
            prevMaxLockAcquireTime = CounterMutation.lockAcquireTime.latency.getSnapshot().getMax();

            this.lockTimeoutMilis = timeoutInMilis;
            if (timeoutInMilis != LOCK_TIMEOUT_MILLIS)
                DatabaseDescriptor.setCounterWriteRpcTimeout(lockTimeoutMilis);
        }

        public void clear()
        {
            ((ClearableHistogram)CounterMutation.locksPerUpdate).clear();
        }

        public void assertLockTimeout(long expected)
        {
            assertEquals(prevLockTimeoutCount + expected, CounterMutation.lockTimeout.getCount());

            long maxLockAcquireTime = CounterMutation.lockAcquireTime.latency.getSnapshot().getMax();
            if (expected > 0)
                assertTrue(lockTimeoutMilis <= maxLockAcquireTime);
            else
                assertTrue(lockTimeoutMilis > maxLockAcquireTime
                           || prevMaxLockAcquireTime <= maxLockAcquireTime);
        }

        public void assertLocksPerUpdate(long expectedCount)
        {
            assertEquals(expectedCount, CounterMutation.locksPerUpdate.getCount());
        }
    }
}
