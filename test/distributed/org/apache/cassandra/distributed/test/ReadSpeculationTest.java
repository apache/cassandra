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

package org.apache.cassandra.distributed.test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.ScheduledThreadPoolExecutorPlus;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceInitializer;
import org.apache.cassandra.distributed.impl.CoordinatorHelper;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyProximity;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.transport.Dispatcher;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ReadSpeculationTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(ReadSpeculationTest.class);
    private static final String TABLE = "tbl";
    private static final String PK_VALUE = "1";

    @Test
    public void speculateTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.set("dynamic_snitch", false))
                                        .withInstanceInitializer(new FixNodeOrderForReads())
                                        .start())
        {
            cluster.forEach(instance -> instance.runOnInstance(() -> {
                // Disable updater since we will force time
                ((ScheduledThreadPoolExecutorPlus) ScheduledExecutors.optionalTasks).remove(CassandraDaemon.SPECULATION_THRESHOLD_UPDATER);
            }));
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + 3 + "}");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH speculative_retry = '2000ms';");

            List<InetAddress> readPlanEndpoints = cluster.get(1).applyOnInstance((none) -> {
                Keyspace keyspace = Keyspace.openIfExists(KEYSPACE);
                ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(TABLE);
                DecoratedKey dk = cfs.decorateKey(bytes(PK_VALUE));
                ReplicaPlan.ForTokenRead plan = ReplicaPlans.forRead(keyspace, cfs.getTableId(), dk.getToken(), null,
                                                                     QUORUM, cfs.metadata().params.speculativeRetry,
                                                                     ReadCoordinator.DEFAULT);
                return plan.contacts().endpointList().stream().map(InetSocketAddress::getAddress).collect(Collectors.toList());
            }, null);
            logger.info("Replicas provided in a read plan contacts: {}", readPlanEndpoints);
            logger.info("Cluster instances: {}", cluster.stream().map(instance -> instance.broadcastAddress().getAddress()).collect(Collectors.toList()));
            int firstReplica = 0;
            int secondReplica = 0;
            for (int i = 1; i <= 3; i++)
            {
                if (match(cluster, i, readPlanEndpoints, 0))
                    firstReplica = i;
                if (match(cluster, i, readPlanEndpoints, 1))
                    secondReplica = i;
            }
            logger.info("1st replica to read from: {}, 2nd replica: {}", firstReplica, secondReplica);
            Assert.assertEquals(1, firstReplica);
            Assert.assertEquals(2, secondReplica);
            // force speculation by dropping all messages sent to the 2nd read replica
            cluster.filters().allVerbs().from(1).to(2).drop();


            cluster.get(1).runOnInstance(() -> {


                // Will speculate: have enough time till RPC timeout and client deadline
                // Request has been submitted 2 seconds ago, and spent 1 second in the queue. Execution has started 1 second ago, and
                // tracked latency is 3 seconds. Speculation timeout is always computed against the start time, so we will speculate
                // exactly once, and after 3 - 1 = 2 seconds.
                new TestScenario(12_000,
                                 12_000,
                                 TimeUnit.SECONDS.toMicros(3),
                                 Config.CQLStartTime.REQUEST,
                                 TimeUnit.SECONDS.toNanos(2),
                                 TimeUnit.SECONDS.toNanos(1))
                .assertWillSpeculate();
                new TestScenario(12_000,
                                 12_000,
                                 TimeUnit.SECONDS.toMicros(3),
                                 Config.CQLStartTime.QUEUE,
                                 TimeUnit.SECONDS.toNanos(2),
                                 TimeUnit.SECONDS.toNanos(1))
                .assertWillSpeculate();

                // Will not speculate: latency is greater than RPC timeout
                // Request has been submitted 2 seconds ago, and spent 1 second in the queue. Execution has started 1 second ago, and
                // tracked latency is 2 seconds. Speculation timeout is always computed against the start time, so we will not speculate
                // since we only have 1.5 seconds to execute request.
                new TestScenario(12_000,
                                 1_500,
                                 TimeUnit.SECONDS.toMicros(2),
                                 Config.CQLStartTime.REQUEST,
                                 TimeUnit.SECONDS.toNanos(3),
                                 TimeUnit.SECONDS.toNanos(1))
                .assertWillNotSpeculate();
                new TestScenario(12_000,
                                 1_500,
                                 TimeUnit.SECONDS.toMicros(2),
                                 Config.CQLStartTime.QUEUE,
                                 TimeUnit.SECONDS.toNanos(3),
                                 TimeUnit.SECONDS.toNanos(1))
                .assertWillNotSpeculate();
                // Will not speculate: speculating will put us over the client deadline, even though RPC timeout is far enough
                // Request has been submitted 11 seconds ago, and spent 10 second in the queue. Execution has started 1 second ago, and
                // tracked latency is 2 seconds. Speculation timeout is always computed against the start time, so we will not speculate
                // since there is only
                new TestScenario(12_000,
                                 10_000,
                                 TimeUnit.SECONDS.toMicros(2),
                                 Config.CQLStartTime.REQUEST,
                                 TimeUnit.SECONDS.toNanos(11),
                                 TimeUnit.SECONDS.toNanos(1))
                .assertWillNotSpeculate();
                new TestScenario(12_000,
                                 1_500,
                                 TimeUnit.SECONDS.toMicros(2),
                                 Config.CQLStartTime.QUEUE,
                                 TimeUnit.SECONDS.toNanos(11),
                                 TimeUnit.SECONDS.toNanos(1))
                .assertWillNotSpeculate();
                new TestScenario(12_000,
                                 2000,
                                 TimeUnit.SECONDS.toMicros(3),
                                 Config.CQLStartTime.QUEUE,
                                 TimeUnit.SECONDS.toNanos(9),
                                 TimeUnit.SECONDS.toNanos(0))
                .assertWillNotSpeculate();
            });
        }
    }

    private static class TestScenario
    {
        final long nativeTimeoutMs;
        final long rpcTimeoutMs;
        // After how much time speculation should be triggered
        final long speculationTimeoutMicros;

        final Config.CQLStartTime cqlStartTime;

        final long enqueuedNsAgo;
        final long startedNsAgo;

        TestScenario(long nativeTimeoutMs, long rpcTimeoutMs, long speculationTimeoutMicros, Config.CQLStartTime cqlStartTime, long enqueuedNsAgo, long startedNsAgo)
        {
            this.nativeTimeoutMs = nativeTimeoutMs;
            this.rpcTimeoutMs = rpcTimeoutMs;
            this.speculationTimeoutMicros = speculationTimeoutMicros;

            this.cqlStartTime = cqlStartTime;

            this.enqueuedNsAgo = enqueuedNsAgo;
            this.startedNsAgo = startedNsAgo;
        }

        private void assertWillSpeculate()
        {
            DatabaseDescriptor.setNativeTransportTimeout(nativeTimeoutMs, TimeUnit.MILLISECONDS);
            DatabaseDescriptor.setReadRpcTimeout(rpcTimeoutMs);
            DatabaseDescriptor.setCQLStartTime(cqlStartTime);

            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
            long speculatedBefore = cfs.metric.speculativeRetries.getCount();
            long before = System.nanoTime();
            cfs.sampleReadLatencyMicros = speculationTimeoutMicros;

            CoordinatorHelper.unsafeExecuteInternal("SELECT * FROM " + KEYSPACE + "." + TABLE + " WHERE pk = " + PK_VALUE,
                                                    ConsistencyLevel.QUORUM,
                                                    ConsistencyLevel.QUORUM,
                                                    new Dispatcher.RequestTime(before - enqueuedNsAgo,
                                                                               before - startedNsAgo));
            long after = System.nanoTime();
            long elapsed = after - before;
            assertThat(elapsed).isGreaterThan(TimeUnit.MICROSECONDS.toNanos(speculationTimeoutMicros) - startedNsAgo);
            long speculatedAfter = cfs.metric.speculativeRetries.getCount();
            Assert.assertEquals(speculatedAfter, speculatedBefore + 1);
        }

        private void assertWillNotSpeculate()
        {
            DatabaseDescriptor.setNativeTransportIdleTimeout(nativeTimeoutMs);
            DatabaseDescriptor.setReadRpcTimeout(rpcTimeoutMs);
            DatabaseDescriptor.setCQLStartTime(cqlStartTime);

            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
            long speculatedBefore = cfs.metric.speculativeRetries.getCount();
            long before = System.nanoTime();
            cfs.sampleReadLatencyMicros = speculationTimeoutMicros;

            try
            {
                CoordinatorHelper.unsafeExecuteInternal("SELECT * FROM " + KEYSPACE + "." + TABLE + " WHERE pk = " + PK_VALUE,
                                                        ConsistencyLevel.QUORUM,
                                                        ConsistencyLevel.QUORUM,
                                                        new Dispatcher.RequestTime(before - enqueuedNsAgo,
                                                                                   before - startedNsAgo));
                throw new AssertionError("Should have timed out");
            }
            catch (ReadTimeoutException t)
            {
                // Expected
            }
            long after = System.nanoTime();
            long elapsed = after - before;
            assertThat(TimeUnit.NANOSECONDS.toMicros(elapsed)).isLessThan(speculationTimeoutMicros);
            long speculatedAfter = cfs.metric.speculativeRetries.getCount();
            Assert.assertEquals(speculatedAfter, speculatedBefore);
        }
    }

    private static boolean match(Cluster cluster, int instanceId, List<InetAddress> readCandidates, int positionInThePlan)
    {
        return cluster.get(instanceId).broadcastAddress().getAddress().equals(readCandidates.get(positionInThePlan));
    }

    public static class FixNodeOrderForReads implements IInstanceInitializer
    {
        @Override
        public void initialise(ClassLoader cl, ThreadGroup group, int node, int generation)
        {
            new ByteBuddy().rebase(NetworkTopologyProximity.class)
                           .method(named("sortedByProximity")).intercept(MethodDelegation.to(FixNodeOrderForReads.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static <C extends ReplicaCollection<? extends C>> C sortedByProximity(final InetAddressAndPort address, C replicas, @SuperCall Callable<C> real) throws Exception
        {
            return replicas.sorted(java.util.Comparator.naturalOrder());
        }
    }
}
