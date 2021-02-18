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

package org.apache.cassandra.locator;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.NeverSpeculativeRetryPolicy;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The test cases check that no false unavailable exception is thrown due to
 * the concurrent modification to the ReplicationStrategy, e.g. alter keyspace.
 *
 * See https://issues.apache.org/jira/browse/CASSANDRA-16545 for details.
 */
@RunWith(BMUnitRunner.class)
@BMRule(name = "FailureDecector sees all nodes as live", // applies to all test cases in the class
        targetClass = "FailureDetector",
        targetMethod = "isAlive",
        action = "return true;")
public class AssureSufficientLiveNodesTest
{
    private static final AtomicInteger testIdGen = new AtomicInteger(0);
    private static final Supplier<String> keyspaceNameGen = () -> "race_" + testIdGen.getAndIncrement();
    private static final String DC1 = "datacenter1";
    private static final String DC2 = "datacenter2";
    private static final String DC3 = "datacenter3";
    private static final int RACE_TEST_LOOPS = 100;
    private static final Token tk = new Murmur3Partitioner.LongToken(0);

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        // Register peers with expected DC for NetworkTopologyStrategy.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                byte[] address = endpoint.addressBytes;
                return "rake" + address[1];
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                byte[] address = endpoint.addressBytes;
                return "datacenter" + address[1];
            }
        });

        List<InetAddressAndPort> instances = ImmutableList.of(
            // datacenter 1
            InetAddressAndPort.getByName("127.1.0.255"), InetAddressAndPort.getByName("127.1.0.254"), InetAddressAndPort.getByName("127.1.0.253"),
            // datacenter 2
            InetAddressAndPort.getByName("127.2.0.255"), InetAddressAndPort.getByName("127.2.0.254"), InetAddressAndPort.getByName("127.2.0.253"),
            // datacenter 3
            InetAddressAndPort.getByName("127.3.0.255"), InetAddressAndPort.getByName("127.3.0.254"), InetAddressAndPort.getByName("127.3.0.253"));

        for (int i = 0; i < instances.size(); i++)
        {
            InetAddressAndPort ip = instances.get(i);
            metadata.updateHostId(UUID.randomUUID(), ip);
            metadata.updateNormalToken(new Murmur3Partitioner.LongToken(i), ip);
        }
    }

    @Test
    public void insufficientLiveNodesTest()
    {
        final KeyspaceParams largeRF = KeyspaceParams.nts("datacenter1", 6);
        // Not a race in fact. It is just testing the Unavailable can be correctly thrown.
        assertThatThrownBy(() ->
           raceOfReplicationStrategyTest(largeRF, largeRF, 1,
                                         keyspace -> ReplicaPlans.forWrite(keyspace, QUORUM, tk, ReplicaPlans.writeNormal))
        ).as("Unavailable should be thrown given 3 live nodes is less than a quorum of 6")
         .isInstanceOf(UnavailableException.class)
         .hasMessageContaining("Cannot achieve consistency level QUORUM");
    }

    @Test
    public void addDatacenterShouldNotCausesUnavailableWithEachQuorumTest() throws Throwable
    {
        // write
        raceOfReplicationStrategyTest(
            // init
            KeyspaceParams.nts(DC1, 3),
            // alter to
            KeyspaceParams.nts(DC1, 3, DC2, 3),
            // test
            keyspace -> ReplicaPlans.forWrite(keyspace, EACH_QUORUM, tk, ReplicaPlans.writeNormal)
        );
        // read
        raceOfReplicationStrategyTest(
            // init
            KeyspaceParams.nts(DC1, 3),
            // alter to
            KeyspaceParams.nts(DC1, 3, DC2, 3),
            // test
            keyspace -> ReplicaPlans.forRead(keyspace, tk, null, EACH_QUORUM, NeverSpeculativeRetryPolicy.INSTANCE)
        );
    }


    @Test
    public void addDatacenterShouldNotCausesUnavailableWithQuorumTest() throws Throwable
    {
        // write
        raceOfReplicationStrategyTest(
            // init. The # of live endpoints is 3.
            KeyspaceParams.nts(DC1, 3),
            // alter to. (3 + 3) / 2 + 1 > 3
            KeyspaceParams.nts(DC1, 3, DC2, 3),
            // test
            keyspace -> ReplicaPlans.forWrite(keyspace, QUORUM, tk, ReplicaPlans.writeNormal)
        );
        raceOfReplicationStrategyTest(
            // init. The # of live endpoints is 3 = 2 + 1
            KeyspaceParams.nts(DC1, 2, DC2, 1),
            // alter to. (3 + 3) / 2 + 1 > 3
            KeyspaceParams.nts(DC1, 2, DC2, 1, DC3, 3),
            // test
            keyspace -> ReplicaPlans.forWrite(keyspace, QUORUM, tk, ReplicaPlans.writeNormal)
        );

        // read
        raceOfReplicationStrategyTest(
            // init
            KeyspaceParams.nts(DC1, 3),
            // alter to
            KeyspaceParams.nts(DC1, 3, DC2, 3),
            // test
            keyspace -> ReplicaPlans.forRead(keyspace, tk, null, QUORUM, NeverSpeculativeRetryPolicy.INSTANCE)
        );
        raceOfReplicationStrategyTest(
            // init. The # of live endpoints is 3 = 2 + 1
            KeyspaceParams.nts(DC1, 2, DC2, 1),
            // alter to. (3 + 3) / 2 + 1 > 3
            KeyspaceParams.nts(DC1, 2, DC2, 1, DC3, 3),
            // test
            keyspace -> ReplicaPlans.forRead(keyspace, tk, null, QUORUM, NeverSpeculativeRetryPolicy.INSTANCE)
        );
    }

    @Test
    public void raceOnRemoveDatacenterNotCausesUnavailable() throws Throwable
    {
        // write
        raceOfReplicationStrategyTest(
            // init
            KeyspaceParams.nts(DC1, 3, DC2, 3),
            // alter to
            KeyspaceParams.nts(DC1, 3),
            // test
            keyspace -> ReplicaPlans.forWrite(keyspace, EACH_QUORUM, tk, ReplicaPlans.writeNormal)
        );

        // read
        raceOfReplicationStrategyTest(
            // init
            KeyspaceParams.nts(DC1, 3, DC2, 3),
            // alter to
            KeyspaceParams.nts(DC1, 3),
            // test
            keyspace -> ReplicaPlans.forRead(keyspace, tk, null, EACH_QUORUM, NeverSpeculativeRetryPolicy.INSTANCE)
        );
    }

    @Test
    public void increaseReplicationFactorShouldNotCausesUnavailableTest() throws Throwable
    {
        // write
        raceOfReplicationStrategyTest(
            // init
            KeyspaceParams.nts(DC1, 1),
            // alter to
            KeyspaceParams.nts(DC1, 3),
            // test
            keyspace -> ReplicaPlans.forWrite(keyspace, LOCAL_QUORUM, tk, ReplicaPlans.writeNormal)
        );

        // read
        raceOfReplicationStrategyTest(
            // init
            KeyspaceParams.nts(DC1, 1),
            // alter to
            KeyspaceParams.nts(DC1, 3),
            // test
            keyspace -> ReplicaPlans.forRead(keyspace, tk, null, LOCAL_QUORUM, NeverSpeculativeRetryPolicy.INSTANCE)
        );
    }

    /**
     * A test runner that runs the `test` while changing the ReplicationStrategy of the raced keyspace.
     * It loops at most for RACE_TEST_LOOPS time if unable to produce the race or any exception.
     */
    private static void raceOfReplicationStrategyTest(KeyspaceParams init,
                                                      KeyspaceParams alterTo,
                                                      int loopCount,
                                                      Consumer<Keyspace> test) throws Throwable
    {
        String keyspaceName = keyspaceNameGen.get();
        KeyspaceMetadata initKsMeta = KeyspaceMetadata.create(keyspaceName, init, Tables.of(SchemaLoader.standardCFMD(keyspaceName, "Bar").build()));
        KeyspaceMetadata alterToKsMeta = initKsMeta.withSwapped(alterTo);
        SchemaTestUtil.announceNewKeyspace(initKsMeta);
        Keyspace racedKs = Keyspace.open(keyspaceName);
        ExecutorService es = Executors.newFixedThreadPool(2);
        try (AutoCloseable ignore = () -> {
            es.shutdown();
            es.awaitTermination(1, TimeUnit.MINUTES);
        })
        {
            for (int i = 0; i < loopCount; i++)
            {
                // reset the keyspace
                racedKs.setMetadata(initKsMeta);
                CountDownLatch trigger = new CountDownLatch(1);
                // starts 2 runnables that could race
                Future<?> f1 = es.submit(() -> {
                    Uninterruptibles.awaitUninterruptibly(trigger);
                    // Update replication strategy
                    racedKs.setMetadata(alterToKsMeta);
                });
                Future<?> f2 = es.submit(() -> {
                    Uninterruptibles.awaitUninterruptibly(trigger);
                    test.accept(racedKs);
                });
                trigger.countDown();
                FBUtilities.waitOnFutures(Arrays.asList(f1, f2));
            }
        }
        catch (RuntimeException rte)
        {
            // extract out the root cause wrapped by `waitOnFutures` and `future.get()`, and rethrow
            if (rte.getCause() != null
                && rte.getCause() instanceof ExecutionException
                && rte.getCause().getCause() != null)
                throw rte.getCause().getCause();
            else
                throw rte;
        }
    }

    private static void raceOfReplicationStrategyTest(KeyspaceParams init,
                                                      KeyspaceParams alterTo,
                                                      Consumer<Keyspace> test) throws Throwable
    {
        raceOfReplicationStrategyTest(init, alterTo, RACE_TEST_LOOPS, test);
    }
}
