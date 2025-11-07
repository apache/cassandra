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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingIs;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingHealthy;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.junit.Assert.fail;

/**
 * Advanced test for write_requests_not_wait_on_pending_replacements feature using ByteBuddy.
 * 
 * This test demonstrates the performance improvement by:
 * 1. Creating a 3-node cluster with RF=3
 * 2. Shutting down one node
 * 3. Starting a replacement node that is slow to respond
 * 4. Showing that writes timeout without the feature
 * 5. Showing that writes succeed with the feature enabled
 */
public class ReplacementPendingWritesTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(ReplacementPendingWritesTest.class);
    private static final String KEYSPACE_NETWORKTOPOLOTY = "replacement_pending_writes_networktopology";
    private static final String KEYSPACE_SIMPLE = "replacement_pending_writes_simple";

    /**
     * Execute a write operation with given consistency to given keyspace and expect it to succeed
     */
    private void executeSuccessfulWrite(IInvokableInstance node, int pk, int ck, int v, ConsistencyLevel cl, String keyspaceName) throws Exception
    {
        node.coordinator().execute("INSERT INTO " + keyspaceName + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                  cl, pk, ck, v);
    }

    /**
     * Execute a write operation with given consistency for given keyspace and expect it to timeout
     */
    private void executeWriteExpectingException(IInvokableInstance node, int pk, int ck, int v, ConsistencyLevel cl, String keyspaceName, String expectedMessage) throws Exception
    {
        try
        {
            node.coordinator().execute("INSERT INTO " + keyspaceName + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                      cl, pk, ck, v);
            fail("Should have thrown a write timeout exception");
        }
        catch (Exception e)
        {
            if (e.getClass().getName().equals(WriteTimeoutException.class.getName()))
            {
                Assert.assertEquals(expectedMessage, e.getMessage());
            }
            else if (e.getClass().getName().equals(UnavailableException.class.getName()))
            {
                Assert.assertEquals("Cannot achieve consistency level " + cl, e.getMessage());
            }
            else
            {
                throw new AssertionError(e);
            }
        }
    }

    /**
     * Test that demonstrates the feature prevents write timeouts in certain cases
     */
    @Test
    public void testWriteBehaviorWhenReplacementPendingNodeIsBusy() throws Exception
    {
        int numStartNodes = 3;
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(numStartNodes);
        try (Cluster cluster = Cluster.build(numStartNodes)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withDynamicPortAllocation(false)
                                      .withRacks(1, 3, numStartNodes)
                                      .withInstanceInitializer(ReplacementPendingWritesTest.BB::install)
                                      .withTokenSupplier(node -> even.token(node == (numStartNodes + 1) ? 2 : node))
                                      .start())
        {
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);
            fixDistributedSchemas(cluster);
            init(cluster);

            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_SIMPLE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};" );
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE_SIMPLE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NETWORKTOPOLOTY + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};" );
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE_NETWORKTOPOLOTY + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            stopUnchecked(nodeToRemove);

            // now create a new node to replace the other node
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove, props -> {
                // since we have a downed host there might be a schema version which is old show up but
                // can't be fetched since the host is down...
                props.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
            });

            // enable the feature(write_requests_not_wait_on_pending_replacements) on node1 and disable the feature on node3
            node1.runOnInstance(
            () -> DatabaseDescriptor.setExcludeReplacementPendingForWrite(true)
            );
            node3.runOnInstance(
            () -> DatabaseDescriptor.setExcludeReplacementPendingForWrite(false)
            );

            // disable messages to new node to simulate new node busy
            IMessageFilters.Filter disableMutationToNewNode = cluster.filters().inbound().verbs(Verb.MUTATION_REQ.id).to(4).drop();

            // Test the feature enabled with pending node busy.
            List<TestData> testDataForFeatureEnabledWhenPendingNodeBusy = getTestDataForFeatureEnabledWhenPendingNodeBusy();
            testHelperQueryExecutor(node1, testDataForFeatureEnabledWhenPendingNodeBusy);

            // Test the feature is disabled with pending node busy
            List<TestData> testDataForFeatureDisabledWhenPendingNodeBusy = getTestDataForFeatureDisabledWhenPendingNodeBusy();
            testHelperQueryExecutor(node3, testDataForFeatureDisabledWhenPendingNodeBusy);
            // enable the new node to receive mutations, both nodes should be able to get successful writes
            disableMutationToNewNode.off();
            List<TestData> testDataForPendingNodeNotBusy = getTestDataForPendingNodeNotBusy(true);
            // test feature enabled
            testHelperQueryExecutor(node1, testDataForPendingNodeNotBusy);
            // test feature disabled
            testDataForPendingNodeNotBusy = getTestDataForPendingNodeNotBusy(false);
            testHelperQueryExecutor(node3, testDataForPendingNodeNotBusy);

            // test if feature is enabled, one natural replica cannot receive mutation, we get write timeout
            // because pending node response will not be counted
            IMessageFilters.Filter disableMutationFrom1To3 = cluster.filters().inbound().verbs(Verb.MUTATION_REQ.id).from(1).to(3).drop();
            List<TestData> testDataForFeatureEnabledWhenNormalNodeBusy = getTestDataForFeatureEnabledWhenNormalNodeBusy();
            testHelperQueryExecutor(node1, testDataForFeatureEnabledWhenNormalNodeBusy);
            disableMutationFrom1To3.off();

            // test if feature is disabled, one natural replica cannot receive mutation, but we are able to get response
            // from the pending node
            IMessageFilters.Filter disableMutationFrom3To1 = cluster.filters().inbound().verbs(Verb.MUTATION_REQ.id).from(3).to(1).drop();
            List<TestData> testDataForFeatureDisabledWhenNormalNodeBusy = getTestDataForFeatureDisabledWhenNormalNodeBusy();
            testHelperQueryExecutor(node3, testDataForFeatureDisabledWhenNormalNodeBusy);
            disableMutationFrom3To1.off();

            // restore the replacement node to let it join
            // unblock node 4 from joining
            replacingNode.runOnInstance(
            () -> {
                Assert.assertTrue(StorageService.instance.isJoining());
                // disable block join and retry later
                BB.keepNodeInPendingState.set(false);
            }
            );

            // resume bootstrap to allow the new node join.
            System.setProperty("cassandra.reset_bootstrap_progress", "false");
            System.setProperty("cassandra.replace_address_first_boot", nodeToRemove.config().broadcastAddress().getAddress().getHostAddress());
            replacingNode.nodetoolResult("bootstrap", "resume", "-f").asserts().success();

            // wait till the replacing node is in the ring
            awaitRingJoin(node1, replacingNode);
            awaitRingJoin(node3, replacingNode);
            awaitRingJoin(replacingNode, node1);
            awaitRingJoin(replacingNode, node3);

            // make sure all nodes are healthy
            awaitRingHealthy(node1);

            assertRingIs(node1, node1, replacingNode, node3);
        }
    }

    public static class BB
    {
        public static final AtomicBoolean keepNodeInPendingState = new AtomicBoolean(true);

        public static void install(ClassLoader cl, Integer i)
        {
            // only install for the new node
            if (i != 4)
                return;
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("bootstrapFinished"))
                           .intercept(MethodDelegation.to(ReplacementPendingWritesTest.BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void bootstrapFinished(@SuperCall Callable<Boolean> zuper) throws Exception
        {
            while (keepNodeInPendingState.get())
            {
                logger.info("Keep node in pending state by throwing error");
                throw new RuntimeException("Keep node in joining state");
            }
            logger.info("Done keep node in pending state. Node will finish join soon.");
            zuper.call();
        }
    }

    private class TestData
    {
        String keyspaceName;
        ConsistencyLevel consistencyLevel;
        boolean exceptionExpected;
        int recievedAcks;

        TestData(String keyspaceName, ConsistencyLevel consistencyLevel, boolean exceptionExpected, int recievedAcks)
        {
            this.keyspaceName = keyspaceName;
            this.consistencyLevel = consistencyLevel;
            this.exceptionExpected = exceptionExpected;
            this.recievedAcks = recievedAcks;
        }
    }

    private List<TestData> getTestDataForFeatureEnabledWhenPendingNodeBusy()
    {
        List<TestData> result = new ArrayList<>();
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            switch (cl)
            {
                case ANY:
                case ONE:
                case TWO:
                case QUORUM:
                case LOCAL_ONE:
                    result.add(new TestData(KEYSPACE_SIMPLE, cl, false, 0));
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, false, 0));
                    break;
                case LOCAL_QUORUM:
                case EACH_QUORUM:
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, false, 0));
                    break;
                case THREE:
                case ALL:
                    result.add(new TestData(KEYSPACE_SIMPLE, cl, true, 2));
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, true, 2));
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    private List<TestData> getTestDataForFeatureDisabledWhenPendingNodeBusy()
    {
        List<TestData> result = new ArrayList<>();
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            switch (cl)
            {
                case ANY:
                case ONE:
                case LOCAL_ONE:
                    result.add(new TestData(KEYSPACE_SIMPLE, cl, false, 0));
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, false, 0));
                    break;
                case LOCAL_QUORUM:
                case EACH_QUORUM:
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, true, 2));
                    break;
                case THREE:
                case ALL:
                    result.add(new TestData(KEYSPACE_SIMPLE, cl, true, 2));
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, true, 2));
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    private List<TestData> getTestDataForPendingNodeNotBusy(boolean featureEnabled)
    {
        List<TestData> result = new ArrayList<>();
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            switch (cl)
            {
                case ANY:
                case ONE:
                case LOCAL_ONE:
                    result.add(new TestData(KEYSPACE_SIMPLE, cl, false, 0));
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, false, 0));
                    break;
                case LOCAL_QUORUM:
                case EACH_QUORUM:
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, false, 0));
                    break;
                case THREE:
                case ALL:
                    int recievedAcks = 3;
                    if (featureEnabled)
                        recievedAcks = 2;
                    result.add(new TestData(KEYSPACE_SIMPLE, cl, true, recievedAcks));
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, true, recievedAcks));
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    private List<TestData> getTestDataForFeatureEnabledWhenNormalNodeBusy()
    {
        List<TestData> result = new ArrayList<>();
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            switch (cl)
            {
                case ANY:
                case ONE:
                case LOCAL_ONE:
                    result.add(new TestData(KEYSPACE_SIMPLE, cl, false, 0));
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, false, 0));
                    break;
                case LOCAL_QUORUM:
                case EACH_QUORUM:
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, true, 1));
                    break;
                case THREE:
                case ALL:
                    result.add(new TestData(KEYSPACE_SIMPLE, cl, true, 1));
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, true, 1));
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    private List<TestData> getTestDataForFeatureDisabledWhenNormalNodeBusy()
    {
        List<TestData> result = new ArrayList<>();
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            switch (cl)
            {
                case ANY:
                case ONE:
                case LOCAL_ONE:
                    result.add(new TestData(KEYSPACE_SIMPLE, cl, false, 0));
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, false, 0));
                    break;
                case LOCAL_QUORUM:
                case EACH_QUORUM:
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, true, 2));
                    break;
                case THREE:
                case ALL:
                    result.add(new TestData(KEYSPACE_SIMPLE, cl, true, 2));
                    result.add(new TestData(KEYSPACE_NETWORKTOPOLOTY, cl, true, 2));
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    private void testHelperQueryExecutor(IInvokableInstance node, List<TestData> testData) throws Exception
    {
        for (TestData singleTestData : testData)
        {
            if (singleTestData.exceptionExpected)
            {
                String expectedMessage = String.format("Operation timed out - received only %d responses.", singleTestData.recievedAcks);
                executeWriteExpectingException(node, 2, 2, 2, singleTestData.consistencyLevel, singleTestData.keyspaceName, expectedMessage);
            }
            else
            {
                executeSuccessfulWrite(node, 1, 1, 1, singleTestData.consistencyLevel, singleTestData.keyspaceName);
            }
        }
    }
}

