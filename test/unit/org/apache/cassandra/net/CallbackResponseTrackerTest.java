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

package org.apache.cassandra.net;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.Util.token;
import static org.apache.cassandra.locator.ReplicaUtils.EP1;
import static org.apache.cassandra.locator.ReplicaUtils.EP2;
import static org.apache.cassandra.locator.ReplicaUtils.EP3;
import static org.apache.cassandra.locator.ReplicaUtils.EP4;
import static org.apache.cassandra.locator.ReplicaUtils.EP5;
import static org.apache.cassandra.locator.ReplicaUtils.EP6;
import static org.apache.cassandra.locator.ReplicaUtils.UNIQUE_EP;
import static org.apache.cassandra.locator.ReplicaUtils.full;

@SuppressWarnings("ZeroLengthArrayAllocation")
public class CallbackResponseTrackerTest
{
    private final ArrayList<InetAddressAndPort> endpoints;
    private static final String DC1 = "datacenter1";
    private static final String DC2 = "datacenter2";

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        // No need to wait so long
        DatabaseDescriptor.setReadRpcTimeout(100);
        DatabaseDescriptor.setWriteRpcTimeout(100);

        ServerTestUtils.prepareServerNoRegister();
        ClusterMetadataTestHelper.register(EP1, DC1, "R1");
        ClusterMetadataTestHelper.register(EP2, DC1, "R1");
        ClusterMetadataTestHelper.register(EP3, DC1, "R1");
        ClusterMetadataTestHelper.register(EP4, DC2, "R1");
        ClusterMetadataTestHelper.register(EP5, DC2, "R1");
        ClusterMetadataTestHelper.register(EP6, DC2, "R2");

        ClusterMetadataTestHelper.join(EP1, token(1));
        ClusterMetadataTestHelper.join(EP2, token(2));
        ClusterMetadataTestHelper.join(EP3, token(3));
        ClusterMetadataTestHelper.join(EP4, token(4));
        ClusterMetadataTestHelper.join(EP5, token(5));
        ClusterMetadataTestHelper.join(EP6, token(6));
    }

    public CallbackResponseTrackerTest() throws UnknownHostException
    {
        endpoints = new ArrayList<>();
        endpoints.add(InetAddressAndPort.getByName("127.0.0.1"));
        endpoints.add(InetAddressAndPort.getByName("127.0.0.2"));
        endpoints.add(InetAddressAndPort.getByName("127.0.0.3"));
    }

    @Test
    public void testHappyPath()
    {
        CallbackResponseTracker tracker = new CallbackResponseTracker(endpoints, 2);
        Assert.assertEquals(2, tracker.requiredResponses);
        Assert.assertFalse(tracker.isSuccessful());

        tracker.recordResponse(endpoints.get(0));
        Assert.assertFalse(tracker.isSuccessful());

        tracker.recordResponse(endpoints.get(2));
        // Hit quorum
        Assert.assertTrue(tracker.isSuccessful());
    }

    @Test
    public void testNullMessageCounted()
    {
        CallbackResponseTracker tracker = new CallbackResponseTracker(endpoints, 2);
        Assert.assertEquals(2, tracker.requiredResponses);
        Assert.assertFalse(tracker.isSuccessful());
        Assert.assertEquals(0, tracker.responseCount());

        tracker.recordResponse(null);
        Assert.assertFalse(tracker.isSuccessful());
        Assert.assertEquals(1, tracker.responseCount());

        tracker.recordResponse(endpoints.get(2));
        // Hit quorum
        Assert.assertTrue(tracker.isSuccessful());
        Assert.assertEquals(2, tracker.responseCount());
    }

    /** Emulate RF=3 CL=ALL w/1 failure */
    @Test
    public void testCannotComplete()
    {
        CallbackResponseTracker tracker = new CallbackResponseTracker(endpoints, 3);
        Assert.assertEquals(3, tracker.requiredResponses);
        Assert.assertFalse(tracker.isSuccessful());
        Assert.assertEquals(0, tracker.responseCount());

        tracker.recordResponse(null);
        Assert.assertFalse(tracker.isSuccessful());
        Assert.assertEquals(1, tracker.responseCount());

        tracker.recordFailure(endpoints.get(1), RequestFailureReason.TIMEOUT);
        Assert.assertFalse(tracker.isSuccessful());
        Assert.assertTrue(tracker.cannotComplete());
        Assert.assertEquals(1, tracker.responseCount());
    }

    /**
     * We expect the tracker to successfully update with new values on subsequent calls to {@link CallbackResponseTracker#endProcessing()}
     */
    @Test
    public void testResponsePostEnd()
    {
        CallbackResponseTracker tracker = new CallbackResponseTracker(endpoints, 2);
        Assert.assertEquals(2, tracker.requiredResponses);
        Assert.assertFalse(tracker.isSuccessful());
        Assert.assertEquals(0, tracker.responseCount());

        tracker.recordResponse(null);
        Assert.assertFalse(tracker.isSuccessful());
        Assert.assertEquals(1, tracker.responseCount());

        tracker.recordResponse(endpoints.get(2));
        // Hit quorum
        Assert.assertTrue(tracker.isSuccessful());
        Assert.assertEquals(2, tracker.responseCount());

        Map<InetAddressAndPort, RequestFailureReason> results = tracker.endProcessing();
        Assert.assertNotNull(tracker.endProcessing());

        // Emulate msg after finalization
        tracker.recordFailure(endpoints.get(2), RequestFailureReason.TIMEOUT);
        Assert.assertEquals(results.size() + 1, tracker.endProcessing().size());

        Map<InetAddressAndPort, RequestFailureReason> newResults = tracker.endProcessing();
        Assert.assertEquals(newResults.size(), tracker.endProcessing().size());
        Assert.assertNotEquals(results.size(), newResults.size());
    }

    @Test
    public void testDCTracking()
    {
        List<InetAddressAndPort> ep = ImmutableList.of(EP1, EP2, EP3, EP4, EP5, EP6);
        int rf = 3;

        String ksName = "testDCTracking";
        TableMetadata.Builder builder = TableMetadata.builder(ksName, "Table1")
                                                     .addPartitionKeyColumn("key", BytesType.instance)
                                                     .addClusteringColumn("col1", AsciiType.instance)
                                                     .addRegularColumn("c1", AsciiType.instance)
                                                     .addRegularColumn("c2", AsciiType.instance)
                                                     .addRegularColumn("one", AsciiType.instance)
                                                     .addRegularColumn("two", AsciiType.instance);
        KeyspaceParams params = KeyspaceParams.nts(DC1, rf, DC2, rf);
        SchemaLoader.createKeyspace(ksName, params, builder);
        Keyspace ks = Keyspace.open(ksName);
        ConsistencyLevel cl = ConsistencyLevel.EACH_QUORUM;
        DecoratedKey dk = Util.dk("key1");
        Token t = Murmur3Partitioner.instance.getToken(dk.getKey());


        List<Replica> replicas = new ArrayList<>();
        for (int i = 0; i < 6; i++)
            replicas.add(full(UNIQUE_EP.get(i)));

        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), replicas.toArray(new Replica[0]));
        EndpointsForToken emptyReplicas = EndpointsForToken.empty(dk.getToken());
        Function<ClusterMetadata, ReplicaPlan.ForWrite> paxRecompute = (newClusterMetadata) -> ReplicaPlans.forWrite(ks, cl, t, ReplicaPlans.writeNormal);
        ReplicaPlan.ForWrite eachQuorumPlan = new ReplicaPlan.ForWrite(ks, ks.getReplicationStrategy(), ConsistencyLevel.EACH_QUORUM, emptyReplicas, targetReplicas, emptyReplicas, targetReplicas, paxRecompute, ClusterMetadata.current().epoch);

        CallbackResponseTracker tracker = new CallbackResponseTracker(ep, 2);
        tracker.enableDCTracking(eachQuorumPlan);

        // Check initial conditions on creation
        Assert.assertEquals(2, tracker.requiredResponses);
        Assert.assertFalse(tracker.hitDCConsistencyLevel());

        // Do our basic happy path test
        tracker.recordResponse(EP1);
        tracker.recordResponse(EP2);
        Assert.assertFalse(tracker.hitDCConsistencyLevel());

        tracker.recordResponse(EP3);
        Assert.assertFalse(tracker.hitDCConsistencyLevel());

        tracker.recordResponse(EP4);
        Assert.assertFalse(tracker.hitDCConsistencyLevel());

        tracker.recordResponse(EP5);
        Assert.assertTrue(tracker.hitDCConsistencyLevel());

        // Confirm hitting all in 2nd DC doesn't trigger
        tracker = new CallbackResponseTracker(ep, rf);
        tracker.enableDCTracking(eachQuorumPlan);
        tracker.recordResponse(EP4);
        tracker.recordResponse(EP5);
        tracker.recordResponse(EP6);
        Assert.assertFalse(tracker.hitDCConsistencyLevel());

        // And then bring in DC 1
        tracker.recordResponse(EP2);
        tracker.recordResponse(EP3);
        Assert.assertTrue(tracker.hitDCConsistencyLevel());
    }

    @Test
    public void testIdealCLTracking()
    {
        List<InetAddressAndPort> ep = ImmutableList.of(EP1, EP2, EP3, EP4, EP5, EP6);
        int rf = 3;

        String ksName = "testIdealTracking";
        TableMetadata.Builder builder = TableMetadata.builder(ksName, "Table1")
                                                     .addPartitionKeyColumn("key", BytesType.instance)
                                                     .addClusteringColumn("col1", AsciiType.instance)
                                                     .addRegularColumn("c1", AsciiType.instance)
                                                     .addRegularColumn("c2", AsciiType.instance)
                                                     .addRegularColumn("one", AsciiType.instance)
                                                     .addRegularColumn("two", AsciiType.instance);
        KeyspaceParams params = KeyspaceParams.nts(DC1, rf, DC2, rf);
        SchemaLoader.createKeyspace(ksName, params, builder);
        Keyspace ks = Keyspace.open(ksName);
        ConsistencyLevel cl = ConsistencyLevel.LOCAL_QUORUM;
        DecoratedKey dk = Util.dk("key1");
        Token t = Murmur3Partitioner.instance.getToken(dk.getKey());

        List<Replica> replicas = new ArrayList<>();
        for (int i = 0; i < 6; i++)
            replicas.add(full(UNIQUE_EP.get(i)));

        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), replicas.toArray(new Replica[0]));
        EndpointsForToken emptyReplicas = EndpointsForToken.empty(dk.getToken());
        Function<ClusterMetadata, ReplicaPlan.ForWrite> paxRecompute = (newClusterMetadata) -> ReplicaPlans.forWrite(ks, cl, t, ReplicaPlans.writeNormal);
        ReplicaPlan.ForWrite localQuorumPlan = new ReplicaPlan.ForWrite(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, emptyReplicas, targetReplicas, emptyReplicas, targetReplicas, paxRecompute, ClusterMetadata.current().epoch);

        CallbackResponseTracker tracker = new CallbackResponseTracker(ep, 2);
        tracker.enableIdealCLTracking(localQuorumPlan.withConsistencyLevel(ConsistencyLevel.EACH_QUORUM));

        // Check initial conditions on creation
        Assert.assertEquals(2, tracker.requiredResponses);
        Assert.assertFalse(tracker.isSuccessful());
        Assert.assertFalse(tracker.hitDCConsistencyLevel());
        Assert.assertFalse(tracker.hitIdealConsistencyLevel());

        // We should:
        //   1: See tracker.isSuccessful update based on local DC acks hitting threshold
        //   2: NEVER see tracker.hitDCConsistencyLevel ever be true (it's a LQ callback handler; that's not initialized)
        //   3: See the idealCL update when we hit a quorum in both DC's
        tracker.recordResponse(EP1);
        Assert.assertFalse(tracker.isSuccessful());
        Assert.assertFalse(tracker.hitDCConsistencyLevel());
        Assert.assertFalse(tracker.hitIdealConsistencyLevel());

        tracker.recordResponse(EP2);
        Assert.assertTrue(tracker.isSuccessful());
        Assert.assertFalse(tracker.hitDCConsistencyLevel());
        Assert.assertFalse(tracker.hitIdealConsistencyLevel());

        tracker.recordResponse(EP3);
        Assert.assertTrue(tracker.isSuccessful());
        Assert.assertFalse(tracker.hitDCConsistencyLevel());
        Assert.assertFalse(tracker.hitIdealConsistencyLevel());

        tracker.recordResponse(EP4);
        Assert.assertTrue(tracker.isSuccessful());
        Assert.assertFalse(tracker.hitDCConsistencyLevel());
        Assert.assertFalse(tracker.hitIdealConsistencyLevel());

        tracker.recordResponse(EP5);
        Assert.assertTrue(tracker.isSuccessful());
        Assert.assertFalse(tracker.hitDCConsistencyLevel());
        Assert.assertTrue(tracker.hitIdealConsistencyLevel());

        tracker.recordResponse(EP6);
        Assert.assertTrue(tracker.isSuccessful());
        Assert.assertFalse(tracker.hitDCConsistencyLevel());
        Assert.assertTrue(tracker.hitIdealConsistencyLevel());
    }
}