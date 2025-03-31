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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.ReadResponseTest;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TruncateResponse;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.EachQuorumResponseHandler;
import org.apache.cassandra.service.LocalQuorumResponseHandler;
import org.apache.cassandra.service.TruncateResponseHandler;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.service.paxos.v1.PrepareCallback;
import org.apache.cassandra.service.paxos.v1.ProposeCallback;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.RandomHelpers;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.Util.token;
import static org.apache.cassandra.locator.ReplicaUtils.EP1;
import static org.apache.cassandra.locator.ReplicaUtils.EP2;
import static org.apache.cassandra.locator.ReplicaUtils.EP3;
import static org.apache.cassandra.locator.ReplicaUtils.EP4;
import static org.apache.cassandra.locator.ReplicaUtils.EP5;
import static org.apache.cassandra.locator.ReplicaUtils.EP6;
import static org.apache.cassandra.locator.ReplicaUtils.EP7;
import static org.apache.cassandra.locator.ReplicaUtils.EP8;
import static org.apache.cassandra.locator.ReplicaUtils.EP9;
import static org.apache.cassandra.locator.ReplicaUtils.EP10;
import static org.apache.cassandra.locator.ReplicaUtils.UNIQUE_EP;
import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.quicktheories.QuickTheory.qt;

@SuppressWarnings({ "unchecked", "rawtypes", "ZeroLengthArrayAllocation" })
public class CallbackPropertyTest
{
    private static final Logger logger = LoggerFactory.getLogger(CallbackPropertyTest.class);

    private enum CallbackType
    {
        NORMAL,
        PAXOS,
        TRUNCATE,
        EACH_QUORUM,
        LOCAL_QUORUM
    }

    /** Enable to print out details of the inferred test configuration setup **/
    private static boolean DEBUG = false;

    public static final String CF_STANDARD = "Standard1";

    public static Keyspace ks;
    public static ColumnFamilyStore cfs;
    public static TableMetadata cfm;

    private static final String DC1 = "datacenter1";
    private static final String DC2 = "datacenter2";

    /** By default, we'll test the CL's listed here unless the runner otherwise constrains things */
    private EnumSet<ConsistencyLevel> testedCL = EnumSet.of(ConsistencyLevel.ALL, ConsistencyLevel.QUORUM, ConsistencyLevel.ONE);

    public static DecoratedKey dk;

    private SinglePartitionReadCommand command;

    private ReplicaPlan.Shared readReplicaPlan;
    private ReplicaPlan.ForWrite writeReplicaPlan;
    private ReplicaPlan.ForPaxosWrite paxosReplicaPlan;
    private ReplicaPlan.ForWrite eachQuorumPlan;
    private ReplicaPlan.ForWrite localQuorumPlan;

    private DataResolver resolver;
    private Dispatcher.RequestTime requestTime;

    private int dc1NodesDown;
    private int dc2NodesDown = 0;
    private int rf;
    private ConsistencyLevel cl;
    private int quorum;

    /** We calculate whether we expect a test to succeed in the runner to then compare against from inside the test case's context */
    private boolean caseExpectsException;

    private Runnable testRunnable;

    @BeforeClass
    public static void setupClass()
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
        ClusterMetadataTestHelper.register(EP4, DC1, "R1");
        ClusterMetadataTestHelper.register(EP5, DC1, "R1");
        ClusterMetadataTestHelper.register(EP6, DC2, "R2");
        ClusterMetadataTestHelper.register(EP7, DC2, "R2");
        ClusterMetadataTestHelper.register(EP8, DC2, "R2");
        ClusterMetadataTestHelper.register(EP9, DC2, "R2");
        ClusterMetadataTestHelper.register(EP10, DC2, "R2");
        // We need to add endpoint's based on 0-4 == datacenter1, 5-9 == datacenter2.
        for (int i = 0; i < 5; i++)
        {
            InetAddressAndPort peer1 = UNIQUE_EP.get(i);
            InetAddressAndPort peer2 = UNIQUE_EP.get(i + 5);

            Token t1 = token(i);
            Token t2 = token(i + 5);

            ClusterMetadataTestHelper.join(peer1, t1);
            ClusterMetadataTestHelper.join(peer2, t2);
        }
    }

    private void runTestMatrix(CallbackType type, Runnable r)
    {
        int minRF = 1;
        testRunnable = r;

        // Based on the type of callback we're testing we'll constrain various properties
        switch (type)
        {
            case PAXOS:
                minRF = 2;
                testedCL = EnumSet.of(ConsistencyLevel.SERIAL);
                break;
            case TRUNCATE:
                testedCL = EnumSet.of(ConsistencyLevel.ALL);
                cl = ConsistencyLevel.ALL;
                break;
            case EACH_QUORUM:
                cl = ConsistencyLevel.EACH_QUORUM;
                break;
            case LOCAL_QUORUM:
                cl = ConsistencyLevel.LOCAL_QUORUM;
                break;
            case NORMAL:
            default:
                // pass -> respect CL and test them all
        }

        if (type == CallbackType.EACH_QUORUM || type == CallbackType.LOCAL_QUORUM)
        {
            qt().forAll(
                SourceDSL.integers().between(minRF, 5).describedAs(rf -> "rf: " + rf),
                SourceDSL.integers().between(0, 5).describedAs(nd1 -> "DC1NodesDown: " + nd1),
                SourceDSL.integers().between(0, 5).describedAs(nd2 -> "DC2NodesDown: " + nd2)
                )
                .checkAssert(this::runMultiDCTest);
        }
        else
        {
            qt().forAll(
                SourceDSL.integers().between(minRF, 10).describedAs(rf -> "rf: " + rf),
                SourceDSL.arbitrary().pick(testedCL.toArray(new ConsistencyLevel[0])).describedAs(cl -> "cl: " + cl),
                SourceDSL.integers().between(0, 10).describedAs(nd -> "nodesDown: " + dc1NodesDown)
                )
                .checkAssert(this::runSingleDCTest);
        }
    }

    private void runSingleDCTest(int rf, ConsistencyLevel cl, int nodesDown)
    {
        if (nodesDown > rf) return;  // Invalid scenario, skip

        this.rf = rf;
        this.cl = cl;
        this.dc1NodesDown = nodesDown;

        String ksName = "RequestCallbackTest" + rf + '_' + cl + '_' + nodesDown + System.nanoTime();
        TableMetadata.Builder builder = TableMetadata.builder(ksName, CF_STANDARD)
                                                     .addPartitionKeyColumn("key", BytesType.instance)
                                                     .addClusteringColumn("col1", AsciiType.instance)
                                                     .addRegularColumn("c1", AsciiType.instance)
                                                     .addRegularColumn("c2", AsciiType.instance)
                                                     .addRegularColumn("one", AsciiType.instance)
                                                     .addRegularColumn("two", AsciiType.instance);

        SchemaLoader.createKeyspace(ksName, KeyspaceParams.simple(rf), builder);

        this.quorum = ReplicaPlans.calculateQuorumMajority(rf);
        if (cl == ConsistencyLevel.ALL) this.quorum = rf;
        if (cl == ConsistencyLevel.ONE) this.quorum = 1;

        this.caseExpectsException = rf - nodesDown < quorum;

        logger.info("RUNNING TEST CASE: rf: {}. cl: {}. nodesDown: {}. quorum: {}. expectedException: {}", rf, cl, nodesDown, quorum, caseExpectsException);

        ks = Keyspace.open(ksName);
        cfs = ks.getColumnFamilyStore(CF_STANDARD);
        cfm = cfs.metadata();
        dk = Util.dk("key1");
        Token t = Murmur3Partitioner.instance.getToken(dk.getKey());
        command = SinglePartitionReadCommand.fullPartitionRead(cfm, FBUtilities.nowInSeconds(), dk);

        List<Replica> replicas = new ArrayList<>();
        for (int i = 0; i < rf; i++)
            replicas.add(full(UNIQUE_EP.get(i)));

        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), replicas.toArray(new Replica[0]));
        EndpointsForToken emptyReplicas = EndpointsForToken.empty(dk.getToken());

        // Since all callbacks excepting multi-DC cl are handled by this test runner, we init replica plans we may not use here. /shrug
        ReplicaPlan.ForTokenRead plan = ReplicaPlans.forRead(ks, dk.getToken(), null, cl, cfs.metadata().params.speculativeRetry);
        readReplicaPlan = ReplicaPlan.shared(new ReplicaPlan.ForTokenRead(ks,
                                                                          ks.getReplicationStrategy(),
                                                                          cl,
                                                                          targetReplicas,
                                                                          targetReplicas,
                                                                          (newClusterMetadata) -> plan,
                                                                          (self) -> { throw new IllegalStateException("Read repair is not supported for these tests"); },
                                                                          ClusterMetadata.current().epoch)
        );

        Function<ClusterMetadata, ReplicaPlan.ForWrite> writeRecompute = (newClusterMetadata) -> ReplicaPlans.forWrite(ks, cl, t, ReplicaPlans.writeNormal);
        writeReplicaPlan = new ReplicaPlan.ForWrite(ks, ks.getReplicationStrategy(), cl, emptyReplicas, targetReplicas, emptyReplicas, targetReplicas, writeRecompute, ClusterMetadata.current().epoch);

        int paxosRequired = ReplicaPlans.calculateQuorumMajority(targetReplicas.size());
        Function<ClusterMetadata, ReplicaPlan.ForWrite> paxosRecompute = (newClusterMetadata) -> ReplicaPlans.forPaxos(ks, dk, cl);
        paxosReplicaPlan = ReplicaPlan.ForPaxosWrite.forTest(ks, cl, emptyReplicas, targetReplicas, emptyReplicas, targetReplicas, paxosRequired, paxosRecompute, ClusterMetadata.current().epoch);

        ReadRepair readRepair = ReadRepair.create(command, readReplicaPlan, requestTime);
        resolver = new DataResolver(command, readReplicaPlan, readRepair, requestTime);

        requestTime = new Dispatcher.RequestTime(Clock.Global.nanoTime());

        testRunnable.run();
    }

    private void runMultiDCTest(int rf, int dc1NodesDown, int dc2NodesDown)
    {
        if (dc1NodesDown > rf) return;  // invalid param arrangement; skip
        if (dc2NodesDown > rf) return;  // invalid param arrangement; skip

        this.rf = rf;
        this.quorum = ReplicaPlans.calculateQuorumMajority(rf);
        this.dc1NodesDown = dc1NodesDown;
        this.dc2NodesDown = dc2NodesDown;

        // Only care about DC1 on local quorum check
        caseExpectsException = cl == ConsistencyLevel.LOCAL_QUORUM
                               ? rf - dc1NodesDown < quorum
                               : rf - dc1NodesDown < quorum || rf - dc2NodesDown < quorum;

        String ksName = "RequestCallbackTest" + rf + '_' + cl + '_' + dc1NodesDown + '_' + dc2NodesDown;
        TableMetadata.Builder builder = TableMetadata.builder(ksName, CF_STANDARD)
                                                     .addPartitionKeyColumn("key", BytesType.instance)
                                                     .addClusteringColumn("col1", AsciiType.instance)
                                                     .addRegularColumn("c1", AsciiType.instance)
                                                     .addRegularColumn("c2", AsciiType.instance)
                                                     .addRegularColumn("one", AsciiType.instance)
                                                     .addRegularColumn("two", AsciiType.instance);

        logger.info("RUNNING TEST CASE: rf: {}. cl: {}. dc1NodesDown: {}. dc2NodesDown: {}, quorum: {}. expectedException: {}", rf, cl, dc1NodesDown, dc2NodesDown, quorum, caseExpectsException);
        KeyspaceParams params = KeyspaceParams.nts(DC1, rf, DC2, rf);
        SchemaLoader.createKeyspace(ksName, params, builder);

        ks = Keyspace.open(ksName);
        cfs = ks.getColumnFamilyStore(CF_STANDARD);
        cfm = cfs.metadata();
        dk = Util.dk("key1");
        Token t = Murmur3Partitioner.instance.getToken(dk.getKey());
        command = SinglePartitionReadCommand.fullPartitionRead(cfm, FBUtilities.nowInSeconds(), dk);

        List<Replica> replicas = new ArrayList<>();
        for (int i = 0; i < rf; i++)
            replicas.add(full(UNIQUE_EP.get(i)));

        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), replicas.toArray(new Replica[0]));
        EndpointsForToken emptyReplicas = EndpointsForToken.empty(dk.getToken());

        Function<ClusterMetadata, ReplicaPlan.ForWrite> paxRecompute = (newClusterMetadata) -> ReplicaPlans.forWrite(ks, cl, t, ReplicaPlans.writeNormal);

        eachQuorumPlan = new ReplicaPlan.ForWrite(ks, ks.getReplicationStrategy(), ConsistencyLevel.EACH_QUORUM, emptyReplicas, targetReplicas, emptyReplicas, targetReplicas, paxRecompute, ClusterMetadata.current().epoch);
        debugLog("********************************** About to create LQP; check init below this");
        localQuorumPlan = new ReplicaPlan.ForWrite(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, emptyReplicas, targetReplicas, emptyReplicas, targetReplicas, paxRecompute, ClusterMetadata.current().epoch);

        requestTime = new Dispatcher.RequestTime(Clock.Global.nanoTime());

        testRunnable.run();
    }

    @Test
    public void testRead()
    {
        runTestMatrix(CallbackType.NORMAL, () -> {
            ReadCallback callback = new ReadCallback(resolver, command, readReplicaPlan, requestTime);
            validateQuorum(callback.blockFor());
            try
            {
                for (int i : shuffleEndpointOrder(rf))
                {
                    InetAddressAndPort ep = UNIQUE_EP.get(i);
                    if (i >= dc1NodesDown)
                    {
                        ReadResponseTest.StubRepairedDataInfo rdi = new ReadResponseTest.StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
                        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(cfm), rdi);
                        callback.onResponse(Message.synthetic(ep, Verb.READ_RSP, response));
                    }
                    else
                        callback.onFailure(ep, RequestFailureReason.TIMEOUT);
                }
                callback.awaitResults();
            }
            catch (Exception e)
            {
                validateExceptionState(ReadTimeoutException.class, e);
                return;
            }
            validateNoException(ReadTimeoutException.class);
        });
    }

    @Test
    public void testWrite()
    {
        runTestMatrix(CallbackType.NORMAL, () -> {
            WriteResponseHandler callback = new WriteResponseHandler(writeReplicaPlan, WriteType.SIMPLE, null, requestTime);
            validateQuorum(callback.blockFor());
            Assert.assertSame(String.format("Got unexpected blockFor / requiredResponses from cl: %s.", cl), quorum, callback.blockFor());
            try
            {
                for (int i : shuffleEndpointOrder(rf))
                {
                    InetAddressAndPort ep = UNIQUE_EP.get(i);
                    if (i >= dc1NodesDown)
                        callback.onResponse(Message.synthetic(ep, Verb.MUTATION_RSP, null));
                    else
                        callback.onFailure(ep, RequestFailureReason.TIMEOUT);
                }
                callback.get();
            }
            catch (Exception e)
            {
                validateExceptionState(WriteTimeoutException.class, e);
                return;
            }
            validateNoException(WriteTimeoutException.class);
        });
    }

    /**
     * Effectively, anything that's a non-zero # of downed nodes can be expected to raise an exception on callback completion.
     * Timeouts get swallowed into and converted to TruncateException's
     */
    @Test
    public void testTruncate()
    {
        runTestMatrix(CallbackType.TRUNCATE, () -> {
            TruncateResponseHandler callback = new TruncateResponseHandler(writeReplicaPlan.contacts().endpoints());
            // Should always have blockFor == entire RF for Truncate
            Assert.assertEquals(String.format("Got unexpected blockFor / requiredResponses from cl %s", cl), rf, callback.blockFor());
            try
            {
                for (int i : shuffleEndpointOrder(rf))
                {
                    InetAddressAndPort ep = UNIQUE_EP.get(i);
                    if (i >= dc1NodesDown)
                        callback.onResponse(Message.synthetic(ep, Verb.PAXOS_PROPOSE_RSP, new TruncateResponse(ks.getName(), CF_STANDARD, true)));
                    else
                        callback.onFailure(ep, RequestFailureReason.TIMEOUT);
                }
                callback.get();
            }
            catch (Exception e)
            {
                validateExceptionState(TruncateException.class, e);
                return;
            }
            validateNoException(TruncateException.class);
        });
    }

    @Test
    public void testEachQuorum()
    {
        runTestMatrix(CallbackType.EACH_QUORUM, () -> {
            debugLog("---------------- CYCLE. rf: {}. nd1: {}. nd2: {}. quorum: {}", rf, dc1NodesDown, dc2NodesDown, quorum);
            EachQuorumResponseHandler callback = new EachQuorumResponseHandler(eachQuorumPlan, null, WriteType.SIMPLE, null, requestTime);
            Assert.assertEquals(String.format("Saw invalid # of responses required; expect quorum from each DC. quorum: %d, blockFor: %d", quorum, callback.blockFor()), quorum * 2, callback.blockFor());
            try
            {
                // Need to process one node per DC on each cycle and run them relative to the nodesDown for that DC
                for (int i : shuffleEndpointOrder(rf))
                {
                    processDCCallback(UNIQUE_EP.get(i), i, dc1NodesDown, callback);
                    // idx is offset from 0 for node idx vs. nodesdown comparison
                    processDCCallback(UNIQUE_EP.get(i + 5), i, dc2NodesDown, callback);
                }
                callback.get();
            }
            catch (Exception e)
            {
                validateExceptionState(WriteTimeoutException.class, e);
                return;
            }
            validateNoException(WriteTimeoutException.class);
        });
    }

    @Test
    public void testLocalQuorum()
    {
        DEBUG = true;
        runTestMatrix(CallbackType.LOCAL_QUORUM, () -> {
            debugLog("---------------- CYCLE. rf: {}. nd1: {}. nd2: {}. quorum: {}", rf, dc1NodesDown, dc2NodesDown, quorum);
            debugLog("writeQuorum on localQuorumPlan: {}", localQuorumPlan.writeQuorum());
            debugLog("participants in lqp: {}", localQuorumPlan.liveAndDown().size());
            LocalQuorumResponseHandler callback = new LocalQuorumResponseHandler(localQuorumPlan, null, WriteType.SIMPLE, null, requestTime);
            Assert.assertEquals(String.format("Saw invalid # of responses required; expect quorum from only primary DC. quorum: %d, blockFor: %d", quorum, callback.blockFor()), quorum, callback.blockFor());
            try
            {
                // Need to process one node per DC on each cycle and run them relative to the nodesDown for that DC
                for (int i : shuffleEndpointOrder(rf))
                {
                    processDCCallback(UNIQUE_EP.get(i), i, dc1NodesDown, callback);
                    // idx is offset from 0 for node idx vs. nodesdown comparison
                    processDCCallback(UNIQUE_EP.get(i + 5), i, dc2NodesDown, callback);
                }
                callback.get();
            }
            catch (Exception e)
            {
                validateExceptionState(WriteTimeoutException.class, e);
                return;
            }
            validateNoException(WriteTimeoutException.class);
        });
    }

    @Test
    public void testPaxosPrepare()
    {
        runTestMatrix(CallbackType.PAXOS, () -> {
            PrepareCallback callback = new PrepareCallback(dk, cfm, paxosReplicaPlan, requestTime);
            validateQuorum(callback.blockFor());
            try
            {
                for (int i : shuffleEndpointOrder(rf))
                {
                    InetAddressAndPort ep = UNIQUE_EP.get(i);
                    if (i >= dc1NodesDown)
                    {
                        Ballot b = Ballot.none();
                        Commit c = new Commit(b, PartitionUpdate.emptyUpdate(cfm, dk));
                        PrepareResponse response = new PrepareResponse(true, c, c);
                        callback.onResponse(Message.synthetic(ep, Verb.PAXOS_PREPARE_RSP, response));
                    }
                    else
                        callback.onFailure(ep, RequestFailureReason.TIMEOUT);
                }
                callback.await();
            }
            catch (Exception e)
            {
                validateExceptionState(WriteTimeoutException.class, e);
                return;
            }
            validateNoException(WriteTimeoutException.class);
            Assert.assertTrue(callback.isPromised());
        });
    }

    /**
     * Regardless of any failures that come in _after_ we've hit success, we should remain flipped to a "promised" state
     */
    @Test
    public void testPaxosPreparePromiseLateRejection()
    {
        runTestMatrix(CallbackType.PAXOS, () -> {
            PrepareCallback callback = new PrepareCallback(dk, cfm, paxosReplicaPlan, requestTime);
            Assert.assertEquals(String.format("Got unexpected blockFor / requiredResponses from cl %s", cl), ReplicaPlans.calculateQuorumMajority(rf), callback.blockFor());
            try
            {
                int responses = 0;
                for (int i : shuffleEndpointOrder(rf))
                {
                    // Fail out after we've hit enough results we should have success
                    if (responses < quorum)
                        callback.onResponse(Message.synthetic(UNIQUE_EP.get(i), Verb.PAXOS_PREPARE_RSP, buildPaxosPrepareResponse(true)));
                    else
                        callback.onResponse(Message.synthetic(UNIQUE_EP.get(i), Verb.PAXOS_PREPARE_RSP, buildPaxosPrepareResponse(false)));
                    ++responses;

                    // Confirm that success is retained after failure receipt
                    Assert.assertTrue("Expected callback to remain promised.", callback.isPromised());

                    // For each response, confirm the state in the callback matches what is expected
                    if (responses < quorum)
                        Assert.assertFalse("Expected callback to be in successful state.", callback.responseTracker().isSuccessful());
                    else
                    {
                        Assert.assertTrue("Expected callback to remain in successful state.", callback.responseTracker().isSuccessful());
                        Assert.assertTrue("Expected callback to remain promised", callback.isPromised());
                    }
                }
                callback.await();
            }
            catch (Exception e)
            {
                Assert.fail(String.format("Saw unexpected exception: %s", e.getMessage()));
            }
        });
    }

    /**
     * Confirm that a promise rejection prior to hitting quorum short-circuits all processing
     */
    @Test
    public void testPaxosPreparePromiseEarlyRejection()
    {
        runTestMatrix(CallbackType.PAXOS, () -> {
            PrepareCallback callback = new PrepareCallback(dk, cfm, paxosReplicaPlan, requestTime);
            Assert.assertEquals(String.format("Got unexpected blockFor / requiredResponses from cl %s", cl), ReplicaPlans.calculateQuorumMajority(rf), callback.blockFor());
            try
            {
                int responses = 0;
                boolean failed = false;
                // Fail out on something <= quorum to make sure promise failure triggers callback failure
                int toFailIndex = RandomHelpers.nextInt(ReplicaPlans.calculateQuorumMajority(rf));
                for (int i : shuffleEndpointOrder(rf))
                {
                    // Fail out on anything > required for quorum or on the randomized failure index
                    if (responses == toFailIndex || responses > quorum)
                    {
                        callback.onResponse(Message.synthetic(UNIQUE_EP.get(i), Verb.PAXOS_PREPARE_RSP, buildPaxosPrepareResponse(false)));
                        failed = true;
                    }
                    else
                    {
                        callback.onResponse(Message.synthetic(UNIQUE_EP.get(i), Verb.PAXOS_PREPARE_RSP, buildPaxosPrepareResponse(true)));
                    }
                    responses++;

                    if (failed)
                        Assert.assertFalse("Expected callback promise to be false after failure index.", callback.isPromised());
                    else
                        Assert.assertTrue("Expected callback to remain promised.", callback.isPromised());
                }

                // Don't expect WTE since latch will count to 0, and we'll fail fast; want callback to gracefully conclude as soon
                // as it's hit the promise rejection scenario
                callback.await();
            }
            catch (Exception e)
            {
                Assert.fail(String.format("Saw unexpected exception: %s", e.getMessage()));
            }
        });
    }

    @Test
    public void testPaxosPropose()
    {
        runTestMatrix(CallbackType.PAXOS, () -> {
            ProposeCallback callback = new ProposeCallback(paxosReplicaPlan, paxosReplicaPlan.requiredParticipants(), false, requestTime);
            validateQuorum(callback.blockFor());
            try
            {
                for (int i : shuffleEndpointOrder(rf))
                {
                    InetAddressAndPort ep = UNIQUE_EP.get(i);
                    if (i >= dc1NodesDown)
                        callback.onResponse(Message.synthetic(ep, Verb.PAXOS_PROPOSE_RSP, true));
                    else
                        callback.onFailure(ep, RequestFailureReason.TIMEOUT);
                }
                // For proposes, the callback immediately returns when executed
                callback.await();
            }
            catch (Exception e)
            {
                validateExceptionState(WriteTimeoutException.class, e);
                return;
            }
            validateNoException(WriteTimeoutException.class);
        });
    }

    private PrepareResponse buildPaxosPrepareResponse(boolean promised)
    {
        Ballot b = Ballot.none();
        Commit c = new Commit(b, PartitionUpdate.emptyUpdate(cfm, dk));
        return new PrepareResponse(promised, c, c);
    }

    /**
     * @param idx Idx relative to 0 being start of this DC, not for entire combined DC set
     * @param dcNodesDown Count of nodes down in this DC
     */
    private void processDCCallback(InetAddressAndPort peer, int idx, int dcNodesDown, RequestCallback callback)
    {
        String dc = DatabaseDescriptor.getLocator().location(peer).datacenter;
        debugLog("peer: {}, idx: {}, dc: {}, dcNodesDown: {}", peer, idx, dc, dcNodesDown);
        if (idx >= dcNodesDown)
        {
            debugLog("- peer SUCCESS CALLBACK.");
            ReadResponseTest.StubRepairedDataInfo rdi = new ReadResponseTest.StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
            ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(cfm), rdi);

            callback.onResponse(Message.synthetic(peer, Verb.READ_RSP, response));
        }
        else
        {
            debugLog("- peer FAILURE CALLBACK.");
            callback.onFailure(peer, RequestFailureReason.TIMEOUT);
        }
    }



    private void validateQuorum(int callbackQuorum)
    {
        Assert.assertSame(String.format("Got unexpected blockFor / requiredResponses from [cl: %s] [rf: %d]", cl, rf), quorum, callbackQuorum);
    }

    private List<Integer> shuffleEndpointOrder(int endpoints)
    {
        List<Integer> result = IntStream.rangeClosed(0, endpoints - 1)
                                        .boxed()
                                        .collect(Collectors.toList());
        Collections.shuffle(result);
        return result;
    }

    private void validateNoException(Class expected) { validateExceptionState(expected, null); }

    private void validateExceptionState(Class expected, Exception seen)
    {
        debugLog("seen exception: {}", seen);

        if (seen != null)
            debugStack(seen);

        if (caseExpectsException && seen == null)
        {
            Assert.fail(String.format("Expected an exception [%s] but didn't see one.", expected));
        }
        else if (caseExpectsException)
        {
            Assert.assertNotNull(String.format("Did not see any exception state. Expected: %s", expected), seen);
            debugLog("Comparing expected: {}, to seen type: {}", expected, seen.getClass());
            Assert.assertSame("Unexpected exception type found.", expected, seen.getClass());
        }
        else
        {
            // In the error case, always print out the stack we didn't expect to hit
            if (seen != null)
            {
                DEBUG = true;
                debugStack(seen);
                DEBUG = false;
                Assert.fail("Expected successful callback resolution but saw exception.");
            }
        }
    }

    private static void debugLog(String msg, Object... args)
    {
        if (DEBUG)
            logger.error(msg, args);
    }

    private void debugStack(Exception e)
    {
        if (DEBUG)
        {
            Assert.assertNotNull(e);
            logger.error("Exception details. Message: {}", e.getMessage());
            for (StackTraceElement ste : e.getStackTrace())
                logger.error("{}", ste);
        }
    }
}