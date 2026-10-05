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

package org.apache.cassandra.sensors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.Predicates;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.CounterMutationCallback;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.MultiRangeReadCommand;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RepairedDataInfo;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.ResponseVerbHandler;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.BatchlogResponseHandler;
import org.apache.cassandra.service.QueryInfoTracker;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosCommit;
import org.apache.cassandra.service.paxos.PaxosPrepare;
import org.apache.cassandra.service.paxos.PaxosPropose;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.service.paxos.v1.PrepareCallback;
import org.apache.cassandra.service.paxos.v1.ProposeCallback;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.range.EndpointGroupingCoordinator;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.mockito.Mockito;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.apache.cassandra.service.paxos.Ballot.Flag.NONE;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.nextBallot;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests to verify that sensors reported from replicas in {@link Message.Header#customParams()} are tracked correctly
 * in the {@link RequestSensors} of the request.
 */
@RunWith(BMUnitRunner.class)
public class ReplicaSensorsTrackingTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static ColumnFamilyStore counterCfs;
    static EndpointsForToken targets;
    static EndpointsForToken pending;
    static Token dummy;

    /**
     * Used by byteman to signal that sensor tracking is done for one of the replica responses. This enables
     * unit tests to start asserting that replica sensors are actually tracked at this point
     */
    static CountDownLatch[] onResponseAboutToStartSignal;
    /**
     * Signalled by units tests once after sensor tracking assertions are done to make sure response is not returned
     * before assertions are completed
     */
    static CountDownLatch[] onResponseStartSignal;
    /**
     * The two latches above use ExecutionTimeSensorAccumulator#onResponse(), which is invoked first thing on the callback
     * own onResponse() and is the last sensor to be populated.
     */

    static AtomicInteger responses = new AtomicInteger(0);

    @BeforeClass
    public static void beforeClass() throws Exception
    {

        CassandraRelevantProperties.SENSORS_FACTORY.setString(ActiveSensorsFactory.class.getName());
        CassandraRelevantProperties.SENSORS_VIA_NATIVE_PROTOCOL.setBoolean(true);

        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace("Foo", KeyspaceParams.simple(3),
                                    SchemaLoader.standardCFMD("Foo", "Bar"),
                                    SchemaLoader.counterCFMD("Foo", "Counter"));
        ks = Keyspace.open("Foo");
        cfs = ks.getColumnFamilyStore("Bar");
        counterCfs = ks.getColumnFamilyStore("Counter");
        dummy = Murmur3Partitioner.instance.getMinimumToken();
        targets = EndpointsForToken.of(dummy,
                                       full(InetAddressAndPort.getByName("127.0.0.255")),
                                       full(InetAddressAndPort.getByName("127.0.0.254")),
                                       full(InetAddressAndPort.getByName("127.0.0.253"))
        );
        pending = EndpointsForToken.empty(DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(0)));
        cfs.sampleReadLatencyMicros = 0;
    }

    @Before
    public void before()
    {
        onResponseAboutToStartSignal = new CountDownLatch[targets.size()];
        onResponseStartSignal = new CountDownLatch[targets.size()];
        for (int i = 0; i < targets.size(); i++)
        {
            onResponseAboutToStartSignal[i] = new CountDownLatch(1);
            onResponseStartSignal[i] = new CountDownLatch(1);
        }
        responses.set(0);
    }

    @After
    public void after()
    {
        // just in case the test failed and the latches were not counted down
        for (int i = 0; i < targets.size(); i++)
        {
            onResponseAboutToStartSignal[i].countDown();
            onResponseStartSignal[i].countDown();
        }
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForReadCallback() throws InterruptedException
    {
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("4"));
        ReadCommand command = Util.cmd(cfs, key).build();
        Message<ReadCommand> readRequest = Message.builder(Verb.READ_REQ, command).build();

        // init request sensors, must happen before the callback is created.
        // WRITE_EXECUTION_TIME and WRITE_BYTES are registered because StorageProxy.read() registers them
        // unconditionally (they are non-zero only for SERIAL reads); they must stay zero for a regular read
        // since ReadCommandVerbHandler only emits READ_BYTES and READ_EXECUTION_TIME.
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(command);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        Sensor actualReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        Sensor actualExecutionTimeSensor = requestSensors.getSensor(context, Type.READ_EXECUTION_TIME).get();
        Sensor actualWriteExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        Sensor actualWriteBytesSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        RequestTracker.instance.set(requestSensors);

        // init callback
        ReplicaPlan.SharedForTokenRead plan = readPlan(ConsistencyLevel.ONE, targets);
        final Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
        final DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = new DigestResolver<>(command, plan, requestTime, QueryInfoTracker.ReadTracker.NOOP);
        final ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback = new ReadCallback<>(resolver, command, plan, requestTime);

        // READ_BYTES is accumulated from replica responses by ResponseVerbHandler (additive).
        Sensor mockingReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingReadSensor.increment(11.0);
        // READ_EXECUTION_TIME is accumulated from replica responses by ResponseVerbHandler via
        // ExecutionTimeSensorAccumulator: the running max is written once when blockFor responses arrive.
        Sensor mockingExecutionTimeSensor = new mockingSensor(context, Type.READ_EXECUTION_TIME);
        mockingExecutionTimeSensor.increment(1_000_000L);

        assertReplicaSensors(readRequest, callback,
                             List.of(Pair.create(actualReadSensor, mockingReadSensor)),
                             List.of(Pair.create(actualExecutionTimeSensor, mockingExecutionTimeSensor)));

        // ReadCommandVerbHandler emits READ_BYTES and READ_EXECUTION_TIME only; write sensors must remain zero.
        assertThat(actualWriteExecutionTimeSensor.getValue())
                .as("WRITE_EXECUTION_TIME must remain zero for regular read (verb handler emits READ_EXECUTION_TIME only)")
                .isZero();
        assertThat(actualWriteBytesSensor.getValue())
                .as("WRITE_BYTES must remain zero for regular read (verb handler emits READ_BYTES only)")
                .isZero();
    }
    
    @Test
    public void testSensorsTrackedForSingleEndpointCallback()
    {
        RequestSensors coordinatorSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        coordinatorSensors.registerSensor(context, Type.READ_BYTES);
        coordinatorSensors.registerSensor(context, Type.INTERNODE_BYTES);
        coordinatorSensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        RequestTracker.instance.set(coordinatorSensors);
        Sensor readBytesSensor = coordinatorSensors.getSensor(context, Type.READ_BYTES).get();
        Sensor execTimeSensor = coordinatorSensors.getSensor(context, Type.READ_EXECUTION_TIME).get();
        Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();

        // Two replicas:
        Replica replica1 = targets.get(0);
        Replica replica2 = targets.get(1);

        // Two vnode ranges, both replicated to both endpoints (RF=2, QUORUM → blockFor=2):
        // handler1 and handler2 represent the two vnode ranges. In production each would cover a
        // distinct sub-range, but for sensor-tracking purposes the range bounds don't matter.
        PartitionRangeReadCommand command = (PartitionRangeReadCommand) Util.cmd(cfs).build();
        AbstractBounds<PartitionPosition> range = command.dataRange().keyRange();
        EndpointsForRange rangeReplicas = EndpointsForRange.of(replica1, replica2);
        ReplicaPlan.SharedForRangeRead sharedPlan1 = rangePlan(ConsistencyLevel.QUORUM, range, rangeReplicas);
        DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver1 =
        new DataResolver<>(command, sharedPlan1, NoopReadRepair.instance, requestTime, QueryInfoTracker.ReadTracker.NOOP);
        ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler1 =
        new ReadCallback<>(resolver1, command, sharedPlan1, requestTime);
        ReplicaPlan.SharedForRangeRead sharedPlan2 = rangePlan(ConsistencyLevel.QUORUM, range, rangeReplicas);
        DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver2 =
        new DataResolver<>(command, sharedPlan2, NoopReadRepair.instance, requestTime, QueryInfoTracker.ReadTracker.NOOP);
        ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler2 =
        new ReadCallback<>(resolver2, command, sharedPlan2, requestTime);

        // Each context covers both vnode ranges — both handlers are added to each context.
        DataLimits.Counter counter1 = DataLimits.NONE.newCounter(command.nowInSec(), true, command.selectsFullPartition(), true);
        EndpointGroupingCoordinator.EndpointQueryContext ctx1 =
        new EndpointGroupingCoordinator.EndpointQueryContext(replica1.endpoint(), counter1, requestTime);
        ctx1.add(handler1);
        ctx1.add(handler2);

        DataLimits.Counter counter2 = DataLimits.NONE.newCounter(command.nowInSec(), true, command.selectsFullPartition(), true);
        EndpointGroupingCoordinator.EndpointQueryContext ctx2 =
        new EndpointGroupingCoordinator.EndpointQueryContext(replica2.endpoint(), counter2, requestTime);
        ctx2.add(handler1);
        ctx2.add(handler2);

        // Wire the shared accumulator after both contexts are known — threshold = distinct endpoint count = 2.
        // (Mirrors the EndpointGroupingCoordinator constructor which does the same after the range-building loop.)
        ExecutionTimeSensorAccumulator sharedAccumulator = new ExecutionTimeSensorAccumulator(2);
        ctx1.setExecTimeAccumulator(sharedAccumulator);
        ctx2.setExecTimeAccumulator(sharedAccumulator);

        // Capture both MULTI_RANGE_REQ ids (in send order) and drop the actual network sends.
        long[] capturedId1 = new long[1];
        long[] capturedId2 = new long[1];
        AtomicInteger sendCount = new AtomicInteger(0);
        MessagingService.instance().outboundSink.add((msg, to) -> {
            if (msg.verb() == Verb.MULTI_RANGE_REQ)
            {
                if (sendCount.getAndIncrement() == 0)
                    capturedId1[0] = msg.id();
                else
                    capturedId2[0] = msg.id();
            }
            return msg.verb() != Verb.MULTI_RANGE_REQ;
        });
        try
        {
            ctx1.queryReplica();
            ctx2.queryReplica();
        }
        finally
        {
            MessagingService.instance().outboundSink.clear();
        }

        // w1: READ_BYTES=42, READ_EXECUTION_TIME=100ms
        Sensor mockReadBytes1 = new mockingSensor(context, Type.READ_BYTES);
        mockReadBytes1.increment(42.0);
        Sensor mockExecTime1 = new mockingSensor(context, Type.READ_EXECUTION_TIME);
        mockExecTime1.increment(100_000_000L);

        // w2: READ_BYTES=17, READ_EXECUTION_TIME=200ms (the slower replica — defines the expected max)
        Sensor mockReadBytes2 = new mockingSensor(context, Type.READ_BYTES);
        mockReadBytes2.increment(17.0);
        Sensor mockExecTime2 = new mockingSensor(context, Type.READ_EXECUTION_TIME);
        mockExecTime2.increment(200_000_000L);

        Message<ReadCommand> fakeReq1 = Message.builder(Verb.MULTI_RANGE_REQ, (ReadCommand) ctx1.multiRangeCommand())
                                               .withId(capturedId1[0]).build();
        Message<ReadCommand> fakeReq2 = Message.builder(Verb.MULTI_RANGE_REQ, (ReadCommand) ctx2.multiRangeCommand())
                                               .withId(capturedId2[0]).build();

        ResponseVerbHandler.instance.doVerb(createMultiRangeReadResponseMessage(fakeReq1, replica1.endpoint(),
                                                                                mockReadBytes1, mockExecTime1));
        ResponseVerbHandler.instance.doVerb(createMultiRangeReadResponseMessage(fakeReq2, replica2.endpoint(),
                                                                                mockReadBytes2, mockExecTime2));

        // READ_BYTES: additive across both endpoint responses.
        assertThat(readBytesSensor.getValue())
                .as("READ_BYTES must be the sum of both replicas' reported values")
                .isEqualTo(mockReadBytes1.getValue() + mockReadBytes2.getValue());

        // READ_EXECUTION_TIME: the shared accumulator flushes max(T_w1, T_w2) = 200ms exactly once.
        assertThat(execTimeSensor.getValue())
                .as("READ_EXECUTION_TIME must be max(T_w1, T_w2) = 200ms, not inflated by the number of vnode ranges")
                .isEqualTo(mockExecTime2.getValue());
    }

    /**
     * Verifies that the wrapper returned by {@link EndpointGroupingCoordinator#execute()} flushes
     * the execution-time sensor on {@code close()} even when the outer iterator is closed before
     * all endpoint responses have arrived — the "early-close / LIMIT-hit" path.
     *
     * <p>Setup: two vnode ranges replicated to two endpoints. {@code endpointContexts.size() == 2},
     * so the shared {@link ExecutionTimeSensorAccumulator} has {@code threshold = 2}. Only replica1
     * responds, bringing the counter to 1. The outer {@link PartitionIterator} is then closed
     * immediately (no {@code hasNext()} / no data consumed). The wrapper's {@code close()} must
     * detect that {@code responseCount < threshold} and synthesize the missing {@code onResponse()}
     * call, causing the accumulated max to be written to the coordinator sensors exactly once.</p>
     */
    @Test
    public void testSensorsTrackedForSingleEndpointCallbackOnEarlyClose()
    {
        RequestSensors coordinatorSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        coordinatorSensors.registerSensor(context, Type.READ_BYTES);
        coordinatorSensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        RequestTracker.instance.set(coordinatorSensors);
        Sensor readBytesSensor = coordinatorSensors.getSensor(context, Type.READ_BYTES).get();
        Sensor execTimeSensor  = coordinatorSensors.getSensor(context, Type.READ_EXECUTION_TIME).get();

        Replica replica1 = targets.get(0);
        Replica replica2 = targets.get(1);

        // Two vnode ranges, both replicated to both replicas (RF=2, QUORUM → blockFor=2).
        // endpointContexts has exactly two entries (one per endpoint) → threshold = 2.
        PartitionRangeReadCommand command = (PartitionRangeReadCommand) Util.cmd(cfs).build();
        AbstractBounds<PartitionPosition> range = command.dataRange().keyRange();
        EndpointsForRange rangeReplicas = EndpointsForRange.of(replica1, replica2);

        ReplicaPlan.ForRangeRead plan1 = new ReplicaPlan.ForRangeRead(ks, ks.getReplicationStrategy(),
                                                                       ConsistencyLevel.QUORUM, range,
                                                                       rangeReplicas, rangeReplicas, 1);
        ReplicaPlan.ForRangeRead plan2 = new ReplicaPlan.ForRangeRead(ks, ks.getReplicationStrategy(),
                                                                       ConsistencyLevel.QUORUM, range,
                                                                       rangeReplicas, rangeReplicas, 1);
        Iterator<ReplicaPlan.ForRangeRead> replicaPlans = Arrays.asList(plan1, plan2).iterator();

        DataLimits.Counter outerCounter = DataLimits.NONE.newCounter(command.nowInSec(), true,
                                                                      command.selectsFullPartition(), true);

        // Capture MULTI_RANGE_REQ message ids and destinations, then drop the sends so no
        // real network I/O happens.  Construction does not send anything; execute() does.
        long[] capturedIds   = new long[2];
        InetAddressAndPort[] capturedDests = new InetAddressAndPort[2];
        AtomicInteger sendIdx = new AtomicInteger(0);

        EndpointGroupingCoordinator coordinator =
                new EndpointGroupingCoordinator(command, outerCounter, replicaPlans,
                                                2 /* concurrencyFactor */,
                                                Dispatcher.RequestTime.forImmediateExecution(),
                                                QueryInfoTracker.ReadTracker.NOOP);

        MessagingService.instance().outboundSink.add((msg, to) -> {
            if (msg.verb() == Verb.MULTI_RANGE_REQ)
            {
                int idx = sendIdx.getAndIncrement();
                capturedIds[idx]   = msg.id();
                capturedDests[idx] = to;
            }
            return msg.verb() != Verb.MULTI_RANGE_REQ;  // drop: do not actually send
        });

        PartitionIterator result;
        try
        {
            // execute() calls queryReplica() on each EndpointQueryContext, firing the sends
            // captured above, and returns the wrapped PartitionIterator whose close() flushes
            // the accumulator regardless of whether any data was consumed.
            result = coordinator.execute();
        }
        finally
        {
            MessagingService.instance().outboundSink.clear();
        }

        // Identify which captured slot belongs to replica1, then retrieve the MultiRangeReadCommand
        // the coordinator built for that endpoint context.
        int idxForReplica1 = capturedDests[0].equals(replica1.endpoint()) ? 0 : 1;
        MultiRangeReadCommand multiRangeCmd1 = coordinator.endpointRanges()
                                                          .stream()
                                                          .filter(c -> c.endpoint().equals(replica1.endpoint()))
                                                          .findFirst().get().multiRangeCommand();

        // Only replica1 responds (replica2 is "slow" / never replies before the LIMIT is hit).
        // accumulator count becomes 1, threshold is 2 → sensor must NOT be flushed yet.
        Sensor mockReadBytes1 = new mockingSensor(context, Type.READ_BYTES);
        mockReadBytes1.increment(75.0);
        Sensor mockExecTime1  = new mockingSensor(context, Type.READ_EXECUTION_TIME);
        mockExecTime1.increment(120_000_000L);

        Message<ReadCommand> fakeReq1 = Message.builder(Verb.MULTI_RANGE_REQ, (ReadCommand) multiRangeCmd1)
                                               .withId(capturedIds[idxForReplica1])
                                               .build();
        ResponseVerbHandler.instance.doVerb(
                createMultiRangeReadResponseMessage(fakeReq1, replica1.endpoint(),
                                                    mockReadBytes1, mockExecTime1));

        assertThat(execTimeSensor.getValue())
                .as("READ_EXECUTION_TIME must NOT be flushed before all endpoints have responded")
                .isZero();

        // Simulate LIMIT-hit: close the outer iterator immediately without calling hasNext().
        // The execute() wrapper's close() must drive the accumulator from count=1 to threshold=2,
        // flushing the accumulated max (= 120ms) exactly once.
        result.close();

        assertThat(readBytesSensor.getValue())
                .as("READ_BYTES must equal replica1's reported value (additive)")
                .isEqualTo(mockReadBytes1.getValue());

        assertThat(execTimeSensor.getValue())
                .as("READ_EXECUTION_TIME must be flushed by execute() wrapper close()")
                .isEqualTo(mockExecTime1.getValue());
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForWriteCallback() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> writeRequest = Message.builder(Verb.MUTATION_REQ, mutation).build();
        assertSensorsTrackedForWriteRequest(writeRequest);
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForWriteCallback_CounterMutation() throws InterruptedException
    {
        // Build a counter mutation request against a real counter table so isCounter() == true.
        Mutation mutation = new RowUpdateBuilder(counterCfs.metadata(), 0, "0").build();
        CounterMutation counterMutation = new CounterMutation(mutation, ConsistencyLevel.ALL);
        Message<CounterMutation> writeRequest = Message.builder(Verb.COUNTER_MUTATION_REQ, counterMutation).build();

        // Set up the leader's RequestSensors.
        RequestSensors leaderSensors = new ActiveRequestSensors();
        Context context = Context.from(counterCfs.metadata());
        leaderSensors.registerSensor(context, Type.WRITE_BYTES);
        leaderSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        Sensor actualWriteSensor = leaderSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualExecutionTimeSensor = leaderSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        RequestTracker.instance.set(leaderSensors);

        // Wire a real CounterMutationCallback as the response handler callback.
        CounterMutationCallback counterCallback = new CounterMutationCallback(writeRequest, writeRequest.from(), leaderSensors);
        AbstractWriteResponseHandler<?> responseHandler = createWriteResponseHandler(ConsistencyLevel.ALL, ConsistencyLevel.ALL,
                                                                                     Dispatcher.RequestTime.forImmediateExecution(), counterCallback);
        // WRITE_BYTES is accumulated from sub-replica responses by ResponseVerbHandler on the leader (additive).
        Sensor mockingWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingWriteSensor.increment(13.0);
        // WRITE_EXECUTION_TIME sub-replica max is accumulated by ResponseVerbHandler via ExecutionTimeSensorAccumulator.
        Sensor mockingExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingExecutionTimeSensor.increment(1_000_000L);

        // Simulate the leader apply time added by counterWriteTask before the sub-replica fan-out.
        double leaderApplyTime = 500_000L;
        leaderSensors.incrementSensor(context, Type.WRITE_EXECUTION_TIME, leaderApplyTime);

        assertReplicaSensors(writeRequest, responseHandler,
                             List.of(Pair.create(actualWriteSensor, mockingWriteSensor)),
                             List.of(Pair.create(actualExecutionTimeSensor, mockingExecutionTimeSensor)));
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForWriteCallback_LoggedBatch() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> writeRequest = Message.builder(Verb.MUTATION_REQ, mutation).build();

        // init request sensors, must happen before the callback is created
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualIndexWriteSensor = requestSensors.getSensor(context, Type.INDEX_WRITE_BYTES).get();
        Sensor actualExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        RequestTracker.instance.set(requestSensors);

        // BatchlogResponseHandler wraps the real WriteResponseHandler; sensors accumulate on the
        // BatchlogResponseHandler instance (its inherited execTimeAccumulator) because that is the
        // object passed to sendToHintedReplicas and registered with MessagingService.
        @SuppressWarnings("unchecked")
        AbstractWriteResponseHandler<IMutation> writeHandler = (AbstractWriteResponseHandler<IMutation>) createWriteResponseHandler(ConsistencyLevel.ALL, ConsistencyLevel.ALL);
        BatchlogResponseHandler.BatchlogCleanup cleanup = new BatchlogResponseHandler.BatchlogCleanup(1, () -> {});
        BatchlogResponseHandler<IMutation> batchHandler = new BatchlogResponseHandler<>(writeHandler, targets.size(), cleanup, Dispatcher.RequestTime.forImmediateExecution());

        // WRITE_BYTES and INDEX_WRITE_BYTES are accumulated from replica responses by ResponseVerbHandler (additive).
        Sensor mockingWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingWriteSensor.increment(13.0);
        Sensor mockingIndexWriteSensor = new mockingSensor(context, Type.INDEX_WRITE_BYTES);
        mockingIndexWriteSensor.increment(7.0);
        // WRITE_EXECUTION_TIME is accumulated via ExecutionTimeSensorAccumulator on the BatchlogResponseHandler:
        // the running max is written once blockFor responses arrive.
        Sensor mockingExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingExecutionTimeSensor.increment(1_000_000L);

        assertReplicaSensors(writeRequest, batchHandler,
                             List.of(Pair.create(actualWriteSensor, mockingWriteSensor),
                                     Pair.create(actualIndexWriteSensor, mockingIndexWriteSensor)),
                             List.of(Pair.create(actualExecutionTimeSensor, mockingExecutionTimeSensor)));
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForWriteCallback_PaxosCommit() throws InterruptedException
    {
        Commit commit = Commit.emptyCommit(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("0")), cfs.metadata());
        Message<Commit> writeRequest = Message.builder(Verb.PAXOS_COMMIT_REQ, commit).build();
        assertSensorsTrackedForWriteRequest(writeRequest);
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForPaxosPrepareCallback() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> prepare = Message.builder(Verb.PAXOS_PREPARE_REQ, mutation).build();

        // init request sensors, must happen before the callback is created.
        // INDEX_WRITE_BYTES is intentionally not registered: prepare only writes to system.paxos, which has no indexes.
        // READ_EXECUTION_TIME is registered because for SERIAL reads the coordinator registers it so that the
        // data-fetch time flows through; it must remain zero here since PrepareVerbHandler emits WRITE_EXECUTION_TIME only.
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        requestSensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        Sensor actualWriteExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        Sensor actualReadExecutionTimeSensor = requestSensors.getSensor(context, Type.READ_EXECUTION_TIME).get();
        RequestTracker.instance.set(requestSensors);

        // init prepare callback
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("0"));
        PrepareCallback callback = new PrepareCallback(key, cfs.metadata(), targets.size(), ConsistencyLevel.ALL, Dispatcher.RequestTime.forImmediateExecution());

        // WRITE_BYTES and READ_BYTES are accumulated from replica responses by ResponseVerbHandler (additive).
        Sensor mockingPrepareWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingPrepareWriteSensor.increment(13.0);
        Sensor mockingPrepareReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingPrepareReadSensor.increment(14.0);
        // WRITE_EXECUTION_TIME is accumulated via ExecutionTimeSensorAccumulator: the running max is written
        // once all targets have responded (paxos awaits all replicas before proceeding to the next phase).
        Sensor mockingPrepareWriteExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingPrepareWriteExecutionTimeSensor.increment(1_000_000L);

        assertReplicaSensors(prepare, callback,
                             List.of(Pair.create(actualWriteSensor, mockingPrepareWriteSensor),
                                     Pair.create(actualReadSensor, mockingPrepareReadSensor)),
                             List.of(Pair.create(actualWriteExecutionTimeSensor, mockingPrepareWriteExecutionTimeSensor)));

        // PrepareVerbHandler emits WRITE_EXECUTION_TIME only; READ_EXECUTION_TIME must remain zero.
        assertThat(actualReadExecutionTimeSensor.getValue())
                .as("READ_EXECUTION_TIME must remain zero for Paxos Prepare (verb handler emits WRITE_EXECUTION_TIME only)")
                .isZero();
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForPaxosProposeCallback() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> propose = Message.builder(Verb.PAXOS_PROPOSE_REQ, mutation).build();

        // init request sensors, must happen before the callback is created.
        // INDEX_WRITE_BYTES is intentionally not registered: propose only writes to system.paxos, which has no indexes.
        // READ_EXECUTION_TIME is registered because for SERIAL reads the coordinator registers it so that the
        // data-fetch time flows through; it must remain zero here since ProposeVerbHandler emits WRITE_EXECUTION_TIME only.
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        requestSensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        Sensor actualWriteExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        Sensor actualReadExecutionTimeSensor = requestSensors.getSensor(context, Type.READ_EXECUTION_TIME).get();
        RequestTracker.instance.set(requestSensors);

        // init propose callback
        ProposeCallback callback = new ProposeCallback(cfs.metadata(), targets.size(), targets.size(), false, ConsistencyLevel.ALL, Dispatcher.RequestTime.forImmediateExecution());

        // WRITE_BYTES and READ_BYTES are accumulated from replica responses by ResponseVerbHandler (additive).
        Sensor mockingProposeWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingProposeWriteSensor.increment(15.0);
        Sensor mockingProposeReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingProposeReadSensor.increment(16.0);
        // WRITE_EXECUTION_TIME is accumulated via ExecutionTimeSensorAccumulator: the running max is written
        // once all targets have responded (paxos awaits all replicas before proceeding to the next phase).
        Sensor mockingProposeWriteExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingProposeWriteExecutionTimeSensor.increment(1_000_000L);

        assertReplicaSensors(propose, callback,
                             List.of(Pair.create(actualWriteSensor, mockingProposeWriteSensor),
                                     Pair.create(actualReadSensor, mockingProposeReadSensor)),
                             List.of(Pair.create(actualWriteExecutionTimeSensor, mockingProposeWriteExecutionTimeSensor)));

        // ProposeVerbHandler emits WRITE_EXECUTION_TIME only; READ_EXECUTION_TIME must remain zero.
        assertThat(actualReadExecutionTimeSensor.getValue())
                .as("READ_EXECUTION_TIME must remain zero for Paxos Propose (verb handler emits WRITE_EXECUTION_TIME only)")
                .isZero();
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForPaxosV2PrepareCallback() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> prepare = Message.builder(Verb.PAXOS2_PREPARE_REQ, mutation).build();

        // init request sensors, must happen before the callback is created.
        // READ_BYTES and WRITE_BYTES are both tracked: v2 Prepare reads and writes system.paxos.
        // Both READ_EXECUTION_TIME and WRITE_EXECUTION_TIME are tracked: system.paxos read time
        // and the promise write time are reported separately by PaxosPrepare.RequestHandler.
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.registerSensor(context, Type.INTERNODE_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        requestSensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        Sensor actualInterNodeSensor = requestSensors.getSensor(context, Type.INTERNODE_BYTES).get();
        Sensor actualWriteExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        Sensor actualReadExecutionTimeSensor = requestSensors.getSensor(context, Type.READ_EXECUTION_TIME).get();
        RequestTracker.instance.set(requestSensors);

        // PaxosPrepare has no accessible constructor outside its package, so we use a mock.
        // We attach a real ExecutionTimeSensorAccumulator so that the Byteman hook on
        // ExecutionTimeSensorAccumulator#onResponse fires correctly.
        ExecutionTimeSensorAccumulator accumulator = new ExecutionTimeSensorAccumulator(targets.size());
        PaxosPrepare mockCallback = Mockito.mock(PaxosPrepare.class);
        Mockito.when(mockCallback.getRequestSensors()).thenReturn(requestSensors);
        Mockito.when(mockCallback.getTableMetadata()).thenReturn(cfs.metadata());
        Mockito.doAnswer(inv -> {
            accumulator.accumulate(inv.getArgument(0), inv.getArgument(1), (double) inv.getArgument(2));
            return null;
        }).when(mockCallback).accumulateExecutionTimeSensor(Mockito.any(), Mockito.any(), Mockito.anyDouble());
        Mockito.doAnswer(inv -> {
            accumulator.onResponse(requestSensors);
            return null;
        }).when(mockCallback).onResponse(Mockito.any());

        // WRITE_BYTES, READ_BYTES and INTERNODE_BYTES are accumulated additively from each replica response.
        Sensor mockingWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingWriteSensor.increment(13.0);
        Sensor mockingReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingReadSensor.increment(14.0);
        Sensor mockingInterNodeSensor = new mockingSensor(context, Type.INTERNODE_BYTES);
        mockingInterNodeSensor.increment(10.0);
        // WRITE_EXECUTION_TIME and READ_EXECUTION_TIME are accumulated via ExecutionTimeSensorAccumulator:
        // the running max for each is written once all targets have responded.
        Sensor mockingWriteExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingWriteExecutionTimeSensor.increment(1_000_000L);
        Sensor mockingReadExecutionTimeSensor = new mockingSensor(context, Type.READ_EXECUTION_TIME);
        mockingReadExecutionTimeSensor.increment(500_000L);

        assertReplicaSensors(prepare, mockCallback,
                             List.of(Pair.create(actualWriteSensor, mockingWriteSensor),
                                     Pair.create(actualReadSensor, mockingReadSensor),
                                     Pair.create(actualInterNodeSensor, mockingInterNodeSensor)),
                             List.of(Pair.create(actualWriteExecutionTimeSensor, mockingWriteExecutionTimeSensor),
                                     Pair.create(actualReadExecutionTimeSensor, mockingReadExecutionTimeSensor)));
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForPaxosV2ProposeCallback() throws InterruptedException
    {
        Mutation mutation = new RowUpdateBuilder(cfs.metadata(), 0, "0").build();
        Message<Mutation> propose = Message.builder(Verb.PAXOS2_PROPOSE_REQ, mutation).build();

        // init request sensors, must happen before the callback is created.
        // READ_BYTES and WRITE_BYTES are both tracked: v2 Propose reads and writes system.paxos.
        // WRITE_EXECUTION_TIME and READ_EXECUTION_TIME are both tracked by the PaxosPropose branch.
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.registerSensor(context, Type.INTERNODE_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        requestSensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualReadSensor = requestSensors.getSensor(context, Type.READ_BYTES).get();
        Sensor actualInterNodeSensor = requestSensors.getSensor(context, Type.INTERNODE_BYTES).get();
        Sensor actualWriteExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        Sensor actualReadExecutionTimeSensor = requestSensors.getSensor(context, Type.READ_EXECUTION_TIME).get();
        RequestTracker.instance.set(requestSensors);

        // PaxosPropose has no accessible constructor outside its package, so we use a mock.
        // We attach a real ExecutionTimeSensorAccumulator so that the Byteman hook on
        // ExecutionTimeSensorAccumulator#onResponse fires correctly.
        ExecutionTimeSensorAccumulator accumulator = new ExecutionTimeSensorAccumulator(targets.size());
        PaxosPropose mockCallback = Mockito.mock(PaxosPropose.class);
        Mockito.when(mockCallback.getRequestSensors()).thenReturn(requestSensors);
        Mockito.when(mockCallback.getTableMetadata()).thenReturn(cfs.metadata());
        Mockito.doAnswer(inv -> {
            accumulator.accumulate(inv.getArgument(0), inv.getArgument(1), (double) inv.getArgument(2));
            return null;
        }).when(mockCallback).accumulateExecutionTimeSensor(Mockito.any(), Mockito.any(), Mockito.anyDouble());
        Mockito.doAnswer(inv -> {
            accumulator.onResponse(requestSensors);
            return null;
        }).when(mockCallback).onResponse(Mockito.any());

        // WRITE_BYTES, READ_BYTES and INTERNODE_BYTES are accumulated additively from each replica response.
        Sensor mockingWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingWriteSensor.increment(15.0);
        Sensor mockingReadSensor = new mockingSensor(context, Type.READ_BYTES);
        mockingReadSensor.increment(16.0);
        Sensor mockingInterNodeSensor = new mockingSensor(context, Type.INTERNODE_BYTES);
        mockingInterNodeSensor.increment(10.0);
        // WRITE_EXECUTION_TIME and READ_EXECUTION_TIME are accumulated via ExecutionTimeSensorAccumulator:
        // the running max for each is written once all targets have responded.
        Sensor mockingWriteExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingWriteExecutionTimeSensor.increment(1_000_000L);
        Sensor mockingReadExecutionTimeSensor = new mockingSensor(context, Type.READ_EXECUTION_TIME);
        mockingReadExecutionTimeSensor.increment(500_000L);

        assertReplicaSensors(propose, mockCallback,
                             List.of(Pair.create(actualWriteSensor, mockingWriteSensor),
                                     Pair.create(actualReadSensor, mockingReadSensor),
                                     Pair.create(actualInterNodeSensor, mockingInterNodeSensor)),
                             List.of(Pair.create(actualWriteExecutionTimeSensor, mockingWriteExecutionTimeSensor),
                                     Pair.create(actualReadExecutionTimeSensor, mockingReadExecutionTimeSensor)));
    }

    @Test
    @BMRule(name = "signals onResponse about to start latches",
    targetClass = "org.apache.cassandra.sensors.ExecutionTimeSensorAccumulator",
    targetMethod = "onResponse",
    targetLocation = "AT EXIT",
    action = "org.apache.cassandra.sensors.ReplicaSensorsTrackingTest.countDownAndAwaitOnResponseLatches();")
    public void testSensorsTrackedForPaxosV2CommitCallback() throws InterruptedException
    {
        Commit commit = Commit.emptyCommit(cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("0")), cfs.metadata());
        Message<Commit> commitMsg = Message.builder(Verb.PAXOS_COMMIT_REQ, commit).build();

        // init request sensors, must happen before the callback is created.
        // READ_BYTES is intentionally not registered: the commit phase never reads system.paxos.
        // INDEX_WRITE_BYTES is registered: commit writes to the user table which may have secondary indexes.
        // WRITE_EXECUTION_TIME is tracked; READ_EXECUTION_TIME is not (no system.paxos read).
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INTERNODE_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualIndexWriteSensor = requestSensors.getSensor(context, Type.INDEX_WRITE_BYTES).get();
        Sensor actualInterNodeSensor = requestSensors.getSensor(context, Type.INTERNODE_BYTES).get();
        Sensor actualWriteExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        RequestTracker.instance.set(requestSensors);

        // PaxosCommit has no accessible constructor outside its package, so we use a mock.
        // We attach a real ExecutionTimeSensorAccumulator so that the Byteman hook on
        // ExecutionTimeSensorAccumulator#onResponse fires correctly.
        ExecutionTimeSensorAccumulator accumulator = new ExecutionTimeSensorAccumulator(targets.size());
        PaxosCommit mockCallback = Mockito.mock(PaxosCommit.class);
        Mockito.when(mockCallback.getRequestSensors()).thenReturn(requestSensors);
        Mockito.when(mockCallback.getTableMetadata()).thenReturn(cfs.metadata());
        Mockito.doAnswer(inv -> {
            accumulator.accumulate(inv.getArgument(0), inv.getArgument(1), (double) inv.getArgument(2));
            return null;
        }).when(mockCallback).accumulateExecutionTimeSensor(Mockito.any(), Mockito.any(), Mockito.anyDouble());
        Mockito.doAnswer(inv -> {
            accumulator.onResponse(requestSensors);
            return null;
        }).when(mockCallback).onResponse(Mockito.any());

        // WRITE_BYTES, INDEX_WRITE_BYTES and INTERNODE_BYTES are accumulated additively from each replica response.
        Sensor mockingWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingWriteSensor.increment(13.0);
        Sensor mockingIndexWriteSensor = new mockingSensor(context, Type.INDEX_WRITE_BYTES);
        mockingIndexWriteSensor.increment(7.0);
        Sensor mockingInterNodeSensor = new mockingSensor(context, Type.INTERNODE_BYTES);
        mockingInterNodeSensor.increment(10.0);
        // WRITE_EXECUTION_TIME is accumulated via ExecutionTimeSensorAccumulator:
        // the running max is written once all targets have responded.
        Sensor mockingWriteExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingWriteExecutionTimeSensor.increment(1_000_000L);

        assertReplicaSensors(commitMsg, mockCallback,
                             List.of(Pair.create(actualWriteSensor, mockingWriteSensor),
                                     Pair.create(actualIndexWriteSensor, mockingIndexWriteSensor),
                                     Pair.create(actualInterNodeSensor, mockingInterNodeSensor)),
                             List.of(Pair.create(actualWriteExecutionTimeSensor, mockingWriteExecutionTimeSensor)));

        // Commit phase never reads system.paxos; READ_BYTES must not be registered.
        assertThat(requestSensors.getSensor(context, Type.READ_BYTES))
                .as("READ_BYTES must not be registered for Paxos v2 Commit (commit phase does not read system.paxos)")
                .isEmpty();
    }

    /**
     * Used by Byteman to count down the onResponseAboutToStartSignal latch and await the onResponseStartSignal latch
     * for the current replica response.
     */
    public static void countDownAndAwaitOnResponseLatches() throws InterruptedException
    {
        int replica = responses.getAndIncrement();
        onResponseAboutToStartSignal[replica].countDown();
        // don't wait indefinitely if the test is stuck.
        assertThat(onResponseStartSignal[replica].await(5, TimeUnit.SECONDS)).isTrue();
    }

    private void assertSensorsTrackedForWriteRequest(Message writeRequest) throws InterruptedException
    {
        // init request sensors, must happen before the callback is created.
        // READ_EXECUTION_TIME is registered because StorageProxy.cas() registers it unconditionally
        // (non-zero only for the CAS precondition read); it must stay zero for plain writes and Paxos
        // Commit since MutationVerbHandler and CommitVerbHandler only emit WRITE_EXECUTION_TIME.
        // INTERNODE_BYTES is derived from the serialized request payload size (sentPayloadSize / tableCount)
        // for WriteCallbackInfo, not from any sensor value in the replica response.
        RequestSensors requestSensors = new ActiveRequestSensors();
        Context context = Context.from(cfs.metadata());
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_EXECUTION_TIME);
        requestSensors.registerSensor(context, Type.READ_EXECUTION_TIME);
        requestSensors.registerSensor(context, Type.INTERNODE_BYTES);
        Sensor actualWriteSensor = requestSensors.getSensor(context, Type.WRITE_BYTES).get();
        Sensor actualIndexWriteSensor = requestSensors.getSensor(context, Type.INDEX_WRITE_BYTES).get();
        Sensor actualExecutionTimeSensor = requestSensors.getSensor(context, Type.WRITE_EXECUTION_TIME).get();
        Sensor actualReadExecutionTimeSensor = requestSensors.getSensor(context, Type.READ_EXECUTION_TIME).get();
        Sensor actualInterNodeSensor = requestSensors.getSensor(context, Type.INTERNODE_BYTES).get();
        RequestTracker.instance.set(requestSensors);

        // init callback
        AbstractWriteResponseHandler<?> callback = createWriteResponseHandler(ConsistencyLevel.ALL, ConsistencyLevel.ALL);

        // WRITE_BYTES and INDEX_WRITE_BYTES are accumulated from replica responses by ResponseVerbHandler (additive).
        Sensor mockingWriteSensor = new mockingSensor(context, Type.WRITE_BYTES);
        mockingWriteSensor.increment(13.0);
        Sensor mockingIndexWriteSensor = new mockingSensor(context, Type.INDEX_WRITE_BYTES);
        mockingIndexWriteSensor.increment(7.0);
        // WRITE_EXECUTION_TIME is accumulated via ExecutionTimeSensorAccumulator: the running max is written
        // once blockFor responses arrive (replicas within a write phase execute in parallel).
        Sensor mockingExecutionTimeSensor = new mockingSensor(context, Type.WRITE_EXECUTION_TIME);
        mockingExecutionTimeSensor.increment(1_000_000L);

        assertReplicaSensors(writeRequest, callback,
                             List.of(Pair.create(actualWriteSensor, mockingWriteSensor), Pair.create(actualIndexWriteSensor, mockingIndexWriteSensor)),
                             List.of(Pair.create(actualExecutionTimeSensor, mockingExecutionTimeSensor)));

        // MutationVerbHandler and CommitVerbHandler emit WRITE_EXECUTION_TIME only; READ_EXECUTION_TIME must remain zero.
        assertThat(actualReadExecutionTimeSensor.getValue())
                .as("READ_EXECUTION_TIME must remain zero for plain write/Paxos Commit (verb handler emits WRITE_EXECUTION_TIME only)")
                .isZero();

        // INTERNODE_BYTES is derived from the serialized request payload size (sentPayloadSize / tableCount),
        // not from any sensor value in the replica response. With a single-table mutation and targets.size()
        // replicas, the expected total is: targets.size() * (payloadSize / 1 table).
        int payloadSize = writeRequest.payloadSize(MessagingService.current_version);
        assertThat(actualInterNodeSensor.getValue())
                .as("INTERNODE_BYTES must equal targets.size() * serialized request payload size")
                .isEqualTo((double) targets.size() * payloadSize);
    }

    private void assertReplicaSensors(Message<?> request, RequestCallback<?> callback,
                                      List<Pair<Sensor, Sensor>> additiveSensors,
                                      List<Pair<Sensor, Sensor>> maxSensors) throws InterruptedException
    {
        assertReplicaSensors(targets, request, callback, false, additiveSensors, maxSensors);
    }

    private void assertReplicaSensors(EndpointsForToken replicaList, Message<?> request, RequestCallback<?> callback,
                                      boolean callbackAlreadyRegistered,
                                      List<Pair<Sensor, Sensor>> additiveSensors,
                                      List<Pair<Sensor, Sensor>> maxSensors) throws InterruptedException
    {
        for (Pair<Sensor, Sensor> pair : additiveSensors)
        {
            assertThat(pair.left.getValue()).isZero();
            assertThat(pair.right.getValue()).isGreaterThan(0);
        }
        // Snapshot any pre-seeded value (e.g. leader apply time) before replica responses arrive.
        // The accumulator adds max(replica_times) on top, so the expected final value is
        // initialValue + pair.right.getValue().
        Map<Sensor, Double> maxSensorInitialValues = new HashMap<>();
        for (Pair<Sensor, Sensor> pair : maxSensors)
        {
            maxSensorInitialValues.put(pair.left, pair.left.getValue());
            assertThat(pair.right.getValue()).isGreaterThan(0);
        }

        // Build the combined sensor array sent in every replica response message.
        Sensor[] allReplicaSensors = Stream.concat(additiveSensors.stream(), maxSensors.stream())
                                           .map(Pair::right)
                                           .toArray(Sensor[]::new);

        for (int responseIdx = 1; responseIdx <= replicaList.size(); responseIdx++)
        {
            simulateResponseFromReplica(replicaList.get(responseIdx - 1), request, callback, callbackAlreadyRegistered, allReplicaSensors);

            // don't wait indefinitely if the test is stuck. Delay the assertion of the await results to give a better
            // chance of a meaningful error by virtue of the core test assertion
            boolean awaitResult = onResponseAboutToStartSignal[responseIdx - 1].await(5, TimeUnit.SECONDS);

            // additive sensors must grow linearly with each response
            for (Pair<Sensor, Sensor> pair : additiveSensors)
                assertThat(pair.left.getValue()).isEqualTo(pair.right.getValue() * responseIdx);

            assertThat(awaitResult).isTrue();
            onResponseStartSignal[responseIdx - 1].countDown();
        }

        // max sensors are written once when the accumulator threshold is reached;
        // the final value is initialValue + max(replica_times)
        for (Pair<Sensor, Sensor> pair : maxSensors)
            assertThat(pair.left.getValue()).isEqualTo(maxSensorInitialValues.get(pair.left) + pair.right.getValue());

        // reset sensors for subsequent assertions within the same test, if any
        for (Pair<Sensor, Sensor> pair : additiveSensors)
            pair.left.reset();
        for (Pair<Sensor, Sensor> pair : maxSensors)
            pair.left.reset();
    }

    private void simulateResponseFromReplica(Replica replica, Message<?> request, RequestCallback<?> callback, boolean callbackAlreadyRegistered, Sensor... sensor)
    {
        new Thread(() -> {
            if (!callbackAlreadyRegistered)
            {
                // AbstractWriteResponseHandler has a special handling for the callback
                if (callback instanceof AbstractWriteResponseHandler)
                    MessagingService.instance().callbacks.addWithExpiration((AbstractWriteResponseHandler<?>) callback, request, replica);
                else
                    MessagingService.instance().callbacks.addWithExpiration(callback, request, replica.endpoint());
            }
            Message<?> response = createResponseMessageWithSensor(request, replica.endpoint(), sensor);
            ResponseVerbHandler.instance.doVerb(response);
        }).start();
    }

    private ReplicaPlan.SharedForTokenRead readPlan(ConsistencyLevel consistencyLevel, EndpointsForToken replicas)
    {
        return ReplicaPlan.shared(new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), consistencyLevel, replicas, replicas));
    }

    private ReplicaPlan.SharedForRangeRead rangePlan(ConsistencyLevel consistencyLevel, AbstractBounds<PartitionPosition> range, EndpointsForRange replicas)
    {
        return ReplicaPlan.shared(new ReplicaPlan.ForRangeRead(ks, ks.getReplicationStrategy(), consistencyLevel, range, replicas, replicas, 1));
    }

    private Message<?> createResponseMessageWithSensor(Message<?> request, InetAddressAndPort endpoint, Sensor... sensors)
    {
        if (request.verb() == Verb.MULTI_RANGE_REQ)
            return createMultiRangeReadResponseMessage(request, endpoint, sensors);
        else if (request.verb() == Verb.READ_REQ)
            return createReadResponseMessage(request, endpoint, sensors);
        else if (request.verb() == Verb.MUTATION_REQ)
            return createResponseMessage(Verb.MUTATION_RSP, NoPayload.noPayload, endpoint, request.id(), sensors);
        else if (request.verb() == Verb.COUNTER_MUTATION_REQ)
            return createResponseMessage(Verb.COUNTER_MUTATION_RSP, NoPayload.noPayload, endpoint, request.id(), sensors);
        else if (request.verb() == Verb.PAXOS_PREPARE_REQ)
        {
            DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes("4"));
            Ballot ballot = nextBallot(NONE);
            Commit commit = Commit.newPrepare(key, cfs.metadata(), ballot);
            return createResponseMessage(Verb.PAXOS_PREPARE_RSP, new PrepareResponse(false, commit, commit), endpoint, request.id(), sensors);
        }
        else if (request.verb() == Verb.PAXOS_PROPOSE_REQ)
            return createResponseMessage(Verb.PAXOS_PROPOSE_RSP, true, endpoint, request.id(), sensors);
        else if (request.verb() == Verb.PAXOS_COMMIT_REQ)
            return createResponseMessage(Verb.PAXOS_COMMIT_RSP, NoPayload.noPayload, endpoint, request.id(), sensors);
        else if (request.verb() == Verb.PAXOS2_PREPARE_REQ)
            return createResponseMessage(Verb.PAXOS2_PREPARE_RSP, NoPayload.noPayload, endpoint, request.id(), sensors);
        else if (request.verb() == Verb.PAXOS2_PROPOSE_REQ)
            return createResponseMessage(Verb.PAXOS2_PROPOSE_RSP, NoPayload.noPayload, endpoint, request.id(), sensors);
        else
            throw new IllegalArgumentException("Unsupported verb: " + request.verb());
    }

    private Message<ReadResponse> createMultiRangeReadResponseMessage(Message<?> request, InetAddressAndPort endpoint, Sensor... sensors)
    {
        MultiRangeReadCommand multiRangeCommand = (MultiRangeReadCommand) request.payload;
        UnfilteredPartitionIterator data = Mockito.mock(UnfilteredPartitionIterator.class);
        Mockito.when(data.metadata()).thenReturn(multiRangeCommand.metadata());
        Mockito.when(data.hasNext()).thenReturn(false);
        ReadResponse response = multiRangeCommand.createResponse(data, RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);
        Message.Builder<ReadResponse> builder = Message.builder(Verb.MULTI_RANGE_RSP, response)
                                                       .from(endpoint)
                                                       .withId(request.id());

        for (Sensor sensor : sensors)
            builder.withCustomParam(SensorsCustomParams.paramForRequestSensor(sensor).get(), SensorsCustomParams.sensorValueAsBytes(sensor.getValue()));

        return builder.build();
    }

    private Message<ReadResponse> createReadResponseMessage(Message<?> request, InetAddressAndPort endpoint, Sensor... sensors)
    {
        UnfilteredPartitionIterator data = Mockito.mock(UnfilteredPartitionIterator.class);
        Mockito.when(data.metadata()).thenReturn(((ReadCommand) request.payload).metadata());
        ReadResponse response = ReadResponse.createDataResponse(data, (ReadCommand) request.payload, RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);
        Message.Builder<ReadResponse> builder = Message.builder(Verb.READ_RSP, response)
                                                       .from(endpoint)
                                                       .withId(request.id());

        for (Sensor sensor : sensors)
            builder.withCustomParam(SensorsCustomParams.paramForRequestSensor(sensor).get(), SensorsCustomParams.sensorValueAsBytes(sensor.getValue()));

        return builder.build();
    }

    private <T> Message<T> createResponseMessage(Verb responseVerb, T payload, InetAddressAndPort from, long id, Sensor... sensors)
    {
        Message.Builder<T> builder = Message.builder(responseVerb, payload)
                                            .from(from)
                                            .withId(id);

        for (Sensor sensor : sensors)
            builder.withCustomParam(SensorsCustomParams.paramForRequestSensor(sensor).get(), SensorsCustomParams.sensorValueAsBytes(sensor.getValue()));

        return builder.build();
    }

    private static AbstractWriteResponseHandler<?> createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal)
    {
        return createWriteResponseHandler(cl, ideal, Dispatcher.RequestTime.forImmediateExecution());
    }

    private static AbstractWriteResponseHandler<?> createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal, Dispatcher.RequestTime requestTime)
    {
        return createWriteResponseHandler(cl, ideal, requestTime, null);
    }

    private static AbstractWriteResponseHandler<?> createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal, Dispatcher.RequestTime requestTime, Runnable callback)
    {
        return ks.getReplicationStrategy().getWriteResponseHandler(ReplicaPlans.forWrite(ks, cl, targets, pending, Predicates.alwaysTrue(), ReplicaPlans.writeAll),
                                                                   callback, WriteType.SIMPLE, null, requestTime, ideal);
    }

    static class mockingSensor extends Sensor
    {
        public mockingSensor(Context context, Type type)
        {
            super(context, type);
        }
    }
}
