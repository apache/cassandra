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

package org.apache.cassandra.service.reads.repair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.reads.ReadCallback;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class BlockingReadRepairTest extends AbstractReadRepairTest
{
    private static class InstrumentedReadRepairHandler
            extends BlockingPartitionRepair
    {
        public InstrumentedReadRepairHandler(Map<Replica, Mutation> repairs, ReplicaPlan.ForWrite writePlan)
        {
            super(Util.dk("not a real usable value"), repairs, writePlan, e -> targets.contains(e));
        }

        Map<InetAddressAndPort, Mutation> mutationsSent = new HashMap<>();

        protected void sendRR(Message<Mutation> message, InetAddressAndPort endpoint)
        {
            mutationsSent.put(endpoint, message.payload);
        }
    }

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        configureClass(ReadRepairStrategy.BLOCKING);
    }

    private static InstrumentedReadRepairHandler createRepairHandler(Map<Replica, Mutation> repairs, ReplicaPlan.ForWrite writePlan)
    {
        return new InstrumentedReadRepairHandler(repairs, writePlan);
    }

    private static InstrumentedReadRepairHandler createRepairHandler(Map<Replica, Mutation> repairs)
    {
        EndpointsForRange replicas = EndpointsForRange.copyOf(Lists.newArrayList(repairs.keySet()));
        return createRepairHandler(repairs, repairPlan(replicas, replicas));
    }

    private static class InstrumentedBlockingReadRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
            extends BlockingReadRepair<E, P> implements InstrumentedReadRepair<E, P>
    {
        public InstrumentedBlockingReadRepair(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime)
        {
            super(command, replicaPlan, queryStartNanoTime);
        }

        Set<InetAddressAndPort> readCommandRecipients = new HashSet<>();
        ReadCallback readCallback = null;

        @Override
        void sendReadCommand(Replica to, ReadCallback callback, boolean speculative, boolean trackRepairedStatus)
        {
            assert readCallback == null || readCallback == callback;
            readCommandRecipients.add(to.endpoint());
            readCallback = callback;
        }

        @Override
        public Set<InetAddressAndPort> getReadRecipients()
        {
            return readCommandRecipients;
        }

        @Override
        public ReadCallback getReadCallback()
        {
            return readCallback;
        }
    }

    @Override
    public InstrumentedReadRepair createInstrumentedReadRepair(ReadCommand command, ReplicaPlan.Shared<?, ?> replicaPlan, long queryStartNanoTime)
    {
        return new InstrumentedBlockingReadRepair(command, replicaPlan, queryStartNanoTime);
    }

    @Test
    public void consistencyLevelTest() throws Exception
    {
        AbstractReplicationStrategy rs = ks.getReplicationStrategy();
        Assert.assertTrue(ConsistencyLevel.QUORUM.satisfies(ConsistencyLevel.QUORUM, rs));
        Assert.assertTrue(ConsistencyLevel.THREE.satisfies(ConsistencyLevel.QUORUM, rs));
        Assert.assertTrue(ConsistencyLevel.TWO.satisfies(ConsistencyLevel.QUORUM, rs));
        Assert.assertFalse(ConsistencyLevel.ONE.satisfies(ConsistencyLevel.QUORUM, rs));
        Assert.assertFalse(ConsistencyLevel.ANY.satisfies(ConsistencyLevel.QUORUM, rs));
    }


    @Test
    public void additionalMutationRequired() throws Exception
    {

        Mutation repair1 = mutation(cell2);
        Mutation repair2 = mutation(cell1);

        // check that the correct repairs are calculated
        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, repair1);
        repairs.put(replica2, repair2);

        ReplicaPlan.ForWrite writePlan = repairPlan(replicas, EndpointsForRange.copyOf(Lists.newArrayList(repairs.keySet())));
        InstrumentedReadRepairHandler handler = createRepairHandler(repairs, writePlan);

        Assert.assertTrue(handler.mutationsSent.isEmpty());

        // check that the correct mutations are sent
        handler.sendInitialRepairs();
        Assert.assertEquals(2, handler.mutationsSent.size());
        assertMutationEqual(repair1, handler.mutationsSent.get(target1));
        assertMutationEqual(repair2, handler.mutationsSent.get(target2));

        // check that a combined mutation is speculatively sent to the 3rd target
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertEquals(1, handler.mutationsSent.size());
        assertMutationEqual(resolved, handler.mutationsSent.get(target3));

        // check repairs stop blocking after receiving 2 acks
        Assert.assertFalse(getCurrentRepairStatus(handler));
        handler.ack(target1);
        Assert.assertFalse(getCurrentRepairStatus(handler));
        handler.ack(target3);
        Assert.assertTrue(getCurrentRepairStatus(handler));

    }

    /**
     * If we've received enough acks, we shouldn't send any additional mutations
     */
    @Test
    public void noAdditionalMutationRequired() throws Exception
    {
        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, mutation(cell2));
        repairs.put(replica2, mutation(cell1));

        InstrumentedReadRepairHandler handler = createRepairHandler(repairs);
        handler.sendInitialRepairs();
        handler.ack(target1);
        handler.ack(target2);

        // both replicas have acked, we shouldn't send anything else out
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertTrue(handler.mutationsSent.isEmpty());
    }

    /**
     * If there are no additional nodes we can send mutations to, we... shouldn't
     */
    @Test
    public void noAdditionalMutationPossible() throws Exception
    {
        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, mutation(cell2));
        repairs.put(replica2, mutation(cell1));

        InstrumentedReadRepairHandler handler = createRepairHandler(repairs);
        handler.sendInitialRepairs();

        // we've already sent mutations to all candidates, so we shouldn't send any more
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertTrue(handler.mutationsSent.isEmpty());
    }

    /**
     * If we didn't send a repair to a replica because there wasn't a diff with the
     * resolved column family, we shouldn't send it a speculative mutation
     */
    @Test
    public void mutationsArentSentToInSyncNodes() throws Exception
    {
        Mutation repair1 = mutation(cell2);

        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, repair1);

        // check that the correct initial mutations are sent out
        InstrumentedReadRepairHandler handler = createRepairHandler(repairs, repairPlan(replicas, EndpointsForRange.of(replica1, replica2)));
        handler.sendInitialRepairs();
        Assert.assertEquals(1, handler.mutationsSent.size());
        Assert.assertTrue(handler.mutationsSent.containsKey(target1));

        // check that speculative mutations aren't sent to target2
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertEquals(1, handler.mutationsSent.size());
        Assert.assertTrue(handler.mutationsSent.containsKey(target3));
    }

    @Test
    public void onlyBlockOnQuorum()
    {
        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, mutation(cell1));
        repairs.put(replica2, mutation(cell2));
        repairs.put(replica3, mutation(cell3));
        Assert.assertEquals(3, repairs.size());

        InstrumentedReadRepairHandler handler = createRepairHandler(repairs);
        handler.sendInitialRepairs();

        Assert.assertFalse(getCurrentRepairStatus(handler));
        handler.ack(target1);
        Assert.assertFalse(getCurrentRepairStatus(handler));

        // here we should stop blocking, even though we've sent 3 repairs
        handler.ack(target2);
        Assert.assertTrue(getCurrentRepairStatus(handler));
    }

    /**
     * For dc local consistency levels, noop mutations and responses from remote dcs should not affect effective blockFor
     */
    @Test
    public void remoteDCTest() throws Exception
    {
        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, mutation(cell1));

        Replica remote1 = ReplicaUtils.full(InetAddressAndPort.getByName("10.0.0.1"));
        Replica remote2 = ReplicaUtils.full(InetAddressAndPort.getByName("10.0.0.2"));
        repairs.put(remote1, mutation(cell1));

        EndpointsForRange participants = EndpointsForRange.of(replica1, replica2, remote1, remote2);
        ReplicaPlan.ForWrite writePlan = repairPlan(replicaPlan(ks, ConsistencyLevel.LOCAL_QUORUM, participants));
        InstrumentedReadRepairHandler handler = createRepairHandler(repairs, writePlan);
        handler.sendInitialRepairs();
        Assert.assertEquals(2, handler.mutationsSent.size());
        Assert.assertTrue(handler.mutationsSent.containsKey(replica1.endpoint()));
        Assert.assertTrue(handler.mutationsSent.containsKey(remote1.endpoint()));

        Assert.assertEquals(1, handler.waitingOn());
        Assert.assertFalse(getCurrentRepairStatus(handler));

        handler.ack(remote1.endpoint());
        Assert.assertEquals(1, handler.waitingOn());
        Assert.assertFalse(getCurrentRepairStatus(handler));

        handler.ack(replica1.endpoint());
        Assert.assertEquals(0, handler.waitingOn());
        Assert.assertTrue(getCurrentRepairStatus(handler));
    }

    private boolean getCurrentRepairStatus(BlockingPartitionRepair handler)
    {
        return handler.awaitRepairsUntil(nanoTime(), NANOSECONDS);
    }
}
