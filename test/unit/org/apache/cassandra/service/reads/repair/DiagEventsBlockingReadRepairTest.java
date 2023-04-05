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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.google.common.collect.Lists;

import org.apache.cassandra.locator.ReplicaPlan;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.repair.ReadRepairEvent.ReadRepairEventType;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Variation of {@link BlockingReadRepair} using diagnostic events instead of instrumentation for test validation.
 */
public class DiagEventsBlockingReadRepairTest extends AbstractReadRepairTest
{

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        OverrideConfigurationLoader.override((config) -> {
            config.diagnostic_events_enabled = true;
        });
        configureClass(ReadRepairStrategy.BLOCKING);
    }

    @After
    public void unsubscribeAll()
    {
        DiagnosticEventService.instance().cleanup();
    }

    @Test
    public void additionalMutationRequired()
    {
        Mutation repair1 = mutation(cell2);
        Mutation repair2 = mutation(cell1);

        // check that the correct repairs are calculated
        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, repair1);
        repairs.put(replica2, repair2);


        ReplicaPlan.ForWrite writePlan = repairPlan(replicas, EndpointsForRange.copyOf(Lists.newArrayList(repairs.keySet())));
        DiagnosticPartitionReadRepairHandler handler = createRepairHandler(repairs, writePlan);

        Assert.assertTrue(handler.updatesByEp.isEmpty());

        // check that the correct mutations are sent
        handler.sendInitialRepairs();
        Assert.assertEquals(2, handler.updatesByEp.size());

        Assert.assertEquals(repair1.toString(), handler.updatesByEp.get(target1));
        Assert.assertEquals(repair2.toString(), handler.updatesByEp.get(target2));

        // check that a combined mutation is speculatively sent to the 3rd target
        handler.updatesByEp.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertEquals(1, handler.updatesByEp.size());
        Assert.assertEquals(resolved.toString(), handler.updatesByEp.get(target3));

        // check repairs stop blocking after receiving 2 acks
        Assert.assertFalse(getCurrentRepairStatus(handler));
        handler.ack(target1);
        Assert.assertFalse(getCurrentRepairStatus(handler));
        handler.ack(target3);
        Assert.assertTrue(getCurrentRepairStatus(handler));
    }

    private boolean getCurrentRepairStatus(BlockingPartitionRepair handler)
    {
        return handler.awaitRepairsUntil(nanoTime(), NANOSECONDS);
    }

    public InstrumentedReadRepair createInstrumentedReadRepair(ReadCommand command, ReplicaPlan.Shared<?,?> replicaPlan, long queryStartNanoTime)
    {
        return new DiagnosticBlockingRepairHandler(command, replicaPlan, queryStartNanoTime);
    }

    private static DiagnosticPartitionReadRepairHandler createRepairHandler(Map<Replica, Mutation> repairs, ReplicaPlan.ForWrite writePlan)
    {
        return new DiagnosticPartitionReadRepairHandler(key, repairs, writePlan);
    }

    private static class DiagnosticBlockingRepairHandler extends BlockingReadRepair implements InstrumentedReadRepair
    {
        private Set<InetAddressAndPort> recipients = Collections.emptySet();
        private ReadCallback readCallback = null;

        DiagnosticBlockingRepairHandler(ReadCommand command, ReplicaPlan.Shared<?,?> replicaPlan, long queryStartNanoTime)
        {
            super(command, replicaPlan, queryStartNanoTime);
            DiagnosticEventService.instance().subscribe(ReadRepairEvent.class, this::onRepairEvent);
        }

        private void onRepairEvent(ReadRepairEvent e)
        {
            if (e.getType() == ReadRepairEventType.START_REPAIR) recipients = new HashSet<>(e.destinations);
            else if (e.getType() == ReadRepairEventType.SPECULATED_READ) recipients.addAll(e.destinations);
            Assert.assertEquals(new HashSet<>(targets), new HashSet<>(e.allEndpoints));
            Assert.assertNotNull(e.toMap());
        }

        @Override
        void sendReadCommand(Replica to, ReadCallback callback, boolean speculative, boolean trackRepairedStatus)
        {
            assert readCallback == null || readCallback == callback;
            readCallback = callback;
        }

        Iterable<InetAddressAndPort> getCandidatesForToken(Token token)
        {
            return targets;
        }

        public Set<InetAddressAndPort> getReadRecipients()
        {
            return recipients;
        }

        public ReadCallback getReadCallback()
        {
            return readCallback;
        }
    }

    private static class DiagnosticPartitionReadRepairHandler<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
            extends BlockingPartitionRepair
    {
        private final Map<InetAddressAndPort, String> updatesByEp = new HashMap<>();

        private static Predicate<InetAddressAndPort> isLocal()
        {
            List<InetAddressAndPort> candidates = targets;
            return e -> candidates.contains(e);
        }

        DiagnosticPartitionReadRepairHandler(DecoratedKey key, Map<Replica, Mutation> repairs, ReplicaPlan.ForWrite writePlan)
        {
            super(key, repairs, writePlan, isLocal());
            DiagnosticEventService.instance().subscribe(PartitionRepairEvent.class, this::onRepairEvent);
        }

        private void onRepairEvent(PartitionRepairEvent e)
        {
            updatesByEp.put(e.destination, e.mutationSummary);
            Assert.assertNotNull(e.toMap());
        }

        protected void sendRR(Message<Mutation> message, InetAddressAndPort endpoint)
        {
        }
    }
}
