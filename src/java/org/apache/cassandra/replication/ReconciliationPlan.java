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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.MutationSummary.CoordinatorSummary;

public class ReconciliationPlan
{
    private final ImmutableMap<InetAddressAndPort, Log2OffsetsMap.Immutable> txPlan;

    public ReconciliationPlan(ImmutableMap<InetAddressAndPort, Log2OffsetsMap.Immutable> txPlan)
    {
        this.txPlan = txPlan;
    }

    public Set<InetAddressAndPort> nodes()
    {
        return txPlan.keySet();
    }

    public Log2OffsetsMap.Immutable peerReconciliation(InetAddressAndPort to)
    {
        return txPlan.get(to);
    }

    public Log2OffsetsMap.Immutable offsetsFor(InetAddressAndPort node)
    {
        return txPlan.get(node);
    }

    public boolean isEmpty()
    {
        return txPlan.isEmpty();
    }

    private static class PlanBuilder
    {
        final InetAddressAndPort node;

        final MutationSummary summary;
        final Map<InetAddressAndPort, Log2OffsetsMap.Immutable.Builder> peerReconciliations = new HashMap<>();

        public PlanBuilder(InetAddressAndPort node, MutationSummary summary)
        {
            this.node = node;
            this.summary = summary;
        }

        public void send(InetAddressAndPort to, Offsets sequenceIds)
        {
            peerReconciliations.computeIfAbsent(to, ep -> new Log2OffsetsMap.Immutable.Builder()).add(sequenceIds);
        }

        ReconciliationPlan build()
        {
            ImmutableMap.Builder<InetAddressAndPort, Log2OffsetsMap.Immutable> builder = ImmutableMap.builder();
            peerReconciliations.forEach((to, ids) -> builder.put(to, ids.build()));
            return new ReconciliationPlan(builder.build());
        }
    }

    // TODO (desired): rework Offsets set logic usage to use Offsets#RangeIterator instead of rematerializing sets
    private static class CoordinatorLogReconciliation
    {
        final CoordinatorLogId logId;
        Offsets.Immutable reconciled;
        Offsets.Immutable unreconciled;

        Map<InetAddressAndPort, Offsets> unreconciledNodes = new HashMap<>();

        CoordinatorLogReconciliation(CoordinatorLogId logId)
        {
            this.logId = logId;
        }

        void addPeerSummary(InetAddressAndPort peer, CoordinatorSummary summary)
        {
            Preconditions.checkArgument(summary.logId().equals(logId));
            reconciled = Offsets.Immutable.union(reconciled, summary.reconciled);
            unreconciled = Offsets.Immutable.union(unreconciled, summary.unreconciled);
            unreconciledNodes.put(peer, summary.unreconciled);
        }

        void createPlan(Map<InetAddressAndPort, PlanBuilder> plan)
        {
            // remove reconciled ids
            Offsets.Immutable allIds = Offsets.Immutable.difference(unreconciled, reconciled);
            for (InetAddressAndPort receiver : plan.keySet())
            {
                Offsets.Immutable missing = Offsets.Immutable.difference(allIds, unreconciledNodes.get(receiver));
                if (missing.isEmpty())
                    continue;

                // TODO: look into more intelligent ways to distribute mutation requests
                for (Map.Entry<InetAddressAndPort, Offsets> sender : unreconciledNodes.entrySet())
                {
                    if (sender.getKey().equals(receiver))
                        continue;

                    Offsets senderIds = sender.getValue();
                    PlanBuilder senderPlan = plan.get(sender.getKey());

                    Offsets.Immutable requestedIds = Offsets.Immutable.intersection(missing, senderIds);
                    senderPlan.send(receiver, requestedIds);

                    missing = Offsets.Immutable.difference(missing, requestedIds);
                    if (missing.rangeCount() == 0)
                        break;
                }
            }
        }
    }

    public static Map<InetAddressAndPort, ReconciliationPlan> calculateReconciliation(Map<InetAddressAndPort, MutationSummary> summaries)
    {
        Map<InetAddressAndPort, PlanBuilder> planBuilders = new HashMap<>();
        Map<CoordinatorLogId, CoordinatorLogReconciliation> coordinatorReconciliations = new HashMap<>();

        // organize data by peer and log id
        summaries.forEach((node, summary) -> {

            planBuilders.put(node, new PlanBuilder(node, summary));

            for (int i=0; i<summary.size(); i++)
            {
                CoordinatorSummary coordinatorSummary = summary.get(i);
                CoordinatorLogReconciliation reconciliation = coordinatorReconciliations.computeIfAbsent(coordinatorSummary.logId(), CoordinatorLogReconciliation::new);
                reconciliation.addPeerSummary(node, coordinatorSummary);
            }
        });

        coordinatorReconciliations.values().forEach(planBuilder -> planBuilder.createPlan(planBuilders));

        Map<InetAddressAndPort, ReconciliationPlan> plans = new HashMap<>();
        planBuilders.forEach((node, planBuilder) -> {
            ReconciliationPlan plan = planBuilder.build();
            if (!plan.isEmpty())
                plans.put(node, plan);
        });
        return plans;
    }

    public static final IVersionedSerializer<ReconciliationPlan> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(ReconciliationPlan plan, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(plan.txPlan.size());
            for (Map.Entry<InetAddressAndPort, Log2OffsetsMap.Immutable> entry : plan.txPlan.entrySet())
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(entry.getKey(), out, version);
                Log2OffsetsMap.Immutable.serializer.serialize(entry.getValue(), out, version);
            }
        }

        @Override
        public ReconciliationPlan deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readInt();
            ImmutableMap.Builder<InetAddressAndPort, Log2OffsetsMap.Immutable> builder = ImmutableMap.builderWithExpectedSize(size);
            for (int i = 0; i < size; i++)
            {
                InetAddressAndPort endpoint = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version);
                Log2OffsetsMap.Immutable offsets = Log2OffsetsMap.Immutable.serializer.deserialize(in, version);
                builder.put(endpoint, offsets);
            }
            return new ReconciliationPlan(builder.build());
        }

        @Override
        public long serializedSize(ReconciliationPlan plan, int version)
        {
            long size = TypeSizes.sizeof(plan.txPlan.size());
            for (Map.Entry<InetAddressAndPort, Log2OffsetsMap.Immutable> entry : plan.txPlan.entrySet())
            {
                size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(entry.getKey(), version);
                size += Log2OffsetsMap.Immutable.serializer.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    };
}
