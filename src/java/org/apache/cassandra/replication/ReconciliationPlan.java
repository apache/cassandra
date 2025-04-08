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
import com.google.common.collect.Sets;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.MutationSummary.CoordinatorSummary;

public class ReconciliationPlan
{
    private final ImmutableMap<InetAddressAndPort, PeerReconciliation> txPlan;

    public static class PeerReconciliation
    {
        private final ImmutableMap<CoordinatorLogId, Offsets> coordinatorIds;

        public PeerReconciliation(ImmutableMap<CoordinatorLogId, Offsets> coordinatorIds)
        {
            this.coordinatorIds = coordinatorIds;
        }

        @Override
        public String toString() {
            return "PeerReconciliation{" +
                    "coordinatorIds=" + coordinatorIds +
                    '}';
        }

        public Set<ShortMutationId> ids()
        {
            int size = 0;
            for (Offsets offsets : coordinatorIds.values())
                size += offsets.offsetCount();
            Set<ShortMutationId> ids = Sets.newHashSetWithExpectedSize(size);
            for (Offsets offsets : coordinatorIds.values())
                offsets.collectIds(ids);
            return ids;
        }

        static class Builder
        {
            private final InetAddressAndPort to;
            private final Map<CoordinatorLogId, Offsets> coordinatorIds = new HashMap<>();

            public Builder(InetAddressAndPort to)
            {
                this.to = to;
            }

            void send(CoordinatorLogId logId, Offsets offsets)
            {
                Offsets existing = coordinatorIds.get(logId);
                if (existing != null)
                    coordinatorIds.put(logId, Offsets.union(existing, offsets));
                else
                    coordinatorIds.put(logId, offsets);
            }

            PeerReconciliation build()
            {
                return new PeerReconciliation(ImmutableMap.copyOf(coordinatorIds));
            }
        }

        public static final IVersionedSerializer<PeerReconciliation> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(PeerReconciliation reconciliation, DataOutputPlus out, int version) throws IOException
            {
                out.writeInt(reconciliation.coordinatorIds.size());
                for (Offsets offsets : reconciliation.coordinatorIds.values())
                    Offsets.serializer.serialize(offsets, out, version);
            }

            @Override
            public PeerReconciliation deserialize(DataInputPlus in, int version) throws IOException
            {
                int size = in.readInt();
                ImmutableMap.Builder<CoordinatorLogId, Offsets> builder = ImmutableMap.builderWithExpectedSize(size);
                for (int i = 0; i < size; i++)
                {
                    Offsets offsets = Offsets.serializer.deserialize(in, version);
                    builder.put(offsets.logId(), offsets);
                }
                return new PeerReconciliation(builder.build());
            }

            @Override
            public long serializedSize(PeerReconciliation reconciliation, int version)
            {
                long size = TypeSizes.INT_SIZE;
                for (Offsets offsets : reconciliation.coordinatorIds.values())
                    size += Offsets.serializer.serializedSize(offsets, version);

                return size;
            }
        };
    }

    public ReconciliationPlan(ImmutableMap<InetAddressAndPort, PeerReconciliation> txPlan)
    {
        this.txPlan = txPlan;
    }

    public Set<InetAddressAndPort> nodes()
    {
        return txPlan.keySet();
    }

    public PeerReconciliation peerReconciliation(InetAddressAndPort to)
    {
        return txPlan.get(to);
    }

    public Set<ShortMutationId> idsFor(InetAddressAndPort node)
    {
        return txPlan.get(node).ids();
    }

    public boolean isEmpty()
    {
        return txPlan.isEmpty();
    }

    private static class PlanBuilder
    {
        final InetAddressAndPort node;

        final MutationSummary summary;
        final Map<InetAddressAndPort, PeerReconciliation.Builder> peerReconciliations = new HashMap<>();

        public PlanBuilder(InetAddressAndPort node, MutationSummary summary)
        {
            this.node = node;
            this.summary = summary;
        }

        public void send(InetAddressAndPort to, CoordinatorLogId logId, Offsets sequenceIds)
        {
            peerReconciliations.computeIfAbsent(to, PeerReconciliation.Builder::new).send(logId, sequenceIds);
        }

        ReconciliationPlan build()
        {
            ImmutableMap.Builder<InetAddressAndPort, PeerReconciliation> builder = ImmutableMap.builder();
            peerReconciliations.forEach((to, peerReconciliation) -> builder.put(to, peerReconciliation.build()));
            return new ReconciliationPlan(builder.build());
        }
    }

    // TODO (desired): rework Offsets set logic usage to use Offsets#RangeIterator instead of rematerializing sets
    private static class CoordinatorLogReconciliation
    {
        final CoordinatorLogId logId;
        Offsets reconciled;
        Offsets unreconciled;

        Map<InetAddressAndPort, Offsets> unreconciledNodes = new HashMap<>();

        CoordinatorLogReconciliation(CoordinatorLogId logId)
        {
            this.logId = logId;
        }

        void addPeerSummary(InetAddressAndPort peer, CoordinatorSummary summary)
        {
            Preconditions.checkArgument(summary.logId().equals(logId));
            reconciled = Offsets.union(reconciled, summary.reconciled);
            unreconciled = Offsets.union(unreconciled, summary.unreconciled);
            unreconciledNodes.put(peer, summary.unreconciled);
        }

        void createPlan(Map<InetAddressAndPort, PlanBuilder> plan)
        {
            // remove reconciled ids
            Offsets allIds = Offsets.difference(unreconciled, reconciled);
            for (InetAddressAndPort receiver : plan.keySet())
            {
                Offsets missing = Offsets.difference(allIds, unreconciledNodes.get(receiver));
                if (missing.isEmpty())
                    continue;

                // TODO: look into more intelligent ways to distribute mutation requests
                for (Map.Entry<InetAddressAndPort, Offsets> sender : unreconciledNodes.entrySet())
                {
                    if (sender.getKey().equals(receiver))
                        continue;

                    Offsets senderIds = sender.getValue();
                    PlanBuilder senderPlan = plan.get(sender.getKey());

                    Offsets requestedIds = Offsets.intersection(missing, senderIds);
                    senderPlan.send(receiver, logId, requestedIds);

                    missing = Offsets.difference(missing, requestedIds);
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
            for (Map.Entry<InetAddressAndPort, PeerReconciliation> entry : plan.txPlan.entrySet())
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(entry.getKey(), out, version);
                PeerReconciliation.serializer.serialize(entry.getValue(), out, version);
            }
        }

        @Override
        public ReconciliationPlan deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readInt();
            ImmutableMap.Builder<InetAddressAndPort, PeerReconciliation> builder = ImmutableMap.builderWithExpectedSize(size);
            for (int i = 0; i < size; i++)
                builder.put(InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version),
                            PeerReconciliation.serializer.deserialize(in, version));
            return new ReconciliationPlan(builder.build());
        }

        @Override
        public long serializedSize(ReconciliationPlan plan, int version)
        {
            long size = TypeSizes.INT_SIZE;
            for (Map.Entry<InetAddressAndPort, PeerReconciliation> entry : plan.txPlan.entrySet())
            {
                size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(entry.getKey(), version);
                size += PeerReconciliation.serializer.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    };
}
