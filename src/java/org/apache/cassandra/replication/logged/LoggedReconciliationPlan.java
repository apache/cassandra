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
package org.apache.cassandra.replication.logged;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.replication.ReconciliationPlan;
import org.apache.cassandra.replication.logged.LoggedMutationSummary.CoordinatorSummary;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LoggedReconciliationPlan implements ReconciliationPlan
{
    private final ImmutableMap<InetAddressAndPort, PeerReconciliation> txPlan;

    static class PeerReconciliation
    {
        private final ImmutableMap<CoordinatorLogId, Offsets> coordinatorIds;

        public PeerReconciliation(ImmutableMap<CoordinatorLogId, Offsets> coordinatorIds)
        {
            this.coordinatorIds = coordinatorIds;
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
    }

    public LoggedReconciliationPlan(ImmutableMap<InetAddressAndPort, PeerReconciliation> txPlan)
    {
        this.txPlan = txPlan;
    }

    @Override
    public Set<InetAddressAndPort> nodes()
    {
        return txPlan.keySet();
    }

    @Override
    public Set<MutationId> idsFor(InetAddressAndPort node)
    {
        throw new UnsupportedOperationException();
//        return txPlan.get(node);
    }

    private static class PlanBuilder
    {
        final InetAddressAndPort node;

        final LoggedMutationSummary summary;
        final Map<InetAddressAndPort, PeerReconciliation.Builder> peerReconciliations = new HashMap<>();

        public PlanBuilder(InetAddressAndPort node, LoggedMutationSummary summary)
        {
            this.node = node;
            this.summary = summary;
        }

        public void send(InetAddressAndPort to, CoordinatorLogId logId, Offsets sequenceIds)
        {
            peerReconciliations.computeIfAbsent(to, PeerReconciliation.Builder::new).send(logId, sequenceIds);
        }

        LoggedReconciliationPlan build()
        {
            ImmutableMap.Builder<InetAddressAndPort, PeerReconciliation> builder = ImmutableMap.builder();
            peerReconciliations.forEach((to, peerReconciliation) -> builder.put(to, peerReconciliation.build()));
            return new LoggedReconciliationPlan(builder.build());
        }
    }

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
            Preconditions.checkArgument(summary.logId.equals(logId));
            reconciled = Offsets.union(reconciled, summary.reconciled);
            unreconciled = Offsets.union(unreconciled, summary.unreconciled);
            unreconciledNodes.put(peer, summary.unreconciled);
        }

        void createPlan(Map<InetAddressAndPort, PlanBuilder> plan)
        {
            // remove reconciled ids
            Offsets allIds = Offsets.difference(unreconciled, reconciled);
            for (Map.Entry<InetAddressAndPort, Offsets> receiver : unreconciledNodes.entrySet())
            {
                Offsets missing = Offsets.difference(allIds, receiver.getValue());
                if (missing.isEmpty())
                    continue;

                // TODO: look into more intelligent ways to distribute mutation requests
                for (Map.Entry<InetAddressAndPort, Offsets> sender : unreconciledNodes.entrySet())
                {
                    if (sender.getKey().equals(receiver.getKey()))
                        continue;

                    Offsets senderIds = sender.getValue();
                    PlanBuilder senderPlan = plan.get(sender.getKey());

                    Offsets requestedIds = Offsets.intersection(missing, senderIds);
                    senderPlan.send(receiver.getKey(), logId, requestedIds);

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
        summaries.forEach((node, summary0) -> {

            LoggedMutationSummary summary = (LoggedMutationSummary) summary0;
            planBuilders.put(node, new PlanBuilder(node, summary));

            for (int i=0; i<summary.size(); i++)
            {
                CoordinatorSummary coordinatorSummary = summary.get(i);
                CoordinatorLogReconciliation reconciliation = coordinatorReconciliations.computeIfAbsent(coordinatorSummary.logId, CoordinatorLogReconciliation::new);
                reconciliation.addPeerSummary(node, coordinatorSummary);
            }
        });

        coordinatorReconciliations.values().forEach(planBuilder -> planBuilder.createPlan(planBuilders));

        Map<InetAddressAndPort, ReconciliationPlan> plans = new HashMap<>();
        planBuilders.forEach((node, planBuilder) -> plans.put(node, planBuilder.build()));
        return plans;
    }
}
