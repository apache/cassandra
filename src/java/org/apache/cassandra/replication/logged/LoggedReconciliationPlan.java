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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.ReconciliationPlan;
import org.apache.cassandra.replication.logged.LoggedMutationSummary.CoordinatorSummary;
import org.apache.cassandra.service.tracking.CoordinatorLogId;
import org.apache.cassandra.service.tracking.SequenceIds;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LoggedReconciliationPlan implements ReconciliationPlan
{
    private final ImmutableMap<InetAddressAndPort, ImmutableSet<MutationId>> txPlan;

    public LoggedReconciliationPlan(ImmutableMap<InetAddressAndPort, ImmutableSet<MutationId>> txPlan)
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
        return txPlan.get(node);
    }

    private static class Builder
    {
        final InetAddressAndPort node;
        final LoggedMutationSummary summary;

        public Builder(InetAddressAndPort node, LoggedMutationSummary summary)
        {
            this.node = node;
            this.summary = summary;
        }

        public void unionReconciledSequences(Map<CoordinatorLogId, SequenceIds> unifiedReconciliations)
        {
            for (int i=0; i<summary.size(); i++)
            {
                CoordinatorSummary coordinatorSummary = summary.get(i);
                if (!unifiedReconciliations.containsKey(coordinatorSummary.logId))
                    unifiedReconciliations.put(coordinatorSummary.logId, coordinatorSummary.reconciledIds.copy());
                else
                    unifiedReconciliations.get(coordinatorSummary.logId).addAll(coordinatorSummary.reconciledIds);
            }
        }
    }

    public static Map<InetAddressAndPort, ReconciliationPlan> calculateReconciliation(Map<InetAddressAndPort, MutationSummary> summaries)
    {
        Map<InetAddressAndPort, Builder> plans = new HashMap<>();
        summaries.forEach((node, summary)
                                  -> plans.put(node, new Builder(node, (LoggedMutationSummary) summary))
        );


        // calculate the union of all log->reconcilied sequences
        // this is used to prevent noop read reconciliations caused by races between replicas fully reconciliing
        // a mutation and excluding it from their summary, and nodes including them in their summaries
        Map<CoordinatorLogId, SequenceIds> unifiedReconciliations = new HashMap<>();
        plans.forEach( (node, plan) -> plan.unionReconciledSequences(unifiedReconciliations));


        throw new UnsupportedOperationException();
    }
}
