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

package org.apache.cassandra.db.view;

import java.util.Optional;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;

public final class ViewUtils
{
    private ViewUtils()
    {
    }

    /**
     * Calculate the natural endpoint for the view.
     *
     * The view natural endpoint is the endpoint which has the same cardinality as this node in the replication factor.
     * The cardinality is the number at which this node would store a piece of data, given the change in replication
     * factor. If the keyspace's replication strategy is a NetworkTopologyStrategy, we filter the ring to contain only
     * nodes in the local datacenter when calculating cardinality.
     *
     * For example, if we have the following ring:
     *   {@code A, T1 -> B, T2 -> C, T3 -> A}
     *
     * For the token T1, at RF=1, A would be included, so A's cardinality for T1 is 1. For the token T1, at RF=2, B would
     * be included, so B's cardinality for token T1 is 2. For token T3, at RF = 2, A would be included, so A's cardinality
     * for T3 is 2.
     *
     * For a view whose base token is T1 and whose view token is T3, the pairings between the nodes would be:
     *  A writes to C (A's cardinality is 1 for T1, and C's cardinality is 1 for T3)
     *  B writes to A (B's cardinality is 2 for T1, and A's cardinality is 2 for T3)
     *  C writes to B (C's cardinality is 3 for T1, and B's cardinality is 3 for T3)
     *
     * @return Optional.empty() if this method is called using a base token which does not belong to this replica
     */
    public static Optional<Replica> getViewNaturalEndpoint(AbstractReplicationStrategy replicationStrategy, Token baseToken, Token viewToken)
    {
        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();
        EndpointsForToken naturalBaseReplicas = replicationStrategy.getNaturalReplicasForToken(baseToken);
        EndpointsForToken naturalViewReplicas = replicationStrategy.getNaturalReplicasForToken(viewToken);

        Optional<Replica> localReplica = Iterables.tryFind(naturalViewReplicas, Replica::isSelf).toJavaUtil();
        if (localReplica.isPresent())
            return localReplica;

        // We only select replicas from our own DC
        // TODO: this is poor encapsulation, leaking implementation details of replication strategy
        Predicate<Replica> isLocalDC = r -> !(replicationStrategy instanceof NetworkTopologyStrategy)
                || DatabaseDescriptor.getEndpointSnitch().getDatacenter(r).equals(localDataCenter);

        // We have to remove any endpoint which is shared between the base and the view, as it will select itself
        // and throw off the counts otherwise.
        EndpointsForToken baseReplicas = naturalBaseReplicas.filter(
                r -> !naturalViewReplicas.endpoints().contains(r.endpoint()) && isLocalDC.test(r)
        );
        EndpointsForToken viewReplicas = naturalViewReplicas.filter(
                r -> !naturalBaseReplicas.endpoints().contains(r.endpoint()) && isLocalDC.test(r)
        );

        // The replication strategy will be the same for the base and the view, as they must belong to the same keyspace.
        // Since the same replication strategy is used, the same placement should be used and we should get the same
        // number of replicas for all of the tokens in the ring.
        assert baseReplicas.size() == viewReplicas.size() : "Replication strategy should have the same number of endpoints for the base and the view";

        int baseIdx = -1;
        for (int i=0; i<baseReplicas.size(); i++)
        {
            if (baseReplicas.get(i).isSelf())
            {
                baseIdx = i;
                break;
            }
        }

        if (baseIdx < 0)
            //This node is not a base replica of this key, so we return empty
            return Optional.empty();

        return Optional.of(viewReplicas.get(baseIdx));
    }
}
