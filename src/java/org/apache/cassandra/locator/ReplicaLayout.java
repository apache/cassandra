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

package org.apache.cassandra.locator;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.utils.FBUtilities;

import java.util.Set;
import java.util.function.Predicate;

/**
 * The relevant replicas for an operation over a given range or token.
 *
 * @param <E>
 */
public abstract class ReplicaLayout<E extends Endpoints<E>>
{
    private final E natural;
    // the snapshot of the replication strategy that corresponds to the replica layout
    private final AbstractReplicationStrategy replicationStrategy;

    ReplicaLayout(AbstractReplicationStrategy replicationStrategy, E natural)
    {
        this.replicationStrategy = replicationStrategy;
        this.natural = natural;
    }

    /**
     * The 'natural' owners of the ring position(s), as implied by the current ring layout.
     * This excludes any pending owners, i.e. those that are in the process of taking ownership of a range, but
     * have not yet finished obtaining their view of the range.
     */
    public final E natural()
    {
        return natural;
    }

    public final AbstractReplicationStrategy replicationStrategy()
    {
        return replicationStrategy;
    }

    /**
     * All relevant owners of the ring position(s) for this operation, as implied by the current ring layout.
     * For writes, this will include pending owners, and for reads it will be equivalent to natural()
     */
    public E all()
    {
        return natural;
    }

    public String toString()
    {
        return "ReplicaLayout [ natural: " + natural + " ]";
    }

    public static class ForTokenRead extends ReplicaLayout<EndpointsForToken> implements ForToken
    {
        public ForTokenRead(AbstractReplicationStrategy replicationStrategy, EndpointsForToken natural)
        {
            super(replicationStrategy, natural);
        }

        @Override
        public Token token()
        {
            return natural().token();
        }

        public ReplicaLayout.ForTokenRead filter(Predicate<Replica> filter)
        {
            EndpointsForToken filtered = natural().filter(filter);
            // AbstractReplicaCollection.filter returns itself if all elements match the filter
            if (filtered == natural()) return this;
            return new ReplicaLayout.ForTokenRead(replicationStrategy(), filtered);
        }
    }

    public static class ForRangeRead extends ReplicaLayout<EndpointsForRange> implements ForRange
    {
        final AbstractBounds<PartitionPosition> range;

        public ForRangeRead(AbstractReplicationStrategy replicationStrategy, AbstractBounds<PartitionPosition> range, EndpointsForRange natural)
        {
            super(replicationStrategy, natural);
            this.range = range;
        }

        @Override
        public AbstractBounds<PartitionPosition> range()
        {
            return range;
        }

        public ReplicaLayout.ForRangeRead filter(Predicate<Replica> filter)
        {
            EndpointsForRange filtered = natural().filter(filter);
            // AbstractReplicaCollection.filter returns itself if all elements match the filter
            if (filtered == natural()) return this;
            return new ReplicaLayout.ForRangeRead(replicationStrategy(), range(), filtered);
        }
    }

    public static class ForWrite<E extends Endpoints<E>> extends ReplicaLayout<E>
    {
        final E all;
        final E pending;

        ForWrite(AbstractReplicationStrategy replicationStrategy, E natural, E pending, E all)
        {
            super(replicationStrategy, natural);
            assert pending != null && !haveWriteConflicts(natural, pending);
            if (all == null)
                all = Endpoints.concat(natural, pending);
            this.all = all;
            this.pending = pending;
        }

        public final E all()
        {
            return all;
        }

        public final E pending()
        {
            return pending;
        }

        public String toString()
        {
            return "ReplicaLayout [ natural: " + natural() + ", pending: " + pending + " ]";
        }
    }

    public static class ForTokenWrite extends ForWrite<EndpointsForToken> implements ForToken
    {
        public ForTokenWrite(AbstractReplicationStrategy replicationStrategy, EndpointsForToken natural, EndpointsForToken pending)
        {
            this(replicationStrategy, natural, pending, null);
        }
        public ForTokenWrite(AbstractReplicationStrategy replicationStrategy, EndpointsForToken natural, EndpointsForToken pending, EndpointsForToken all)
        {
            super(replicationStrategy, natural, pending, all);
        }

        @Override
        public Token token() { return natural().token(); }

        public ForTokenWrite filter(Predicate<Replica> filter)
        {
            EndpointsForToken filtered = all().filter(filter);
            // AbstractReplicaCollection.filter returns itself if all elements match the filter
            if (filtered == all()) return this;
            if (pending().isEmpty()) return new ForTokenWrite(replicationStrategy(), filtered, pending(), filtered);
            // unique by endpoint, so can for efficiency filter only on endpoint
            return new ForTokenWrite(
                    replicationStrategy(),
                    natural().keep(filtered.endpoints()),
                    pending().keep(filtered.endpoints()),
                    filtered
            );
        }
    }

    public interface ForRange
    {
        public AbstractBounds<PartitionPosition> range();
    }

    public interface ForToken
    {
        public Token token();
    }

    /**
     * Gets the 'natural' and 'pending' replicas that own a given token, with no filtering or processing.
     *
     * Since a write is intended for all nodes (except, unless necessary, transient replicas), this method's
     * only responsibility is to fetch the 'natural' and 'pending' replicas, then resolve any conflicts
     * {@link ReplicaLayout#haveWriteConflicts(Endpoints, Endpoints)}
     */
    public static ReplicaLayout.ForTokenWrite forTokenWriteLiveAndDown(Keyspace keyspace, Token token)
    {
        // TODO: these should be cached, not the natural replicas
        // TODO: race condition to fetch these. implications??
        AbstractReplicationStrategy replicationStrategy = keyspace.getReplicationStrategy();
        EndpointsForToken natural = EndpointsForToken.natural(replicationStrategy, token);
        EndpointsForToken pending = EndpointsForToken.pending(keyspace, token);
        return forTokenWrite(replicationStrategy, natural, pending);
    }

    public static ReplicaLayout.ForTokenWrite forTokenWrite(AbstractReplicationStrategy replicationStrategy, EndpointsForToken natural, EndpointsForToken pending)
    {
        if (haveWriteConflicts(natural, pending))
        {
            natural = resolveWriteConflictsInNatural(natural, pending);
            pending = resolveWriteConflictsInPending(natural, pending);
        }
        return new ReplicaLayout.ForTokenWrite(replicationStrategy, natural, pending);
    }

    /**
     * Detect if we have any endpoint in both pending and full; this can occur either due to races (there is no isolation)
     * or because an endpoint is transitioning between full and transient replication status.
     *
     * We essentially always prefer the full version for writes, because this is stricter.
     *
     * For transient->full transitions:
     *
     *   Since we always write to any pending transient replica, effectively upgrading it to full for the transition duration,
     *   it might at first seem to be OK to continue treating the conflict replica as its 'natural' transient form,
     *   as there is always a quorum of nodes receiving the write.  However, ring ownership changes are not atomic or
     *   consistent across the cluster, and it is possible for writers to see different ring states.
     *
     *   Furthermore, an operator would expect that the full node has received all writes, with no extra need for repair
     *   (as the normal contract dictates) when it completes its transition.
     *
     *   While we cannot completely eliminate risks due to ring inconsistencies, this approach is the most conservative
     *   available to us today to mitigate, and (we think) the easiest to reason about.
     *
     * For full->transient transitions:
     *
     *   In this case, things are dicier, because in theory we can trigger this change instantly.  All we need to do is
     *   drop some data, surely?
     *
     *   Ring movements can put us in a pickle; any other node could believe us to be full when we have become transient,
     *   and perform a full data request to us that we believe ourselves capable of answering, but that we are not.
     *   If the ring is inconsistent, it's even feasible that a transient request would be made to the node that is losing
     *   its transient status, that also does not know it has yet done so, resulting in all involved nodes being unaware
     *   of the data inconsistency.
     *
     *   This happens because ring ownership changes are implied by a single node; not all owning nodes get a say in when
     *   the transition takes effect.  As such, a node can hold an incorrect belief about its own ownership ranges.
     *
     *   This race condition is somewhat inherent in present day Cassandra, and there's actually a limit to what we can do about it.
     *   It is a little more dangerous with transient replication, however, because we can completely answer a request without
     *   ever touching a digest, meaning we are less likely to attempt to repair any inconsistency.
     *
     *   We aren't guaranteed to contact any different nodes for the data requests, of course, though we at least have a chance.
     *
     * Note: If we have any pending transient->full movement, we need to move the full replica to our 'natural' bucket
     * to avoid corrupting our count.  This is fine for writes, all we're doing is ensuring we always write to the node,
     * instead of selectively.
     *
     * @param natural
     * @param pending
     * @param <E>
     * @return
     */
    static <E extends Endpoints<E>> boolean haveWriteConflicts(E natural, E pending)
    {
        Set<InetAddressAndPort> naturalEndpoints = natural.endpoints();
        for (InetAddressAndPort pendingEndpoint : pending.endpoints())
        {
            if (naturalEndpoints.contains(pendingEndpoint))
                return true;
        }
        return false;
    }

    /**
     * MUST APPLY FIRST
     * See {@link ReplicaLayout#haveWriteConflicts}
     * @return a 'natural' replica collection, that has had its conflicts with pending repaired
     */
    @VisibleForTesting
    static EndpointsForToken resolveWriteConflictsInNatural(EndpointsForToken natural, EndpointsForToken pending)
    {
        EndpointsForToken.Builder resolved = natural.newBuilder(natural.size());
        for (Replica replica : natural)
        {
            // always prefer the full natural replica, if there is a conflict
            if (replica.isTransient())
            {
                Replica conflict = pending.byEndpoint().get(replica.endpoint());
                if (conflict != null)
                {
                    // it should not be possible to have conflicts of the same replication type for the same range
                    assert conflict.isFull();
                    // If we have any pending transient->full movement, we need to move the full replica to our 'natural' bucket
                    // to avoid corrupting our count
                    resolved.add(conflict);
                    continue;
                }
            }
            resolved.add(replica);
        }
        return resolved.build();
    }

    /**
     * MUST APPLY SECOND
     * See {@link ReplicaLayout#haveWriteConflicts}
     * @return a 'pending' replica collection, that has had its conflicts with natural repaired
     */
    @VisibleForTesting
    static EndpointsForToken resolveWriteConflictsInPending(EndpointsForToken natural, EndpointsForToken pending)
    {
        return pending.without(natural.endpoints());
    }

    /**
     * @return the read layout for a token - this includes only live natural replicas, i.e. those that are not pending
     * and not marked down by the failure detector. these are reverse sorted by the badness score of the configured snitch
     */
    public static ReplicaLayout.ForTokenRead forTokenReadLiveSorted(AbstractReplicationStrategy replicationStrategy, Token token)
    {
        EndpointsForToken replicas = replicationStrategy.getNaturalReplicasForToken(token);
        replicas = DatabaseDescriptor.getEndpointSnitch().sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), replicas);
        replicas = replicas.filter(FailureDetector.isReplicaAlive);
        return new ReplicaLayout.ForTokenRead(replicationStrategy, replicas);
    }

    /**
     * TODO: we should really double check that the provided range does not overlap multiple token ring regions
     * @return the read layout for a range - this includes only live natural replicas, i.e. those that are not pending
     * and not marked down by the failure detector. these are reverse sorted by the badness score of the configured snitch
     */
    static ReplicaLayout.ForRangeRead forRangeReadLiveSorted(AbstractReplicationStrategy replicationStrategy, AbstractBounds<PartitionPosition> range)
    {
        EndpointsForRange replicas = replicationStrategy.getNaturalReplicas(range.right);
        replicas = DatabaseDescriptor.getEndpointSnitch().sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), replicas);
        replicas = replicas.filter(FailureDetector.isReplicaAlive);
        return new ReplicaLayout.ForRangeRead(replicationStrategy, range, replicas);
    }
}
