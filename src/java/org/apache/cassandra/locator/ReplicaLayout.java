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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import java.util.function.Predicate;

/**
 * The relevant replicas for an operation over a given range or token.
 *
 * @param <E>
 */
public abstract class ReplicaLayout<E extends Endpoints<E>>
{
    private final E natural;

    ReplicaLayout(E natural)
    {
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
        public ForTokenRead(EndpointsForToken natural)
        {
            super(natural);
        }
        @Override
        public Token token()
        {
            return natural().token();
        }

        public ReplicaLayout.ForTokenRead filter(Predicate<Replica> filter)
        {
            return new ReplicaLayout.ForTokenRead(
                    natural().filter(filter)
            );
        }
    }

    public static class ForRangeRead extends ReplicaLayout<EndpointsForRange> implements ForRange
    {
        final AbstractBounds<PartitionPosition> range;

        public ForRangeRead(AbstractBounds<PartitionPosition> range, EndpointsForRange natural)
        {
            super(natural);
            this.range = range;
        }

        @Override
        public AbstractBounds<PartitionPosition> range()
        {
            return range;
        }

        public ReplicaLayout.ForRangeRead filter(Predicate<Replica> filter)
        {
            return new ReplicaLayout.ForRangeRead(
                    range(), natural().filter(filter)
            );
        }
    }

    public static class ForWrite<E extends Endpoints<E>> extends ReplicaLayout<E>
    {
        final E all;
        final E pending;

        ForWrite(E natural, E pending, E all)
        {
            super(natural);
            assert pending != null && !Endpoints.haveConflicts(natural, pending);
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
        public ForTokenWrite(EndpointsForToken natural, EndpointsForToken pending)
        {
            this(natural, pending, null);
        }
        public ForTokenWrite(EndpointsForToken natural, EndpointsForToken pending, EndpointsForToken all)
        {
            super(natural, pending, all);
        }

        @Override
        public Token token()
        {
            return natural().token();
        }
        public ReplicaLayout.ForTokenWrite filter(Predicate<Replica> filter)
        {
            return new ReplicaLayout.ForTokenWrite(
                    natural().filter(filter),
                    pending().filter(filter),
                    all().filter(filter)
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

    public static ReplicaLayout.ForTokenWrite forSingleReplicaWrite(Token token, Replica replica)
    {
        // synthetically create a LiveForToken as well, without testing our liveness
        EndpointsForToken singleReplica = EndpointsForToken.of(token, replica);
        return new ReplicaLayout.ForTokenWrite(singleReplica, EndpointsForToken.empty(token), singleReplica);
    }

    public static ReplicaLayout.ForTokenWrite forTokenWriteLiveAndDown(Keyspace keyspace, Token token)
    {
        // TODO: race condition to fetch these. implications??
        EndpointsForToken natural = keyspace.getReplicationStrategy().getNaturalReplicasForToken(token);
        EndpointsForToken pending = StorageService.instance.getTokenMetadata().pendingEndpointsForToken(token, keyspace.getName());
        return forTokenWrite(natural, pending);
    }

    public static ReplicaLayout.ForTokenWrite forTokenWrite(EndpointsForToken natural, EndpointsForToken pending)
    {
        if (Endpoints.haveConflicts(natural, pending))
        {
            natural = Endpoints.resolveConflictsInNatural(natural, pending);
            pending = Endpoints.resolveConflictsInPending(natural, pending);
        }
        return new ReplicaLayout.ForTokenWrite(natural, pending);
    }

    public static ReplicaLayout.ForTokenRead forSingleReplicaRead(Token token, Replica replica)
    {
        // synthetically create a LiveForToken as well, without testing our liveness
        EndpointsForToken singleReplica = EndpointsForToken.of(token, replica);
        return new ReplicaLayout.ForTokenRead(singleReplica);
    }

    public static ReplicaLayout.ForRangeRead forSingleReplicaRead(AbstractBounds<PartitionPosition> range, Replica replica)
    {
        // synthetically create a LiveForToken as well, without testing our liveness
        // TODO: this EndpointsForRange.of is potentially dangerous - we should refactor AbstractBounds and Range<Token> into a single class
        EndpointsForRange singleReplica = EndpointsForRange.of(replica);
        return new ReplicaLayout.ForRangeRead(range, singleReplica);
    }

    public static ReplicaLayout.ForTokenRead forTokenReadLiveAndDown(Keyspace keyspace, Token token)
    {
        EndpointsForToken replicas = keyspace.getReplicationStrategy().getNaturalReplicasForToken(token);
        replicas = DatabaseDescriptor.getEndpointSnitch().sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), replicas);
        return new ReplicaLayout.ForTokenRead(replicas);
    }

    public static ReplicaLayout.ForRangeRead forRangeReadLiveAndDown(Keyspace keyspace, AbstractBounds<PartitionPosition> range)
    {
        EndpointsForRange replicas = keyspace.getReplicationStrategy().getNaturalReplicas(range.right);
        replicas = DatabaseDescriptor.getEndpointSnitch().sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), replicas);
        return new ReplicaLayout.ForRangeRead(range, replicas);
    }

}
