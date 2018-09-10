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

import com.google.common.collect.Iterables;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;

import java.util.function.Predicate;

public abstract class ReplicaPlan<
        E extends Endpoints<E>,
        P extends ReplicaPlan<E, P>>
{
    protected final Keyspace keyspace;
    protected final ConsistencyLevel consistencyLevel;

    // all nodes we will contact via any mechanism, including hints
    // i.e., for:
    //  - reads, only live natural replicas
    //      ==> liveOnly.natural().subList(0, blockFor + initial speculate)
    //  - writes, includes all full, and any pending replicas, (and only any necessary transient ones to make up the difference)
    //      ==> liveAndDown.natural().filter(isFull) ++ liveAndDown.pending() ++ liveOnly.natural.filter(isTransient, req)
    //  - paxos, includes all liveOnly replicas (natural+pending), for this DC if SERIAL_LOCAL
    //      ==> liveOnly.all()  (if consistencyLevel.isDCLocal(), then .filter(consistencyLevel.isLocal))
    private final E contact;

    ReplicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, E contact)
    {
        assert contact != null;
        this.keyspace = keyspace;
        this.consistencyLevel = consistencyLevel;
        this.contact = contact;
    }

    protected abstract P copy(ConsistencyLevel newConsistencyLevel, E newContact);

    public P withConsistencyLevel(ConsistencyLevel newConsistencylevel)
    {
        return copy(newConsistencylevel, contact);
    }
    public P withContact(E newContact)
    {
        return copy(consistencyLevel, newContact);
    }
    public E contact()
    {
        return contact;
    }
    public boolean contacts(Replica replica)
    {
        return contact.contains(replica);
    }

    public abstract int blockFor();
    public abstract void assureSufficientReplicas();

    public Keyspace keyspace()
    {
        return keyspace;
    }

    public ConsistencyLevel consistencyLevel()
    {
        return consistencyLevel;
    }

    public static abstract class ForRead<E extends Endpoints<E>, P extends ReplicaPlan<E, P>>
            extends ReplicaPlan<E, P>
    {
        // all nodes we *could* contact via any mechanism, including hints
        // i.e., for
        //   - reads, only live and natural endpoints
        //      ==> liveOnly.natural()
        //   - writes, includes all down, pending, full and transient nodes
        //      ==> liveAndDown.all()
        //   - paxos, includes all liveOnly replicas (natural+pending), for this DC if SERIAL_LOCAL
        //      ==> liveOnly.all()  (if consistencyLevel.isDCLocal(), then .filter(consistencyLevel.isLocal))
        private final E candidates;

        ForRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, E candidates, E contact)
        {
            super(keyspace, consistencyLevel, contact);
            this.candidates = candidates;
        }

        public E candidates()
        {
            return candidates;
        }
        public int blockFor()
        {
            return consistencyLevel.blockFor(keyspace);
        }
        public void assureSufficientReplicas()
        {
            consistencyLevel.assureSufficientReplicasForRead(keyspace, candidates());
        }
        public Replica getReplicaFor(InetAddressAndPort endpoint)
        {
            return candidates().byEndpoint().get(endpoint);
        }
        public E allUncontactedCandidates()
        {
            return candidates().filter(r -> !contacts(r));
        }
        public Replica firstUncontactedCandidate(Predicate<Replica> extraPredicate)
        {
            return Iterables.tryFind(candidates(), r -> extraPredicate.test(r) && !contacts(r)).orNull();
        }

        public String toString()
        {
            return "ReplicaPlan.ForRead [ CL: " + consistencyLevel + " keyspace: " + keyspace + " candidates: " + candidates + " contact: " + contact() + " ]";
        }
    }

    public static class ForTokenRead extends ForRead<EndpointsForToken, ReplicaPlan.ForTokenRead>
    {
        public ForTokenRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForToken candidates, EndpointsForToken contact)
        {
            super(keyspace, consistencyLevel, candidates, contact);
        }

        @Override
        protected ReplicaPlan.ForTokenRead copy(ConsistencyLevel newConsistencyLevel, EndpointsForToken newContact)
        {
            return new ReplicaPlan.ForTokenRead(keyspace, newConsistencyLevel, candidates(), newContact);
        }
    }

    public static class ForRangeRead extends ForRead<EndpointsForRange, ReplicaPlan.ForRangeRead>
    {
        final AbstractBounds<PartitionPosition> range;
        public ForRangeRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, AbstractBounds<PartitionPosition> range, EndpointsForRange candidates, EndpointsForRange contact)
        {
            super(keyspace, consistencyLevel, candidates, contact);
            this.range = range;
        }

        public AbstractBounds<PartitionPosition> range() { return range; }

        @Override
        protected ReplicaPlan.ForRangeRead copy(ConsistencyLevel newConsistencyLevel, EndpointsForRange newContact)
        {
            return new ReplicaPlan.ForRangeRead(keyspace, newConsistencyLevel, range, candidates(), newContact);
        }
    }

    public static abstract class ForWrite<E extends Endpoints<E>, P extends ReplicaPlan<E, P>>
            extends ReplicaPlan<E, P>
    {
        // TODO: this is only needed because of poor isolation of concerns elsewhere - we can remove it soon, and will do so in a follow-up patch
        final E pending;
        final E liveAndDown;
        final E liveOnly;

        ForWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, E pending, E liveAndDown, E liveOnly, E contact)
        {
            super(keyspace, consistencyLevel, contact);
            this.pending = pending;
            this.liveAndDown = liveAndDown;
            this.liveOnly = liveOnly;
        }

        public void assureSufficientReplicas()
        {
            consistencyLevel.assureSufficientReplicasForWrite(keyspace, liveOnly(), pending());
        }
        public int blockFor()
        {
            return consistencyLevel.blockForWrite(keyspace, pending());
        }

        public E pending() { return pending; }
        public E liveAndDown() { return liveAndDown; }
        public E liveOnly() { return liveOnly; }
        public E liveUncontacted()
        {
            return liveOnly().filter(r -> !contacts(r));
        }
        public boolean isAlive(Replica replica)
        {
            return liveOnly.endpoints().contains(replica.endpoint());
        }

        public String toString()
        {
            return "ReplicaPlan.ForWrite [ CL: " + consistencyLevel + " keyspace: " + keyspace + " liveAndDown: " + liveAndDown + " live: " + liveOnly + " contact: " + contact() +  " ]";
        }
    }

    public static class ForTokenWrite
            extends ForWrite<EndpointsForToken, ReplicaPlan.ForTokenWrite>
    {
        public ForTokenWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForToken pending, EndpointsForToken liveAndDown, EndpointsForToken liveOnly, EndpointsForToken contact)
        {
            super(keyspace, consistencyLevel, pending, liveAndDown, liveOnly, contact);
        }
        @Override
        protected ReplicaPlan.ForTokenWrite copy(ConsistencyLevel newConsistencyLevel, EndpointsForToken newContact)
        {
            return new ReplicaPlan.ForTokenWrite(keyspace, newConsistencyLevel, pending(), liveAndDown(), liveOnly(), newContact);
        }
    }

    public static class ForPaxosWrite
            extends ForWrite<EndpointsForToken, ReplicaPlan.ForPaxosWrite>
    {
        final int requiredParticipants;
        ForPaxosWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForToken pending, EndpointsForToken liveAndDown, EndpointsForToken liveOnly, EndpointsForToken contact, int requiredParticipants)
        {
            super(keyspace, consistencyLevel, pending, liveAndDown, liveOnly, contact);
            this.requiredParticipants = requiredParticipants;
        }

        @Override
        protected ReplicaPlan.ForPaxosWrite copy(ConsistencyLevel newConsistencyLevel, EndpointsForToken newContact)
        {
            return new ReplicaPlan.ForPaxosWrite(keyspace, newConsistencyLevel, pending(), liveAndDown(), liveOnly(), newContact, requiredParticipants);
        }

        public int requiredParticipants()
        {
            return requiredParticipants;
        }
    }

    /**
     * Used by AbstractReadExecutor, {Data,Digest}Resolver and ReadRepair to share a ReplicaPlan whose 'contact' replicas
     * we progressively modify via various forms of speculation (initial speculation, rr-read and rr-write)
     * @param <P>
     */
    public static class Shared<P extends ReplicaPlan<?, ?>>
    {
        private P replicaPlan;
        public Shared(P replicaPlan) { this.replicaPlan = replicaPlan; }
        public void set(P newReplicaPlan) { this.replicaPlan = newReplicaPlan; }
        public P get() { return replicaPlan; }
    }

}
