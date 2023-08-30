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
import java.util.function.Supplier;

public interface ReplicaPlan<E extends Endpoints<E>, P extends ReplicaPlan<E, P>>
{
    Keyspace keyspace();
    AbstractReplicationStrategy replicationStrategy();
    ConsistencyLevel consistencyLevel();

    E contacts();

    Replica lookup(InetAddressAndPort endpoint);
    P withContacts(E contacts);

    interface ForRead<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> extends ReplicaPlan<E, P>
    {
        int readQuorum();
        E readCandidates();

        default Replica firstUncontactedCandidate(Predicate<Replica> extraPredicate)
        {
            return Iterables.tryFind(readCandidates(), r -> extraPredicate.test(r) && !contacts().contains(r)).orNull();
        }
    }

    abstract class AbstractReplicaPlan<E extends Endpoints<E>, P extends ReplicaPlan<E, P>> implements ReplicaPlan<E, P>
    {
        protected final Keyspace keyspace;
        protected final ConsistencyLevel consistencyLevel;
        // The snapshot of the replication strategy when instantiating.
        // It could be different than the one fetched from Keyspace later, e.g. RS altered during the query.
        // Use the snapshot to calculate {@code blockFor} in order to have a consistent view of RS for the query.
        protected final AbstractReplicationStrategy replicationStrategy;

        // all nodes we will contact via any mechanism, including hints
        // i.e., for:
        //  - reads, only live natural replicas
        //      ==> live.natural().subList(0, blockFor + initial speculate)
        //  - writes, includes all full, and any pending replicas, (and only any necessary transient ones to make up the difference)
        //      ==> liveAndDown.natural().filter(isFull) ++ liveAndDown.pending() ++ live.natural.filter(isTransient, req)
        //  - paxos, includes all live replicas (natural+pending), for this DC if SERIAL_LOCAL
        //      ==> live.all()  (if consistencyLevel.isDCLocal(), then .filter(consistencyLevel.isLocal))
        private final E contacts;

        AbstractReplicaPlan(Keyspace keyspace, AbstractReplicationStrategy replicationStrategy, ConsistencyLevel consistencyLevel, E contacts)
        {
            assert contacts != null;
            this.keyspace = keyspace;
            this.replicationStrategy = replicationStrategy;
            this.consistencyLevel = consistencyLevel;
            this.contacts = contacts;
        }

        public E contacts() { return contacts; }

        public Keyspace keyspace() { return keyspace; }
        public AbstractReplicationStrategy replicationStrategy() { return replicationStrategy; }
        public ConsistencyLevel consistencyLevel() { return consistencyLevel; }
    }

    public static abstract class AbstractForRead<E extends Endpoints<E>, P extends ForRead<E, P>> extends AbstractReplicaPlan<E, P> implements ForRead<E, P>
    {
        // all nodes we *could* contacts; typically all natural replicas that are believed to be alive
        // we will consult this collection to find uncontacted nodes we might contact if we doubt we will meet consistency level
        final E candidates;

        AbstractForRead(Keyspace keyspace, AbstractReplicationStrategy replicationStrategy, ConsistencyLevel consistencyLevel, E candidates, E contacts)
        {
            super(keyspace, replicationStrategy, consistencyLevel, contacts);
            this.candidates = candidates;
        }

        public int readQuorum() { return consistencyLevel.blockFor(replicationStrategy); }

        public E readCandidates() { return candidates; }

        public Replica firstUncontactedCandidate(Predicate<Replica> extraPredicate)
        {
            return Iterables.tryFind(readCandidates(), r -> extraPredicate.test(r) && !contacts().contains(r)).orNull();
        }

        public Replica lookup(InetAddressAndPort endpoint)
        {
            return readCandidates().byEndpoint().get(endpoint);
        }

        public String toString()
        {
            return "ReplicaPlan.ForRead [ CL: " + consistencyLevel + " keyspace: " + keyspace + " candidates: " + candidates + " contacts: " + contacts() + " ]";
        }
    }

    public static class ForTokenRead extends AbstractForRead<EndpointsForToken, ForTokenRead>
    {
        public ForTokenRead(Keyspace keyspace,
                            AbstractReplicationStrategy replicationStrategy,
                            ConsistencyLevel consistencyLevel,
                            EndpointsForToken candidates,
                            EndpointsForToken contacts)
        {
            super(keyspace, replicationStrategy, consistencyLevel, candidates, contacts);
        }

        public ForTokenRead withContacts(EndpointsForToken newContact)
        {
            return new ForTokenRead(keyspace, replicationStrategy, consistencyLevel, candidates, newContact);
        }
    }

    public static class ForRangeRead extends AbstractForRead<EndpointsForRange, ForRangeRead>
    {
        final AbstractBounds<PartitionPosition> range;
        final int vnodeCount;

        public ForRangeRead(Keyspace keyspace,
                            AbstractReplicationStrategy replicationStrategy,
                            ConsistencyLevel consistencyLevel,
                            AbstractBounds<PartitionPosition> range,
                            EndpointsForRange candidates,
                            EndpointsForRange contact,
                            int vnodeCount)
        {
            super(keyspace, replicationStrategy, consistencyLevel, candidates, contact);
            this.range = range;
            this.vnodeCount = vnodeCount;
        }

        public AbstractBounds<PartitionPosition> range() { return range; }

        /**
         * @return number of vnode ranges covered by the range
         */
        public int vnodeCount() { return vnodeCount; }

        public ForRangeRead withContacts(EndpointsForRange newContact)
        {
            return new ForRangeRead(keyspace, replicationStrategy, consistencyLevel, range, readCandidates(), newContact, vnodeCount);
        }
    }

    public static class ForFullRangeRead extends ForRangeRead
    {
        public ForFullRangeRead(Keyspace keyspace,
                                AbstractReplicationStrategy replicationStrategy,
                                ConsistencyLevel consistencyLevel,
                                AbstractBounds<PartitionPosition> range,
                                EndpointsForRange candidates,
                                EndpointsForRange contact,
                                int vnodeCount)
        {
            super(keyspace, replicationStrategy, consistencyLevel, range, candidates, contact, vnodeCount);
        }

        @Override
        public int readQuorum()
        {
            return candidates.size();
        }
    }

    public static class ForWrite extends AbstractReplicaPlan<EndpointsForToken, ForWrite>
    {
        // TODO: this is only needed because of poor isolation of concerns elsewhere - we can remove it soon, and will do so in a follow-up patch
        final EndpointsForToken pending;
        final EndpointsForToken liveAndDown;
        final EndpointsForToken live;

        public ForWrite(Keyspace keyspace, AbstractReplicationStrategy replicationStrategy, ConsistencyLevel consistencyLevel, EndpointsForToken pending, EndpointsForToken liveAndDown, EndpointsForToken live, EndpointsForToken contact)
        {
            super(keyspace, replicationStrategy, consistencyLevel, contact);
            this.pending = pending;
            this.liveAndDown = liveAndDown;
            this.live = live;
        }

        public int writeQuorum() { return consistencyLevel.blockForWrite(replicationStrategy, pending()); }

        /** Replicas that a region of the ring is moving to; not yet ready to serve reads, but should receive writes */
        public EndpointsForToken pending() { return pending; }

        /** Replicas that can participate in the write - this always includes all nodes (pending and natural) in all DCs, except for paxos LOCAL_QUORUM (which is local DC only) */
        public EndpointsForToken liveAndDown() { return liveAndDown; }

        /** The live replicas present in liveAndDown, usually derived from FailureDetector.isReplicaAlive */
        public EndpointsForToken live() { return live; }

        /** Calculate which live endpoints we could have contacted, but chose not to */
        public EndpointsForToken liveUncontacted() { return live().filter(r -> !contacts().contains(r)); }

        /** Test liveness, consistent with the upfront analysis done for this operation (i.e. test membership of live()) */
        public boolean isAlive(Replica replica) { return live.endpoints().contains(replica.endpoint()); }

        public Replica lookup(InetAddressAndPort endpoint)
        {
            return liveAndDown().byEndpoint().get(endpoint);
        }

        private ForWrite copy(ConsistencyLevel newConsistencyLevel, EndpointsForToken newContact)
        {
            return new ForWrite(keyspace, replicationStrategy, newConsistencyLevel, pending(), liveAndDown(), live(), newContact);
        }

        ForWrite withConsistencyLevel(ConsistencyLevel newConsistencylevel) { return copy(newConsistencylevel, contacts()); }
        public ForWrite withContacts(EndpointsForToken newContact) { return copy(consistencyLevel, newContact); }

        public String toString()
        {
            return "ReplicaPlan.ForWrite [ CL: " + consistencyLevel + " keyspace: " + keyspace + " liveAndDown: " + liveAndDown + " live: " + live + " contacts: " + contacts() +  " ]";
        }
    }

    public static class ForPaxosWrite extends ForWrite
    {
        final int requiredParticipants;

        ForPaxosWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForToken pending, EndpointsForToken liveAndDown, EndpointsForToken live, EndpointsForToken contact, int requiredParticipants)
        {
            super(keyspace, keyspace.getReplicationStrategy(), consistencyLevel, pending, liveAndDown, live, contact);
            this.requiredParticipants = requiredParticipants;
        }

        public int requiredParticipants() { return requiredParticipants; }
    }

    /**
     * Used by AbstractReadExecutor, {Data,Digest}Resolver and ReadRepair to share a ReplicaPlan whose 'contacts' replicas
     * we progressively modify via various forms of speculation (initial speculation, rr-read and rr-write)
     *
     * The internal reference is not volatile, despite being shared between threads.  The initial reference provided to
     * the constructor should be visible by the normal process of sharing data between threads (i.e. executors, etc)
     * and any updates will either be seen or not seen, perhaps not promptly, but certainly not incompletely.
     * The contained ReplicaPlan has only final member properties, so it cannot be seen partially initialised.
     *
     * TODO: there's no reason this couldn't be achieved instead by a ReplicaPlan with mutable contacts,
     *       simplifying the hierarchy
     */
    public interface Shared<E extends Endpoints<E>, P extends ReplicaPlan<E, P>> extends Supplier<P>
    {
        /**
         * add the provided replica to this shared plan, by updating the internal reference
         */
        public void addToContacts(Replica replica);
        /**
         * get the shared replica plan, non-volatile (so maybe stale) but no risk of partially initialised
         */
        public P get();
    }

    public static class SharedForTokenRead implements Shared<EndpointsForToken, ForTokenRead>
    {
        private ForTokenRead replicaPlan;
        SharedForTokenRead(ForTokenRead replicaPlan) { this.replicaPlan = replicaPlan; }
        public void addToContacts(Replica replica) { replicaPlan = replicaPlan.withContacts(Endpoints.append(replicaPlan.contacts(), replica)); }
        public ForTokenRead get() { return replicaPlan; }
    }

    public static class SharedForRangeRead implements Shared<EndpointsForRange, ForRangeRead>
    {
        private ForRangeRead replicaPlan;
        SharedForRangeRead(ForRangeRead replicaPlan) { this.replicaPlan = replicaPlan; }
        public void addToContacts(Replica replica) { replicaPlan = replicaPlan.withContacts(Endpoints.append(replicaPlan.contacts(), replica)); }
        public ForRangeRead get() { return replicaPlan; }
    }

    public static SharedForTokenRead shared(ForTokenRead replicaPlan) { return new SharedForTokenRead(replicaPlan); }
    public static SharedForRangeRead shared(ForRangeRead replicaPlan) { return new SharedForRangeRead(replicaPlan); }

}
