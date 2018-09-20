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

public abstract class ReplicaPlan<E extends Endpoints<E>>
{
    protected final Keyspace keyspace;
    protected final ConsistencyLevel consistencyLevel;

    // all nodes we will contact via any mechanism, including hints
    // i.e., for:
    //  - reads, only live natural replicas
    //      ==> live.natural().subList(0, blockFor + initial speculate)
    //  - writes, includes all full, and any pending replicas, (and only any necessary transient ones to make up the difference)
    //      ==> liveAndDown.natural().filter(isFull) ++ liveAndDown.pending() ++ live.natural.filter(isTransient, req)
    //  - paxos, includes all live replicas (natural+pending), for this DC if SERIAL_LOCAL
    //      ==> live.all()  (if consistencyLevel.isDCLocal(), then .filter(consistencyLevel.isLocal))
    private final E contacts;

    ReplicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, E contacts)
    {
        assert contacts != null;
        this.keyspace = keyspace;
        this.consistencyLevel = consistencyLevel;
        this.contacts = contacts;
    }

    public abstract int blockFor();

    public E contacts() { return contacts; }
    public boolean contacts(Replica replica) { return contacts.contains(replica); }
    public Keyspace keyspace() { return keyspace; }
    public ConsistencyLevel consistencyLevel() { return consistencyLevel; }

    public static abstract class ForRead<E extends Endpoints<E>> extends ReplicaPlan<E>
    {
        // all nodes we *could* contacts; typically all natural replicas that are believed to be alive
        // we will consult this collection to find uncontacted nodes we might contact if we doubt we will meet consistency level
        private final E candidates;

        ForRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, E candidates, E contact)
        {
            super(keyspace, consistencyLevel, contact);
            this.candidates = candidates;
        }

        public int blockFor() { return consistencyLevel.blockFor(keyspace); }

        public E candidates() { return candidates; }

        public E uncontactedCandidates()
        {
            return candidates().filter(r -> !contacts(r));
        }

        public Replica firstUncontactedCandidate(Predicate<Replica> extraPredicate)
        {
            return Iterables.tryFind(candidates(), r -> extraPredicate.test(r) && !contacts(r)).orNull();
        }

        public Replica getReplicaFor(InetAddressAndPort endpoint)
        {
            return candidates().byEndpoint().get(endpoint);
        }

        public String toString()
        {
            return "ReplicaPlan.ForRead [ CL: " + consistencyLevel + " keyspace: " + keyspace + " candidates: " + candidates + " contacts: " + contacts() + " ]";
        }
    }

    public static class ForTokenRead extends ForRead<EndpointsForToken>
    {
        public ForTokenRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForToken candidates, EndpointsForToken contact)
        {
            super(keyspace, consistencyLevel, candidates, contact);
        }

        ForTokenRead withContact(EndpointsForToken newContact)
        {
            return new ForTokenRead(keyspace, consistencyLevel, candidates(), newContact);
        }
    }

    public static class ForRangeRead extends ForRead<EndpointsForRange>
    {
        final AbstractBounds<PartitionPosition> range;

        public ForRangeRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, AbstractBounds<PartitionPosition> range, EndpointsForRange candidates, EndpointsForRange contact)
        {
            super(keyspace, consistencyLevel, candidates, contact);
            this.range = range;
        }

        public AbstractBounds<PartitionPosition> range() { return range; }

        ForRangeRead withContact(EndpointsForRange newContact)
        {
            return new ForRangeRead(keyspace, consistencyLevel, range, candidates(), newContact);
        }
    }

    public static abstract class ForWrite<E extends Endpoints<E>> extends ReplicaPlan<E>
    {
        // TODO: this is only needed because of poor isolation of concerns elsewhere - we can remove it soon, and will do so in a follow-up patch
        final E pending;
        final E liveAndDown;
        final E live;

        ForWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, E pending, E liveAndDown, E live, E contact)
        {
            super(keyspace, consistencyLevel, contact);
            this.pending = pending;
            this.liveAndDown = liveAndDown;
            this.live = live;
        }

        public int blockFor() { return consistencyLevel.blockForWrite(keyspace, pending()); }

        /** Replicas that a region of the ring is moving to; not yet ready to serve reads, but should receive writes */
        public E pending() { return pending; }
        /** Replicas that can participate in the write - this always includes all nodes (pending and natural) in all DCs, except for paxos LOCAL_QUORUM (which is local DC only) */
        public E liveAndDown() { return liveAndDown; }
        /** The live replicas present in liveAndDown, usually derived from FailureDetector.isReplicaAlive */
        public E live() { return live; }
        /** Calculate which live endpoints we could have contacted, but chose not to */
        public E liveUncontacted() { return live().filter(r -> !contacts(r)); }
        /** Test liveness, consistent with the upfront analysis done for this operation (i.e. test membership of live()) */
        public boolean isAlive(Replica replica) { return live.endpoints().contains(replica.endpoint()); }

        public String toString()
        {
            return "ReplicaPlan.ForWrite [ CL: " + consistencyLevel + " keyspace: " + keyspace + " liveAndDown: " + liveAndDown + " live: " + live + " contacts: " + contacts() +  " ]";
        }
    }

    public static class ForTokenWrite extends ForWrite<EndpointsForToken>
    {
        public ForTokenWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForToken pending, EndpointsForToken liveAndDown, EndpointsForToken live, EndpointsForToken contact)
        {
            super(keyspace, consistencyLevel, pending, liveAndDown, live, contact);
        }

        private ReplicaPlan.ForTokenWrite copy(ConsistencyLevel newConsistencyLevel, EndpointsForToken newContact)
        {
            return new ReplicaPlan.ForTokenWrite(keyspace, newConsistencyLevel, pending(), liveAndDown(), live(), newContact);
        }

        ForTokenWrite withConsistencyLevel(ConsistencyLevel newConsistencylevel) { return copy(newConsistencylevel, contacts()); }
        public ForTokenWrite withContact(EndpointsForToken newContact) { return copy(consistencyLevel, newContact); }
    }

    public static class ForPaxosWrite extends ForWrite<EndpointsForToken>
    {
        final int requiredParticipants;

        ForPaxosWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForToken pending, EndpointsForToken liveAndDown, EndpointsForToken live, EndpointsForToken contact, int requiredParticipants)
        {
            super(keyspace, consistencyLevel, pending, liveAndDown, live, contact);
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
     */
    public interface Shared<E extends Endpoints<E>, P extends ReplicaPlan<E>>
    {
        /**
         * add the provided replica to this shared plan, by updating the internal reference
         */
        public void addToContacts(Replica replica);
        /**
         * get the shared replica plan, non-volatile (so maybe stale) but no risk of partially initialised
         */
        public P get();
        /**
         * get the shared replica plan, non-volatile (so maybe stale) but no risk of partially initialised,
         * but replace its 'contacts' with those provided
         */
        public abstract P getWithContacts(E endpoints);
    }

    public static class SharedForTokenRead implements Shared<EndpointsForToken, ForTokenRead>
    {
        private ForTokenRead replicaPlan;
        SharedForTokenRead(ForTokenRead replicaPlan) { this.replicaPlan = replicaPlan; }
        public void addToContacts(Replica replica) { replicaPlan = replicaPlan.withContact(Endpoints.append(replicaPlan.contacts(), replica)); }
        public ForTokenRead get() { return replicaPlan; }
        public ForTokenRead getWithContacts(EndpointsForToken newContact) { return replicaPlan.withContact(newContact); }
    }

    public static class SharedForRangeRead implements Shared<EndpointsForRange, ForRangeRead>
    {
        private ForRangeRead replicaPlan;
        SharedForRangeRead(ForRangeRead replicaPlan) { this.replicaPlan = replicaPlan; }
        public void addToContacts(Replica replica) { replicaPlan = replicaPlan.withContact(Endpoints.append(replicaPlan.contacts(), replica)); }
        public ForRangeRead get() { return replicaPlan; }
        public ForRangeRead getWithContacts(EndpointsForRange newContact) { return replicaPlan.withContact(newContact); }
    }

    public static SharedForTokenRead shared(ForTokenRead replicaPlan) { return new SharedForTokenRead(replicaPlan); }
    public static SharedForRangeRead shared(ForRangeRead replicaPlan) { return new SharedForRangeRead(replicaPlan); }

}
