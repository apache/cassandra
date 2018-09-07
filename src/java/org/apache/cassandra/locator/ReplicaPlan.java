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

import java.util.function.Predicate;

public abstract class ReplicaPlan<
        E extends Endpoints<E>,
        L extends ReplicaLayout<E>,
        P extends ReplicaPlan<E, ? extends L, P>>
{
    protected final Keyspace keyspace;
    protected final ConsistencyLevel consistencyLevel;

    private final L liveAndDown;
    private final L liveOnly;

    // all nodes we *could* contact via any mechanism, including hints
    // i.e., for
    //   - reads, only live and natural endpoints
    //      ==> liveOnly.natural()
    //   - writes, includes all down, pending, full and transient nodes (liveAndDown.all())
    //      ==> liveAndDown.natural(isFull) ++ liveAndDown.pending()
    //   - paxos, includes all liveOnly replicas (natural+pending), for this DC if SERIAL_LOCAL
    //      ==> liveOnly.all()  (if consistencyLevel.isDCLocal(), then .filter(consistencyLevel.isLocal))
    private final E candidates;

    // all nodes we will contact via any mechanism, including hints
    // i.e., for:
    //  - reads, only live natural replicas
    //      ==> liveOnly.natural().subList(0, blockFor + initial speculate)
    //  - writes, includes all full, and any pending replicas, (and only any necessary transient ones to make up the difference)
    //      ==> liveAndDown.natural().filter(isFull) ++ liveAndDown.pending() ++ liveOnly.natural.filter(isTransient, req)
    //  - paxos, includes all liveOnly replicas (natural+pending), for this DC if SERIAL_LOCAL
    //      ==> liveOnly.all()  (if consistencyLevel.isDCLocal(), then .filter(consistencyLevel.isLocal))
    private final E contact;

    ReplicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, L liveAndDown, L liveOnly, E candidates, E contact)
    {
        assert contact != null;
        this.keyspace = keyspace;
        this.consistencyLevel = consistencyLevel;
        this.liveAndDown = liveAndDown;
        this.liveOnly = liveOnly;
        this.candidates = candidates;
        this.contact = contact;
    }

    protected abstract P copy(ConsistencyLevel newConsistencyLevel, E newContact);

    public P withContact(E newContact)
    {
        return copy(consistencyLevel, newContact);
    }

    public P withConsistencyLevel(ConsistencyLevel newConsistencylevel)
    {
        return copy(newConsistencylevel, contact);
    }

    public E contact()
    {
        return contact;
    }
    public E candidates()
    {
        return candidates;
    }

    public L liveAndDown()
    {
        return liveAndDown;
    }
    public L liveOnly()
    {
        return liveOnly;
    }

    public E allLiveUncontactedCandidates()
    {
        return candidates.filter(r -> !contacts(r) && isAlive(r));
    }

    public Replica firstLiveUncontactedCandidate(Predicate<Replica> extraPredicate)
    {
        return Iterables.tryFind(candidates, r -> extraPredicate.test(r) && !contacts(r) && isAlive(r)).orNull();
    }

    public boolean isAlive(Replica replica)
    {
        return liveOnly.all().endpoints().contains(replica.endpoint());
    }

    public boolean contacts(Replica replica)
    {
        return contact.contains(replica);
    }

    public abstract int blockFor();
    public abstract void assureSufficientReplicas();

    public Replica getReplicaFor(InetAddressAndPort endpoint)
    {
        return liveAndDown.all().byEndpoint().get(endpoint);
    }

    public Keyspace keyspace()
    {
        return keyspace;
    }

    public ConsistencyLevel consistencyLevel()
    {
        return consistencyLevel;
    }

    public String toString()
    {
        return "ReplicaPlan [ CL: " + consistencyLevel + " keyspace: " + keyspace + " layout: " + liveAndDown + " liveOnly: " + liveOnly() + " selected:" + contact + " ]";
    }

    public static abstract class ForRead<E extends Endpoints<E>, L extends ReplicaLayout<E>, P extends ReplicaPlan<E, L, P>>
            extends ReplicaPlan<E, L, P> 
    {
        ForRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, L liveAndDown, L liveOnly, E candidates, E contact)
        {
            super(keyspace, consistencyLevel, liveAndDown, liveOnly, candidates, contact);
        }
        public int blockFor()
        {
            return consistencyLevel.blockFor(keyspace);
        }
        public void assureSufficientReplicas()
        {
            consistencyLevel.assureSufficientReplicasForRead(keyspace, liveOnly().all());
        }
    }

    public static class ForTokenRead extends ForRead<EndpointsForToken, ReplicaLayout.ForTokenRead, ReplicaPlan.ForTokenRead>
    {
        public ForTokenRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaLayout.ForTokenRead liveAndDown, ReplicaLayout.ForTokenRead liveOnly, EndpointsForToken candidates, EndpointsForToken contact)
        {
            super(keyspace, consistencyLevel, liveAndDown, liveOnly, candidates, contact);
        }

        @Override
        protected ReplicaPlan.ForTokenRead copy(ConsistencyLevel newConsistencyLevel, EndpointsForToken newContact)
        {
            return new ReplicaPlan.ForTokenRead(keyspace, newConsistencyLevel, liveAndDown(), liveOnly(), candidates(), newContact);
        }
    }

    public static class ForRangeRead extends ForRead<EndpointsForRange, ReplicaLayout.ForRangeRead, ReplicaPlan.ForRangeRead>
    {
        public ForRangeRead(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaLayout.ForRangeRead liveAndDown, ReplicaLayout.ForRangeRead liveOnly, EndpointsForRange candidates, EndpointsForRange contact)
        {
            super(keyspace, consistencyLevel, liveAndDown, liveOnly, candidates, contact);
        }

        @Override
        protected ReplicaPlan.ForRangeRead copy(ConsistencyLevel newConsistencyLevel, EndpointsForRange newContact)
        {
            return new ReplicaPlan.ForRangeRead(keyspace, newConsistencyLevel, liveAndDown(), liveOnly(), candidates(), newContact);
        }
    }

    public static abstract class ForWrite<E extends Endpoints<E>, L extends ReplicaLayout.ForWrite<E>, P extends ReplicaPlan<E, L, P>>
            extends ReplicaPlan<E, L, P>
    {
        ForWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, L liveAndDown, L liveOnly, E candidates, E contact)
        {
            super(keyspace, consistencyLevel, liveAndDown, liveOnly, candidates, contact);
        }
        public int blockFor()
        {
            return consistencyLevel.blockForWrite(keyspace, liveAndDown().pending);
        }
        public void assureSufficientReplicas()
        {
            consistencyLevel.assureSufficientReplicasForWrite(keyspace, liveOnly().all(), liveAndDown().pending());
        }
    }

    public static class ForTokenWrite
            extends ForWrite<EndpointsForToken, ReplicaLayout.ForTokenWrite, ReplicaPlan.ForTokenWrite>
    {
        public ForTokenWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaLayout.ForTokenWrite liveAndDown, ReplicaLayout.ForTokenWrite liveOnly, EndpointsForToken candidates, EndpointsForToken contact)
        {
            super(keyspace, consistencyLevel, liveAndDown, liveOnly, candidates, contact);
        }
        @Override
        protected ReplicaPlan.ForTokenWrite copy(ConsistencyLevel newConsistencyLevel, EndpointsForToken newContact)
        {
            return new ReplicaPlan.ForTokenWrite(keyspace, newConsistencyLevel, liveAndDown(), liveOnly(), candidates(), newContact);
        }
    }

    public static class ForPaxosWrite
            extends ForWrite<EndpointsForToken, ReplicaLayout.ForTokenWrite, ReplicaPlan.ForPaxosWrite>
    {
        final int requiredParticipants;
        ForPaxosWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaLayout.ForTokenWrite liveAndDown, ReplicaLayout.ForTokenWrite liveOnly, EndpointsForToken candidates, EndpointsForToken contact, int requiredParticipants)
        {
            super(keyspace, consistencyLevel, liveAndDown, liveOnly, candidates, contact);
            this.requiredParticipants = requiredParticipants;
        }

        @Override
        protected ReplicaPlan.ForPaxosWrite copy(ConsistencyLevel newConsistencyLevel, EndpointsForToken newContact)
        {
            return new ReplicaPlan.ForPaxosWrite(keyspace, newConsistencyLevel, liveAndDown(), liveOnly(), candidates(), newContact, requiredParticipants);
        }

        public int requiredParticipants()
        {
            return requiredParticipants;
        }
    }

    public static class ForRangeWrite
            extends ForWrite<EndpointsForRange, ReplicaLayout.ForRangeWrite, ReplicaPlan.ForRangeWrite>
    {
        public ForRangeWrite(Keyspace keyspace, ConsistencyLevel consistencyLevel, ReplicaLayout.ForRangeWrite liveAndDown, ReplicaLayout.ForRangeWrite liveOnly, EndpointsForRange candidates, EndpointsForRange contact)
        {
            super(keyspace, consistencyLevel, liveAndDown, liveOnly, candidates, contact);
        }
        @Override
        protected ReplicaPlan.ForRangeWrite copy(ConsistencyLevel newConsistencyLevel, EndpointsForRange newContact)
        {
            return new ReplicaPlan.ForRangeWrite(keyspace, newConsistencyLevel, liveAndDown(), liveOnly(), candidates(), newContact);
        }
    }

}
