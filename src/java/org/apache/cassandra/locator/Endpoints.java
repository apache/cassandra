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

import org.apache.cassandra.locator.ReplicaCollection.Mutable.Conflict;
import org.apache.cassandra.utils.FBUtilities;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class Endpoints<E extends Endpoints<E>> extends AbstractReplicaCollection<E>
{
    static final Map<InetAddressAndPort, Replica> EMPTY_MAP = Collections.unmodifiableMap(new LinkedHashMap<>());

    volatile Map<InetAddressAndPort, Replica> byEndpoint;

    Endpoints(List<Replica> list, boolean isSnapshot, Map<InetAddressAndPort, Replica> byEndpoint)
    {
        super(list, isSnapshot);
        this.byEndpoint = byEndpoint;
    }

    @Override
    public Set<InetAddressAndPort> endpoints()
    {
        return byEndpoint().keySet();
    }

    public Map<InetAddressAndPort, Replica> byEndpoint()
    {
        Map<InetAddressAndPort, Replica> map = byEndpoint;
        if (map == null)
            byEndpoint = map = buildByEndpoint(list);
        return map;
    }

    public boolean contains(InetAddressAndPort endpoint, boolean isFull)
    {
        Replica replica = byEndpoint().get(endpoint);
        return replica != null && replica.isFull() == isFull;
    }

    @Override
    public boolean contains(Replica replica)
    {
        return replica != null
                && Objects.equals(
                        byEndpoint().get(replica.endpoint()),
                        replica);
    }

    private static Map<InetAddressAndPort, Replica> buildByEndpoint(List<Replica> list)
    {
        // TODO: implement a delegating map that uses our superclass' list, and is immutable
        Map<InetAddressAndPort, Replica> byEndpoint = new LinkedHashMap<>(list.size());
        for (Replica replica : list)
        {
            Replica prev = byEndpoint.put(replica.endpoint(), replica);
            assert prev == null : "duplicate endpoint in EndpointsForRange: " + prev + " and " + replica;
        }

        return Collections.unmodifiableMap(byEndpoint);
    }

    public E withoutSelf()
    {
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        return filter(r -> !self.equals(r.endpoint()));
    }

    public E without(Set<InetAddressAndPort> remove)
    {
        return filter(r -> !remove.contains(r.endpoint()));
    }

    public E keep(Set<InetAddressAndPort> keep)
    {
        return filter(r -> keep.contains(r.endpoint()));
    }

    public E keep(Iterable<InetAddressAndPort> endpoints)
    {
        ReplicaCollection.Mutable<E> copy = newMutable(
                endpoints instanceof Collection<?>
                        ? ((Collection<InetAddressAndPort>) endpoints).size()
                        : size()
        );
        Map<InetAddressAndPort, Replica> byEndpoint = byEndpoint();
        for (InetAddressAndPort endpoint : endpoints)
        {
            Replica keep = byEndpoint.get(endpoint);
            if (keep == null)
                continue;
            copy.add(keep, ReplicaCollection.Mutable.Conflict.DUPLICATE);
        }
        return copy.asSnapshot();
    }

    /**
     * Care must be taken to ensure no conflicting ranges occur in pending and natural.
     * Conflicts can occur for two reasons:
     *   1) due to lack of isolation when reading pending/natural
     *   2) because a movement that changes the type of replication from transient to full must be handled
     *      differently for reads and writes (with the reader treating it as transient, and writer as full)
     *
     * The method haveConflicts() below, and resolveConflictsInX, are used to detect and resolve any issues
     */
    public static <E extends Endpoints<E>> E concat(E natural, E pending)
    {
        return AbstractReplicaCollection.concat(natural, pending, Conflict.NONE);
    }

    public static <E extends Endpoints<E>> boolean haveConflicts(E natural, E pending)
    {
        Set<InetAddressAndPort> naturalEndpoints = natural.endpoints();
        for (InetAddressAndPort pendingEndpoint : pending.endpoints())
        {
            if (naturalEndpoints.contains(pendingEndpoint))
                return true;
        }
        return false;
    }

    // must apply first
    public static <E extends Endpoints<E>> E resolveConflictsInNatural(E natural, E pending)
    {
        return natural.filter(r -> !r.isTransient() || !pending.contains(r.endpoint(), true));
    }

    // must apply second
    public static <E extends Endpoints<E>> E resolveConflictsInPending(E natural, E pending)
    {
        return pending.without(natural.endpoints());
    }

}
