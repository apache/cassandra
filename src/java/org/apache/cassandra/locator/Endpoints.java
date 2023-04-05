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

import org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict;
import org.apache.cassandra.utils.FBUtilities;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A collection of Endpoints for a given ring position.  This will typically reside in a ReplicaLayout,
 * representing some subset of the endpoints for the Token or Range
 * @param <E> The concrete type of Endpoints, that will be returned by the modifying methods
 */
public abstract class Endpoints<E extends Endpoints<E>> extends AbstractReplicaCollection<E>
{
    static ReplicaMap<InetAddressAndPort> endpointMap(ReplicaList list) { return new ReplicaMap<>(list, Replica::endpoint); }
    static final ReplicaMap<InetAddressAndPort> EMPTY_MAP = endpointMap(EMPTY_LIST);

    // volatile not needed, as has only final members,
    // besides (transitively) those that cache objects that themselves have only final members
    ReplicaMap<InetAddressAndPort> byEndpoint;

    Endpoints(ReplicaList list, ReplicaMap<InetAddressAndPort> byEndpoint)
    {
        super(list);
        this.byEndpoint = byEndpoint;
    }

    @Override
    public Set<InetAddressAndPort> endpoints()
    {
        return byEndpoint().keySet();
    }

    public InetAddressAndPort endpoint(int i)
    {
        return get(i).endpoint();
    }

    public List<InetAddressAndPort> endpointList()
    {
        return asList(Replica::endpoint);
    }

    public Map<InetAddressAndPort, Replica> byEndpoint()
    {
        ReplicaMap<InetAddressAndPort> map = byEndpoint;
        if (map == null)
            byEndpoint = map = endpointMap(list);
        return map;
    }

    @Override
    public boolean contains(Replica replica)
    {
        return replica != null
                && Objects.equals(
                        byEndpoint().get(replica.endpoint()),
                        replica);
    }

    public boolean contains(InetAddressAndPort endpoint)
    {
        return endpoint != null && byEndpoint().containsKey(endpoint);
    }

    public E withoutSelf()
    {
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        return filter(r -> !self.equals(r.endpoint()));
    }

    public Replica selfIfPresent()
    {
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        return byEndpoint().get(self);
    }

    /**
     * @return a collection without the provided endpoints, otherwise in the same order as this collection
     */
    public E without(Set<InetAddressAndPort> remove)
    {
        return filter(r -> !remove.contains(r.endpoint()));
    }

    /**
     * @return a collection with only the provided endpoints (ignoring any not present), otherwise in the same order as this collection
     */
    public E keep(Set<InetAddressAndPort> keep)
    {
        return filter(r -> keep.contains(r.endpoint()));
    }

    /**
     * @return a collection containing the Replica from this collection for the provided endpoints, in the order of the provided endpoints
     */
    public E select(Iterable<InetAddressAndPort> endpoints, boolean ignoreMissing)
    {
        Builder<E> copy = newBuilder(
                endpoints instanceof Collection<?>
                        ? ((Collection<InetAddressAndPort>) endpoints).size()
                        : size()
        );
        Map<InetAddressAndPort, Replica> byEndpoint = byEndpoint();
        for (InetAddressAndPort endpoint : endpoints)
        {
            Replica select = byEndpoint.get(endpoint);
            if (select == null)
            {
                if (!ignoreMissing)
                    throw new IllegalArgumentException(endpoint + " is not present in " + this);
                continue;
            }
            copy.add(select, Builder.Conflict.DUPLICATE);
        }
        return copy.build();
    }

    /**
     * Care must be taken to ensure no conflicting ranges occur in pending and natural.
     * Conflicts can occur for two reasons:
     *   1) due to lack of isolation when reading pending/natural
     *   2) because a movement that changes the type of replication from transient to full must be handled
     *      differently for reads and writes (with the reader treating it as transient, and writer as full)
     *
     * The method {@link ReplicaLayout#haveWriteConflicts} can be used to detect and resolve any issues
     */
    public static <E extends Endpoints<E>> E concat(E natural, E pending)
    {
        return AbstractReplicaCollection.concat(natural, pending, Conflict.NONE);
    }

    public static <E extends Endpoints<E>> E append(E replicas, Replica extraReplica)
    {
        Builder<E> builder = replicas.newBuilder(replicas.size() + 1);
        builder.addAll(replicas);
        builder.add(extraReplica, Conflict.NONE);
        return builder.build();
    }

}
