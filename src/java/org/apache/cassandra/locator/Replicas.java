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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A collection like class for Replica objects. Since the Replica class contains inetaddress, range, and
 * transient replication status, basic contains and remove methods can be ambiguous. Replicas forces you
 * to be explicit about what you're checking the container for, or removing from it.
 */
public abstract class Replicas implements Iterable<Replica>
{

    public abstract boolean add(Replica replica);
    public abstract void addAll(Iterable<Replica> replicas);
    public abstract void removeEndpoint(InetAddressAndPort endpoint);
    public abstract void removeReplica(Replica replica);
    public abstract int size();

    public Iterable<InetAddressAndPort> asEndpoints()
    {
        return Iterables.transform(this, Replica::getEndpoint);
    }

    public Set<InetAddressAndPort> asEndpointSet()
    {
        Set<InetAddressAndPort> result = Sets.newHashSetWithExpectedSize(size());
        for (Replica replica: this)
        {
            result.add(replica.getEndpoint());
        }
        return result;
    }

    public List<InetAddressAndPort> asEndpointList()
    {
        List<InetAddressAndPort> result = new ArrayList<>(size());
        for (Replica replica: this)
        {
            result.add(replica.getEndpoint());
        }
        return result;
    }

    public Iterable<Range<Token>> asRanges()
    {
        return Iterables.transform(this, Replica::getRange);
    }

    public Set<Range<Token>> asRangeSet()
    {
        Set<Range<Token>> result = Sets.newHashSetWithExpectedSize(size());
        for (Replica replica: this)
        {
            result.add(replica.getRange());
        }
        return result;
    }

    public Iterable<Range<Token>> fullRanges()
    {
        return Iterables.transform(Iterables.filter(this, Replica::isFull), Replica::getRange);
    }

    public boolean containsEndpoint(InetAddressAndPort endpoint)
    {
        return Iterables.any(this, r -> r.getEndpoint().equals(endpoint));
    }

    /**
     * Remove by endpoint. Ranges are ignored when determining what to remove
     */
    public void removeEndpoints(Replicas toRemove)
    {
        if (Iterables.all(this, Replica::isFull) && Iterables.all(toRemove, Replica::isFull))
        {
            for (Replica remove: toRemove)
            {
                removeEndpoint(remove.getEndpoint());
            }
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public void removeReplicas(Replicas toRemove)
    {
        if (Iterables.all(this, Replica::isFull) && Iterables.all(toRemove, Replica::isFull))
        {
            for (Replica remove: toRemove)
            {
                removeReplica(remove);
            }
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    private static abstract class ImmutableReplicaContainer extends Replicas
    {
        @Override
        public boolean add(Replica replica)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addAll(Iterable<Replica> replicas)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeEndpoint(InetAddressAndPort endpoint)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeReplica(Replica replica)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static Replicas filter(Replicas source, Predicate<Replica> predicate)
    {
        Iterable<Replica> iterable = Iterables.filter(source, predicate);
        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return Iterables.size(iterable);
            }

            public Iterator<Replica> iterator()
            {
                return iterable.iterator();
            }
        };
    }

    public static Replicas filterOnEndpoints(Replicas source, Predicate<InetAddressAndPort> predicate)
    {
        Iterable<Replica> iterable = Iterables.filter(source, r -> predicate.apply(r.getEndpoint()));
        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return Iterables.size(iterable);
            }

            public Iterator<Replica> iterator()
            {
                return iterable.iterator();
            }
        };
    }

    public static Replicas filterLocalEndpoint(Replicas replicas)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        return filterOnEndpoints(replicas, e -> !e.equals(local));
    }

    public static Replicas concatNaturalAndPending(Replicas natural, Replicas pending)
    {
        Iterable<Replica> iterable;
        if (Iterables.all(natural, Replica::isFull) && Iterables.all(pending, Replica::isFull))
        {
            iterable = Iterables.concat(natural, pending);
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }

        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return natural.size() + pending.size();
            }

            public Iterator<Replica> iterator()
            {
                return iterable.iterator();
            }
        };
    }

    public static Replicas concat(Iterable<Replicas> replicasIterable)
    {
        Iterable<Replica> iterable = Iterables.concat(replicasIterable);
        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return Iterables.size(iterable);
            }

            public Iterator<Replica> iterator()
            {
                return iterable.iterator();
            }
        };
    }

    public static Replicas of(Collection<Replica> replicas)
    {
        return new ImmutableReplicaContainer()
        {
            public int size()
            {
                return replicas.size();
            }

            public Iterator<Replica> iterator()
            {
                return replicas.iterator();
            }
        };
    }

    public static Replicas singleton(Replica replica)
    {
        return of(Collections.singleton(replica));
    }

    /**
     * Basically a placeholder for places new logic for transient replicas should go
     */
    public static void checkFull(Replica replica)
    {
        if (!replica.isFull())
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    /**
     * Basically a placeholder for places new logic for transient replicas should go
     */
    public static void checkFull(Iterable<Replica> replicas)
    {
        if (!Iterables.all(replicas, Replica::isFull))
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public static void checkAllFull(Iterable<? extends Replicas> replicas)
    {
        for (Replicas iterable: replicas)
        {
            checkFull(iterable);
        }
    }

    public static List<String> stringify(Replicas replicas, boolean withPort)
    {
        List<String> stringEndpoints = new ArrayList<>(replicas.size());
        for (Replica replica: replicas)
        {
            stringEndpoints.add(replica.getEndpoint().getHostAddress(withPort));
        }
        return stringEndpoints;
    }
}
