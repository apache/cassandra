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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public abstract class Replicas implements Iterable<Replica>
{

    public abstract void add(Replica replica);
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

    public static Replicas concatNaturalAndPendingAndFilter(Replicas natural, Replicas pending, Predicate<Replica> filter)
    {
        Iterable<Replica> unfilteredIterable;
        if (Iterables.all(natural, Replica::isFull) && Iterables.all(pending, Replica::isFull))
        {
            unfilteredIterable = Iterables.concat(natural, pending);
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }

        Iterable<Replica> iterable = filter != null ? Iterables.filter(unfilteredIterable, filter) : unfilteredIterable;

        return new Replicas()
        {
            public void add(Replica replica)
            {
                throw new UnsupportedOperationException();
            }

            public void addAll(Iterable<Replica> replicas)
            {
                throw new UnsupportedOperationException();
            }

            public void removeEndpoint(InetAddressAndPort endpoint)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void removeReplica(Replica replica)
            {
                throw new UnsupportedOperationException();
            }

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

    public static Replicas concatNaturalAndPending(Replicas natural, Replicas pending)
    {
        return concatNaturalAndPendingAndFilter(natural, pending, null);
    }

    public static Replicas of(Collection<Replica> replicas)
    {
        return new Replicas()
        {
            public void add(Replica replica)
            {
                throw new UnsupportedOperationException();
            }

            public void addAll(Iterable<Replica> replicas)
            {
                throw new UnsupportedOperationException();
            }

            public void removeEndpoint(InetAddressAndPort endpoint)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void removeReplica(Replica replica)
            {
                throw new UnsupportedOperationException();
            }

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

}
