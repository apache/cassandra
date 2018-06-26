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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class ReplicaList extends ReplicaCollection
{
    static final ReplicaList EMPTY = new ReplicaList(ImmutableList.of());

    private final List<Replica> replicaList;

    public ReplicaList()
    {
        this(new ArrayList<>());
    }

    public ReplicaList(int capacity)
    {
        this(new ArrayList<>(capacity));
    }

    public ReplicaList(ReplicaList from)
    {
        this(new ArrayList<>(from.replicaList));
    }

    public ReplicaList(ReplicaCollection from)
    {
        this(new ArrayList<>(from.size()));
        addAll(from);
    }

    public ReplicaList(Collection<Replica> from)
    {
        this(new ArrayList<>(from));
    }

    private ReplicaList(List<Replica> replicaList)
    {
        this.replicaList = replicaList;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaList that = (ReplicaList) o;
        return Objects.equals(replicaList, that.replicaList);
    }

    public int hashCode()
    {
        return replicaList.hashCode();
    }

    @Override
    public String toString()
    {
        return replicaList.toString();
    }

    @Override
    public boolean add(Replica replica)
    {
        Preconditions.checkNotNull(replica);
        return replicaList.add(replica);
    }

    @Override
    public void addAll(Iterable<Replica> replicas)
    {
        Iterables.addAll(replicaList, replicas);
    }

    @Override
    public int size()
    {
        return replicaList.size();
    }

    @Override
    protected Collection<Replica> getUnmodifiableCollection()
    {
        return Collections.unmodifiableCollection(replicaList);
    }

    @Override
    public Iterator<Replica> iterator()
    {
        return replicaList.iterator();
    }

    public Replica get(int idx)
    {
        return replicaList.get(idx);
    }

    @Override
    public void removeEndpoint(InetAddressAndPort endpoint)
    {
        for (int i=replicaList.size()-1; i>=0; i--)
        {
            if (replicaList.get(i).getEndpoint().equals(endpoint))
            {
                replicaList.remove(i);
            }
        }
    }

    @Override
    public void removeReplica(Replica replica)
    {
        replicaList.remove(replica);
    }

    @Override
    public boolean containsEndpoint(InetAddressAndPort endpoint)
    {
        for (int i=0; i<size(); i++)
        {
            if (replicaList.get(i).getEndpoint().equals(endpoint))
                return true;
        }
        return false;
    }

    public ReplicaList filter(Predicate<Replica> predicate)
    {
        ArrayList<Replica> newReplicaList = size() < 10 ? new ArrayList<>(size()) : new ArrayList<>();
        for (int i=0; i<size(); i++)
        {
            Replica replica = replicaList.get(i);
            if (predicate.test(replica))
            {
                newReplicaList.add(replica);
            }
        }
        return new ReplicaList(newReplicaList);
    }

    public void sort(Comparator<Replica> comparator)
    {
        replicaList.sort(comparator);
    }

    public static ReplicaList intersectEndpoints(ReplicaList l1, ReplicaList l2)
    {
        Replicas.checkFull(l1);
        Replicas.checkFull(l2);
        // Note: we don't use Guava Sets.intersection() for 3 reasons:
        //   1) retainAll would be inefficient if l1 and l2 are large but in practice both are the replicas for a range and
        //   so will be very small (< RF). In that case, retainAll is in fact more efficient.
        //   2) we do ultimately need a list so converting everything to sets don't make sense
        //   3) l1 and l2 are sorted by proximity. The use of retainAll  maintain that sorting in the result, while using sets wouldn't.
        Collection<InetAddressAndPort> endpoints = l2.asEndpointList();
        return l1.filter(r -> endpoints.contains(r.getEndpoint()));
    }

    public static ReplicaList of()
    {
        return new ReplicaList(0);
    }

    public static ReplicaList of(Replica replica)
    {
        ReplicaList replicaList = new ReplicaList(1);
        replicaList.add(replica);
        return replicaList;
    }

    public static ReplicaList of(Replica... replicas)
    {
        ReplicaList replicaList = new ReplicaList(replicas.length);
        for (Replica replica: replicas)
        {
            replicaList.add(replica);
        }
        return replicaList;
    }

    public ReplicaList subList(int fromIndex, int toIndex)
    {
        return new ReplicaList(replicaList.subList(fromIndex, toIndex));
    }

    public ReplicaList normalizeByRange()
    {
        if (Iterables.all(this, Replica::isFull))
        {
            ReplicaList normalized = new ReplicaList(size());
            for (Replica replica: this)
            {
                replica.addNormalizeByRange(normalized);
            }

            return normalized;
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public static ReplicaList immutableCopyOf(ReplicaCollection replicas)
    {
        return new ReplicaList(ImmutableList.<Replica>builder().addAll(replicas).build());
    }

    public static ReplicaList immutableCopyOf(ReplicaList replicas)
    {
        return new ReplicaList(ImmutableList.copyOf(replicas.replicaList));
    }

    public static ReplicaList immutableCopyOf(Replica... replicas)
    {
        return new ReplicaList(ImmutableList.copyOf(replicas));
    }

    public static ReplicaList empty()
    {
        return new ReplicaList();
    }

    /**
     * For allocating ReplicaLists where the final size is unknown, but
     * should be less than the given size. Prevents overallocations in cases
     * where there are less than the default ArrayList size, and defers to the
     * ArrayList algorithm where there might be more
     */
    public static ReplicaList withMaxSize(int size)
    {
        return size < 10 ? new ReplicaList(size) : new ReplicaList();
    }
}
