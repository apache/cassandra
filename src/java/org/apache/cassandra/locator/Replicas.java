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
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

public class Replicas
{
    public static Collection<InetAddressAndPort> asEndpoints(Collection<Replica> replicas)
    {
        return Collections2.transform(replicas, Replica::getEndpoint);
    }

    public static Collection<Range<Token>> asRanges(Collection<Replica> replicas)
    {
        return Collections2.transform(replicas, Replica::getRange);
    }

    public static Iterable<Range<Token>> asRanges(Iterable<Replica> replicas)
    {
        return Iterables.transform(replicas, Replica::getRange);
    }

    public static Iterable<InetAddressAndPort> asEndpoints(Iterable<Replica> replicas)
    {
        return Iterables.transform(replicas, Replica::getEndpoint);
    }

    public static List<InetAddressAndPort> asEndpointList(Collection<Replica> replicas)
    {
        List<InetAddressAndPort> endpoints = new ArrayList<>(replicas.size());
        endpoints.addAll(asEndpoints(replicas));
        return endpoints;
    }

    public static Set<InetAddressAndPort> asEndpointSet(Collection<Replica> replicas)
    {
        Set<InetAddressAndPort> endpoints = Sets.newHashSetWithExpectedSize(replicas.size());
        endpoints.addAll(asEndpoints(replicas));
        return endpoints;
    }

    public static List<Replica> listIntersection(List<Replica> l1, List<Replica> l2)
    {
        Replicas.checkFull(l1);
        Replicas.checkFull(l2);
        // Note: we don't use Guava Sets.intersection() for 3 reasons:
        //   1) retainAll would be inefficient if l1 and l2 are large but in practice both are the replicas for a range and
        //   so will be very small (< RF). In that case, retainAll is in fact more efficient.
        //   2) we do ultimately need a list so converting everything to sets don't make sense
        //   3) l1 and l2 are sorted by proximity. The use of retainAll  maintain that sorting in the result, while using sets wouldn't.
        Collection<InetAddressAndPort> endpoints = asEndpointList(l2);
        List<Replica> result = new ArrayList<>(l1.size());
        result.addAll(Collections2.filter(l1, r -> endpoints.contains(r.getEndpoint())));
        return result;
    }

    public static Iterable<Range<Token>> fullRanges(Collection<Replica> replicas)
    {
        return asRanges(Iterables.filter(replicas, Replica::isFull));
    }

    public static List<Replica> fullStandins(Collection<InetAddressAndPort> endpoints)
    {
        return Lists.newArrayList(Iterables.transform(endpoints, Replica::fullStandin));
    }

    public static Iterable<Replica> decorateRanges(InetAddressAndPort endpoint, boolean full, Iterable<Range<Token>> ranges)
    {
        return Iterables.transform(ranges, range -> new Replica(endpoint, range, full));

    }

    public static List<Replica> normalize(Collection<Replica> replicas)
    {
        if (Iterables.all(replicas, Replica::isFull))
        {
            Preconditions.checkArgument(Iterables.all(replicas, Replica::isFull));
            List<Replica> normalized = new ArrayList<>(replicas.size());
            for (Replica replica: replicas)
            {
                normalized.addAll(replica.normalize());
            }

           return normalized;
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public static Set<Replica> subtractAll(Replica replica, Collection<Replica> toSubtract)
    {
        if (replica.isFull() && Iterables.all(toSubtract, Replica::isFull))
        {
            return Sets.newHashSet(decorateRanges(replica.getEndpoint(),
                                                  replica.isFull(),
                                                  replica.getRange().subtractAll(asRanges(toSubtract))));
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public static boolean intersects(Replica lhs, Replica rhs)
    {
        if (lhs.isFull() && rhs.isFull())
        {
            return lhs.getRange().intersects(rhs.getRange());
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

//    public static Collection<Replica> decorateEndpoints(Collection<InetAddressAndPort> endpoints, boolean full)
//    {
//        return Collections2.transform(endpoints, e -> new Replica(e, full));
//    }
//
//    public static List<Replica> decorateEndpointList(List<InetAddressAndPort> endpoints, boolean full)
//    {
//        return Lists.newArrayList(Iterables.transform(endpoints, e -> new Replica(e, full)));
//    }

    public static List<String> stringify(Iterable<Replica> replicas, boolean withPort)
    {
        List<String> stringEndpoints = new ArrayList<>();
        for (Replica replica: replicas)
        {
            stringEndpoints.add(replica.getEndpoint().getHostAddress(withPort));
        }
        return stringEndpoints;
    }

    public static boolean containsEndpoint(Iterable<Replica> replicas, InetAddressAndPort endpoint)
    {
        return Iterables.any(replicas, r -> r.getEndpoint().equals(endpoint));
    }

    public static Iterable<Replica> concatNaturalAndPending(Iterable<Replica> natural, Iterable<Replica> pending)
    {
        if (Iterables.all(natural, Replica::isFull) && Iterables.all(pending, Replica::isFull))
        {
            return Iterables.concat(natural, pending);
        }
        else
        {
            // mixed / full / pending may have duplicates depending on how we eventually calculate pending ranges
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public static Set<Replica> difference(Collection<Replica> newReplicas, Collection<Replica> oldReplicas)
    {
        if (Iterables.all(newReplicas, Replica::isFull) && Iterables.all(oldReplicas, Replica::isFull))
        {
            Set<InetAddressAndPort> oldEndpoints = asEndpointSet(oldReplicas);
            return Sets.newHashSet(Iterables.filter(newReplicas, r -> !oldEndpoints.contains(r.getEndpoint())));
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
    }

    public static void removeEndpoint(Collection<Replica> replicas, InetAddressAndPort toRemove)
    {
        replicas.removeIf(r -> r.getEndpoint().equals(toRemove));
    }

    /**
     * Remove by endpoint. Ranges are ignored when determining what to remove
     */
    public static void removeEndpoints(Collection<Replica> replicas, Collection<Replica> toRemove)
    {
        if (Iterables.all(replicas, Replica::isFull) && Iterables.all(toRemove, Replica::isFull))
        {
            for (Replica remove: toRemove)
            {
                removeEndpoint(replicas, remove.getEndpoint());
            }
        }
        else
        {
            // FIXME: add support for transient replicas
            throw new UnsupportedOperationException("transient replicas are currently unsupported");
        }
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

    public static Iterable<Replica> filterLocalReplica(Iterable<Replica> replicas)
    {
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        return Iterables.filter(replicas, r -> !r.getEndpoint().equals(local));
    }
}
