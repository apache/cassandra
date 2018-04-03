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

import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.cassandra.utils.FBUtilities;

public class Replicas
{
    public static Collection<InetAddressAndPort> asEndpoints(Collection<Replica> replicas)
    {
        return Collections2.transform(replicas, Replica::getEndpoint);
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

    public static Collection<Replica> decorateEndpoints(Collection<InetAddressAndPort> endpoints, boolean full)
    {
        return Collections2.transform(endpoints, e -> new Replica(e, full));
    }

    public static List<Replica> decorateEndpointList(List<InetAddressAndPort> endpoints, boolean full)
    {
        return Lists.newArrayList(Iterables.transform(endpoints, e -> new Replica(e, full)));
    }

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
            Set<InetAddressAndPort> newEndpoints = asEndpointSet(newReplicas);
            Set<InetAddressAndPort> oldEndpoints = asEndpointSet(oldReplicas);
            return Sets.newHashSet(decorateEndpoints(Sets.difference(newEndpoints, oldEndpoints), true));
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

    public static void removeAll(Collection<Replica> replicas, Collection<Replica> toRemove)
    {
        if (Iterables.all(replicas, Replica::isFull) && Iterables.all(toRemove, Replica::isFull))
        {
            replicas.removeAll(toRemove);
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
