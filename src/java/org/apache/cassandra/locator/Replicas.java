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
import java.util.function.Predicate;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.google.common.collect.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;

import static com.google.common.collect.Iterables.all;

public class Replicas
{

    public static int countFull(ReplicaCollection<?> replicas)
    {
        int count = 0;
        for (Replica replica : replicas)
            if (replica.isFull())
                ++count;
        return count;
    }

    public static class ReplicaCount
    {
        int fullReplicas;
        int transientReplicas;

        public int allReplicas()
        {
            return fullReplicas + transientReplicas;
        }

        public int fullReplicas()
        {
            return fullReplicas;
        }

        public int transientReplicas()
        {
            return transientReplicas;
        }

        public void increment(Replica replica)
        {
            if (replica.isFull()) ++fullReplicas;
            else ++transientReplicas;
        }

        public boolean hasAtleast(int allReplicas, int fullReplicas)
        {
            return this.fullReplicas >= fullReplicas
                    && this.allReplicas() >= allReplicas;
        }
    }

    public static ReplicaCount countInOurDc(ReplicaCollection<?> replicas)
    {
        ReplicaCount count = new ReplicaCount();
        Predicate<Replica> inOurDc = InOurDc.replicas();
        for (Replica replica : replicas)
            if (inOurDc.test(replica))
                count.increment(replica);
        return count;
    }

    /**
     * count the number of full and transient replicas, separately, for each DC
     */
    public static ObjectObjectHashMap<String, ReplicaCount> countPerDc(Collection<String> dataCenters, Iterable<Replica> replicas)
    {
        ObjectObjectHashMap<String, ReplicaCount> perDc = new ObjectObjectHashMap<>(dataCenters.size());
        for (String dc: dataCenters)
            perDc.put(dc, new ReplicaCount());

        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        for (Replica replica : replicas)
        {
            String dc = snitch.getDatacenter(replica);
            perDc.get(dc).increment(replica);
        }
        return perDc;
    }

    /**
     * increment each of the map's DC entries for each matching replica provided
     */
    public static void addToCountPerDc(ObjectIntHashMap<String> perDc, Iterable<Replica> replicas, int add)
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        for (Replica replica : replicas)
        {
            String dc = snitch.getDatacenter(replica);
            perDc.addTo(dc, add);
        }
    }

    /**
     * A placeholder for areas of the code that cannot yet handle transient replicas, but should do so in future
     */
    public static void temporaryAssertFull(Replica replica)
    {
        if (!replica.isFull())
        {
            throw new UnsupportedOperationException("transient replicas are currently unsupported: " + replica);
        }
    }

    /**
     * A placeholder for areas of the code that cannot yet handle transient replicas, but should do so in future
     */
    public static void temporaryAssertFull(Iterable<Replica> replicas)
    {
        if (!all(replicas, Replica::isFull))
        {
            throw new UnsupportedOperationException("transient replicas are currently unsupported: " + Iterables.toString(replicas));
        }
    }

    /**
     * For areas of the code that should never see a transient replica
     */
    public static void assertFull(Iterable<Replica> replicas)
    {
        if (!all(replicas, Replica::isFull))
        {
            throw new UnsupportedOperationException("transient replicas are currently unsupported: " + Iterables.toString(replicas));
        }
    }

    public static List<String> stringify(ReplicaCollection<?> replicas, boolean withPort)
    {
        List<String> stringEndpoints = new ArrayList<>(replicas.size());
        for (Replica replica: replicas)
        {
            stringEndpoints.add(replica.endpoint().getHostAddress(withPort));
        }
        return stringEndpoints;
    }

}
