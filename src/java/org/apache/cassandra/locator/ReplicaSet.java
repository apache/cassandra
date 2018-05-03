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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class ReplicaSet extends Replicas
{
    private final Set<Replica> replicaSet;

    public ReplicaSet()
    {
        replicaSet = new HashSet<>();
    }

    public ReplicaSet(Replicas replicas)
    {
        replicaSet = Sets.newHashSetWithExpectedSize(replicas.size());
        Iterables.addAll(replicaSet, replicas);
    }

    @Override
    public void add(Replica replica)
    {
        replicaSet.add(replica);
    }

    @Override
    public void addAll(Iterable<Replica> replicas)
    {
        Iterables.addAll(replicaSet, replicas);
    }

    @Override
    public void removeEndpoint(InetAddressAndPort endpoint)
    {
        replicaSet.removeIf(r -> r.getEndpoint().equals(endpoint));
    }

    @Override
    public int size()
    {
        return replicaSet.size();
    }

    @Override
    public Iterator<Replica> iterator()
    {
        return replicaSet.iterator();
    }
}
