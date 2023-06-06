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

package org.apache.cassandra.distributed.util;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.collect.Iterators;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

/**
 * A CQL driver {@link LoadBalancingPolicy} that only connects to a fixed host.
 */
public class SingleHostLoadBalancingPolicy implements LoadBalancingPolicy
{
    private final InetAddress address;
    private Host host;

    public SingleHostLoadBalancingPolicy(InetAddress address)
    {
        this.address = address;
    }

    protected final List<Host> hosts = new CopyOnWriteArrayList<>();

    @Override
    public void init(Cluster cluster, Collection<Host> hosts)
    {
        host = hosts.stream()
                    .filter(h -> h.getBroadcastAddress().equals(address)).findFirst()
                    .orElseThrow(() -> new AssertionError("The host should be a contact point"));
        this.hosts.add(host);
    }

    @Override
    public HostDistance distance(Host host)
    {
        return HostDistance.LOCAL;
    }

    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement)
    {
        return Iterators.singletonIterator(host);
    }

    @Override
    public void onAdd(Host host)
    {
        // no-op
    }

    @Override
    public void onUp(Host host)
    {
        // no-op
    }

    @Override
    public void onDown(Host host)
    {
        // no-op
    }

    @Override
    public void onRemove(Host host)
    {
        // no-op
    }

    @Override
    public void close()
    {
        // no-op
    }
}