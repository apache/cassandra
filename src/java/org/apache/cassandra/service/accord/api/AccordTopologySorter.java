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

package org.apache.cassandra.service.accord.api;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Set;

import accord.api.TopologySorter;
import accord.local.Node;
import accord.topology.ShardSelection;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.SortedList;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.Endpoint;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.accord.AccordEndpointMapper;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Sortable;

public class AccordTopologySorter implements TopologySorter
{
    public static class Supplier implements TopologySorter.Supplier
    {
        private final AccordEndpointMapper mapper;
        private final IEndpointSnitch snitch;

        public Supplier(AccordEndpointMapper mapper, IEndpointSnitch snitch)
        {
            checkSnitchSupported(snitch);
            this.mapper = mapper;
            this.snitch = snitch;
        }

        @Override
        public TopologySorter get(Topology topologies)
        {
            return create(topologies.nodes());
        }

        @Override
        public TopologySorter get(Topologies topologies)
        {
            return create(topologies.nodes());
        }

        private AccordTopologySorter create(SortedList<Node.Id> nodes)
        {
            SortableEndpoints endpoints = SortableEndpoints.from(nodes, mapper);
            Comparator<Endpoint> comparator = snitch.endpointComparator(FBUtilities.getBroadcastAddressAndPort(), endpoints);
            return new AccordTopologySorter(mapper, comparator);
        }
    }

    private final AccordEndpointMapper mapper;

    private final Comparator<Endpoint> comparator;
    private AccordTopologySorter(AccordEndpointMapper mapper, Comparator<Endpoint> comparator)
    {
        this.mapper = mapper;
        this.comparator = comparator;
    }

    public static void checkSnitchSupported(IEndpointSnitch snitch)
    {
        if (!snitch.supportCompareByEndpoint())
        {
            if (snitch instanceof DynamicEndpointSnitch)
                snitch = ((DynamicEndpointSnitch) snitch).subsnitch;
            throw new IllegalArgumentException("Unsupported snitch " + snitch.getClass() + "; supportCompareByEndpoint returned false");
        }
    }

    @Override
    public int compare(Node.Id node1, Node.Id node2, ShardSelection shards)
    {
        return comparator.compare(() -> mapper.mappedEndpoint(node1), () -> mapper.mappedEndpoint(node2));
    }

    private static class EndpointTuple implements Endpoint
    {
        final InetAddressAndPort endpoint;

        private EndpointTuple(InetAddressAndPort endpoint)
        {
            this.endpoint = endpoint;
        }

        @Override
        public InetAddressAndPort endpoint()
        {
            return endpoint;
        }
    }

    private static class SortableEndpoints extends ArrayList<EndpointTuple> implements Sortable<EndpointTuple, SortableEndpoints>
    {
        public SortableEndpoints(int initialCapacity)
        {
            super(initialCapacity);
        }

        public SortableEndpoints sorted(Comparator<? super EndpointTuple> comparator)
        {
            sort(comparator);
            return this;
        }

        static SortableEndpoints from(Set<Node.Id> nodes, AccordEndpointMapper mapper)
        {
            SortableEndpoints result = new SortableEndpoints(nodes.size());
            nodes.forEach(id -> result.add(new EndpointTuple(mapper.mappedEndpoint(id))));
            return result;
        }
    }
}
