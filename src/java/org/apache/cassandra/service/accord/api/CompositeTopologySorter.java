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

import accord.api.TopologySorter;
import accord.local.Node;
import accord.topology.ShardSelection;
import accord.topology.Topologies;
import accord.topology.Topology;

public class CompositeTopologySorter implements TopologySorter
{
    public static class Supplier implements TopologySorter.Supplier
    {
        private final TopologySorter.Supplier[] delegates;

        private Supplier(TopologySorter.Supplier[] delegates)
        {
            this.delegates = delegates;
        }

        @Override
        public TopologySorter get(Topology topologies)
        {
            TopologySorter[] sorters = new TopologySorter[delegates.length];
            for (int i = 0; i < sorters.length; i++)
                sorters[i] = delegates[i].get(topologies);
            return new CompositeTopologySorter(sorters);
        }

        @Override
        public TopologySorter get(Topologies topologies)
        {
            TopologySorter[] sorters = new TopologySorter[delegates.length];
            for (int i = 0; i < sorters.length; i++)
                sorters[i] = delegates[i].get(topologies);
            return new CompositeTopologySorter(sorters);
        }
    }

    private final TopologySorter[] delegates;

    private CompositeTopologySorter(TopologySorter[] delegates)
    {
        this.delegates = delegates;
    }

    public static TopologySorter.Supplier create(TopologySorter.Supplier... delegates)
    {
        switch (delegates.length)
        {
            case 0:     throw new IllegalArgumentException("Can not create an empty sorter");
            case 1:     return delegates[0];
            default:    return new CompositeTopologySorter.Supplier(delegates);
        }
    }

    @Override
    public int compare(Node.Id node1, Node.Id node2, ShardSelection shards)
    {
        for (int i = 0; i < delegates.length; i++)
        {
            int rc = delegates[i].compare(node1, node2, shards);
            if (rc != 0) return rc;
        }
        return 0;
    }
}
