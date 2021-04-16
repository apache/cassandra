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

import java.net.InetSocketAddress;
import java.util.Set;

import org.apache.cassandra.utils.FBUtilities;

/**
 * This interface helps determine location of node in the datacenter relative to another node.
 * Give a node A and another node B it can tell if A and B are on the same rack or in the same
 * datacenter.
 */

public interface IEndpointSnitch
{
    /**
     * returns a String representing the rack the given endpoint belongs to
     */
    public String getRack(InetAddressAndPort endpoint);

    /**
     * returns a String representing the rack current endpoint belongs to
     */
    default public String getLocalRack()
    {
        return getRack(FBUtilities.getBroadcastAddressAndPort());
    }

    /**
     * returns a String representing the datacenter the given endpoint belongs to
     */
    public String getDatacenter(InetAddressAndPort endpoint);

    /**
     * returns a String representing the datacenter current endpoint belongs to
     */
    default public String getLocalDatacenter()
    {
        return getDatacenter(FBUtilities.getBroadcastAddressAndPort());
    }

    default String getDatacenter(InetSocketAddress endpoint)
    {
        return getDatacenter(InetAddressAndPort.getByAddress(endpoint));
    }

    default public String getDatacenter(Replica replica)
    {
        return getDatacenter(replica.endpoint());
    }

    /**
     * returns a new <tt>List</tt> sorted by proximity to the given endpoint
     */
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(final InetAddressAndPort address, C addresses);

    /**
     * compares two endpoints in relation to the target endpoint, returning as Comparator.compare would
     */
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2);

    /**
     * called after Gossiper instance exists immediately before it starts gossiping
     */
    public void gossiperStarting();

    /**
     * Returns whether for a range query doing a query against merged is likely
     * to be faster than 2 sequential queries, one against l1 followed by one against l2.
     */
    public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2);

    /**
     * Determine if the datacenter or rack values in the current node's snitch conflict with those passed in parameters.
     */
    default boolean validate(Set<String> datacenters, Set<String> racks)
    {
        return true;
    }
}
