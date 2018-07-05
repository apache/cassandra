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

import com.google.common.collect.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;

public abstract class AbstractEndpointSnitch implements IEndpointSnitch
{
    public abstract int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2);

    /**
     * Sorts the <tt>Collection</tt> of node addresses by proximity to the given address
     * @param address the address to sort by proximity to
     * @param unsortedAddress the nodes to sort
     * @return a new sorted <tt>List</tt>
     */
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(final InetAddressAndPort address, C unsortedAddress)
    {
        return unsortedAddress.sorted((r1, r2) -> compareEndpoints(address, r1, r2));
    }

    public void gossiperStarting()
    {
        // noop by default
    }

    public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
    {
        // Querying remote DC is likely to be an order of magnitude slower than
        // querying locally, so 2 queries to local nodes is likely to still be
        // faster than 1 query involving remote ones
        boolean mergedHasRemote = hasRemoteNode(merged);
        return mergedHasRemote
             ? hasRemoteNode(l1) || hasRemoteNode(l2)
             : true;
    }

    private boolean hasRemoteNode(ReplicaCollection<?> l)
    {
        String localDc = DatabaseDescriptor.getLocalDataCenter();
        return Iterables.any(l, replica -> !localDc.equals(getDatacenter(replica)));
    }
}
