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

import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;

public abstract class AbstractEndpointSnitch implements IEndpointSnitch
{
    public abstract int compareEndpoints(InetAddressAndPort target, InetAddressAndPort a1, InetAddressAndPort a2);

    /**
     * Sorts the <tt>Collection</tt> of node addresses by proximity to the given address
     * @param address the address to sort by proximity to
     * @param unsortedAddress the nodes to sort
     * @return a new sorted <tt>List</tt>
     */
    public List<InetAddressAndPort> getSortedListByProximity(InetAddressAndPort address, Collection<InetAddressAndPort> unsortedAddress)
    {
        List<InetAddressAndPort> preferred = new ArrayList<>(unsortedAddress);
        sortByProximity(address, preferred);
        return preferred;
    }

    /**
     * Sorts the <tt>List</tt> of node addresses, in-place, by proximity to the given address
     * @param address the address to sort the proximity by
     * @param addresses the nodes to sort
     */
    public void sortByProximity(final InetAddressAndPort address, List<InetAddressAndPort> addresses)
    {
        Collections.sort(addresses, new Comparator<InetAddressAndPort>()
        {
            public int compare(InetAddressAndPort a1, InetAddressAndPort a2)
            {
                return compareEndpoints(address, a1, a2);
            }
        });
    }

    public void gossiperStarting()
    {
        // noop by default
    }

    public boolean isWorthMergingForRangeQuery(List<InetAddressAndPort> merged, List<InetAddressAndPort> l1, List<InetAddressAndPort> l2)
    {
        // Querying remote DC is likely to be an order of magnitude slower than
        // querying locally, so 2 queries to local nodes is likely to still be
        // faster than 1 query involving remote ones
        boolean mergedHasRemote = hasRemoteNode(merged);
        return mergedHasRemote
             ? hasRemoteNode(l1) || hasRemoteNode(l2)
             : true;
    }

    private boolean hasRemoteNode(List<InetAddressAndPort> l)
    {
        String localDc = DatabaseDescriptor.getLocalDataCenter();
        for (InetAddressAndPort ep : l)
        {
            if (!localDc.equals(getDatacenter(ep)))
                return true;
        }
        return false;
    }
}
