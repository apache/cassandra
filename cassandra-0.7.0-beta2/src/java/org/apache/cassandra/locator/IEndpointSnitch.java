/**
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

import org.apache.cassandra.dht.Token;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This interface helps determine location of node in the data center relative to another node.
 * Give a node A and another node B it can tell if A and B are on the same rack or in the same
 * data center.
 */

public interface IEndpointSnitch
{
    /**
     * returns a String repesenting the rack this endpoint belongs to
     */
    public String getRack(InetAddress endpoint);

    /**
     * returns a String representing the datacenter this endpoint belongs to
     */
    public String getDatacenter(InetAddress endpoint);

    /**
     * returns a new <tt>List</tt> sorted by proximity to the given endpoint
     */
    public List<InetAddress> getSortedListByProximity(InetAddress address, Collection<InetAddress> unsortedAddress);

    /**
     * This method will sort the <tt>List</tt> by proximity to the given address.
     */
    public List<InetAddress> sortByProximity(InetAddress address, List<InetAddress> addresses);

    /**
     * compares two endpoints in relation to the target endpoint, returning as Comparator.compare would
     */
    public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2);

    /**
     * returns a list of cached endpoints for a given token.
     */
    public ArrayList<InetAddress> getCachedEndpoints(Token t);

    /**
     * puts an address in the cache for a given token.
     */
    public void cacheEndpoint(Token t, ArrayList<InetAddress> addr);

    /**
     * clears all cache values.
     */
    public void clearEndpointCache();
}
