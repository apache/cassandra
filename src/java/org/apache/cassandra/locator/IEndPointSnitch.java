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

import java.net.UnknownHostException;

import java.net.InetAddress;
import java.util.Set;
import java.util.List;
import java.util.Collection;


/**
 * This interface helps determine location of node in the data center relative to another node.
 * Give a node A and another node B it can tell if A and B are on the same rack or in the same
 * data center.
 *
 * Not all methods will be germate to all implementations.  Throw UnsupportedOperation as necessary.
 */

public interface IEndPointSnitch
{
    /**
     * Helps determine if 2 nodes are in the same rack in the data center.
     * @param host a specified endpoint
     * @param host2 another specified endpoint
     * @return true if on the same rack false otherwise
     * @throws UnknownHostException
     */
    public boolean isOnSameRack(InetAddress host, InetAddress host2) throws UnknownHostException;
    
    /**
     * Helps determine if 2 nodes are in the same data center.
     * @param host a specified endpoint
     * @param host2 another specified endpoint
     * @return true if in the same data center false otherwise
     * @throws UnknownHostException
     */
    public boolean isInSameDataCenter(InetAddress host, InetAddress host2) throws UnknownHostException;
    
    /**
     * Given endpoints this method will help us know the datacenter name where the node is located at.
     */
    public String getLocation(InetAddress endpoint) throws UnknownHostException;

    /**
     * This method will sort the Set<InetAddress> according to the proximity of the given address.
     */
    public List<InetAddress> sortByProximity(InetAddress address, Collection<InetAddress> unsortedAddress);
}
