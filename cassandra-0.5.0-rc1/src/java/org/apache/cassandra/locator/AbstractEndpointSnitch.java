package org.apache.cassandra.locator;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public abstract class AbstractEndpointSnitch implements IEndPointSnitch
{
    /**
     * Helps determine if 2 nodes are in the same rack in the data center.
     * @param host a specified endpoint
     * @param host2 another specified endpoint
     * @return true if on the same rack false otherwise
     * @throws UnknownHostException
     */
    abstract public boolean isOnSameRack(InetAddress host, InetAddress host2) throws UnknownHostException;

    /**
     * Helps determine if 2 nodes are in the same data center.
     * @param host a specified endpoint
     * @param host2 another specified endpoint
     * @return true if in the same data center false otherwise
     * @throws UnknownHostException
     */
    abstract public boolean isInSameDataCenter(InetAddress host, InetAddress host2) throws UnknownHostException;

    /**
     * Given endpoints this method will help us know the datacenter name where the node is located at.
     */
    abstract public String getLocation(InetAddress endpoint) throws UnknownHostException;

    public List<InetAddress> getSortedListByProximity(final InetAddress address, Collection<InetAddress> unsortedAddress)
    {
        List<InetAddress> preferred = new ArrayList<InetAddress>(unsortedAddress);
        sortByProximity(address, preferred);
        return preferred;
    }

    public List<InetAddress> sortByProximity(final InetAddress address, List<InetAddress> addresses)
    {
        Collections.sort(addresses, new Comparator<InetAddress>()
        {
            public int compare(InetAddress a1, InetAddress a2)
            {
                try
                {
                    if (address.equals(a1) && !address.equals(a2))
                        return -1;
                    if (address.equals(a2) && !address.equals(a1))
                        return 1;
                    if (isOnSameRack(address, a1) && !isOnSameRack(address, a2))
                        return -1;
                    if (isOnSameRack(address, a2) && !isOnSameRack(address, a1))
                        return 1;
                    if (isInSameDataCenter(address, a1) && !isInSameDataCenter(address, a2))
                        return -1;
                    if (isInSameDataCenter(address, a2) && !isInSameDataCenter(address, a1))
                        return 1;
                    return 0;
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            }
        });
        return addresses;
    }
}
