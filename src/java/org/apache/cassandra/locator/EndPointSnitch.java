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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class EndPointSnitch implements IEndPointSnitch
{
    public boolean isOnSameRack(InetAddress host, InetAddress host2) throws UnknownHostException
    {
        /*
         * Look at the IP Address of the two hosts. Compare 
         * the 3rd octet. If they are the same then the hosts
         * are in the same rack else different racks. 
        */
        byte[] ip = host.getAddress();
        byte[] ip2 = host2.getAddress();

        return ip[2] == ip2[2];
    }

    public boolean isInSameDataCenter(InetAddress host, InetAddress host2) throws UnknownHostException
    {
        /*
         * Look at the IP Address of the two hosts. Compare 
         * the 2nd octet. If they are the same then the hosts
         * are in the same datacenter else different datacenter. 
        */
        byte[] ip = host.getAddress();
        byte[] ip2 = host2.getAddress();

        return ip[1] == ip2[1];
    }

    public String getLocation(InetAddress endpoint) throws UnknownHostException
    {
        throw new UnknownHostException("Not Supported");
    }

    public List<InetAddress> sortByProximity(final InetAddress address, Collection<InetAddress> unsortedAddress)
    {
        List<InetAddress> preferred = new ArrayList<InetAddress>(unsortedAddress);
        Collections.sort(preferred, new Comparator<InetAddress>()
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

        return preferred;
    }
}
