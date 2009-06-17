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

import java.net.*;

import org.apache.cassandra.net.EndPoint;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class EndPointSnitch implements IEndPointSnitch
{
    public boolean isOnSameRack(EndPoint host, EndPoint host2) throws UnknownHostException
    {
        /*
         * Look at the IP Address of the two hosts. Compare 
         * the 3rd octet. If they are the same then the hosts
         * are in the same rack else different racks. 
        */        
        byte[] ip = getIPAddress(host.getHost());
        byte[] ip2 = getIPAddress(host2.getHost());
        
        return ( ip[2] == ip2[2] );
    }
    
    public boolean isInSameDataCenter(EndPoint host, EndPoint host2) throws UnknownHostException
    {
        /*
         * Look at the IP Address of the two hosts. Compare 
         * the 2nd octet. If they are the same then the hosts
         * are in the same datacenter else different datacenter. 
        */
        byte[] ip = getIPAddress(host.getHost());
        byte[] ip2 = getIPAddress(host2.getHost());
        
        return ( ip[1] == ip2[1] );
    }
    
    private byte[] getIPAddress(String host) throws UnknownHostException
    {
        InetAddress ia = InetAddress.getByName(host);         
        return ia.getAddress();
    }
}
