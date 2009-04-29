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

package org.apache.cassandra.net;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ConnectionStatistics
{
    private String localHost;
    private int localPort;
    private String remoteHost;
    private int remotePort;
    private int totalConnections;
    private int connectionsInUse;

    ConnectionStatistics(EndPoint localEp, EndPoint remoteEp, int tc, int ciu)
    {
        localHost = localEp.getHost();
        localPort = localEp.getPort();
        remoteHost = remoteEp.getHost();
        remotePort = remoteEp.getPort();
        totalConnections = tc;
        connectionsInUse = ciu;
    }
    
    public String getLocalHost()
    {
        return localHost;
    }
    
    public int getLocalPort()
    {
        return localPort;
    }
    
    public String getRemoteHost()
    {
        return remoteHost;
    }
    
    public int getRemotePort()
    {
        return remotePort;
    }
    
    public int getTotalConnections()
    {
        return totalConnections;
    }
    
    public int getConnectionInUse()
    {
        return connectionsInUse;
    }

    public String toString()
    {
        return localHost + ":" + localPort + "->" + remoteHost + ":" + remotePort + " Total Connections open : " + totalConnections + " Connections in use : " + connectionsInUse;
    }
}