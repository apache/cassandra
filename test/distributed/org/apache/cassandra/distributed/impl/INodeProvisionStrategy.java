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

package org.apache.cassandra.distributed.impl;

import java.util.Map;
import javax.annotation.Nullable;

import org.apache.cassandra.net.SocketUtils;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;

@Shared(inner = INTERFACES)
public interface INodeProvisionStrategy
{
    static final int OneNetworkInterfaceStoragePort = 7011;
    static final int OneNetworkInterfaceNativeTransportPort = 9041;
    static final int OneNetworkInterfaceJMXPort = 7199;
    static final int MultipleNetworksInterfaceStoragePort = 7012;
    static final int MultipleNetworksInterfaceNativeTransportPort = 9042;
    static final int MultipleNetworksInterfaceJMXPort = 7199;

    enum Strategy
    {
        OneNetworkInterface
        {
            @Override
            INodeProvisionStrategy create(int subnet, @Nullable Map<String, Integer> portMap)
            {
                String ipAdress = "127.0." + subnet + ".1";
                return new INodeProvisionStrategy()
                {
                    @Override
                    public String seedIp()
                    {
                        return ipAdress;
                    }

                    @Override
                    public int seedPort()
                    {
                        return storagePort(1);
                    }

                    @Override
                    public String ipAddress(int nodeNum)
                    {
                        return ipAdress;
                    }

                    @Override
                    public int storagePort(int nodeNum)
                    {
                        if (portMap != null)
                        {
                            return portMap.computeIfAbsent("storagePort@node" + nodeNum, key -> SocketUtils.findAvailablePort(seedIp(), OneNetworkInterfaceStoragePort + nodeNum));
                        }
                        return OneNetworkInterfaceStoragePort + nodeNum;
                    }

                    @Override
                    public int nativeTransportPort(int nodeNum)
                    {
                        if (portMap != null)
                        {
                            return portMap.computeIfAbsent("nativeTransportPort@node" + nodeNum, key -> SocketUtils.findAvailablePort(seedIp(), OneNetworkInterfaceNativeTransportPort + nodeNum));
                        }
                        return OneNetworkInterfaceNativeTransportPort + nodeNum;
                    }

                    @Override
                    public int jmxPort(int nodeNum)
                    {
                        if (portMap != null)
                        {
                            return portMap.computeIfAbsent("jmxPort@node" + nodeNum, key -> SocketUtils.findAvailablePort(seedIp(), OneNetworkInterfaceJMXPort + nodeNum));
                        }
                        return OneNetworkInterfaceJMXPort + nodeNum;
                    }
                };
            }
        },
        MultipleNetworkInterfaces
        {
            @Override
            INodeProvisionStrategy create(int subnet, @Nullable Map<String, Integer> portMap)
            {
                String ipPrefix = "127.0." + subnet + '.';
                return new INodeProvisionStrategy()
                {

                    @Override
                    public String seedIp()
                    {
                        return ipPrefix + '1';
                    }

                    @Override
                    public int seedPort()
                    {
                        return storagePort(1);
                    }

                    @Override
                    public String ipAddress(int nodeNum)
                    {
                        return ipPrefix + nodeNum;
                    }

                    @Override
                    public int storagePort(int nodeNum)
                    {
                        if (portMap != null)
                        {
                            return portMap.computeIfAbsent("storagePort@node" + nodeNum, key -> SocketUtils.findAvailablePort(ipAddress(nodeNum), MultipleNetworksInterfaceStoragePort));
                        }
                        return MultipleNetworksInterfaceStoragePort;
                    }

                    @Override
                    public int nativeTransportPort(int nodeNum)
                    {
                        if (portMap != null)
                        {
                            return portMap.computeIfAbsent("nativeTransportPort@node" + nodeNum, key -> SocketUtils.findAvailablePort(ipAddress(nodeNum), MultipleNetworksInterfaceNativeTransportPort));
                        }
                        return MultipleNetworksInterfaceNativeTransportPort;
                    }

                    @Override
                    public int jmxPort(int nodeNum)
                    {
                        if (portMap != null)
                        {
                            return portMap.computeIfAbsent("jmxPort@node" + nodeNum, key -> SocketUtils.findAvailablePort(ipAddress(nodeNum), MultipleNetworksInterfaceJMXPort));
                        }
                        return MultipleNetworksInterfaceJMXPort;
                    }
                };
            }
        };

        INodeProvisionStrategy create(int subnet)
        {
            return create(subnet, null);
        }

        abstract INodeProvisionStrategy create(int subnet, @Nullable Map<String, Integer> portMap);
    }

    String seedIp();

    int seedPort();

    String ipAddress(int nodeNum);

    int storagePort(int nodeNum);

    int nativeTransportPort(int nodeNum);

    int jmxPort(int nodeNum);
}
