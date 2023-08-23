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
                            return portMap.computeIfAbsent("storagePort@node" + nodeNum, key -> SocketUtils.findAvailablePort(seedIp(), 7011 + nodeNum));
                        }
                        return 7011 + nodeNum;
                    }

                    @Override
                    public int nativeTransportPort(int nodeNum)
                    {
                        if (portMap != null)
                        {
                            return portMap.computeIfAbsent("nativeTransportPort@node" + nodeNum, key -> SocketUtils.findAvailablePort(seedIp(), 9041 + nodeNum));
                        }
                        return 9041 + nodeNum;
                    }

                    @Override
                    public int jmxPort(int nodeNum)
                    {
                        if (portMap != null)
                        {
                            return portMap.computeIfAbsent("jmxPort@node" + nodeNum, key -> SocketUtils.findAvailablePort(seedIp(), 7199 + nodeNum));
                        }
                        return 7199 + nodeNum;
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
                            return portMap.computeIfAbsent("storagePort@node" + nodeNum, key -> SocketUtils.findAvailablePort(ipAddress(nodeNum), 7012));
                        }
                        return 7012;
                    }

                    @Override
                    public int nativeTransportPort(int nodeNum)
                    {
                        if (portMap != null)
                        {
                            return portMap.computeIfAbsent("nativeTransportPort@node" + nodeNum, key -> SocketUtils.findAvailablePort(ipAddress(nodeNum), 9042));
                        }
                        return 9042;
                    }

                    @Override
                    public int jmxPort(int nodeNum)
                    {
                        if (portMap != null)
                        {
                            return portMap.computeIfAbsent("jmxPort@node" + nodeNum, key -> SocketUtils.findAvailablePort(ipAddress(nodeNum), 7199));
                        }
                        return 7199;
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
