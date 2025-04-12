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
    @FunctionalInterface
    interface Factory
    {
        INodeProvisionStrategy create(int subnet, @Nullable Map<String, Integer> portMap);
    }

    enum Strategy implements Factory
    {
        OneNetworkInterface
        {
            @Override
            public INodeProvisionStrategy create(int subnet, @Nullable Map<String, Integer> portMap)
            {
                String ipAdress = "127.0." + subnet + ".1";
                return new AbstractNodeProvisionStrategy(portMap)
                {

                    @Override
                    protected int computeStoragePort(int nodeNum)
                    {
                        return 7011 + nodeNum;
                    }

                    @Override
                    protected int computeNativeTransportPort(int nodeNum)
                    {
                        return 9041 + nodeNum;
                    }

                    @Override
                    protected int computeJmxPort(int nodeNum)
                    {
                        return 7199 + nodeNum;
                    }

                    @Override
                    public int seedNodeNum()
                    {
                        return 1;
                    }

                    @Override
                    public String ipAddress(int nodeNum)
                    {
                        return ipAdress;
                    }
                };
            }
        },
        MultipleNetworkInterfaces
        {
            @Override
            public INodeProvisionStrategy create(int subnet, @Nullable Map<String, Integer> portMap)
            {
                String ipPrefix = "127.0." + subnet + '.';
                return new AbstractNodeProvisionStrategy(portMap)
                {
                    @Override
                    public int seedNodeNum()
                    {
                        return 1;
                    }

                    @Override
                    public String ipAddress(int nodeNum)
                    {
                        return ipPrefix + nodeNum;
                    }
                };
            }
        };
    }

    int seedNodeNum();

    String ipAddress(int nodeNum);

    int storagePort(int nodeNum);

    int nativeTransportPort(int nodeNum);

    int jmxPort(int nodeNum);

    abstract class AbstractNodeProvisionStrategy implements INodeProvisionStrategy
    {
        @Nullable
        private final Map<String, Integer> portMap;

        protected AbstractNodeProvisionStrategy(@Nullable Map<String, Integer> portMap)
        {
            this.portMap = portMap;
        }

        protected int computeStoragePort(int nodeNum)
        {
            return 7012;
        }

        protected int computeNativeTransportPort(int nodeNum)
        {
            return 9042;
        }

        protected int computeJmxPort(int nodeNum)
        {
            return 7199;
        }

        @Override
        public int storagePort(int nodeNum)
        {
            if (portMap != null)
            {
                return portMap.computeIfAbsent("storagePort@node" + nodeNum, key -> SocketUtils.findAvailablePort(ipAddress(nodeNum), computeStoragePort(nodeNum)));
            }
            return computeStoragePort(nodeNum);
        }

        @Override
        public int nativeTransportPort(int nodeNum)
        {
            if (portMap != null)
            {
                return portMap.computeIfAbsent("nativeTransportPort@node" + nodeNum, key -> SocketUtils.findAvailablePort(ipAddress(nodeNum), computeNativeTransportPort(nodeNum)));
            }
            return computeNativeTransportPort(nodeNum);
        }

        @Override
        public int jmxPort(int nodeNum)
        {
            if (portMap != null)
            {
                return portMap.computeIfAbsent("jmxPort@node" + nodeNum, key -> SocketUtils.findAvailablePort(ipAddress(nodeNum), computeJmxPort(nodeNum)));
            }
            return computeJmxPort(nodeNum);
        }
    }
}
