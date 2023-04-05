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

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;

@Shared(inner = INTERFACES)
public interface INodeProvisionStrategy
{
    public enum Strategy
    {
        OneNetworkInterface
        {
            INodeProvisionStrategy create(int subnet) {
                return new INodeProvisionStrategy()
                {
                    public String seedIp()
                    {
                        return "127.0." + subnet + ".1";
                    }

                    public int seedPort()
                    {
                        return 7012;
                    }

                    public String ipAddress(int nodeNum)
                    {
                        return "127.0." + subnet + ".1";
                    }

                    public int storagePort(int nodeNum)
                    {
                        return 7011 + nodeNum;
                    }

                    public int nativeTransportPort(int nodeNum)
                    {
                        return 9041 + nodeNum;
                    }
                };
            }
        },
        MultipleNetworkInterfaces
        {
            INodeProvisionStrategy create(int subnet) {
                String ipPrefix = "127.0." + subnet + ".";
                return new INodeProvisionStrategy()
                {
                    public String seedIp()
                    {
                        return ipPrefix + "1";
                    }

                    public int seedPort()
                    {
                        return 7012;
                    }

                    public String ipAddress(int nodeNum)
                    {
                        return ipPrefix + nodeNum;
                    }

                    public int storagePort(int nodeNum)
                    {
                        return 7012;
                    }

                    public int nativeTransportPort(int nodeNum)
                    {
                        return 9042;
                    }
                };
            }
        };
        abstract INodeProvisionStrategy create(int subnet);
    }

    abstract String seedIp();
    abstract int seedPort();
    abstract String ipAddress(int nodeNum);
    abstract int storagePort(int nodeNum);
    abstract int nativeTransportPort(int nodeNum);
}
