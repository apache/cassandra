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

package org.apache.cassandra.distributed.api;

import org.apache.cassandra.distributed.impl.NetworkTopology;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Pair;

import java.util.Map;
import java.util.UUID;

public interface IInstanceConfig
{
    int num();
    UUID hostId();
    InetAddressAndPort broadcastAddressAndPort();
    NetworkTopology networkTopology();

    default public String localRack()
    {
        return networkTopology().localRack(broadcastAddressAndPort());
    }

    default public String localDatacenter()
    {
        return networkTopology().localDC(broadcastAddressAndPort());
    }

    /**
     * write the specified parameters to the Config object; we do not specify Config as the type to support a Config
     * from any ClassLoader; the implementation must not directly access any fields of the Object, or cast it, but
     * must use the reflection API to modify the state
     */
    void propagate(Object writeToConfig);

    /**
     * Validates whether the config properties are within range of accepted values.
     */
    void validate();
    Object get(String fieldName);
    String getString(String fieldName);
    int getInt(String fieldName);
    boolean has(Feature featureFlag);
}
