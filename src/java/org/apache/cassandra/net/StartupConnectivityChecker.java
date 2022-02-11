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

package org.apache.cassandra.net;

import java.util.function.Function;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Performs cluster connectivity checks during startup
 */
public interface StartupConnectivityChecker
{
    /**
     * @param peers               The currently known peers in the cluster; argument is not modified.
     * @param getDatacenterSource A function for mapping peers to their datacenter.
     * @return true if the requested percentage of peers are marked ALIVE in gossip and have their connections opened;
     * else false.
     */
    boolean execute(ImmutableSet<InetAddressAndPort> peers, Function<InetAddressAndPort, String> getDatacenterSource);
}
