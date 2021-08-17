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
package org.apache.cassandra.transport;

import java.net.InetAddress;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

public final class ClientStat
{
    public static final String INET_ADDRESS = "inetAddress";
    public static final String PROTOCOL_VERSION = "protocolVersion";
    public static final String LAST_SEEN_TIME = "lastSeenTime";

    final InetAddress remoteAddress;
    final ProtocolVersion protocolVersion;
    final long lastSeenTime;

    ClientStat(InetAddress remoteAddress, ProtocolVersion protocolVersion, long lastSeenTime)
    {
        this.remoteAddress = remoteAddress;
        this.lastSeenTime = lastSeenTime;
        this.protocolVersion = protocolVersion;
    }

    @Override
    public String toString()
    {
        return String.format("ClientStat{%s, %s, %d}", remoteAddress, protocolVersion, lastSeenTime);
    }

    public Map<String, String> asMap()
    {
        return ImmutableMap.<String, String>builder()
                           .put(INET_ADDRESS, remoteAddress.toString())
                           .put(PROTOCOL_VERSION, protocolVersion.toString())
                           .put(LAST_SEEN_TIME, String.valueOf(lastSeenTime))
                           .build();
    }
}
