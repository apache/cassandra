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

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Map of hosts to their known current messaging versions.
 */
public class EndpointMessagingVersions
{
    public volatile int minClusterVersion = MessagingService.current_version;
    private static final Logger logger = LoggerFactory.getLogger(EndpointMessagingVersions.class);

    // protocol versions of the other nodes in the cluster
    private final ConcurrentMap<InetAddressAndPort, Integer> versions = new NonBlockingHashMap<>();

    /**
     * @return the last version associated with address, or @param version if this is the first such version
     */
    public int set(InetAddressAndPort endpoint, int version)
    {
        logger.trace("Setting version {} for {}", version, endpoint);

        Integer v = versions.put(endpoint, version);
        minClusterVersion = Collections.min(versions.values());
        return v == null ? version : v;
    }

    public void reset(InetAddressAndPort endpoint)
    {
        logger.trace("Resetting version for {}", endpoint);
        versions.remove(endpoint);
        if (!versions.values().isEmpty())
            minClusterVersion = Collections.min(versions.values());
    }

    /**
     * Returns the messaging-version as announced by the given node but capped
     * to the min of the version as announced by the node and {@link MessagingService#current_version}.
     */
    public int get(InetAddressAndPort endpoint)
    {
        Integer v = versions.get(endpoint);
        if (v == null)
        {
            // we don't know the version. assume current. we'll know soon enough if that was incorrect.
            logger.trace("Assuming current protocol version for {}", endpoint);
            return MessagingService.current_version;
        }
        else
            return Math.min(v, MessagingService.current_version);
    }

    public int get(String endpoint) throws UnknownHostException
    {
        return get(InetAddressAndPort.getByName(endpoint));
    }

    /**
     * Returns the messaging-version exactly as announced by the given endpoint.
     */
    public int getRaw(InetAddressAndPort endpoint)
    {
        Integer v = versions.get(endpoint);
        if (v == null)
            throw new IllegalStateException("getRawVersion() was called without checking knowsVersion() result first");
        return v;
    }

    public boolean knows(InetAddressAndPort endpoint)
    {
        return versions.containsKey(endpoint);
    }
}
