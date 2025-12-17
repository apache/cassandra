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

package org.apache.cassandra.gms;


import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;

import static org.apache.cassandra.distributed.impl.TestEndpointCache.toCassandraInetAddressAndPort;

public class GossipExpiryHelper
{
    public static IIsolatedExecutor.SerializableRunnable evictExpiredFromGossip(IInvokableInstance instance)
    {
        InetSocketAddress address = instance.config().broadcastAddress();
        return () -> {
            Logger logger = LoggerFactory.getLogger(Gossiper.class);
            Directory directory = ClusterMetadata.current().directory;
            long now = System.currentTimeMillis();
            InetAddressAndPort endpoint =  toCassandraInetAddressAndPort(address);
            EndpointState epState = Gossiper.instance.endpointStateMap.get(endpoint);
            if (epState == null)
            {
                logger.info("Test helper found no gossip state for endpoint {}", endpoint);
                return;
            }
            logger.info("Test helper triggering expiry check at {} for {} (joined: {}, alive: {})",
                        now,
                        endpoint,
                        directory.allJoinedEndpoints().contains(endpoint),
                        epState.isAlive());
            FailureDetector.instance.forceConviction(endpoint);
            Gossiper.instance.evictIfExpired(endpoint, epState, directory, now);
        };
    }
}
