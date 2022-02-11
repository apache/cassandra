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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.utils.concurrent.CountDownLatch.newCountDownLatch;

/**
 * A {@link StartupConnectivityChecker} that ensures that all-but-one nodes in each datacenter are available
 * during Cassandra startup.
 */
public class StrictStartupConnectivityChecker extends AbstractStartupConnectivityChecker
{
    private final boolean blockForRemoteDcs;
    private final Map<String, CountDownLatch> peersToBlockPerDc = new HashMap<>();

    public StrictStartupConnectivityChecker()
    {
        this(DatabaseDescriptor.getBlockForPeersTimeoutInSeconds(), TimeUnit.SECONDS,
             DatabaseDescriptor.getBlockForPeersInRemoteDatacenters());
    }

    @VisibleForTesting
    StrictStartupConnectivityChecker(long timeout, TimeUnit unit, boolean blockForRemoteDcs)
    {
        super(timeout, unit);
        this.blockForRemoteDcs = blockForRemoteDcs;
    }

    @Override
    protected Map<String, CountDownLatch> calculatePeersToBlockPerDc(Multimap<String, InetAddressAndPort> peersByDatacenter)
    {
        for (String datacenter : peersByDatacenter.keys())
        {
            int peersToBlock = Math.max(peersByDatacenter.get(datacenter).size() - 1, 0);
            peersToBlockPerDc.put(datacenter, newCountDownLatch(peersToBlock));
        }
        return peersToBlockPerDc;
    }

    @Override
    protected Multimap<String, InetAddressAndPort> filterNodes(Multimap<String, InetAddressAndPort> nodesByDatacenter)
    {
        nodesByDatacenter = super.filterNodes(nodesByDatacenter);
        String localDc = getDatacenterSource().apply(localAddress);
        if (blockForRemoteDcs)
        {
            logger.info("Blocking coordination until only a single peer is DOWN in each datacenter, timeout={}s",
                        TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));
        }
        else
        {
            // In the case where we do not want to block startup on remote datacenters (e.g. because clients only use
            // LOCAL_X consistency levels), we remove all other datacenter hosts from the mapping, and we only wait
            // on the remaining local datacenter.
            nodesByDatacenter.keySet().retainAll(Collections.singleton(localDc));
            logger.info("Blocking coordination until only a single peer is DOWN in the local datacenter, timeout={}s",
                        TimeUnit.NANOSECONDS.toSeconds(timeoutNanos));
        }
        return nodesByDatacenter;
    }

    @Override
    protected boolean executeInternal(Multimap<String, InetAddressAndPort> peersByDatacenter)
    {
        // send out a ping message to open up the non-gossip connections to all peers. Note that this sends the
        // ping messages to _all_ peers, not just the ones we block for in dcToRemainingPeers.
        sendPingMessages(getAckTracker(), getAckTracker().getPeers(), getDatacenterSource(), peersToBlockPerDc);
        return waitForNodesUntilTimeout(peersToBlockPerDc.values(), timeoutNanos, getElaspedTimeNanos());
    }
}
