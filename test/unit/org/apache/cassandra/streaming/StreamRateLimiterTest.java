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

package org.apache.cassandra.streaming;

import java.net.UnknownHostException;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamRateLimiterTest
{
    InetAddressAndPort REMOTE_PEER_ADDRESS;

    @Before
    public void prepareServer() throws UnknownHostException
    {
        ServerTestUtils.daemonInitialization();
        ServerTestUtils.prepareServer();
        REMOTE_PEER_ADDRESS = InetAddressAndPort.getByName("127.0.0.4");
    }

    @Test
    public void testIsRateLimited()
    {
        // Enable rate limiting for local traffic and inter-DC traffic
        StorageService.instance.setStreamThroughputMbitPerSec(200);
        StorageService.instance.setInterDCStreamThroughputMbitPerSec(200);

        // Rate-limiter enabled for a local peer
        assertTrue(StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort()).isRateLimited());

        // Rate-limiter enabled for a remote peer
        assertTrue(StreamManager.getRateLimiter(REMOTE_PEER_ADDRESS).isRateLimited());

        // Disable rate limiting for local traffic, but enable it for inter-DC traffic
        StorageService.instance.setStreamThroughputMbitPerSec(0);
        StorageService.instance.setInterDCStreamThroughputMbitPerSec(200);

        // Rate-limiter disabled for a local peer
        assertFalse(StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort()).isRateLimited());

        // Rate-limiter enabled for a remote peer
        assertTrue(StreamManager.getRateLimiter(REMOTE_PEER_ADDRESS).isRateLimited());

        // Enable rate limiting for local traffic, but disable it for inter-DC traffic
        StorageService.instance.setStreamThroughputMbitPerSec(200);
        StorageService.instance.setInterDCStreamThroughputMbitPerSec(0);

        // Rate-limiter enabled for a local peer
        assertTrue(StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort()).isRateLimited());

        // Rate-limiter enabled for a remote peer (because there is a local rate-limit)
        assertTrue(StreamManager.getRateLimiter(REMOTE_PEER_ADDRESS).isRateLimited());

        // Disable rate liming for local and inter-DC traffic
        StorageService.instance.setStreamThroughputMbitPerSec(0);
        StorageService.instance.setInterDCStreamThroughputMbitPerSec(0);

        // Rate-limiter enabled for a local and remote peers
        assertFalse(StreamManager.getRateLimiter(FBUtilities.getBroadcastAddressAndPort()).isRateLimited());
        assertFalse(StreamManager.getRateLimiter(REMOTE_PEER_ADDRESS).isRateLimited());
    }

    @Test
    public void testEntireSSTableStreamingIsRateLimited()
    {
        // Enable rate limiting for local traffic and inter-DC traffic
        StorageService.instance.setEntireSSTableStreamThroughputMebibytesPerSec(200);
        StorageService.instance.setEntireSSTableInterDCStreamThroughputMebibytesPerSec(200);

        // Rate-limiter enabled for a local peer
        assertTrue(StreamManager.getEntireSSTableRateLimiter(FBUtilities.getBroadcastAddressAndPort()).isRateLimited());

        // Rate-limiter enabled for a remote peer
        assertTrue(StreamManager.getEntireSSTableRateLimiter(REMOTE_PEER_ADDRESS).isRateLimited());

        // Disable rate limiting for local traffic, but enable it for inter-DC traffic
        StorageService.instance.setEntireSSTableStreamThroughputMebibytesPerSec(0);
        StorageService.instance.setEntireSSTableInterDCStreamThroughputMebibytesPerSec(200);

        // Rate-limiter disabled for a local peer
        assertFalse(StreamManager.getEntireSSTableRateLimiter(FBUtilities.getBroadcastAddressAndPort()).isRateLimited());

        // Rate-limiter enabled for a remote peer
        assertTrue(StreamManager.getEntireSSTableRateLimiter(REMOTE_PEER_ADDRESS).isRateLimited());

        // Enable rate limiting for local traffic, but disable it for inter-DC traffic
        StorageService.instance.setEntireSSTableStreamThroughputMebibytesPerSec(200);
        StorageService.instance.setEntireSSTableInterDCStreamThroughputMebibytesPerSec(0);

        // Rate-limiter enabled for a local peer
        assertTrue(StreamManager.getEntireSSTableRateLimiter(FBUtilities.getBroadcastAddressAndPort()).isRateLimited());

        // Rate-limiter enabled for a remote peer (because there is a local rate-limit)
        assertTrue(StreamManager.getEntireSSTableRateLimiter(REMOTE_PEER_ADDRESS).isRateLimited());

        // Disable rate liming for local and inter-DC traffic
        StorageService.instance.setEntireSSTableStreamThroughputMebibytesPerSec(0);
        StorageService.instance.setEntireSSTableInterDCStreamThroughputMebibytesPerSec(0);

        // Rate-limiter enabled for a local and remote peers
        assertFalse(StreamManager.getEntireSSTableRateLimiter(FBUtilities.getBroadcastAddressAndPort()).isRateLimited());
        assertFalse(StreamManager.getEntireSSTableRateLimiter(REMOTE_PEER_ADDRESS).isRateLimited());
    }
}