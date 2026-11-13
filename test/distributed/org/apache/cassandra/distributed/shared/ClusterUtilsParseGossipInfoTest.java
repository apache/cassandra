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

package org.apache.cassandra.distributed.shared;

import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for ClusterUtils.parseGossipInfo method.
 *
 * CASSANDRA-21097: Tests that parseGossipInfo handles both endpoint address formats:
 * - "/127.0.0.1" (hostname not resolved)
 * - "localhost/127.0.0.1" (hostname cached after getHostName() call)
 */
public class ClusterUtilsParseGossipInfoTest
{
    /**
     * Tests parsing gossip info with standard format where endpoint starts with "/".
     * This is the format when InetAddress hostname has NOT been resolved.
     */
    @Test
    public void testParseGossipInfoWithSlashPrefix()
    {
        String gossipInfo = "/127.0.0.1:7012\n" +
                            "  generation:1234567890\n" +
                            "  heartbeat:100\n" +
                            "/127.0.0.2:7012\n" +
                            "  generation:1234567891\n" +
                            "  heartbeat:101\n";

        Map<String, Map<String, String>> result = ClusterUtils.parseGossipInfo(gossipInfo);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("/127.0.0.1:7012"));
        assertTrue(result.containsKey("/127.0.0.2:7012"));
        assertEquals("1234567890", result.get("/127.0.0.1:7012").get("generation"));
        assertEquals("100", result.get("/127.0.0.1:7012").get("heartbeat"));
        assertEquals("1234567891", result.get("/127.0.0.2:7012").get("generation"));
        assertEquals("101", result.get("/127.0.0.2:7012").get("heartbeat"));
    }

    /**
     * Tests parsing gossip info with hostname prefix format.
     * This is the format when InetAddress.getHostName() has been called,
     * causing the hostname to be cached in the InetAddress object.
     *
     * CASSANDRA-21097: This test would fail before the fix because the parser
     * only checked for lines starting with "/" and would throw NPE when
     * encountering "localhost/127.0.0.1" format.
     */
    @Test
    public void testParseGossipInfoWithHostnamePrefix()
    {
        String gossipInfo = "localhost/127.0.0.1:7012\n" +
                            "  generation:1234567890\n" +
                            "  heartbeat:100\n" +
                            "localhost/127.0.0.2:7012\n" +
                            "  generation:1234567891\n" +
                            "  heartbeat:101\n";

        Map<String, Map<String, String>> result = ClusterUtils.parseGossipInfo(gossipInfo);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("localhost/127.0.0.1:7012"));
        assertTrue(result.containsKey("localhost/127.0.0.2:7012"));
        assertEquals("1234567890", result.get("localhost/127.0.0.1:7012").get("generation"));
        assertEquals("100", result.get("localhost/127.0.0.1:7012").get("heartbeat"));
        assertEquals("1234567891", result.get("localhost/127.0.0.2:7012").get("generation"));
        assertEquals("101", result.get("localhost/127.0.0.2:7012").get("heartbeat"));
    }

    /**
     * Tests parsing gossip info with mixed formats (both with and without hostname).
     */
    @Test
    public void testParseGossipInfoWithMixedFormats()
    {
        String gossipInfo = "/127.0.0.1:7012\n" +
                            "  generation:1234567890\n" +
                            "  heartbeat:100\n" +
                            "localhost/127.0.0.2:7012\n" +
                            "  generation:1234567891\n" +
                            "  heartbeat:101\n";

        Map<String, Map<String, String>> result = ClusterUtils.parseGossipInfo(gossipInfo);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("/127.0.0.1:7012"));
        assertTrue(result.containsKey("localhost/127.0.0.2:7012"));
    }

    /**
     * Tests parsing gossip info without port numbers.
     */
    @Test
    public void testParseGossipInfoWithoutPort()
    {
        String gossipInfo = "/127.0.0.1\n" +
                            "  generation:1234567890\n" +
                            "  heartbeat:100\n" +
                            "localhost/127.0.0.2\n" +
                            "  generation:1234567891\n" +
                            "  heartbeat:101\n";

        Map<String, Map<String, String>> result = ClusterUtils.parseGossipInfo(gossipInfo);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("/127.0.0.1"));
        assertTrue(result.containsKey("localhost/127.0.0.2"));
    }

}
