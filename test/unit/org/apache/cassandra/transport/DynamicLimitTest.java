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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.transport.ProtocolTestHelper.cleanupPeers;
import static org.apache.cassandra.transport.ProtocolTestHelper.setStaticLimitInConfig;
import static org.apache.cassandra.transport.ProtocolTestHelper.setupPeer;
import static org.apache.cassandra.transport.ProtocolTestHelper.updatePeerInfo;
import static org.junit.Assert.assertEquals;

public class DynamicLimitTest
{
    @BeforeClass
    public static void setup()
    {
        CQLTester.prepareServer();
    }

    @Test
    public void disableDynamicLimitWithSystemProperty() throws Throwable
    {
        // Dynamic limiting of the max negotiable protocol version can be
        // disabled with a system property

        // ensure that no static limit is configured
        setStaticLimitInConfig(null);

        // set the property which disables dynamic limiting
        System.setProperty(ConfiguredLimit.DISABLE_MAX_PROTOCOL_AUTO_OVERRIDE, "true");
        // insert a legacy peer into system.peers and also
        InetAddress peer = null;
        try
        {
            peer = setupPeer("127.1.0.1", "2.2.0");
            ConfiguredLimit limit = ConfiguredLimit.newLimit();
            assertEquals(ProtocolVersion.MAX_SUPPORTED_VERSION, limit.getMaxVersion());

            // clearing the property after the limit has been returned has no effect
            System.clearProperty(ConfiguredLimit.DISABLE_MAX_PROTOCOL_AUTO_OVERRIDE);
            limit.updateMaxSupportedVersion();
            assertEquals(ProtocolVersion.MAX_SUPPORTED_VERSION, limit.getMaxVersion());

            // a new limit should now be dynamic
            limit = ConfiguredLimit.newLimit();
            assertEquals(ProtocolVersion.V3, limit.getMaxVersion());
        }
        finally
        {
            System.clearProperty(ConfiguredLimit.DISABLE_MAX_PROTOCOL_AUTO_OVERRIDE);
            cleanupPeers(peer);
        }
    }

    @Test
    public void disallowLoweringMaxVersion() throws Throwable
    {
        // Lowering the max version once connections have been established is a problem
        // for some clients. So for a dynamic limit, if notifications of peer versions
        // trigger a change to the max version, it's only allowed to increase the max
        // negotiable version

        InetAddress peer = null;
        try
        {
            // ensure that no static limit is configured
            setStaticLimitInConfig(null);
            ConfiguredLimit limit = ConfiguredLimit.newLimit();
            assertEquals(ProtocolVersion.MAX_SUPPORTED_VERSION, limit.getMaxVersion());

            peer = setupPeer("127.1.0.1", "3.0.0");
            limit.updateMaxSupportedVersion();
            assertEquals(ProtocolVersion.MAX_SUPPORTED_VERSION, limit.getMaxVersion());

            // learn that peer doesn't actually fully support V4, behaviour should remain the same
            updatePeerInfo(peer, "2.2.0");
            limit.updateMaxSupportedVersion();
            assertEquals(ProtocolVersion.MAX_SUPPORTED_VERSION, limit.getMaxVersion());

            // finally learn that peer2 has been upgraded, just for completeness
            updatePeerInfo(peer, "3.3.0");
            limit.updateMaxSupportedVersion();
            assertEquals(ProtocolVersion.MAX_SUPPORTED_VERSION, limit.getMaxVersion());

        } finally {
            cleanupPeers(peer);
        }
    }
}
