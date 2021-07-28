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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;

import static org.apache.cassandra.config.DatabaseDescriptor.getEndpointSnitch;
import static org.apache.cassandra.net.OutboundConnectionsTest.LOCAL_ADDR;
import static org.apache.cassandra.net.OutboundConnectionsTest.REMOTE_ADDR;

public class OutboundConnectionSettingsTest
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Test (expected = IllegalArgumentException.class)
    public void build_SmallSendSize()
    {
        test(settings -> settings.withSocketSendBufferSizeInBytes(999));
    }

    @Test (expected = IllegalArgumentException.class)
    public void build_SendSizeLessThanZero()
    {
        test(settings -> settings.withSocketSendBufferSizeInBytes(-1));
    }

    @Test (expected = IllegalArgumentException.class)
    public void build_TcpConnectTimeoutLessThanZero()
    {
        test(settings -> settings.withTcpConnectTimeoutInMS(-1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void build_TcpUserTimeoutLessThanZero()
    {
        test(settings -> settings.withTcpUserTimeoutInMS(-1));
    }

    @Test
    public void build_TcpUserTimeoutEqualsZero()
    {
        test(settings -> settings.withTcpUserTimeoutInMS(0));
    }

    private static void test(Function<OutboundConnectionSettings, OutboundConnectionSettings> f)
    {
        f.apply(new OutboundConnectionSettings(LOCAL_ADDR)).withDefaults(ConnectionCategory.MESSAGING);
    }

    private static class TestSnitch extends AbstractEndpointSnitch
    {
        private final Map<InetAddressAndPort, String> nodeToDc = new HashMap<>();

        void add(InetAddressAndPort node, String dc)
        {
            nodeToDc.put(node, dc);
        }

        public String getRack(InetAddressAndPort endpoint)
        {
            return null;
        }

        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return nodeToDc.get(endpoint);
        }

        public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
        {
            return 0;
        }
    }

    @Test
    public void shouldCompressConnection_None()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.none);
        Assert.assertFalse(OutboundConnectionSettings.shouldCompressConnection(getEndpointSnitch(), LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_DifferentDc()
    {
        TestSnitch snitch = new TestSnitch();
        snitch.add(LOCAL_ADDR, "dc1");
        snitch.add(REMOTE_ADDR, "dc2");
        DatabaseDescriptor.setEndpointSnitch(snitch);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertTrue(OutboundConnectionSettings.shouldCompressConnection(getEndpointSnitch(), LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_All()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.all);
        Assert.assertTrue(OutboundConnectionSettings.shouldCompressConnection(getEndpointSnitch(), LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_SameDc()
    {
        TestSnitch snitch = new TestSnitch();
        snitch.add(LOCAL_ADDR, "dc1");
        snitch.add(REMOTE_ADDR, "dc1");
        DatabaseDescriptor.setEndpointSnitch(snitch);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertFalse(OutboundConnectionSettings.shouldCompressConnection(getEndpointSnitch(), LOCAL_ADDR, REMOTE_ADDR));
    }

}
