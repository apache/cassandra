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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;

import static org.apache.cassandra.net.OutboundConnectionsTest.LOCAL_ADDR;
import static org.apache.cassandra.net.OutboundConnectionsTest.REMOTE_ADDR;

public class OutboundConnectionSettingsTest
{
    private static final String DC1 = "dc1";
    private static final String DC2 = "dc2";
    private static final String RACK = "rack1";

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Before
    public void reset()
    {
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
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

    @Test
    public void shouldCompressConnection_None()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.none);
        Assert.assertFalse(OutboundConnectionSettings.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_DifferentDc()
    {
        ClusterMetadataTestHelper.register(LOCAL_ADDR, DC1, RACK);
        ClusterMetadataTestHelper.register(REMOTE_ADDR, DC2, RACK);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertTrue(OutboundConnectionSettings.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_MetadataNotInitialized()
    {
        // if cluster metadata isn't yet available then we assume that every peer is remote.
        // connections will be re-established once the cluster metadata service becomes available
        ClusterMetadataTestHelper.register(LOCAL_ADDR, DC1, RACK);
        ClusterMetadataTestHelper.register(REMOTE_ADDR, DC2, RACK);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertTrue(OutboundConnectionSettings.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_All()
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.all);
        Assert.assertTrue(OutboundConnectionSettings.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

    @Test
    public void shouldCompressConnection_SameDc()
    {
        ClusterMetadataTestHelper.register(LOCAL_ADDR, DC1, RACK);
        ClusterMetadataTestHelper.register(REMOTE_ADDR, DC1, RACK);
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.dc);
        Assert.assertFalse(OutboundConnectionSettings.shouldCompressConnection(LOCAL_ADDR, REMOTE_ADDR));
    }

}
