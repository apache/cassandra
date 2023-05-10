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

package org.apache.cassandra.locator;

import java.io.IOException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.DefaultCloudMetadataServiceConnector;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIP_DISABLE_THREAD_VALIDATION;
import static org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.METADATA_URL_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class CloudstackSnitchTest
{
    private static String az;

    @BeforeClass
    public static void setup() throws Exception
    {
        GOSSIP_DISABLE_THREAD_VALIDATION.setBoolean(true);
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
    }

    @Before
    public void resetCMS()
    {
        ServerTestUtils.resetCMS();
    }

    @Test
    public void testRacks() throws IOException, ConfigurationException
    {
        az = "ch-gva-1";

        DefaultCloudMetadataServiceConnector spiedConnector = spy(new DefaultCloudMetadataServiceConnector(
        new SnitchProperties(Pair.create(METADATA_URL_PROPERTY, "http://127.0.0.1"))));

        doReturn(az).when(spiedConnector).apiCall(any());

        CloudstackSnitch snitch = new CloudstackSnitch(spiedConnector);
        InetAddressAndPort local = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort nonlocal = InetAddressAndPort.getByName("127.0.0.7");

        Token t1 = ClusterMetadata.current().partitioner.getRandomToken();
        ClusterMetadataTestHelper.addEndpoint(nonlocal, t1, "ch-zrh", "2");

        assertEquals("ch-zrh", snitch.getDatacenter(nonlocal));
        assertEquals("2", snitch.getRack(nonlocal));

        assertEquals("ch-gva", snitch.getDatacenter(local));
        assertEquals("1", snitch.getRack(local));
    }

    @Test
    public void testNewRegions() throws IOException, ConfigurationException
    {
        az = "us-east-1a";

        DefaultCloudMetadataServiceConnector spiedConnector = spy(new DefaultCloudMetadataServiceConnector(
        new SnitchProperties(Pair.create(METADATA_URL_PROPERTY, "http://127.0.0.1"))));

        doReturn(az).when(spiedConnector).apiCall(any());

        CloudstackSnitch snitch = new CloudstackSnitch(spiedConnector);

        InetAddressAndPort local = InetAddressAndPort.getByName("127.0.0.1");

        assertEquals("us-east", snitch.getDatacenter(local));
        assertEquals("1a", snitch.getRack(local));
    }
}
