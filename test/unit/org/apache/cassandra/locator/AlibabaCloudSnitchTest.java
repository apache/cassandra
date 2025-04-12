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
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.DefaultCloudMetadataServiceConnector;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.METADATA_URL_PROPERTY;
import static org.apache.cassandra.locator.AlibabaCloudLocationProvider.DEFAULT_METADATA_SERVICE_URL;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class AlibabaCloudSnitchTest
{
    static String az;

    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
    }

    @Before
    public void resetCMS()
    {
        ServerTestUtils.resetCMS();
    }

    @Test
    public void testRac() throws IOException, ConfigurationException
    {
        az = "cn-hangzhou-f";

        DefaultCloudMetadataServiceConnector spiedConnector = spy(new DefaultCloudMetadataServiceConnector(
        new SnitchProperties(Pair.create(METADATA_URL_PROPERTY, DEFAULT_METADATA_SERVICE_URL))));

        doReturn(az).when(spiedConnector).apiCall(any());

        // for registering a new node, location is obtained from the cloud metadata service
        AlibabaCloudLocationProvider locationProvider = new AlibabaCloudLocationProvider(spiedConnector);
        assertEquals("cn-hangzhou", locationProvider.initialLocation().datacenter);
        assertEquals("f", locationProvider.initialLocation().rack);

        AlibabaCloudSnitch snitch = new AlibabaCloudSnitch(spiedConnector);
        assertEquals("cn-hangzhou", snitch.getLocalDatacenter());
        assertEquals("f", snitch.getLocalRack());
    }

    @Test
    public void testNewRegions() throws IOException, ConfigurationException
    {
        az = "us-east-1a";
        DefaultCloudMetadataServiceConnector spiedConnector = spy(new DefaultCloudMetadataServiceConnector(
        new SnitchProperties(Pair.create(METADATA_URL_PROPERTY, DEFAULT_METADATA_SERVICE_URL))));

        doReturn(az).when(spiedConnector).apiCall(any());

        // for registering a new node, location is obtained from the cloud metadata service
        AlibabaCloudLocationProvider locationProvider = new AlibabaCloudLocationProvider(spiedConnector);
        assertEquals("us-east", locationProvider.initialLocation().datacenter);
        assertEquals("1a", locationProvider.initialLocation().rack);

        AlibabaCloudSnitch snitch = new AlibabaCloudSnitch(spiedConnector);
        assertEquals("us-east", snitch.getLocalDatacenter());
        assertEquals("1a", snitch.getLocalRack());
    }
}
