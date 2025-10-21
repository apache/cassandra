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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.DefaultCloudMetadataServiceConnector;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.utils.Pair;

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
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void resetCMS()
    {
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
    }

    @Test
    public void testRacks() throws IOException, ConfigurationException
    {
        az = "ch-gva-1";

        DefaultCloudMetadataServiceConnector spiedConnector = spy(new DefaultCloudMetadataServiceConnector(
        new SnitchProperties(Pair.create(METADATA_URL_PROPERTY, "http://127.0.0.1"))));

        doReturn(az).when(spiedConnector).apiCall(any());

        // for registering a new node, location is obtained from the cloud metadata service
        CloudstackLocationProvider locationProvider = new CloudstackLocationProvider(spiedConnector);
        assertEquals("ch-gva", locationProvider.initialLocation().datacenter);
        assertEquals("1", locationProvider.initialLocation().rack);

        CloudstackSnitch snitch = new CloudstackSnitch(spiedConnector);
        assertEquals("ch-gva", snitch.getLocalDatacenter());
        assertEquals("1", snitch.getLocalRack());
    }

    @Test
    public void testNewRegions() throws IOException, ConfigurationException
    {
        az = "us-east-1a";

        DefaultCloudMetadataServiceConnector spiedConnector = spy(new DefaultCloudMetadataServiceConnector(
        new SnitchProperties(Pair.create(METADATA_URL_PROPERTY, "http://127.0.0.1"))));

        doReturn(az).when(spiedConnector).apiCall(any());

        // for registering a new node, location is obtained from the cloud metadata service
        CloudstackLocationProvider locationProvider = new CloudstackLocationProvider(spiedConnector);
        assertEquals("us-east", locationProvider.initialLocation().datacenter);
        assertEquals("1a", locationProvider.initialLocation().rack);

        CloudstackSnitch snitch = new CloudstackSnitch(spiedConnector);
        assertEquals("us-east", snitch.getLocalDatacenter());
        assertEquals("1a", snitch.getLocalRack());
    }
}
