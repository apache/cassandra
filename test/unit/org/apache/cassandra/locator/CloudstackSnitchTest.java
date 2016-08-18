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
import java.net.InetAddress;
import java.util.EnumMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;

public class CloudstackSnitchTest
{
    private static String az;

    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        Keyspace.setInitialized();
        StorageService.instance.initServer(0);
    }

    private class TestCloudstackSnitch extends CloudstackSnitch
    {
        public TestCloudstackSnitch() throws IOException, ConfigurationException
        {
            super();
        }

        @Override
        String csMetadataEndpoint() throws ConfigurationException
        {
            return "";
        }

        @Override
        String csQueryMetadata(String endpoint) throws IOException, ConfigurationException
        {
            return az;
        }
    }

    @Test
    public void testRacks() throws IOException, ConfigurationException
    {
        az = "ch-gva-1";
        CloudstackSnitch snitch = new TestCloudstackSnitch();
        InetAddress local = InetAddress.getByName("127.0.0.1");
        InetAddress nonlocal = InetAddress.getByName("127.0.0.7");

        Gossiper.instance.addSavedEndpoint(nonlocal);
        Map<ApplicationState, VersionedValue> stateMap = new EnumMap<>(ApplicationState.class);
        stateMap.put(ApplicationState.DC, StorageService.instance.valueFactory.datacenter("ch-zrh"));
        stateMap.put(ApplicationState.RACK, StorageService.instance.valueFactory.rack("2"));
        Gossiper.instance.getEndpointStateForEndpoint(nonlocal).addApplicationStates(stateMap);

        assertEquals("ch-zrh", snitch.getDatacenter(nonlocal));
        assertEquals("2", snitch.getRack(nonlocal));

        assertEquals("ch-gva", snitch.getDatacenter(local));
        assertEquals("1", snitch.getRack(local));

    }

    @Test
    public void testNewRegions() throws IOException, ConfigurationException
    {
        az = "ch-gva-1";
        CloudstackSnitch snitch = new TestCloudstackSnitch();
        InetAddress local = InetAddress.getByName("127.0.0.1");

        assertEquals("ch-gva", snitch.getDatacenter(local));
        assertEquals("1", snitch.getRack(local));
    }

    @AfterClass
    public static void tearDown()
    {
        StorageService.instance.stopClient();
    }
}
