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
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Assert;
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

import static org.apache.cassandra.locator.Ec2Snitch.EC2_NAMING_LEGACY;
import static org.junit.Assert.assertEquals;

public class EC2SnitchTest
{
    private static String az;

    private final SnitchProperties legacySnitchProps = new SnitchProperties()
    {
        public String get(String propertyName, String defaultValue)
        {
            return propertyName.equals("ec2_naming_scheme") ? EC2_NAMING_LEGACY : super.get(propertyName, defaultValue);
        }
    };

    @BeforeClass
    public static void setup() throws Exception
    {
        System.setProperty(Gossiper.Props.DISABLE_THREAD_VALIDATION, "true");
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        Keyspace.setInitialized();
        StorageService.instance.initServer(0);
    }

    private class TestEC2Snitch extends Ec2Snitch
    {
        public TestEC2Snitch() throws IOException, ConfigurationException
        {
            super();
        }

        public TestEC2Snitch(SnitchProperties props) throws IOException, ConfigurationException
        {
            super(props);
        }

        @Override
        String awsApiCall(String url) throws IOException, ConfigurationException
        {
            return az;
        }
    }

    @Test
    public void testLegacyRac() throws IOException, ConfigurationException
    {
        az = "us-east-1d";
        Ec2Snitch snitch = new TestEC2Snitch(legacySnitchProps);
        InetAddressAndPort local = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort nonlocal = InetAddressAndPort.getByName("127.0.0.7");

        Gossiper.instance.addSavedEndpoint(nonlocal);
        Map<ApplicationState, VersionedValue> stateMap = new EnumMap<>(ApplicationState.class);
        stateMap.put(ApplicationState.DC, StorageService.instance.valueFactory.datacenter("us-west"));
        stateMap.put(ApplicationState.RACK, StorageService.instance.valueFactory.datacenter("1a"));
        Gossiper.instance.getEndpointStateForEndpoint(nonlocal).addApplicationStates(stateMap);

        assertEquals("us-west", snitch.getDatacenter(nonlocal));
        assertEquals("1a", snitch.getRack(nonlocal));

        assertEquals("us-east", snitch.getDatacenter(local));
        assertEquals("1d", snitch.getRack(local));
    }

    @Test
    public void testLegacyNewRegions() throws IOException, ConfigurationException
    {
        az = "us-east-2d";
        Ec2Snitch snitch = new TestEC2Snitch(legacySnitchProps);
        InetAddressAndPort local = InetAddressAndPort.getByName("127.0.0.1");
        assertEquals("us-east-2", snitch.getDatacenter(local));
        assertEquals("2d", snitch.getRack(local));
    }

    @Test
    public void testFullNamingScheme() throws IOException, ConfigurationException
    {
        InetAddressAndPort local = InetAddressAndPort.getByName("127.0.0.1");
        az = "us-east-2d";
        Ec2Snitch snitch = new TestEC2Snitch();

        assertEquals("us-east-2", snitch.getDatacenter(local));
        assertEquals("us-east-2d", snitch.getRack(local));

        az = "us-west-1a";
        snitch = new TestEC2Snitch();

        assertEquals("us-west-1", snitch.getDatacenter(local));
        assertEquals("us-west-1a", snitch.getRack(local));
    }

    @Test
    public void validateDatacenter_RequiresLegacy_CorrectAmazonName()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east-1");
        Assert.assertFalse(Ec2Snitch.validate(datacenters, Collections.emptySet(), true));
    }

    @Test
    public void validateDatacenter_RequiresLegacy_LegacyName()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east");
        Assert.assertTrue(Ec2Snitch.validate(datacenters, Collections.emptySet(), true));
    }

    @Test
    public void validate_RequiresLegacy_HappyPath()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east");
        Set<String> racks = new HashSet<>();
        racks.add("1a");
        Assert.assertTrue(Ec2Snitch.validate(datacenters, racks, true));
    }

    @Test
    public void validate_RequiresLegacy_HappyPathWithDCSuffix()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east_CUSTOM_SUFFIX");
        Set<String> racks = new HashSet<>();
        racks.add("1a");
        Assert.assertTrue(Ec2Snitch.validate(datacenters, racks, true));
    }

    @Test
    public void validateRack_RequiresAmazonName_CorrectAmazonName()
    {
        Set<String> racks = new HashSet<>();
        racks.add("us-east-1a");
        Assert.assertTrue(Ec2Snitch.validate(Collections.emptySet(), racks, false));
    }

    @Test
    public void validateRack_RequiresAmazonName_LegacyName()
    {
        Set<String> racks = new HashSet<>();
        racks.add("1a");
        Assert.assertFalse(Ec2Snitch.validate(Collections.emptySet(), racks, false));
    }

    @Test
    public void validate_RequiresAmazonName_HappyPath()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east-1");
        Set<String> racks = new HashSet<>();
        racks.add("us-east-1a");
        Assert.assertTrue(Ec2Snitch.validate(datacenters, racks, false));
    }

    @Test
    public void validate_RequiresAmazonName_HappyPathWithDCSuffix()
    {
        Set<String> datacenters = new HashSet<>();
        datacenters.add("us-east-1_CUSTOM_SUFFIX");
        Set<String> racks = new HashSet<>();
        racks.add("us-east-1a");
        Assert.assertTrue(Ec2Snitch.validate(datacenters, racks, false));
    }

    @AfterClass
    public static void tearDown()
    {
        StorageService.instance.stopClient();
    }
}
