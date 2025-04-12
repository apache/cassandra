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
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.utils.Pair;
import org.mockito.stubbing.Answer;

import static org.apache.cassandra.locator.Ec2MultiRegionSnitch.PRIVATE_IP_QUERY;
import static org.apache.cassandra.locator.Ec2MultiRegionSnitch.PUBLIC_IP_QUERY;
import static org.apache.cassandra.locator.Ec2Snitch.EC2_NAMING_LEGACY;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class Ec2SnitchTest
{
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
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void resetCMS()
    {
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
    }

    @Test
    public void testLegacyRac() throws Exception
    {
        Ec2MetadataServiceConnector connectorMock = mock(Ec2MetadataServiceConnector.class);
        doReturn("us-east-1d").when(connectorMock).apiCall(anyString());
        doReturn(legacySnitchProps).when(connectorMock).getProperties();

        Ec2LocationProvider locationProvider = new Ec2LocationProvider(connectorMock);
        testLegacyRacInternal(locationProvider);

        Ec2Snitch snitch = new Ec2Snitch(connectorMock);
        testLegacyRacInternal(snitch);
    }

    @Test
    public void testMultiregionLegacyRac() throws Exception
    {
        Ec2MetadataServiceConnector spy = spy(Ec2MetadataServiceConnector.create(legacySnitchProps));
        doReturn(legacySnitchProps).when(spy).getProperties();
        doAnswer((Answer<String>) invocation -> {
            String query = invocation.getArgument(0);
            return (PUBLIC_IP_QUERY.equals(query) || PRIVATE_IP_QUERY.equals(query)) ? "127.0.0.1" : "us-east-1d";
        }).when(spy).apiCall(anyString());

        Ec2LocationProvider locationProvider = new Ec2LocationProvider(spy);
        testLegacyRacInternal(locationProvider);

        Ec2Snitch snitch = new Ec2MultiRegionSnitch(spy);
        testLegacyRacInternal(snitch);
    }

    @Test
    public void testLegacyNewRegions() throws Exception
    {
        Ec2MetadataServiceConnector spy = spy(Ec2MetadataServiceConnector.create(legacySnitchProps));
        doReturn(legacySnitchProps).when(spy).getProperties();
        doReturn("us-east-2d").when(spy).apiCall(anyString());
        testLegacyNewRegionsInternal(new Ec2LocationProvider(spy));
        testLegacyNewRegionsInternal(new Ec2Snitch(spy));
    }

    @Test
    public void testLegacyMultiRegionNewRegions() throws Exception
    {
        Ec2MetadataServiceConnector spy = spy(Ec2MetadataServiceConnector.create(legacySnitchProps));
        doReturn(legacySnitchProps).when(spy).getProperties();
        doAnswer((Answer<String>) invocation -> {
            String query = invocation.getArgument(0);
            return (PUBLIC_IP_QUERY.equals(query) || PRIVATE_IP_QUERY.equals(query)) ? "127.0.0.1" : "us-east-2d";
        }).when(spy).apiCall(anyString());

        testLegacyNewRegionsInternal(new Ec2LocationProvider(spy));
        testLegacyNewRegionsInternal(new Ec2MultiRegionSnitch(spy));
    }

    @Test
    public void testFullNamingScheme() throws Exception
    {
        Ec2MetadataServiceConnector connectorMock = mock(Ec2MetadataServiceConnector.class);
        when(connectorMock.apiCall(anyString())).thenReturn("us-east-2d");
        when(connectorMock.getProperties()).thenReturn(new SnitchProperties());

        Ec2LocationProvider locationProvider = new Ec2LocationProvider(connectorMock);
        assertEquals("us-east-2", locationProvider.initialLocation().datacenter);
        assertEquals("us-east-2d", locationProvider.initialLocation().rack);

        Ec2Snitch snitch = new Ec2Snitch(connectorMock);
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east-2", snitch.getLocalDatacenter());
        assertEquals("us-east-2d", snitch.getLocalRack());

        Ec2MetadataServiceConnector multiRegionConnectorMock = mock(Ec2MetadataServiceConnector.class);
        when(multiRegionConnectorMock.getProperties()).thenReturn(new SnitchProperties());
        when(multiRegionConnectorMock.apiCall(anyString())).then((Answer<String>) invocation -> {
            String query = invocation.getArgument(0);
            return (PUBLIC_IP_QUERY.equals(query) || PRIVATE_IP_QUERY.equals(query)) ? "127.0.0.1" : "us-east-2d";
        });

        Ec2LocationProvider mrLocationProvider = new Ec2LocationProvider(connectorMock);
        assertEquals("us-east-2", mrLocationProvider.initialLocation().datacenter);
        assertEquals("us-east-2d", mrLocationProvider.initialLocation().rack);

        Ec2MultiRegionSnitch mrSnitch = new Ec2MultiRegionSnitch(multiRegionConnectorMock);
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east-2", mrSnitch.getLocalDatacenter());
        assertEquals("us-east-2d", mrSnitch.getLocalRack());
    }

    @Test
    public void validateDatacenter_RequiresLegacy_CorrectAmazonName() throws IOException
    {
        // legacy scheme / standard dc / ignore racks
        assertValid(true, "us-east-1");
    }

    @Test
    public void validateDatacenter_RequiresLegacy_LegacyName() throws IOException
    {
        // legacy scheme / legacy dc / ignore racks
        assertValid(true, "us-east");
    }

    @Test
    public void validate_RequiresLegacy_HappyPath() throws IOException
    {
        // legacy scheme / legacy dc / legacy rack
        assertValid(true, "us-east", "1a");
    }

    @Test
    public void validate_RequiresLegacy_HappyPathWithDCSuffix() throws IOException
    {
        // legacy scheme / custom suffix dc / legacy rack
        assertValid(true, "us-east_CUSTOM_SUFFIX", "1a");
    }

    @Test
    public void validateRack_RequiresAmazonName_CorrectAmazonName() throws IOException
    {
        // standard scheme / ignore dc / non legacy rack
        assertValid(false, "", "us-east-1a");
    }

    @Test(expected = ConfigurationException.class)
    public void validateRack_RequiresAmazonName_LegacyName() throws IOException
    {
        // standard scheme / ignore dc / legacy rack
        assertValid(false, "", "1a");
    }

    @Test
    public void validate_RequiresAmazonName_HappyPath() throws IOException
    {
        // standard scheme / standard dc / standard rack
        assertValid(false, "us-east-1", "us-east-1a");
    }

    @Test
    public void validate_RequiresAmazonName_HappyPathWithDCSuffix() throws IOException
    {
        // standard scheme / custom suffix dc / standard rack
        assertValid(false, "us-east-1_CUSTOM_SUFFIX", "us-east-1a");
    }

    /**
     * Validate upgrades in legacy mode for regions that didn't change name between the standard and legacy modes.
     */
    @Test
    public void validate_RequiresLegacy_DCValidStandardAndLegacy() throws IOException
    {
        // legacy scheme / standard dc / legacy racks
        assertValid(true, "us-west-2", "2a", "2b");
    }

    /**
     * Check that racks names are enough to detect a mismatch in naming conventions.
     */
    @Test(expected = ConfigurationException.class)
    public void validate_RequiresLegacy_RackInvalidForLegacy() throws Exception
    {
        // legacy scheme / standard dc / standard rack
        assertValid(true, "us-west-2", "us-west-2a");
    }

    private static void assertValid(boolean useLegacy, String datacenter, String...racks) throws IOException
    {
        SnitchProperties properties = new SnitchProperties(Pair.create(Ec2LocationProvider.SNITCH_PROP_NAMING_SCHEME,
                                                                       useLegacy ? Ec2LocationProvider.EC2_NAMING_LEGACY
                                                                                 : Ec2LocationProvider.EC2_NAMING_STANDARD));
        Ec2MetadataServiceConnector connectorMock = mock(Ec2MetadataServiceConnector.class);
        when(connectorMock.getProperties()).thenReturn(properties);
        Ec2LocationProvider provider = new Ec2LocationProvider(connectorMock);

        Multimap<String, InetAddressAndPort> dcRacks = HashMultimap.create();
        for (String rack : racks)
            dcRacks.put(rack, null); // endpoints are used
        Directory directory = new Directory()
        {
            @Override
            public Map<String, Multimap<String, InetAddressAndPort>> allDatacenterRacks()
            {
                return Collections.singletonMap(datacenter, dcRacks);
            }

            @Override
            public Multimap<String, InetAddressAndPort> datacenterRacks(String datacenter)
            {
                return dcRacks;
            }
        };
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, directory);

        provider.validate(metadata);
    }

    private void testLegacyRacInternal(Ec2LocationProvider provider) throws Exception
    {
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east", provider.initialLocation().datacenter);
        assertEquals("1d", provider.initialLocation().rack);
    }

    private void testLegacyRacInternal(Ec2Snitch snitch) throws Exception
    {
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east", snitch.getLocalDatacenter());
        assertEquals("1d", snitch.getLocalRack());
    }

    private void testLegacyNewRegionsInternal(Ec2LocationProvider provider) throws Exception
    {
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east-2", provider.initialLocation().datacenter);
        assertEquals("2d", provider.initialLocation().rack);
    }

    private void testLegacyNewRegionsInternal(Ec2Snitch snitch) throws Exception
    {
        // for registering a new node, location is obtained from the cloud metadata service
        assertEquals("us-east-2", snitch.getLocalDatacenter());
        assertEquals("2d", snitch.getLocalRack());
    }
}
