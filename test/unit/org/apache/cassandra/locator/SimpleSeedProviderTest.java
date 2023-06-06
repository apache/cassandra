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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.MockedStatic;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.cassandra.locator.SimpleSeedProvider.RESOLVE_MULTIPLE_IP_ADDRESSES_PER_DNS_RECORD_KEY;
import static org.apache.cassandra.locator.SimpleSeedProvider.SEEDS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockStatic;

public class SimpleSeedProviderTest
{
    private static final String dnsName1 = "dns-name-1";
    private static final String dnsName2 = "dns-name-2";

    @Test
    public void testSeedsResolution() throws Throwable
    {
        MockedStatic<InetAddressAndPort> inetAddressAndPortMock = null;
        MockedStatic<DatabaseDescriptor> descriptorMock = null;
        MockedStatic<FBUtilities> fbUtilitiesMock = null;

        try
        {
            byte[] addressBytes1 = new byte[]{ 127, 0, 0, 1 };
            byte[] addressBytes2 = new byte[]{ 127, 0, 0, 2 };
            byte[] addressBytes3 = new byte[]{ 127, 0, 0, 3 };
            InetAddressAndPort address1 = new InetAddressAndPort(InetAddress.getByAddress(addressBytes1), addressBytes1, 7000);
            InetAddressAndPort address2 = new InetAddressAndPort(InetAddress.getByAddress(addressBytes2), addressBytes2, 7000);
            InetAddressAndPort address3 = new InetAddressAndPort(InetAddress.getByAddress(addressBytes3), addressBytes3, 7000);

            inetAddressAndPortMock = mockStatic(InetAddressAndPort.class);
            inetAddressAndPortMock.when(() -> InetAddressAndPort.getByName(dnsName1)).thenReturn(address1);
            inetAddressAndPortMock.when(() -> InetAddressAndPort.getAllByName(dnsName1)).thenReturn(singletonList(address1));
            inetAddressAndPortMock.when(() -> InetAddressAndPort.getAllByName(dnsName2)).thenReturn(asList(address2, address3));
            inetAddressAndPortMock.when(() -> InetAddressAndPort.getByName(dnsName2)).thenReturn(address2);

            fbUtilitiesMock = mockStatic(FBUtilities.class);
            fbUtilitiesMock.when(FBUtilities::getLocalAddressAndPort).thenReturn(address1);

            descriptorMock = mockStatic(DatabaseDescriptor.class);

            Map<String, String> seedProviderArgs = new HashMap<>();

            //
            // dns 1 without multiple ips per record
            //
            seedProviderArgs.put(SEEDS_KEY, dnsName1);
            // resolve_multiple_ip_addresses_per_dns_record is implicitly false here

            descriptorMock.when(DatabaseDescriptor::loadConfig).thenReturn(getConfig(seedProviderArgs));
            SimpleSeedProvider provider = new SimpleSeedProvider(null);
            List<InetAddressAndPort> seeds = provider.getSeeds();

            assertEquals(1, seeds.size());
            assertEquals(address1, seeds.get(0));

            //
            // dns 2 without multiple ips per record
            //
            seedProviderArgs.put(SEEDS_KEY, dnsName2);
            // resolve_multiple_ip_addresses_per_dns_record is implicitly false here

            descriptorMock.when(DatabaseDescriptor::loadConfig).thenReturn(getConfig(seedProviderArgs));
            provider = new SimpleSeedProvider(null);
            seeds = provider.getSeeds();

            assertEquals(1, seeds.size());
            assertTrue(seeds.contains(address2));

            //
            // dns 1 with multiple ips per record
            //
            seedProviderArgs.put(SEEDS_KEY, dnsName1);
            seedProviderArgs.put(RESOLVE_MULTIPLE_IP_ADDRESSES_PER_DNS_RECORD_KEY, "true");

            descriptorMock.when(DatabaseDescriptor::loadConfig).thenReturn(getConfig(seedProviderArgs));
            provider = new SimpleSeedProvider(null);
            seeds = provider.getSeeds();

            assertEquals(1, seeds.size());
            assertEquals(address1, seeds.get(0));

            //
            // dns 2 with multiple ips per record
            //
            seedProviderArgs.put(SEEDS_KEY, dnsName2);
            seedProviderArgs.put(RESOLVE_MULTIPLE_IP_ADDRESSES_PER_DNS_RECORD_KEY, "true");

            descriptorMock.when(DatabaseDescriptor::loadConfig).thenReturn(getConfig(seedProviderArgs));
            provider = new SimpleSeedProvider(null);
            seeds = provider.getSeeds();

            assertEquals(2, seeds.size());
            assertTrue(seeds.containsAll(asList(address2, address3)));

            //
            // dns 1 and dns 2 without multiple ips per record
            //
            seedProviderArgs.put(SEEDS_KEY, format("%s,%s", dnsName1, dnsName2));
            seedProviderArgs.put(RESOLVE_MULTIPLE_IP_ADDRESSES_PER_DNS_RECORD_KEY, "false");

            descriptorMock.when(DatabaseDescriptor::loadConfig).thenReturn(getConfig(seedProviderArgs));
            provider = new SimpleSeedProvider(null);
            seeds = provider.getSeeds();

            assertEquals(2, seeds.size());
            assertTrue(seeds.containsAll(asList(address1, address2)));

            //
            // dns 1 and dns 2 with multiple ips per record
            //
            seedProviderArgs.put(SEEDS_KEY, format("%s,%s", dnsName1, dnsName2));
            seedProviderArgs.put(RESOLVE_MULTIPLE_IP_ADDRESSES_PER_DNS_RECORD_KEY, "true");

            descriptorMock.when(DatabaseDescriptor::loadConfig).thenReturn(getConfig(seedProviderArgs));
            provider = new SimpleSeedProvider(null);
            seeds = provider.getSeeds();

            assertEquals(3, seeds.size());
            assertTrue(seeds.containsAll(asList(address1, address2, address3)));
        }
        finally
        {
            if (inetAddressAndPortMock != null)
                inetAddressAndPortMock.close();

            if (descriptorMock != null)
                descriptorMock.close();

            if (fbUtilitiesMock != null)
                fbUtilitiesMock.close();
        }
    }

    private static Config getConfig(Map<String, String> parameters)
    {
        Config config = new Config();
        config.seed_provider = new ParameterizedClass();
        config.seed_provider.class_name = SimpleSeedProvider.class.getName();
        config.seed_provider.parameters = parameters;
        return config;
    }
}
