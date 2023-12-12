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
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.FileWriter;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.MockedStatic;


import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.cassandra.locator.UrlSeedProvider.URL_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockStatic;

public class UrlSeedProviderTest
{

    @Test
    public void testSeedsResolution() throws Throwable
    {
        byte[] addressBytes1 = new byte[]{ 127, 0, 0, 1 };
        byte[] addressBytes2 = new byte[]{ 127, 0, 0, 2 };
        byte[] addressBytes3 = new byte[]{ 127, 0, 0, 3 };
        InetAddressAndPort address1 = new InetAddressAndPort(InetAddress.getByAddress(addressBytes1), addressBytes1, 7000);
        InetAddressAndPort address2 = new InetAddressAndPort(InetAddress.getByAddress(addressBytes2), addressBytes2, 7001);
        InetAddressAndPort address3 = new InetAddressAndPort(InetAddress.getByAddress(addressBytes3), addressBytes3, 7000);

        File f = FileUtils.createTempFile("USP", ".txt");
        try {
            try (FileWriter writer = f.newWriter(File.WriteMode.OVERWRITE)) {
                writer.write(address1.getHostAddressAndPort());
                writer.write("\n");
                writer.write(address2.getHostAddressAndPort());
                writer.write("\n");
                writer.write(address3.getHostAddress(false));
                writer.write("\n");
            }

            URL dataUrl = f.toJavaIOFile().toURI().toURL();
            Map<String, String> seedProviderArgs = new HashMap<>();
            seedProviderArgs.put(URL_KEY, dataUrl.toString());


            MockedStatic<DatabaseDescriptor> descriptorMock = mockStatic(DatabaseDescriptor.class);
            descriptorMock.when(DatabaseDescriptor::loadConfig).thenReturn(getConfig(seedProviderArgs));

            UrlSeedProvider provider = new UrlSeedProvider(null);
            List<InetAddressAndPort> seeds = provider.getSeeds();

            assertEquals(3, seeds.size());
            assertEquals(address1, seeds.get(0));
            assertEquals(address2, seeds.get(1));
            assertEquals(address3, seeds.get(2));


        }
        finally
        {
            f.delete();
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
