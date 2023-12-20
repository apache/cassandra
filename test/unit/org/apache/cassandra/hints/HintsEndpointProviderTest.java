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

package org.apache.cassandra.hints;

import java.util.Optional;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HintsEndpointProviderTest
{
    @BeforeClass
    public static void setup()
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testVersionForUnknownEndpoint()
    {
        HintsEndpointProvider.DefaultHintsEndpointProvider provider = new HintsEndpointProvider.DefaultHintsEndpointProvider();

        Optional<Integer> version = provider.versionForEndpoint(FBUtilities.getBroadcastAddressAndPort());

        assertFalse("Version should not be present for unknown endpoint", version.isPresent());
    }

    @Test
    public void testVersionForKnownEndpoint()
    {
        InetAddressAndPort knownEndpointAddress = FBUtilities.getBroadcastAddressAndPort().withPort(9999);
        int version = 333;
        MessagingService.instance().versions.set(knownEndpointAddress, version);

        HintsEndpointProvider.DefaultHintsEndpointProvider provider = new HintsEndpointProvider.DefaultHintsEndpointProvider();
        Optional<Integer> versionOption = provider.versionForEndpoint(knownEndpointAddress);

        assertTrue("Version should be present for a known endpoint", versionOption.isPresent());
        assertEquals(version, versionOption.get().intValue());
    }
}