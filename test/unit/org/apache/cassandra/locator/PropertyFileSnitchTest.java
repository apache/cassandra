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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.locator.TopologyFileLocationProvider.PROPERTIES_FILENAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link PropertyFileSnitch}.
 */
public class PropertyFileSnitchTest
{
    private Path effectiveFile;
    private Path backupFile;
    private InetAddressAndPort localAddress;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup() throws ConfigurationException, IOException
    {
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
        String confFile = FBUtilities.resourceToFile(PROPERTIES_FILENAME);
        effectiveFile = Paths.get(confFile);
        backupFile = Paths.get(confFile + ".bak");
        localAddress = FBUtilities.getBroadcastAddressAndPort();

        Files.copy(effectiveFile, backupFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    @Test
    public void localLocationsPresentInConfig() throws IOException
    {
        replaceConfigFile(Collections.singletonMap(localAddress.getHostAddressAndPort(), "DC1:RAC2"));
        PropertyFileSnitch snitch = new PropertyFileSnitch();
        // for registering a new node, location is obtained from the snitch config
        assertEquals("DC1", snitch.getLocalDatacenter());
        assertEquals("RAC2", snitch.getLocalRack());
    }

    @Test
    public void locationsNotPresentInConfig() throws IOException
    {
        replaceConfigFile(Collections.singletonMap("default", "XXX_DEFAULT_DC:XXX_DEFAULT_RACK"));
        PropertyFileSnitch snitch = new PropertyFileSnitch();

        // for registering a new node, location is obtained from the snitch config
        assertEquals("XXX_DEFAULT_DC", snitch.getLocalDatacenter());
        assertEquals("XXX_DEFAULT_RACK", snitch.getLocalRack());
    }

    @Test
    public void localAndDefaultLocationNotPresentInConfig() throws IOException
    {
        replaceConfigFile(Collections.emptyMap());
        try
        {
            PropertyFileSnitch snitch = new PropertyFileSnitch();
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            String expectedMessage = String.format("Snitch definitions at %s do not define a location for this node's " +
                                                   "broadcast address %s, nor does it provides a default",
                                                   PROPERTIES_FILENAME, localAddress);
            assertTrue(e.getMessage().contains(expectedMessage));
        }
    }

    @After
    public void restoreOrigConfigFile() throws IOException
    {
        if (Files.exists(backupFile))
        {
            Files.copy(backupFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            Files.delete(backupFile);
        }
    }

    private void replaceConfigFile(Map<String, String> replacements) throws IOException
    {
        List<String> newLines = replacements.entrySet()
                                            .stream()
                                            .map(e -> toLine(e.getKey(), e.getValue()))
                                            .collect(Collectors.toList());
        Files.write(effectiveFile, newLines, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private String toLine(String key, String value)
    {
        // make a suitably escaped key=value string to write to a properties file
        return key.replaceAll(Matcher.quoteReplacement(":"), "\\\\:") + '=' + value;
    }
}
