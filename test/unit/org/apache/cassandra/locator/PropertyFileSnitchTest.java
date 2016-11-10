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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.net.InetAddresses;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link PropertyFileSnitch}.
 */
public class PropertyFileSnitchTest
{
    private Path effectiveFile;
    private Path backupFile;

    private VersionedValue.VersionedValueFactory valueFactory;
    private Map<InetAddress, Set<Token>> tokenMap;

    @Before
    public void setup() throws ConfigurationException, IOException
    {
        String confFile = FBUtilities.resourceToFile(PropertyFileSnitch.SNITCH_PROPERTIES_FILENAME);
        effectiveFile = Paths.get(confFile);
        backupFile = Paths.get(confFile + ".bak");

        restoreOrigConfigFile();

        InetAddress[] hosts = {
            InetAddress.getByName("127.0.0.1"), // this exists in the config file
            InetAddress.getByName("127.0.0.2"), // this exists in the config file
            InetAddress.getByName("127.0.0.9"), // this does not exist in the config file
        };

        IPartitioner partitioner = new RandomPartitioner();
        valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
        tokenMap = new HashMap<>();

        for (InetAddress host : hosts)
        {
            Set<Token> tokens = Collections.singleton(partitioner.getRandomToken());
            Gossiper.instance.initializeNodeUnsafe(host, UUID.randomUUID(), 1);
            Gossiper.instance.injectApplicationState(host, ApplicationState.TOKENS, valueFactory.tokens(tokens));

            setNodeShutdown(host);
            tokenMap.put(host, tokens);
        }
    }

    private void restoreOrigConfigFile() throws IOException
    {
        if (Files.exists(backupFile))
        {
            Files.copy(backupFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            Files.delete(backupFile);
        }
    }

    private void replaceConfigFile(Map<String, String> replacements) throws IOException
    {
        List<String> lines = Files.readAllLines(effectiveFile, StandardCharsets.UTF_8);
        List<String> newLines = new ArrayList<>(lines.size());
        Set<String> replaced = new HashSet<>();

        for (String line : lines)
        {
            String[] info = line.split("=");
            if (info.length == 2 && replacements.containsKey(info[0]))
            {
                String replacement = replacements.get(info[0]);
                if (!replacement.isEmpty()) // empty means remove this line
                    newLines.add(info[0] + '=' + replacement);

                replaced.add(info[0]);
            }
            else
            {
                newLines.add(line);
            }
        }

        // add any new lines that were not replaced
        for (Map.Entry<String, String> replacement : replacements.entrySet())
        {
            if (replaced.contains(replacement.getKey()))
                continue;

            if (!replacement.getValue().isEmpty()) // empty means remove this line so do nothing here
                newLines.add(replacement.getKey() + '=' + replacement.getValue());
        }

        Files.write(effectiveFile, newLines, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void setNodeShutdown(InetAddress host)
    {
        StorageService.instance.getTokenMetadata().removeEndpoint(host);
        Gossiper.instance.injectApplicationState(host, ApplicationState.STATUS, valueFactory.shutdown(true));
        Gossiper.instance.markDead(host, Gossiper.instance.getEndpointStateForEndpoint(host));
    }

    private void setNodeLive(InetAddress host)
    {
        Gossiper.instance.injectApplicationState(host, ApplicationState.STATUS, valueFactory.normal(tokenMap.get(host)));
        Gossiper.instance.realMarkAlive(host, Gossiper.instance.getEndpointStateForEndpoint(host));
        StorageService.instance.getTokenMetadata().updateNormalTokens(tokenMap.get(host), host);
    }

    private static void checkEndpoint(final AbstractNetworkTopologySnitch snitch,
                                      final String endpointString, final String expectedDatacenter,
                                      final String expectedRack)
    {
        final InetAddress endpoint = InetAddresses.forString(endpointString);
        assertEquals(expectedDatacenter, snitch.getDatacenter(endpoint));
        assertEquals(expectedRack, snitch.getRack(endpoint));
    }

    /**
     * Test that changing rack for a host in the configuration file is only effective if the host is not live.
     * The original configuration file contains: 127.0.0.1=DC1:RAC1
     */
    @Test
    public void testChangeHostRack() throws Exception
    {
        final InetAddress host = InetAddress.getByName("127.0.0.1");
        final PropertyFileSnitch snitch = new PropertyFileSnitch(/*refreshPeriodInSeconds*/1);
        checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC1");

        try
        {
            setNodeLive(host);

            Files.copy(effectiveFile, backupFile);
            replaceConfigFile(Collections.singletonMap(host.getHostAddress(), "DC1:RAC2"));

            Thread.sleep(1500);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC1");

            setNodeShutdown(host);
            replaceConfigFile(Collections.singletonMap(host.getHostAddress(), "DC1:RAC2"));

            Thread.sleep(1500);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC2");
        }
        finally
        {
            restoreOrigConfigFile();
            setNodeShutdown(host);
        }
    }

    /**
     * Test that changing dc for a host in the configuration file is only effective if the host is not live.
     * The original configuration file contains: 127.0.0.1=DC1:RAC1
     */
    @Test
    public void testChangeHostDc() throws Exception
    {
        final InetAddress host = InetAddress.getByName("127.0.0.1");
        final PropertyFileSnitch snitch = new PropertyFileSnitch(/*refreshPeriodInSeconds*/1);
        checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC1");

        try
        {
            setNodeLive(host);

            Files.copy(effectiveFile, backupFile);
            replaceConfigFile(Collections.singletonMap(host.getHostAddress(), "DC2:RAC1"));

            Thread.sleep(1500);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC1");

            setNodeShutdown(host);
            replaceConfigFile(Collections.singletonMap(host.getHostAddress(), "DC2:RAC1"));

            Thread.sleep(1500);
            checkEndpoint(snitch, host.getHostAddress(), "DC2", "RAC1");
        }
        finally
        {
            restoreOrigConfigFile();
            setNodeShutdown(host);
        }
    }

    /**
     * Test that adding a host to the configuration file changes the host dc and rack only if the host
     * is not live. The original configuration file does not contain 127.0.0.9 and so it should use
     * the default default=DC1:r1.
     */
    @Test
    public void testAddHost() throws Exception
    {
        final InetAddress host = InetAddress.getByName("127.0.0.9");
        final PropertyFileSnitch snitch = new PropertyFileSnitch(/*refreshPeriodInSeconds*/1);
        checkEndpoint(snitch, host.getHostAddress(), "DC1", "r1"); // default

        try
        {
            setNodeLive(host);

            Files.copy(effectiveFile, backupFile);
            replaceConfigFile(Collections.singletonMap(host.getHostAddress(), "DC2:RAC2")); // add this line if not yet there

            Thread.sleep(1500);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "r1"); // unchanged

            setNodeShutdown(host);
            replaceConfigFile(Collections.singletonMap(host.getHostAddress(), "DC2:RAC2")); // add this line if not yet there

            Thread.sleep(1500);
            checkEndpoint(snitch, host.getHostAddress(), "DC2", "RAC2"); // changed
        }
        finally
        {
            restoreOrigConfigFile();
            setNodeShutdown(host);
        }
    }

    /**
     * Test that removing a host from the configuration file changes the host rack only if the host
     * is not live. The original configuration file contains 127.0.0.2=DC1:RAC2 and default=DC1:r1 so removing
     * this host should result in a different rack if the host is not live.
     */
    @Test
    public void testRemoveHost() throws Exception
    {
        final InetAddress host = InetAddress.getByName("127.0.0.2");
        final PropertyFileSnitch snitch = new PropertyFileSnitch(/*refreshPeriodInSeconds*/1);
        checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC2");

        try
        {
            setNodeLive(host);

            Files.copy(effectiveFile, backupFile);
            replaceConfigFile(Collections.singletonMap(host.getHostAddress(), "")); // removes line if found

            Thread.sleep(1500);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC2"); // unchanged

            setNodeShutdown(host);
            replaceConfigFile(Collections.singletonMap(host.getHostAddress(), "")); // removes line if found

            Thread.sleep(1500);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "r1"); // default
        }
        finally
        {
            restoreOrigConfigFile();
            setNodeShutdown(host);
        }
    }

    /**
     * Test that we can change the default only if this does not result in any live node changing dc or rack.
     * The configuration file contains default=DC1:r1 and we change it to default=DC2:r2. Let's use host 127.0.0.9
     * since it is not in the configuration file.
     */
    @Test
    public void testChangeDefault() throws Exception
    {
        final InetAddress host = InetAddress.getByName("127.0.0.9");
        final PropertyFileSnitch snitch = new PropertyFileSnitch(/*refreshPeriodInSeconds*/1);
        checkEndpoint(snitch, host.getHostAddress(), "DC1", "r1"); // default

        try
        {
            setNodeLive(host);

            Files.copy(effectiveFile, backupFile);
            replaceConfigFile(Collections.singletonMap("default", "DC2:r2")); // change default

            Thread.sleep(1500);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "r1"); // unchanged

            setNodeShutdown(host);
            replaceConfigFile(Collections.singletonMap("default", "DC2:r2")); // change default again (refresh file update)

            Thread.sleep(1500);
            checkEndpoint(snitch, host.getHostAddress(), "DC2", "r2"); // default updated
        }
        finally
        {
            restoreOrigConfigFile();
            setNodeShutdown(host);
        }
    }
}
