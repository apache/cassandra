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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

/**
 * Unit tests for {@link GossipingPropertyFileSnitch}.
 */
public class GossipingPropertyFileSnitchTest
{
    @Test
    public void testAutoReloadConfig() throws Exception
    {
        String confFile = FBUtilities.resourceToFile(SnitchProperties.RACKDC_PROPERTY_FILENAME);
        
        final GossipingPropertyFileSnitch snitch = new GossipingPropertyFileSnitch(/*refreshPeriodInSeconds*/1);
        YamlFileNetworkTopologySnitchTest.checkEndpoint(snitch, FBUtilities.getBroadcastAddress().getHostAddress(), "DC1", "RAC1");

        final Path effectiveFile = Paths.get(confFile);
        final Path backupFile = Paths.get(confFile + ".bak");
        final Path modifiedFile = Paths.get(confFile + ".mod");
        
        try
        {
            Files.copy(effectiveFile, backupFile);
            Files.copy(modifiedFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            
            Thread.sleep(1500);
            
            YamlFileNetworkTopologySnitchTest.checkEndpoint(snitch, FBUtilities.getBroadcastAddress().getHostAddress(), "DC2", "RAC2");
        }
        finally
        {
            Files.copy(backupFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            Files.delete(backupFile);
        }
    }
}
