/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.SchemaConstants;

public class MigrateLegacySystemDataTest
{
    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testMigrateSystemDataIfNeeded() throws Throwable
    {
        File[] dataDirectories = DatabaseDescriptor.getAllDataFileLocations();
        File legacySystemDataDirectory = dataDirectories[0];

        // verify files exist in legacy directory
        Collection<File> legacyTableDirectories = getSystemTableDirectories(legacySystemDataDirectory);
        Assert.assertTrue(legacyTableDirectories.size() > 0);

        File newSystemDataDirectory = dataDirectories[0].resolveSibling("new_system_directory");
        try
        {
            DatabaseDescriptor.setSpecificLocationForLocalSystemData(newSystemDataDirectory);
            newSystemDataDirectory.tryCreateDirectories();

            // verify no table directories in new system dir before migration
            Collection<File> newTableDirectories = getSystemTableDirectories(newSystemDataDirectory);
            Assert.assertEquals(0, newTableDirectories.size());

            CassandraDaemon daemon = new CassandraDaemon();
            daemon.migrateSystemDataIfNeeded();

            // verify table directories are migrated in new system dir
            newTableDirectories = getSystemTableDirectories(newSystemDataDirectory);
            Assert.assertEquals(legacyTableDirectories.size(), newTableDirectories.size());
        }
        finally
        {
            newSystemDataDirectory.deleteRecursive();
        }
    }

    private Set<File> getSystemTableDirectories(File systemDataDirectory)
    {
        return Arrays.stream(systemDataDirectory.tryList())
                     .filter(f -> SchemaConstants.isLocalSystemKeyspace(f.name()))
                     .flatMap(f -> Arrays.stream(f.tryList()))
                     .collect(Collectors.toSet());
    }
}
