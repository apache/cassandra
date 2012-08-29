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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StorageServiceServerTest
{
    @Test
    public void testRegularMode() throws IOException, InterruptedException, ConfigurationException
    {
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        StorageService.instance.initServer(0);
        for (String path : DatabaseDescriptor.getAllDataFileLocations())
        {
            // verify that storage directories are there.
            assertTrue(new File(path).exists());
        }
        // a proper test would be to call decommission here, but decommission() mixes both shutdown and datatransfer
        // calls.  This test is only interested in the shutdown-related items which a properly handled by just
        // stopping the client.
        //StorageService.instance.decommission();
        StorageService.instance.stopClient();
    }

    @Test
    public void testGetAllRangesEmpty()
    {
        List<Token> toks = Collections.emptyList();
        assertEquals(Collections.emptyList(), StorageService.instance.getAllRanges(toks));
    }

    @Test
    public void testSnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.takeSnapshot("snapshot", new String[0]);
    }

    @Test
    public void testColumnFamilySnapshot() throws IOException
    {
        // no need to insert extra data, even an "empty" database will have a little information in the system keyspace
        StorageService.instance.takeColumnFamilySnapshot(Table.SYSTEM_KS, "Schema", "cf_snapshot");
    }

}
