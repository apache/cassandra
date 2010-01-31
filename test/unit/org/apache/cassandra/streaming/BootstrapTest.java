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
package org.apache.cassandra.streaming;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;

import org.junit.Test;

public class BootstrapTest
{
    @Test
    public void testGetNewNames() throws IOException
    {
        PendingFile[] pendingFiles = new PendingFile[3];
        pendingFiles[0] = new PendingFile("/baz/foo/Standard1-500-Data.db", 100, "Keyspace1");
        pendingFiles[1] = new PendingFile("/bar/foo/Standard1-500-Index.db", 100, "Keyspace1");
        pendingFiles[2] = new PendingFile("/bad/foo/Standard1-500-Filter.db", 100, "Keyspace1");
        StreamInitiateVerbHandler bivh = new StreamInitiateVerbHandler();
        Map<String, String> fileNames = bivh.getNewNames(pendingFiles);
        Map<String, String> paths = new HashMap<String, String>();
        for (String ssName : fileNames.keySet())
            paths.put(ssName, DatabaseDescriptor.getNextAvailableDataLocation());
        assertEquals(1, paths.size());
        String result = fileNames.get("Keyspace1-Standard1-500");
        assertEquals(true, result.contains("Standard1"));
        assertEquals(true, result.contains("Data.db"));
        assertEquals(1, fileNames.entrySet().size());

        assertTrue(new File(bivh.getNewFileNameFromOldContextAndNames(fileNames, paths, pendingFiles[0])).getName().matches("Standard1-tmp-\\d+-Data.db"));
        assertTrue(new File(bivh.getNewFileNameFromOldContextAndNames(fileNames, paths, pendingFiles[1])).getName().matches("Standard1-tmp-\\d+-Index.db"));
        assertTrue(new File(bivh.getNewFileNameFromOldContextAndNames(fileNames, paths, pendingFiles[2])).getName().matches("Standard1-tmp-\\d+-Filter.db"));
    }
}
