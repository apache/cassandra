package org.apache.cassandra.io.sstable;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.File;

import org.junit.Test;

import org.apache.cassandra.db.Table;

public class DescriptorTest
{
    @Test
    public void testLegacy()
    {
        Descriptor descriptor = Descriptor.fromFilename(new File("Keyspace1"), "userActionUtilsKey-9-Data.db").left;
        assert descriptor.version.equals(Descriptor.LEGACY_VERSION);
        assert descriptor.usesOldBloomFilter;
    }

    @Test
    public void testExtractKeyspace()
    {
        // Test a path representing a SNAPSHOT directory
        String dirPath = "Keyspace10" + File.separator + Table.SNAPSHOT_SUBDIR_NAME + File.separator + System.currentTimeMillis();
        assertKeyspace("Keyspace10", dirPath);

        // Test a path representing a regular SSTables directory
        dirPath = "Keyspace11";
        assertKeyspace("Keyspace11", dirPath);
    }

    @Test
    public void testVersion()
    {
        // letter only
        Descriptor desc = Descriptor.fromFilename(new File("Keyspace1"), "Standard1-h-1-Data.db").left;
        assert "h".equals(desc.version);
        assert desc.tracksMaxTimestamp;

        // multiple letters
        desc = Descriptor.fromFilename(new File("Keyspace1"), "Standard1-ha-1-Data.db").left;
        assert "ha".equals(desc.version);
        assert desc.tracksMaxTimestamp;

        // hypothetical two-letter g version
        desc = Descriptor.fromFilename(new File("Keyspace1"), "Standard1-gz-1-Data.db").left;
        assert "gz".equals(desc.version);
        assert !desc.tracksMaxTimestamp;
    }

    private void assertKeyspace(String expectedKsName, String dirPath) {
        File dir = new File(dirPath);
        dir.deleteOnExit();

        // Create and check.
        if (!dir.mkdirs())
            throw new RuntimeException("Unable to create directories:" + dirPath);

        String currentKsName = Descriptor.extractKeyspaceName(dir);
        assert expectedKsName.equals(currentKsName);
    }
}
