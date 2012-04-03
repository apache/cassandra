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

import org.apache.cassandra.utils.FilterFactory;
import org.junit.Test;
import static org.junit.Assert.*;

public class DescriptorTest
{
    @Test
    public void testLegacy()
    {
        Descriptor descriptor = Descriptor.fromFilename("Keyspace1-userActionUtilsKey-9-Data.db");

        assert descriptor.version.equals(Descriptor.Version.LEGACY);
        assert descriptor.version.filterType == FilterFactory.Type.SHA;
    }

    @Test
    public void testVersion()
    {
        // letter only
        Descriptor desc = Descriptor.fromFilename("Keyspace1-Standard1-h-1-Data.db");
        assert "h".equals(desc.version.toString());

        // multiple letters
        desc = Descriptor.fromFilename("Keyspace1-Standard1-ha-1-Data.db");
        assert "ha".equals(desc.version.toString());

        // hypothetical two-letter g version
        desc = Descriptor.fromFilename("Keyspace1-Standard1-gz-1-Data.db");
        assert "gz".equals(desc.version.toString());
        assert !desc.version.tracksMaxTimestamp;
    }

    @Test
    public void testMurmurBloomFilter()
    {
        Descriptor desc = Descriptor.fromFilename("Keyspace1-Standard1-hz-1-Data.db");
        assertEquals("hz", desc.version.toString());
        assertEquals(desc.version.filterType, FilterFactory.Type.MURMUR2);

        desc = Descriptor.fromFilename("Keyspace1-Standard1-ia-1-Data.db");
        assertEquals("ia", desc.version.toString());
        assertEquals(desc.version.filterType, FilterFactory.Type.MURMUR3);
    }
}
