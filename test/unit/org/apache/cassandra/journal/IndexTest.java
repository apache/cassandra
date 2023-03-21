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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.file.Files;

import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexTest
{
    private static final int[] EMPTY = new int[0];

    @Test
    public void testInMemoryIndexBasics()
    {
        InMemoryIndex<TimeUUID> index = InMemoryIndex.create(TimeUUIDKeySupport.INSTANCE);

        TimeUUID key0 = nextTimeUUID();
        TimeUUID key1 = nextTimeUUID();
        TimeUUID key2 = nextTimeUUID();
        TimeUUID key3 = nextTimeUUID();
        TimeUUID key4 = nextTimeUUID();

        assertEquals(0, index.lookUp(key0).length);
        assertEquals(0, index.lookUp(key1).length);
        assertEquals(0, index.lookUp(key2).length);
        assertEquals(0, index.lookUp(key3).length);
        assertEquals(0, index.lookUp(key4).length);

        int val11 = 1100;
        int val21 = 2100;
        int val22 = 2200;
        int val31 = 3100;
        int val32 = 3200;
        int val33 = 3300;

        index.update(key1, val11);
        index.update(key2, val21);
        index.update(key2, val22);
        index.update(key3, val31);
        index.update(key3, val32);
        index.update(key3, val33);

        assertArrayEquals(EMPTY, index.lookUp(key0));
        assertArrayEquals(new int[] { val11 }, index.lookUp(key1));
        assertArrayEquals(new int[] { val21, val22 }, index.lookUp(key2));
        assertArrayEquals(new int[] { val31, val32, val33 }, index.lookUp(key3));
        assertArrayEquals(EMPTY, index.lookUp(key4));

        assertEquals(key1, index.firstId());
        assertEquals(key3, index.lastId());

        assertFalse(index.mayContainId(key0, TimeUUIDKeySupport.INSTANCE));
         assertTrue(index.mayContainId(key1, TimeUUIDKeySupport.INSTANCE));
         assertTrue(index.mayContainId(key2, TimeUUIDKeySupport.INSTANCE));
         assertTrue(index.mayContainId(key3, TimeUUIDKeySupport.INSTANCE));
        assertFalse(index.mayContainId(key4, TimeUUIDKeySupport.INSTANCE));
    }

    @Test
    public void testInMemoryIndexPersists() throws IOException
    {
        InMemoryIndex<TimeUUID> inMemory = InMemoryIndex.create(TimeUUIDKeySupport.INSTANCE);

        TimeUUID key0 = nextTimeUUID();
        TimeUUID key1 = nextTimeUUID();
        TimeUUID key2 = nextTimeUUID();
        TimeUUID key3 = nextTimeUUID();
        TimeUUID key4 = nextTimeUUID();

        int val11 = 1100;
        int val21 = 2100;
        int val22 = 2200;
        int val31 = 3100;
        int val32 = 3200;
        int val33 = 3300;

        inMemory.update(key1, val11);
        inMemory.update(key2, val21);
        inMemory.update(key2, val22);
        inMemory.update(key3, val31);
        inMemory.update(key3, val32);
        inMemory.update(key3, val33);

        File directory = new File(Files.createTempDirectory(null));
        directory.deleteOnExit();
        Descriptor descriptor = Descriptor.create(directory, System.currentTimeMillis(), 1);
        inMemory.persist(descriptor);

        try (OnDiskIndex<TimeUUID> onDisk = OnDiskIndex.open(descriptor, TimeUUIDKeySupport.INSTANCE))
        {
            assertArrayEquals(EMPTY, onDisk.lookUp(key0));
            assertArrayEquals(new int[] { val11 }, onDisk.lookUp(key1));
            assertArrayEquals(new int[] { val21, val22 }, onDisk.lookUp(key2));
            assertArrayEquals(new int[] { val31, val32, val33 }, onDisk.lookUp(key3));
            assertArrayEquals(EMPTY, onDisk.lookUp(key4));

            assertEquals(key1, onDisk.firstId());
            assertEquals(key3, onDisk.lastId());

            assertFalse(onDisk.mayContainId(key0, TimeUUIDKeySupport.INSTANCE));
             assertTrue(onDisk.mayContainId(key1, TimeUUIDKeySupport.INSTANCE));
             assertTrue(onDisk.mayContainId(key2, TimeUUIDKeySupport.INSTANCE));
             assertTrue(onDisk.mayContainId(key3, TimeUUIDKeySupport.INSTANCE));
            assertFalse(onDisk.mayContainId(key4, TimeUUIDKeySupport.INSTANCE));
        }
    }
}