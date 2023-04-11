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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;

public class SyncedOffsetsTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void testCommonCase() throws IOException
    {
        testReadWrite(512, true);
        testReadWrite(512, false);
    }

    @Test
    public void testResize() throws IOException
    {
        testReadWrite(2048, true);
        testReadWrite(2048, false);
    }

    private void testReadWrite(int n, boolean syncOnMark) throws IOException
    {
        File directory = new File(Files.createTempDirectory(null));
        directory.deleteOnExit();

        Descriptor descriptor = Descriptor.create(directory, System.currentTimeMillis(), 1);

        SyncedOffsets active = SyncedOffsets.active(descriptor, syncOnMark);
        for (int i = 0; i < n; i++)
            active.mark(i);
        assertEquals(n - 1, active.syncedOffset());
        active.close();

        SyncedOffsets loaded = SyncedOffsets.load(descriptor);
        assertEquals(n - 1, loaded.syncedOffset());
        loaded.close();
    }
}
