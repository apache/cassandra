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
package org.apache.cassandra.hints;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static junit.framework.Assert.*;

public class HintsCatalogTest
{
    @Test
    public void loadCompletenessAndOrderTest() throws IOException
    {
        File directory = Files.createTempDirectory(null).toFile();
        try
        {
            loadCompletenessAndOrderTest(directory);
        }
        finally
        {
            directory.deleteOnExit();
        }
    }

    public static void loadCompletenessAndOrderTest(File directory) throws IOException
    {
        UUID hostId1 = UUID.randomUUID();
        UUID hostId2 = UUID.randomUUID();

        long timestamp1 = System.currentTimeMillis();
        long timestamp2 = System.currentTimeMillis() + 1;
        long timestamp3 = System.currentTimeMillis() + 2;
        long timestamp4 = System.currentTimeMillis() + 3;

        HintsDescriptor descriptor1 = new HintsDescriptor(hostId1, timestamp1);
        HintsDescriptor descriptor2 = new HintsDescriptor(hostId2, timestamp3);
        HintsDescriptor descriptor3 = new HintsDescriptor(hostId2, timestamp2);
        HintsDescriptor descriptor4 = new HintsDescriptor(hostId1, timestamp4);

        writeDescriptor(directory, descriptor1);
        writeDescriptor(directory, descriptor2);
        writeDescriptor(directory, descriptor3);
        writeDescriptor(directory, descriptor4);

        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
        assertEquals(2, catalog.stores().count());

        HintsStore store1 = catalog.get(hostId1);
        assertNotNull(store1);
        assertEquals(descriptor1, store1.poll());
        assertEquals(descriptor4, store1.poll());
        assertNull(store1.poll());

        HintsStore store2 = catalog.get(hostId2);
        assertNotNull(store2);
        assertEquals(descriptor3, store2.poll());
        assertEquals(descriptor2, store2.poll());
        assertNull(store2.poll());
    }

    @SuppressWarnings("EmptyTryBlock")
    private static void writeDescriptor(File directory, HintsDescriptor descriptor) throws IOException
    {
        try (HintsWriter ignored = HintsWriter.create(directory, descriptor))
        {
        }
    }
}
