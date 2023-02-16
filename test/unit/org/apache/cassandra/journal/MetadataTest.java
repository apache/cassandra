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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static org.junit.Assert.assertEquals;

public class MetadataTest
{
    @Test
    public void testUpdate()
    {
        Random rng = new Random();

        int host1 = rng.nextInt();
        int host2 = host1 + 1;
        int host3 = host2 + 1;
        int host4 = host3 + 1;
        int host5 = host4 + 1;

        Metadata metadata = Metadata.create();

        metadata.update(set(host1));
        metadata.update(set(host2, host3));
        metadata.update(set(host1, host4));
        metadata.update(set(host1, host2, host3, host4));

        assertEquals(set(host1, host2, host3, host4), metadata.hosts());
        assertEquals(3, metadata.count(host1));
        assertEquals(2, metadata.count(host2));
        assertEquals(2, metadata.count(host3));
        assertEquals(2, metadata.count(host4));
        assertEquals(0, metadata.count(host5));
        assertEquals(4, metadata.totalCount());
    }

    @Test
    public void testWriteRead() throws IOException
    {
        Random rng = new Random();

        int host1 = rng.nextInt();
        int host2 = host1 + 1;
        int host3 = host2 + 1;
        int host4 = host3 + 1;
        int host5 = host4 + 1;

        Metadata metadata = Metadata.create();

        metadata.update(set(host1));
        metadata.update(set(host2, host3));
        metadata.update(set(host1, host4));
        metadata.update(set(host1, host2, host3, host4));

        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            metadata.write(out);
            ByteBuffer serialized = out.buffer();

            try (DataInputBuffer in = new DataInputBuffer(serialized, false))
            {
                Metadata deserialized = Metadata.read(in);

                assertEquals(set(host1, host2, host3, host4), deserialized.hosts());
                assertEquals(3, deserialized.count(host1));
                assertEquals(2, deserialized.count(host2));
                assertEquals(2, deserialized.count(host3));
                assertEquals(2, deserialized.count(host4));
                assertEquals(0, deserialized.count(host5));
                assertEquals(4, deserialized.totalCount());
            }
        }
    }

    private static Set<Integer> set(Integer... ids)
    {
        return new HashSet<>(Arrays.asList(ids));
    }
}
