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
package org.apache.cassandra.io.sstable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class IndexSummaryTest
{
    @Test
    public void testGetKey()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        for (int i = 0; i < 100; i++)
            assertEquals(random.left.get(i).key, ByteBuffer.wrap(random.right.getKey(i)));
    }

    @Test
    public void testBinarySearch()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        for (int i = 0; i < 100; i++)
            assertEquals(i, random.right.binarySearch(random.left.get(i)));
    }

    @Test
    public void testGetPosition()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 2);
        for (int i = 0; i < 50; i++)
            assertEquals(i*2, random.right.getPosition(i));
    }

    @Test
    public void testSerialization() throws IOException
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        ByteArrayOutputStream aos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(aos);
        IndexSummary.serializer.serialize(random.right, dos);
        // write junk
        dos.writeUTF("JUNK");
        dos.writeUTF("JUNK");
        FileUtils.closeQuietly(dos);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(aos.toByteArray()));
        IndexSummary is = IndexSummary.serializer.deserialize(dis, DatabaseDescriptor.getPartitioner());
        for (int i = 0; i < 100; i++)
            assertEquals(i, is.binarySearch(random.left.get(i)));
        // read the junk
        assertEquals(dis.readUTF(), "JUNK");
        assertEquals(dis.readUTF(), "JUNK");
        FileUtils.closeQuietly(dis);
    }

    @Test
    public void testAddEmptyKey() throws Exception
    {
        IPartitioner p = new RandomPartitioner();
        IndexSummaryBuilder builder = new IndexSummaryBuilder(1, 1);
        builder.maybeAddEntry(p.decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER), 0);
        IndexSummary summary = builder.build(p);
        assertEquals(1, summary.size());
        assertEquals(0, summary.getPosition(0));
        assertArrayEquals(new byte[0], summary.getKey(0));

        ByteArrayOutputStream aos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(aos);
        IndexSummary.serializer.serialize(summary, dos);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(aos.toByteArray()));
        IndexSummary loaded = IndexSummary.serializer.deserialize(dis, p);

        assertEquals(1, loaded.size());
        assertEquals(summary.getPosition(0), loaded.getPosition(0));
        assertArrayEquals(summary.getKey(0), summary.getKey(0));
    }

    private Pair<List<DecoratedKey>, IndexSummary> generateRandomIndex(int size, int interval)
    {
        List<DecoratedKey> list = Lists.newArrayList();
        IndexSummaryBuilder builder = new IndexSummaryBuilder(list.size(), interval);
        for (int i = 0; i < size; i++)
        {
            UUID uuid = UUID.randomUUID();
            DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(uuid));
            list.add(key);
        }
        Collections.sort(list);
        for (int i = 0; i < size; i++)
            builder.maybeAddEntry(list.get(i), i);
        IndexSummary summary = builder.build(DatabaseDescriptor.getPartitioner());
        return Pair.create(list, summary);
    }
}
