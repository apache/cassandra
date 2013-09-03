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

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.junit.Test;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class IndexSummaryTest
{
    @Test
    public void testAddEmptyKey() throws Exception
    {
        IPartitioner p = new RandomPartitioner();
        IndexSummaryBuilder builder = new IndexSummaryBuilder(1);
        builder.maybeAddEntry(p.decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER), 0);
        IndexSummary summary = builder.build(p);
        assertEquals(1, summary.size());
        assertEquals(0, summary.getPosition(0));
        assertArrayEquals(new byte[0], summary.getKey(0));

        ByteArrayDataOutput bout = ByteStreams.newDataOutput();
        IndexSummary.serializer.serialize(summary, bout);
        ByteArrayDataInput bin = ByteStreams.newDataInput(bout.toByteArray());
        IndexSummary loaded = IndexSummary.serializer.deserialize(bin, p);

        assertEquals(1, loaded.size());
        assertEquals(summary.getPosition(0), loaded.getPosition(0));
        assertArrayEquals(summary.getKey(0), summary.getKey(0));
    }
}
