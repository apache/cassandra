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
package org.apache.cassandra.index.sai.disk.v1.bbtree;

import java.nio.ByteBuffer;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.index.sai.disk.ResettableByteBuffersIndexOutput;
import org.apache.cassandra.index.sai.disk.io.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

import static org.junit.Assert.assertEquals;

public class LeafOrderMapTest extends SAIRandomizedTester
{
    @Test
    public void test() throws Exception
    {
        int[] array = new int[1024];
        for (int x=0; x < array.length; x++)
        {
            array[x] = x;
        }
        shuffle(array);

        ResettableByteBuffersIndexOutput out = new ResettableByteBuffersIndexOutput("");

        LeafOrderMap.write(array, array.length, array.length - 1, out);

        IndexInput input = new ByteBuffersIndexInput(new ByteBuffersDataInput(Lists.newArrayList(ByteBuffer.wrap(out.toArrayCopy()))), "");

        final byte bits = (byte) DirectWriter.unsignedBitsRequired(array.length - 1);

        for (int index = 0; index < array.length; index++)
        {
            LongValues reader = DirectReader.getInstance(new SeekingRandomAccessInput(input), bits);

            int value = Math.toIntExact(reader.get(index));

            assertEquals(array[index], value);
        }
    }
}
