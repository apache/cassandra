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
package org.apache.cassandra.index.sai.disk.v1.kdtree;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.ResettableByteBuffersIndexOutput;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

public class LeafOrderMapTest extends SaiRandomizedTest
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

        var out = new ResettableByteBuffersIndexOutput(1024, "");

        LeafOrderMap.write(array, array.length, array.length - 1, out);

        var input = out.toIndexInput();

        final byte bits = (byte) DirectWriter.unsignedBitsRequired(array.length - 1);
        LongValues reader = DirectReader.getInstance(new SeekingRandomAccessInput(input), bits);

        for (int x=0; x < array.length; x++)
        {
            int value = LeafOrderMap.getValue(x, reader);

            assertEquals(array[x], value);
        }
    }
}
