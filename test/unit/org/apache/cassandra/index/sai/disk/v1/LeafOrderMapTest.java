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
package org.apache.cassandra.index.sai.disk.v1;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.store.ByteArrayIndexInput;
import org.apache.lucene.util.packed.DirectWriter;

public class LeafOrderMapTest extends NdiRandomizedTest
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

        RAMIndexOutput out = new RAMIndexOutput("");

        LeafOrderMap.write(array, array.length, array.length - 1, out);

        ByteArrayIndexInput input = new ByteArrayIndexInput("", out.getBytes(), 0, (int)out.getFilePointer());

        final byte bits = (byte) DirectWriter.unsignedBitsRequired(array.length - 1);
        DirectReaders.Reader reader = DirectReaders.getReaderForBitsPerValue(bits);

        for (int x=0; x < array.length; x++)
        {
            int value = LeafOrderMap.getValue(new SeekingRandomAccessInput(input), 0, x, reader);

            assertEquals(array[x], value);
        }
    }
}
