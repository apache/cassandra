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

import org.junit.Assert;
import org.junit.Test;

import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.bkd.MutablePointsReaderUtils;

public class BKDTreeRamBufferTest
{
    @Test
    public void shouldKeepInsertionOrder()
    {
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);
        int currentValue = 202;
        for (int i = 0; i < 100; ++i)
        {
            byte[] scratch = new byte[Integer.BYTES];
            NumericUtils.intToSortableBytes(currentValue--, scratch, 0);
            buffer.addPackedValue(i, new BytesRef(scratch));
        }

        final MutablePointValues pointValues = buffer.asPointValues();

        for (int i = 0; i < 100; ++i)
        {
            // expect insertion order
            Assert.assertEquals(i, pointValues.getDocID(i));
            BytesRef ref = new BytesRef();
            pointValues.getValue(i, ref);
            Assert.assertEquals(202 - i, NumericUtils.sortableBytesToInt(ref.bytes, ref.offset));
        }
    }

    @Test
    public void shouldBeSortable()
    {
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);
        int value = 301;
        for (int i = 0; i < 100; ++i)
        {
            byte[] scratch = new byte[Integer.BYTES];
            NumericUtils.intToSortableBytes(value--, scratch, 0);
            buffer.addPackedValue(i, new BytesRef(scratch));
        }

        final MutablePointValues pointValues = buffer.asPointValues();

        MutablePointsReaderUtils.sort(100, Integer.BYTES, pointValues, 0, Math.toIntExact(pointValues.size()));

        for (int i = 0; i < 100; ++i)
        {
            // expect reverse order after sorting
            Assert.assertEquals(99 - i, pointValues.getDocID(i));
            BytesRef ref = new BytesRef();
            pointValues.getValue(i, ref);
            Assert.assertEquals(202 + i, NumericUtils.sortableBytesToInt(ref.bytes, ref.offset));
        }
    }
}
