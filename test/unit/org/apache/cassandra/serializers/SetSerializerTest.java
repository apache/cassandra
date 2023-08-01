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

package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Range;
import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.utils.ByteBufferUtil.UNSET_BYTE_BUFFER;

public class SetSerializerTest
{
    @Test
    public void testGetIndexFromSerialized()
    {
        testGetIndexFromSerialized(true);
        testGetIndexFromSerialized(false);
    }

    private static void testGetIndexFromSerialized(boolean isMultiCell)
    {
        SetType<Integer> type = SetType.getInstance(Int32Type.instance, isMultiCell);
        AbstractType<Integer> nameType = type.nameComparator();
        SetSerializer<Integer> serializer = type.getSerializer();

        Set<Integer> set = new HashSet<>(Arrays.asList(1, 3, 4, 6));
        ByteBuffer bb = type.decompose(set);

        assertEquals(-1, serializer.getIndexFromSerialized(bb, nameType.decompose(0), nameType));
        assertEquals(0, serializer.getIndexFromSerialized(bb, nameType.decompose(1), nameType));
        assertEquals(-1, serializer.getIndexFromSerialized(bb, nameType.decompose(2), nameType));
        assertEquals(1, serializer.getIndexFromSerialized(bb, nameType.decompose(3), nameType));
        assertEquals(2, serializer.getIndexFromSerialized(bb, nameType.decompose(4), nameType));
        assertEquals(-1, serializer.getIndexFromSerialized(bb, nameType.decompose(5), nameType));
        assertEquals(3, serializer.getIndexFromSerialized(bb, nameType.decompose(6), nameType));
        assertEquals(-1, serializer.getIndexFromSerialized(bb, nameType.decompose(7), nameType));

        assertEquals(Range.closed(0, Integer.MAX_VALUE), serializer.getIndexesRangeFromSerialized(bb, UNSET_BYTE_BUFFER, UNSET_BYTE_BUFFER, nameType));

        assertEquals(Range.closedOpen(0, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(0), UNSET_BYTE_BUFFER, nameType));
        assertEquals(Range.closedOpen(0, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(1), UNSET_BYTE_BUFFER, nameType));
        assertEquals(Range.closedOpen(1, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(2), UNSET_BYTE_BUFFER, nameType));
        assertEquals(Range.closedOpen(1, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(3), UNSET_BYTE_BUFFER, nameType));
        assertEquals(Range.closedOpen(2, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(4), UNSET_BYTE_BUFFER, nameType));
        assertEquals(Range.closedOpen(3, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(5), UNSET_BYTE_BUFFER, nameType));
        assertEquals(Range.closedOpen(3, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(6), UNSET_BYTE_BUFFER, nameType));
        assertEquals(Range.closedOpen(0, 0), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(7), UNSET_BYTE_BUFFER, nameType));

        assertEquals(Range.closedOpen(0, 0), serializer.getIndexesRangeFromSerialized(bb, UNSET_BYTE_BUFFER, nameType.decompose(0), nameType));
        assertEquals(Range.closedOpen(0, 1), serializer.getIndexesRangeFromSerialized(bb, UNSET_BYTE_BUFFER, nameType.decompose(1), nameType));
        assertEquals(Range.closedOpen(0, 1), serializer.getIndexesRangeFromSerialized(bb, UNSET_BYTE_BUFFER, nameType.decompose(2), nameType));
        assertEquals(Range.closedOpen(0, 2), serializer.getIndexesRangeFromSerialized(bb, UNSET_BYTE_BUFFER, nameType.decompose(3), nameType));
        assertEquals(Range.closedOpen(0, 3), serializer.getIndexesRangeFromSerialized(bb, UNSET_BYTE_BUFFER, nameType.decompose(4), nameType));
        assertEquals(Range.closedOpen(0, 3), serializer.getIndexesRangeFromSerialized(bb, UNSET_BYTE_BUFFER, nameType.decompose(5), nameType));
        assertEquals(Range.closedOpen(0, 4), serializer.getIndexesRangeFromSerialized(bb, UNSET_BYTE_BUFFER, nameType.decompose(6), nameType));
        assertEquals(Range.closedOpen(0, 4), serializer.getIndexesRangeFromSerialized(bb, UNSET_BYTE_BUFFER, nameType.decompose(7), nameType));

        assertEquals(Range.closedOpen(0, 0), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(0), nameType.decompose(0), nameType));
        assertEquals(Range.closedOpen(0, 1), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(0), nameType.decompose(1), nameType));
        assertEquals(Range.closedOpen(0, 1), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(0), nameType.decompose(2), nameType));
        assertEquals(Range.closedOpen(0, 2), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(0), nameType.decompose(3), nameType));
        assertEquals(Range.closedOpen(0, 3), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(0), nameType.decompose(4), nameType));
        assertEquals(Range.closedOpen(0, 3), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(0), nameType.decompose(5), nameType));
        assertEquals(Range.closedOpen(0, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(0), nameType.decompose(6), nameType));
        assertEquals(Range.closedOpen(0, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(0), nameType.decompose(7), nameType));

        assertEquals(Range.closedOpen(0, 0), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(1), nameType.decompose(0), nameType));
        assertEquals(Range.closedOpen(0, 1), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(1), nameType.decompose(1), nameType));
        assertEquals(Range.closedOpen(0, 1), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(1), nameType.decompose(2), nameType));
        assertEquals(Range.closedOpen(0, 2), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(1), nameType.decompose(3), nameType));
        assertEquals(Range.closedOpen(0, 3), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(1), nameType.decompose(4), nameType));
        assertEquals(Range.closedOpen(0, 3), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(1), nameType.decompose(5), nameType));
        assertEquals(Range.closedOpen(0, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(1), nameType.decompose(6), nameType));
        assertEquals(Range.closedOpen(0, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(1), nameType.decompose(7), nameType));

        assertEquals(Range.closedOpen(1, 1), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(2), nameType.decompose(0), nameType));
        assertEquals(Range.closedOpen(1, 1), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(2), nameType.decompose(1), nameType));
        assertEquals(Range.closedOpen(1, 1), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(2), nameType.decompose(2), nameType));
        assertEquals(Range.closedOpen(1, 2), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(2), nameType.decompose(3), nameType));
        assertEquals(Range.closedOpen(1, 3), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(2), nameType.decompose(4), nameType));
        assertEquals(Range.closedOpen(1, 3), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(2), nameType.decompose(5), nameType));
        assertEquals(Range.closedOpen(1, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(2), nameType.decompose(6), nameType));
        assertEquals(Range.closedOpen(1, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(2), nameType.decompose(7), nameType));

        assertEquals(Range.closedOpen(0, 0), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(0), nameType.decompose(0), nameType));
        assertEquals(Range.closedOpen(0, 1), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(1), nameType.decompose(1), nameType));
        assertEquals(Range.closedOpen(1, 1), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(2), nameType.decompose(2), nameType));
        assertEquals(Range.closedOpen(1, 2), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(3), nameType.decompose(3), nameType));
        assertEquals(Range.closedOpen(2, 3), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(4), nameType.decompose(4), nameType));
        assertEquals(Range.closedOpen(3, 3), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(5), nameType.decompose(5), nameType));
        assertEquals(Range.closedOpen(3, 4), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(6), nameType.decompose(6), nameType));
        assertEquals(Range.closedOpen(0, 0), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(7), nameType.decompose(7), nameType));

        // interval with lower bound greater than upper bound
        assertEquals(Range.closedOpen(0, 0), serializer.getIndexesRangeFromSerialized(bb, nameType.decompose(7), nameType.decompose(0), nameType));
    }
}
