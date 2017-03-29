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

package org.apache.cassandra.db;

import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.UnfilteredDeserializer.OldFormatDeserializer.UnfilteredIterator;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

public class OldFormatDeserializerTest
{
    @Test
    public void testRangeTombstones() throws Exception
    {
        CFMetaData metadata = CFMetaData.Builder.create("ks", "table")
                                                .withPartitioner(Murmur3Partitioner.instance)
                                                .addPartitionKey("k", Int32Type.instance)
                                                .addClusteringColumn("v", Int32Type.instance)
                                                .build();

        Supplier<LegacyLayout.LegacyAtom> atomSupplier = supplier(rt(0, 10, 42),
                                                                  rt(5, 15, 42));

        UnfilteredIterator iterator = new UnfilteredIterator(metadata,
                                                             DeletionTime.LIVE,
                                                             new SerializationHelper(metadata, MessagingService.current_version, SerializationHelper.Flag.LOCAL),
                                                             atomSupplier);

        // As the deletion time are the same, we want this to produce a single range tombstone covering from 0 to 15.

        assertTrue(iterator.hasNext());

        Unfiltered first = iterator.next();
        assertTrue(first.isRangeTombstoneMarker());
        RangeTombstoneMarker start = (RangeTombstoneMarker)first;
        assertTrue(start.isOpen(false));
        assertFalse(start.isClose(false));
        assertEquals(0, toInt(start.openBound(false)));
        assertEquals(42, start.openDeletionTime(false).markedForDeleteAt());

        Unfiltered second = iterator.next();
        assertTrue(second.isRangeTombstoneMarker());
        RangeTombstoneMarker end = (RangeTombstoneMarker)second;
        assertTrue(end.isClose(false));
        assertFalse(end.isOpen(false));
        assertEquals(15, toInt(end.closeBound(false)));
        assertEquals(42, end.closeDeletionTime(false).markedForDeleteAt());

         assertFalse(iterator.hasNext());
    }

    @Test
    public void testRangeTombstonesSameStart() throws Exception
    {
        CFMetaData metadata = CFMetaData.Builder.create("ks", "table")
                                                .withPartitioner(Murmur3Partitioner.instance)
                                                .addPartitionKey("k", Int32Type.instance)
                                                .addClusteringColumn("v", Int32Type.instance)
                                                .build();

        // Multiple RT that have the same start (we _can_ get this in the legacy format!)
        Supplier<LegacyLayout.LegacyAtom> atomSupplier = supplier(rt(1, 2, 3),
                                                                  rt(1, 2, 5),
                                                                  rt(1, 5, 4));

        UnfilteredIterator iterator = new UnfilteredIterator(metadata,
                                                             DeletionTime.LIVE,
                                                             new SerializationHelper(metadata, MessagingService.current_version, SerializationHelper.Flag.LOCAL),
                                                             atomSupplier);

        // We should be entirely ignoring the first tombston (shadowed by 2nd one) so we should generate
        // [1, 2]@5 (2, 5]@4 (but where both range actually form a boundary)

        assertTrue(iterator.hasNext());

        Unfiltered first = iterator.next();
        System.out.println(">> " + first.toString(metadata));
        assertTrue(first.isRangeTombstoneMarker());
        RangeTombstoneMarker start = (RangeTombstoneMarker)first;
        assertTrue(start.isOpen(false));
        assertFalse(start.isClose(false));
        assertEquals(1, toInt(start.openBound(false)));
        assertEquals(5, start.openDeletionTime(false).markedForDeleteAt());

        Unfiltered second = iterator.next();
        assertTrue(second.isRangeTombstoneMarker());
        RangeTombstoneMarker middle = (RangeTombstoneMarker)second;
        assertTrue(middle.isClose(false));
        assertTrue(middle.isOpen(false));
        assertEquals(2, toInt(middle.closeBound(false)));
        assertEquals(2, toInt(middle.openBound(false)));
        assertEquals(5, middle.closeDeletionTime(false).markedForDeleteAt());
        assertEquals(4, middle.openDeletionTime(false).markedForDeleteAt());

        Unfiltered third = iterator.next();
        assertTrue(third.isRangeTombstoneMarker());
        RangeTombstoneMarker end = (RangeTombstoneMarker)third;
        assertTrue(end.isClose(false));
        assertFalse(end.isOpen(false));
        assertEquals(5, toInt(end.closeBound(false)));
        assertEquals(4, end.closeDeletionTime(false).markedForDeleteAt());

        assertFalse(iterator.hasNext());
    }

    private static int toInt(ClusteringPrefix prefix)
    {
        assertTrue(prefix.size() == 1);
        return ByteBufferUtil.toInt(prefix.get(0));
    }

    private static Supplier<LegacyLayout.LegacyAtom> supplier(LegacyLayout.LegacyAtom... atoms)
    {
        return new Supplier<LegacyLayout.LegacyAtom>()
        {
            int i = 0;

            public LegacyLayout.LegacyAtom get()
            {
                return i >= atoms.length ? null : atoms[i++];
            }
        };
    }

    private static LegacyLayout.LegacyAtom rt(int start, int end, int deletion)
    {
        return new LegacyLayout.LegacyRangeTombstone(bound(start, true), bound(end, false), new DeletionTime(deletion, FBUtilities.nowInSeconds()));
    }

    private static LegacyLayout.LegacyBound bound(int b, boolean isStart)
    {
        return new LegacyLayout.LegacyBound(isStart ? ClusteringBound.inclusiveStartOf(ByteBufferUtil.bytes(b)) : ClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(b)),
                                            false,
                                            null);
    }
}
