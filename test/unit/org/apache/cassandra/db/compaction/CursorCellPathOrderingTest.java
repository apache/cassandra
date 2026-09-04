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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/**
 * Tests that the cell-path order of the cursor matches
 * {@link ColumnMetadata#cellPathComparator()} for every complex type.
 *
 * That comparator sets the cell order flush writes to disk. It also sets how the iterator groups
 * cells in a merge. A cursor order that differs groups the wrong cells together, so same-path
 * cells in different sstables never meet.
 *
 * A UDT cell path is a 2-byte field index, and {@link UserType#nameComparator()} is ShortType.
 * ShortType compares the two bytes as a signed short, so an unsigned byte order differs at field
 * index 32768, that is 0x8000. A UDT needs 32769 fields to reach that index, so no real schema
 * reaches it. The two orders must still agree by design, not because schemas stay small.
 */
public class CursorCellPathOrderingTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static void assertOrderMatchesReference(ColumnMetadata column, ByteBuffer p1, ByteBuffer p2)
    {
        int reference = Integer.signum(column.cellPathComparator().compare(CellPath.create(p1), CellPath.create(p2)));
        int cursor = Integer.signum(CursorCompactor.comparePaths(column, p1.duplicate(), p2.duplicate()));
        assertEquals("cursor path order diverges from ColumnMetadata.cellPathComparator for " + column.type +
                     " on paths " + ByteBufferUtil.bytesToHex(p1) + " / " + ByteBufferUtil.bytesToHex(p2),
                     reference, cursor);
        // The two orders must also agree when the pair is swapped.
        int referenceRev = Integer.signum(column.cellPathComparator().compare(CellPath.create(p2), CellPath.create(p1)));
        int cursorRev = Integer.signum(CursorCompactor.comparePaths(column, p2.duplicate(), p1.duplicate()));
        assertEquals(referenceRev, cursorRev);
    }

    @Test
    public void udtFieldIndexBoundary()
    {
        UserType udt = new UserType("ks", ByteBufferUtil.bytes("t"),
                                    Arrays.asList(new FieldIdentifier(ByteBufferUtil.bytes("f1")),
                                                  new FieldIdentifier(ByteBufferUtil.bytes("f2"))),
                                    Arrays.asList(UTF8Type.instance, UTF8Type.instance),
                                    true);
        ColumnMetadata column = ColumnMetadata.regularColumn("ks", "cf", "u", udt, 0);

        // The boundary between signed and unsigned order. ShortType reads 0x8000 as -32768, so
        // the reference puts 0x8000 first.
        assertOrderMatchesReference(column, ByteBufferUtil.bytes((short) 0x7FFF), ByteBufferUtil.bytes((short) 0x8000));
        // Both orders agree below the boundary.
        assertOrderMatchesReference(column, ByteBufferUtil.bytes((short) 0), ByteBufferUtil.bytes((short) 1));
        assertOrderMatchesReference(column, ByteBufferUtil.bytes((short) 7), ByteBufferUtil.bytes((short) 7));
    }

    @Test
    public void collectionPathsStayTypeRouted()
    {
        AbstractType<?> mapType = MapType.getInstance(Int32Type.instance, UTF8Type.instance, true);
        ColumnMetadata column = ColumnMetadata.regularColumn("ks", "cf", "m", mapType, 0);

        // An Int32 map key compares as signed. A byte order would put a negative key after a
        // positive one.
        assertOrderMatchesReference(column, Int32Type.instance.decompose(-3), Int32Type.instance.decompose(5));
        assertOrderMatchesReference(column, Int32Type.instance.decompose(1), Int32Type.instance.decompose(2));
    }

    /**
     * Tests that a list cell path compares in timestamp order.
     *
     * A list cell path is a timeuuid, and TimeUUIDType compares it by its timestamp. The uuid
     * holds that 60-bit timestamp in three parts: time_low in bytes 0 to 3, time_mid in bytes 4
     * and 5, and time_hi in the low 12 bits of bytes 6 and 7.
     *
     * A byte order reads time_low first, but time_low holds the lowest bits of the timestamp. A
     * uuid with a large time_low and a small time_hi therefore sorts later by bytes and earlier by
     * timestamp. A list that takes prepends in one sstable and appends in another gives such
     * pairs.
     */
    @Test
    public void listPathsUseTimeUuidOrder()
    {
        AbstractType<?> listType = ListType.getInstance(UTF8Type.instance, true);
        ColumnMetadata column = ColumnMetadata.regularColumn("ks", "cf", "l", listType, 0);

        // Path a has time_low 0xFFFFFFFF and time_hi 1, which gives a timestamp near 2^48. Path b
        // has time_low 0 and time_hi 2, which gives a timestamp of 2 x 2^48. By bytes a is above
        // b. By timestamp a is below b.
        ByteBuffer a = timeUuid(0xFFFFFFFF00001001L);
        ByteBuffer b = timeUuid(0x0000000000001002L);
        assertOrderMatchesReference(column, a, b);
        assertOrderMatchesReference(column, timeUuid(0x0000000100001001L), timeUuid(0x0000000200001001L));
        assertOrderMatchesReference(column, a.duplicate(), a.duplicate());
    }

    private static ByteBuffer timeUuid(long msb)
    {
        ByteBuffer uuid = ByteBuffer.allocate(16);
        uuid.putLong(msb);
        uuid.putLong(0x8080808080808080L); // sets the variant bits to 10; the rest is arbitrary
        uuid.flip();
        return uuid;
    }
}
