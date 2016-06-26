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
package org.apache.cassandra.index.sasi.disk;

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.obs.BitUtil;

public interface TokenTreeBuilder extends Iterable<Pair<Long, KeyOffsets>>
{
    final static int BLOCK_BYTES = 4096;

    final static int LEAF_ENTRY_TYPE_BYTES = Short.BYTES;
    final static int TOKEN_OFFSET_BYTES = LEAF_ENTRY_TYPE_BYTES;
    final static int LEAF_PARTITON_OFFSET_BYTES = Long.BYTES;
    final static int LEAF_ROW_OFFSET_BYTES = Long.BYTES;

    final static int LEAF_PARTITON_OFFSET_PACKED_BYTES = Integer.BYTES;
    final static int LEAF_ROW_OFFSET_PACKED_BYTES = Integer.BYTES;
    final static int COLLISION_ENTRY_BYTES = LEAF_PARTITON_OFFSET_BYTES + LEAF_ROW_OFFSET_BYTES;

    final static int HEADER_INFO_BYTE_BYTES = Byte.BYTES;
    final static int HEADER_TOKEN_COUNT_BYTES = Short.BYTES;

    final static int ROOT_HEADER_MAGIC_SIZE = Short.BYTES;
    final static int ROOT_HEADER_TOKEN_COUNT_SIZE = Long.BYTES;

    // Partitioner token size in bytes
    final static int TOKEN_BYTES = Long.BYTES;

    // Leaf entry size in bytes, see {@class SimpleLeafEntry} for a full description
    final static int LEAF_ENTRY_BYTES = LEAF_ENTRY_TYPE_BYTES + TOKEN_BYTES + LEAF_PARTITON_OFFSET_BYTES + LEAF_ROW_OFFSET_BYTES;
    // Shared header size in bytes, see {@class AbstractTreeBuilder$Header} for a full description
    final static int SHARED_HEADER_BYTES = HEADER_INFO_BYTE_BYTES + HEADER_TOKEN_COUNT_BYTES + 2 * TOKEN_BYTES;
    // Block header size in bytes, see {@class AbstractTreeBuilder$RootHeader}
    final static int BLOCK_HEADER_BYTES = BitUtil.nextHighestPowerOfTwo(SHARED_HEADER_BYTES + ROOT_HEADER_MAGIC_SIZE + ROOT_HEADER_TOKEN_COUNT_SIZE + 2 * TOKEN_BYTES);

    // Overflow trailer capacity is currently 8 overflow items. Each overflow item consists of two longs.
    final static int OVERFLOW_TRAILER_CAPACITY = 8;
    final static int OVERFLOW_TRAILER_BYTES = OVERFLOW_TRAILER_CAPACITY * COLLISION_ENTRY_BYTES;;
    final static int TOKENS_PER_BLOCK = (TokenTreeBuilder.BLOCK_BYTES - BLOCK_HEADER_BYTES - OVERFLOW_TRAILER_BYTES) / LEAF_ENTRY_BYTES;

    final static int LEGACY_LEAF_ENTRY_BYTES = Short.BYTES + Short.BYTES + TOKEN_BYTES + Integer.BYTES;
    final static int LEGACY_TOKEN_OFFSET_BYTES = 2 * Short.BYTES;
    final static byte LAST_LEAF_SHIFT = 1;

    /**
     * {@code Header} size in bytes.
     */
    final byte ENTRY_TYPE_MASK = 0x03;
    final short AB_MAGIC = 0x5A51;
    final short AC_MAGIC = 0x7C63;

    // note: ordinal positions are used here, do not change order
    enum EntryType
    {
        SIMPLE,
        FACTORED,
        PACKED,
        OVERFLOW;

        public static EntryType of(int ordinal)
        {
            if (ordinal == SIMPLE.ordinal())
                return SIMPLE;

            if (ordinal == FACTORED.ordinal())
                return FACTORED;

            if (ordinal == PACKED.ordinal())
                return PACKED;

            if (ordinal == OVERFLOW.ordinal())
                return OVERFLOW;

            throw new IllegalArgumentException("Unknown ordinal: " + ordinal);
        }
    }

    void add(Long token, long partitionOffset, long rowOffset);
    void add(SortedMap<Long, KeyOffsets> data);
    void add(Iterator<Pair<Long, KeyOffsets>> data);
    void add(TokenTreeBuilder ttb);

    boolean isEmpty();
    long getTokenCount();

    TokenTreeBuilder finish();

    int serializedSize();
    void write(DataOutputPlus out) throws IOException;
}