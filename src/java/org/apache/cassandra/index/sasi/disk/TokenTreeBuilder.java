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

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Pair;

public interface TokenTreeBuilder extends Iterable<Pair<Long, Set<RowOffset>>>
{
    public static final int LONG_BYTES = Long.SIZE / 8;
    public static final int INT_BYTES = Integer.SIZE / 8;
    public static final int SHORT_BYTES = Short.SIZE / 8;

    final int ENTRY_TOKEN_OFFSET = 2;

    /**
     * {@code LeafEntry} size in bytes.
     */
    final int LEAF_ENTRY_BYTES = SHORT_BYTES + 3 * LONG_BYTES;

    final int BLOCK_BYTES = 4096;
    final int BLOCK_HEADER_BYTES = 64;

    /**
      * Overflow trailer capacity is currently 8 overflow items. Each overflow item consists of two longs.
      */
    final int OVERFLOW_TRAILER_CAPACITY = 8;
    final int OVERFLOW_TRAILER_BYTES = OVERFLOW_TRAILER_CAPACITY * (2 * LONG_BYTES);

    final int TOKENS_PER_BLOCK = (BLOCK_BYTES - BLOCK_HEADER_BYTES - OVERFLOW_TRAILER_BYTES) / LEAF_ENTRY_BYTES;
    final byte LAST_LEAF_SHIFT = 1;


    /**
     * {@code Header} size in bytes.
     */
    final byte SHARED_HEADER_BYTES = 1 + SHORT_BYTES + 2 * LONG_BYTES;
    final byte ENTRY_TYPE_MASK = 0x03;
    final short AB_MAGIC = 0x5A51;

    // note: ordinal positions are used here, do not change order
    enum EntryType
    {
        SIMPLE,
        PACKED,
        OVERFLOW;

        public static EntryType of(int ordinal)
        {
            if (ordinal == SIMPLE.ordinal())
                return SIMPLE;

            if (ordinal == PACKED.ordinal())
                return PACKED;

            if (ordinal == OVERFLOW.ordinal())
                return OVERFLOW;

            throw new IllegalArgumentException("Unknown ordinal: " + ordinal);
        }
    }

    void add(Long token, long partitionOffset, long rowOffset);
    void add(SortedMap<Long, Set<RowOffset>> data);
    void add(Iterator<Pair<Long, Set<RowOffset>>> data);
    void add(TokenTreeBuilder ttb);

    boolean isEmpty();
    long getTokenCount();

    TokenTreeBuilder finish();

    int serializedSize();
    void write(DataOutputPlus out) throws IOException;
}