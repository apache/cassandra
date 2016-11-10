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

import java.io.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.composites.CType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.*;

/**
 * Provides helper to serialize, deserialize and use column indexes.
 */
public class IndexHelper
{
    public static void skipBloomFilter(DataInput in) throws IOException
    {
        int size = in.readInt();
        FileUtils.skipBytesFully(in, size);
    }

    /**
     * Skip the index
     * @param in the data input from which the index should be skipped
     * @throws IOException if an I/O error occurs.
     */
    public static void skipIndex(DataInput in) throws IOException
    {
        /* read only the column index list */
        int columnIndexSize = in.readInt();
        /* skip the column index data */
        if (in instanceof FileDataInput)
        {
            FileUtils.skipBytesFully(in, columnIndexSize);
        }
        else
        {
            // skip bytes
            byte[] skip = new byte[columnIndexSize];
            in.readFully(skip);
        }
    }

    /**
     * The index of the IndexInfo in which a scan starting with @name should begin.
     *
     * @param name
     *         name of the index
     *
     * @param indexList
     *          list of the indexInfo objects
     *
     * @param comparator
     *          comparator type
     *
     * @param reversed
     *          is name reversed
     *
     * @return int index
     */
    public static int indexFor(Composite name, List<IndexInfo> indexList, CType comparator, boolean reversed, int lastIndex)
    {
        if (name.isEmpty())
            return lastIndex >= 0 ? lastIndex : reversed ? indexList.size() - 1 : 0;

        if (lastIndex >= indexList.size())
            return -1;

        IndexInfo target = new IndexInfo(name, name, 0, 0);
        /*
        Take the example from the unit test, and say your index looks like this:
        [0..5][10..15][20..25]
        and you look for the slice [13..17].

        When doing forward slice, we we doing a binary search comparing 13 (the start of the query)
        to the lastName part of the index slot. You'll end up with the "first" slot, going from left to right,
        that may contain the start.

        When doing a reverse slice, we do the same thing, only using as a start column the end of the query,
        i.e. 17 in this example, compared to the firstName part of the index slots.  bsearch will give us the
        first slot where firstName > start ([20..25] here), so we subtract an extra one to get the slot just before.
        */
        int startIdx = 0;
        List<IndexInfo> toSearch = indexList;
        if (lastIndex >= 0)
        {
            if (reversed)
            {
                toSearch = indexList.subList(0, lastIndex + 1);
            }
            else
            {
                startIdx = lastIndex;
                toSearch = indexList.subList(lastIndex, indexList.size());
            }
        }
        int index = Collections.binarySearch(toSearch, target, getComparator(comparator, reversed));
        return startIdx + (index < 0 ? -index - (reversed ? 2 : 1) : index);
    }

    public static Comparator<IndexInfo> getComparator(final CType nameComparator, boolean reversed)
    {
        return reversed ? nameComparator.indexReverseComparator() : nameComparator.indexComparator();
    }

    public static class IndexInfo
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new IndexInfo(null, null, 0, 0));

        public final long width;
        public final Composite lastName;
        public final Composite firstName;
        public final long offset;

        public IndexInfo(Composite firstName, Composite lastName, long offset, long width)
        {
            this.firstName = firstName;
            this.lastName = lastName;
            this.offset = offset;
            this.width = width;
        }

        public static class Serializer implements ISerializer<IndexInfo>
        {
            private final CType type;

            public Serializer(CType type)
            {
                this.type = type;
            }

            public void serialize(IndexInfo info, DataOutputPlus out) throws IOException
            {
                type.serializer().serialize(info.firstName, out);
                type.serializer().serialize(info.lastName, out);
                out.writeLong(info.offset);
                out.writeLong(info.width);
            }

            public IndexInfo deserialize(DataInput in) throws IOException
            {
                return new IndexInfo(type.serializer().deserialize(in),
                                     type.serializer().deserialize(in),
                                     in.readLong(),
                                     in.readLong());
            }

            public long serializedSize(IndexInfo info, TypeSizes typeSizes)
            {
                return type.serializer().serializedSize(info.firstName, typeSizes)
                     + type.serializer().serializedSize(info.lastName, typeSizes)
                     + typeSizes.sizeof(info.offset)
                     + typeSizes.sizeof(info.width);
            }
        }

        public long unsharedHeapSize()
        {
            return EMPTY_SIZE + firstName.unsharedHeapSize() + lastName.unsharedHeapSize();
        }
    }
}
