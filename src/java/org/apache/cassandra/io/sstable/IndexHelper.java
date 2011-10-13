/**
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.*;

/**
 * Provides helper to serialize, deserialize and use column indexes.
 */
public class IndexHelper
{

    /**
     * Skip the bloom filter
     * @param in the data input from which the bloom filter should be skipped
     * @throws IOException
     */
    public static void skipBloomFilter(DataInput in) throws IOException
    {
        /* size of the bloom filter */
        int size = in.readInt();
        /* skip the serialized bloom filter */
        if (in instanceof FileDataInput)
        {
            FileUtils.skipBytesFully(in, size);
        }
        else
        {
            // skip bytes
            byte[] skip = new byte[size];
            in.readFully(skip);
        }
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
     * Deserialize the index into a structure and return it
     *
     * @param in - input source
     *
     * @return ArrayList<IndexInfo> - list of de-serialized indexes
     * @throws IOException if an I/O error occurs.
     */
	public static ArrayList<IndexInfo> deserializeIndex(FileDataInput in) throws IOException
	{
		int columnIndexSize = in.readInt();
        if (columnIndexSize == 0)
            return null;
        ArrayList<IndexInfo> indexList = new ArrayList<IndexInfo>();
        FileMark mark = in.mark();
        while (in.bytesPastMark(mark) < columnIndexSize)
        {
            indexList.add(IndexInfo.deserialize(in));
        }
        assert in.bytesPastMark(mark) == columnIndexSize;

        return indexList;
	}

    public static Filter defreezeBloomFilter(FileDataInput file, boolean usesOldBloomFilter) throws IOException
    {
        return defreezeBloomFilter(file, Integer.MAX_VALUE, usesOldBloomFilter);
    }

    /**
     * De-freeze the bloom filter.
     *
     * @param file - source file
     * @param maxSize - sanity check: if filter claimes to be larger than this it is bogus
     * @param useOldBuffer - do we need to reuse old buffer?
     *
     * @return bloom filter summarizing the column information
     * @throws java.io.IOException if an I/O error occurs.
     * Guarantees that file's current position will be just after the bloom filter, even if
     * the filter cannot be deserialized, UNLESS EOFException is thrown.
     */
    public static Filter defreezeBloomFilter(FileDataInput file, long maxSize, boolean useOldBuffer) throws IOException
    {
        int size = file.readInt();
        if (size > maxSize || size <= 0)
            throw new EOFException("bloom filter claims to be " + size + " bytes, longer than entire row size " + maxSize);
        ByteBuffer bytes = file.readBytes(size);

        DataInputStream stream = new DataInputStream(ByteBufferUtil.inputStream(bytes));
        return useOldBuffer
               ? LegacyBloomFilter.serializer().deserialize(stream)
               : BloomFilter.serializer().deserialize(stream);
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
    public static int indexFor(ByteBuffer name, List<IndexInfo> indexList, AbstractType comparator, boolean reversed)
    {
        if (name.remaining() == 0 && reversed)
            return indexList.size() - 1;
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
        int index = Collections.binarySearch(indexList, target, getComparator(comparator, reversed));
        return index < 0 ? -index - (reversed ? 2 : 1) : index;
    }

    public static Comparator<IndexInfo> getComparator(final AbstractType nameComparator, boolean reversed)
    {
        return reversed ? nameComparator.indexReverseComparator : nameComparator.indexComparator;
    }

    public static class IndexInfo
    {
        public final long width;
        public final ByteBuffer lastName;
        public final ByteBuffer firstName;
        public final long offset;

        public IndexInfo(ByteBuffer firstName, ByteBuffer lastName, long offset, long width)
        {
            this.firstName = firstName;
            this.lastName = lastName;
            this.offset = offset;
            this.width = width;
        }

        public void serialize(DataOutput dos) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(firstName, dos);
            ByteBufferUtil.writeWithShortLength(lastName, dos);
            dos.writeLong(offset);
            dos.writeLong(width);
        }

        public int serializedSize()
        {
            return 2 + firstName.remaining() + 2 + lastName.remaining() + 8 + 8;
        }

        public static IndexInfo deserialize(FileDataInput dis) throws IOException
        {
            return new IndexInfo(ByteBufferUtil.readWithShortLength(dis), ByteBufferUtil.readWithShortLength(dis), dis.readLong(), dis.readLong());
        }
    }
}
