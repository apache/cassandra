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

package org.apache.cassandra.io;

import java.io.*;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.BloomFilter;


/**
 * Provides helper to serialize, deserialize and use column indexes.
 */

public class IndexHelper
{

    /**
     * Skip the bloom filter and the index and return the bytes read.
     * @param in the data input from which the bloom filter and index 
     *           should be skipped
     * @return number of bytes read.
     * @throws IOException
     */
    public static int skipBloomFilterAndIndex(DataInput in) throws IOException
    {
        return skipBloomFilter(in) + skipIndex(in);
    }
    
    /**
     * Skip the bloom filter and return the bytes read.
     * @param in the data input from which the bloom filter 
     *           should be skipped
     * @return number of bytes read.
     * @throws IOException
     */
    public static int skipBloomFilter(DataInput in) throws IOException
    {
        int totalBytesRead = 0;
        /* size of the bloom filter */
        int size = in.readInt();
        totalBytesRead += 4;
        /* skip the serialized bloom filter */
        in.skipBytes(size);
        totalBytesRead += size;
        return totalBytesRead;
    }

	/**
	 * Skip the index and return the number of bytes read.
	 * @param file the data input from which the index should be skipped
	 * @return number of bytes read from the data input
	 * @throws IOException
	 */
	private static int skipIndex(DataInput file) throws IOException
	{
        /* read only the column index list */
        int columnIndexSize = file.readInt();
        int totalBytesRead = 4;

        /* skip the column index data */
        file.skipBytes(columnIndexSize);
        totalBytesRead += columnIndexSize;

        return totalBytesRead;
	}
    
    /**
     * Deserialize the index into a structure and return the number of bytes read.
     * @throws IOException
     */
	public static ArrayList<IndexInfo> deserializeIndex(RandomAccessFile in) throws IOException
	{
        ArrayList<IndexInfo> indexList = new ArrayList<IndexInfo>();

		int columnIndexSize = in.readInt();
        long start = in.getFilePointer();
        while (in.getFilePointer() < start + columnIndexSize)
        {
            indexList.add(IndexInfo.deserialize(in));
        }
        assert in.getFilePointer() == start + columnIndexSize;

        return indexList;
	}

    /**
     * Defreeze the bloom filter.
     *
     * @return bloom filter summarizing the column information
     * @throws java.io.IOException
     */
    public static BloomFilter defreezeBloomFilter(RandomAccessFile file) throws IOException
    {
        int size = file.readInt();
        byte[] bytes = new byte[size];
        file.readFully(bytes);
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(bytes, bytes.length);
        return BloomFilter.serializer().deserialize(bufIn);
    }

    /**
     * the index of the IndexInfo in which @name will be found.
     * If the index is @indexList.size(), the @name appears nowhere.
     */
    public static int indexFor(byte[] name, List<IndexInfo> indexList, AbstractType comparator, boolean reversed)
    {
        if (name.length == 0 && reversed)
            return indexList.size() - 1;
        IndexInfo target = new IndexInfo(name, name, 0, 0);
        int index = Collections.binarySearch(indexList, target, getComparator(comparator));
        return index < 0 ? -1 * (index + 1) : index;
    }

    public static Comparator<IndexInfo> getComparator(final AbstractType nameComparator)
    {
        return new Comparator<IndexInfo>()
        {
            public int compare(IndexInfo o1, IndexInfo o2)
            {
                return nameComparator.compare(o1.lastName, o2.lastName);
            }
        };
    }

    public static class IndexInfo
    {
        public final long width;
        public final byte[] lastName;
        public final byte[] firstName;
        public final long offset;

        public IndexInfo(byte[] firstName, byte[] lastName, long offset, long width)
        {
            this.firstName = firstName;
            this.lastName = lastName;
            this.offset = offset;
            this.width = width;
        }

        public void serialize(DataOutput dos) throws IOException
        {
            ColumnSerializer.writeName(firstName, dos);
            ColumnSerializer.writeName(lastName, dos);
            dos.writeLong(offset);
            dos.writeLong(width);
        }

        public int serializedSize()
        {
            return 2 + firstName.length + 2 + lastName.length + 8 + 8;
        }

        public static IndexInfo deserialize(RandomAccessFile dis) throws IOException
        {
            return new IndexInfo(ColumnSerializer.readName(dis), ColumnSerializer.readName(dis), dis.readLong(), dis.readLong());
        }
    }

}
