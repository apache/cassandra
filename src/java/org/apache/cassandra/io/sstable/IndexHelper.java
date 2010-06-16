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
import java.util.*;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.io.util.FileDataInput;

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
        int skipped = in.skipBytes(size);
        if (skipped != size)
            throw new EOFException("attempted to skip " + size + " bytes but only skipped " + skipped);
    }

	/**
	 * Skip the index
	 * @param file the data input from which the index should be skipped
	 * @throws IOException
	 */
	public static void skipIndex(DataInput file) throws IOException
	{
        /* read only the column index list */
        int columnIndexSize = file.readInt();
        /* skip the column index data */
        if (file.skipBytes(columnIndexSize) != columnIndexSize)
            throw new EOFException();
	}
    
    /**
     * Deserialize the index into a structure and return it
     * @throws IOException
     */
	public static ArrayList<IndexInfo> deserializeIndex(FileDataInput in) throws IOException
	{
        ArrayList<IndexInfo> indexList = new ArrayList<IndexInfo>();

		int columnIndexSize = in.readInt();
        in.mark();
        while (in.bytesPastMark() < columnIndexSize)
        {
            indexList.add(IndexInfo.deserialize(in));
        }
        assert in.bytesPastMark() == columnIndexSize;

        return indexList;
	}

    /**
     * Defreeze the bloom filter.
     *
     * @return bloom filter summarizing the column information
     * @throws java.io.IOException
     */
    public static BloomFilter defreezeBloomFilter(DataInput file) throws IOException
    {
        int size = file.readInt();
        byte[] bytes = new byte[size];
        file.readFully(bytes);
        
        ByteArrayInputStream bufIn = new ByteArrayInputStream(bytes);
        return BloomFilter.serializer().deserialize(new DataInputStream(bufIn));
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
            FBUtilities.writeShortByteArray(firstName, dos);
            FBUtilities.writeShortByteArray(lastName, dos);
            dos.writeLong(offset);
            dos.writeLong(width);
        }

        public int serializedSize()
        {
            return 2 + firstName.length + 2 + lastName.length + 8 + 8;
        }

        public static IndexInfo deserialize(FileDataInput dis) throws IOException
        {
            return new IndexInfo(FBUtilities.readShortByteArray(dis), FBUtilities.readShortByteArray(dis), dis.readLong(), dis.readLong());
        }
    }
}
