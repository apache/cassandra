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
 * Author : Karthik Ranganathan ( kranganathan@facebook.com )
 */

public class IndexHelper
{
	/**
	 * Serializes a column index to a data output stream
	 * @param indexSizeInBytes Size of index to be written
	 * @param columnIndexList List of column index entries as objects
	 * @param dos the output stream into which the column index is to be written
	 * @throws IOException
	 */
	public static void serialize(int indexSizeInBytes, List<ColumnIndexInfo> columnIndexList, DataOutputStream dos) throws IOException
	{
		/* if we have no data to index, the write that there is no index present */
		if(indexSizeInBytes == 0 || columnIndexList == null || columnIndexList.size() == 0)
		{
			dos.writeBoolean(false);
		}
		else
		{
	        /* write if we are storing a column index */
	    	dos.writeBoolean(true);
	    	/* write the size of the index */
	    	dos.writeInt(indexSizeInBytes);
	        for( ColumnIndexInfo cIndexInfo : columnIndexList )
	        {
	        	cIndexInfo.serialize(dos);
	        }
		}
	}
    
    /**
     * Skip the bloom filter and the index and return the bytes read.
     * @param in the data input from which the bloom filter and index 
     *           should be skipped
     * @return number of bytes read.
     * @throws IOException
     */
    public static int skipBloomFilterAndIndex(DataInput in) throws IOException
    {
        int totalBytesRead = 0;
        /* size of the bloom filter */
        int size = in.readInt();
        totalBytesRead += 4;
        /* skip the serialized bloom filter */
        in.skipBytes(size);
        totalBytesRead += size;
        /* skip the index on disk */
        /* read if the file has column indexes */
        boolean hasColumnIndexes = in.readBoolean();
        totalBytesRead += 1;
        if ( hasColumnIndexes )
        {
            totalBytesRead += skipIndex(in);
        }
        return totalBytesRead;
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
	public static int skipIndex(DataInput file) throws IOException
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
     * @param tableName
     *@param in Input from which the serialized form of the index is read
     * @param columnIndexList the structure which is filled in with the deserialized index   @return number of bytes read from the input
     * @throws IOException
     */
	public static int deserializeIndex(String tableName, String cfName, DataInput in, List<ColumnIndexInfo> columnIndexList) throws IOException
	{
		/* read only the column index list */
		int columnIndexSize = in.readInt();
		int totalBytesRead = 4;

		/* read the indexes into a separate buffer */
		DataOutputBuffer indexOut = new DataOutputBuffer();
        /* write the data into buffer */
		indexOut.write(in, columnIndexSize);
		totalBytesRead += columnIndexSize;

		/* now deserialize the index list */
        DataInputBuffer indexIn = new DataInputBuffer();
        indexIn.reset(indexOut.getData(), indexOut.getLength());
        
        AbstractType comparator = DatabaseDescriptor.getComparator(tableName, cfName);

        while (indexIn.available() > 0)
        {
            // TODO this is all kinds of messed up
            ColumnIndexInfo cIndexInfo = new ColumnIndexInfo(comparator);
            cIndexInfo = cIndexInfo.deserialize(indexIn);
            columnIndexList.add(cIndexInfo);
        }

        return totalBytesRead;
	}

    /**
     * Returns the range in which a given column falls in the index
     * @param columnIndexList the in-memory representation of the column index
     * @param dataSize the total size of the data
     * @param totalNumCols total number of columns
     * @return an object describing a subrange in which the column is serialized
     */
	static ColumnRange getColumnRangeFromNameIndex(IndexHelper.ColumnIndexInfo cIndexInfo, List<IndexHelper.ColumnIndexInfo> columnIndexList, int dataSize, int totalNumCols)
	{
		/* find the offset for the column */
        int size = columnIndexList.size();
        long start = 0;
        long end = dataSize;
        int numColumns = 0;      
       
        int index = Collections.binarySearch(columnIndexList, cIndexInfo);
        if ( index < 0 )
        {
            /* We are here which means that the requested column is not an index. */
            index = (++index)*(-1);
        }
        else
        {
        	++index;
        }

        /* calculate the starting offset from which we have to read */
        start = (index == 0) ? 0 : columnIndexList.get(index - 1).position();

        if( index < size )
        {
        	end = columnIndexList.get(index).position();
            numColumns = columnIndexList.get(index).count();            
        }
        else
        {
        	end = dataSize;  
            int totalColsIndexed = 0;
            for( IndexHelper.ColumnIndexInfo colPosInfo : columnIndexList )
            {
                totalColsIndexed += colPosInfo.count();
            }
            numColumns = totalNumCols - totalColsIndexed;
        }

        return new ColumnRange(start, end, numColumns);
	}

	/**
	 * Returns the sub-ranges that contain the list of columns in columnNames.
	 * @param columnNames The list of columns whose subranges need to be found
	 * @param columnIndexList the deserialized column indexes
	 * @param dataSize the total size of data
	 * @param totalNumCols the total number of columns
	 * @return a list of subranges which contain all the columns in columnNames
	 */
	public static List<ColumnRange> getMultiColumnRangesFromNameIndex(SortedSet<byte[]> columnNames, List<IndexHelper.ColumnIndexInfo> columnIndexList, int dataSize, int totalNumCols)
	{
		List<ColumnRange> columnRanges = new ArrayList<ColumnRange>();

        if (columnIndexList.size() == 0)
        {
            columnRanges.add(new ColumnRange(0, dataSize, totalNumCols));
        }
        else
        {
            Map<Long, Boolean> offset = new HashMap<Long, Boolean>();
            for (byte[] name : columnNames)
            {
                IndexHelper.ColumnIndexInfo cIndexInfo = new IndexHelper.ColumnIndexInfo(name, 0, 0, (AbstractType)columnNames.comparator());
                ColumnRange columnRange = getColumnRangeFromNameIndex(cIndexInfo, columnIndexList, dataSize, totalNumCols);
                if (offset.get(columnRange.coordinate().start_) == null)
                {
                    columnRanges.add(columnRange);
                    offset.put(columnRange.coordinate().start_, true);
                }
            }
        }

        return columnRanges;
	}

    /**
         * Reads the column name indexes if present. If the
     * indexes are based on time then skip over them.
     */
    public static int readColumnIndexes(RandomAccessFile file, String tableName, String cfName, List<ColumnIndexInfo> columnIndexList) throws IOException
    {
        /* check if we have an index */
        boolean hasColumnIndexes = file.readBoolean();
        int totalBytesRead = 1;
        /* if we do then deserialize the index */
        if (hasColumnIndexes)
        {
            /* read the index */
            totalBytesRead += deserializeIndex(tableName, cfName, file, columnIndexList);
        }
        return totalBytesRead;
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
     * A column range containing the start and end
     * offset of the appropriate column index chunk
     * and the number of columns in that chunk.
     * @author alakshman
     *
     */
    public static class ColumnRange
    {
        private Coordinate coordinate_;
        private int columnCount_;
        
        ColumnRange(long start, long end, int columnCount)
        {
            coordinate_ = new Coordinate(start, end);
            columnCount_ = columnCount;
        }
        
        public Coordinate coordinate()
        {
            return coordinate_;
        }
        
        public int count()
        {
            return columnCount_;
        }                
    }

	/**
	 * A helper class to generate indexes while
     * the columns are sorted by name on disk.
	*/
    public static class ColumnIndexInfo implements Comparable<ColumnIndexInfo>
    {
        private long position_;
        private int columnCount_;
        private byte[] name_;
        private AbstractType comparator_;

        public ColumnIndexInfo(AbstractType comparator_)
        {
            this.comparator_ = comparator_;
        }

        public ColumnIndexInfo(byte[] name, long position, int columnCount, AbstractType comparator)
        {
            this(comparator);
            assert name.length == 0 || !"".equals(comparator.getString(name)); // Todo r/m length == 0 hack
            name_ = name;
            position_ = position;
            columnCount_ = columnCount;
        }
                
        public long position()
        {
            return position_;
        }
        
        public void position(long position)
        {
            position_ = position;
        }
        
        public int count()
        {
            return columnCount_;
        }
        
        public void count(int count)
        {
            columnCount_ = count;
        }

        public int compareTo(ColumnIndexInfo rhs)
        {
            return comparator_.compare(name_, rhs.name_);
        }

        public void serialize(DataOutputStream dos) throws IOException
        {
            dos.writeLong(position());
            dos.writeInt(count());
            ColumnSerializer.writeName(name_, dos);
        }

        public ColumnIndexInfo deserialize(DataInputStream dis) throws IOException
        {
            long position = dis.readLong();
            int columnCount = dis.readInt();
            byte[] name = ColumnSerializer.readName(dis);
            return new ColumnIndexInfo(name, position, columnCount, comparator_);
        }

        public int size()
        {
            // serialized size -- CS.writeName includes a 2-byte length prefix
            return 8 + 4 + 2 + name_.length;
        }

        public byte[] name()
        {
            return name_;
        }
    }
}
