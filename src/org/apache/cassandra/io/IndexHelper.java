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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.TypeInfo;
import org.apache.cassandra.io.SSTable.KeyPositionInfo;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;


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
     * @param in Input from which the serialized form of the index is read
     * @param columnIndexList the structure which is filled in with the deserialized index
     * @return number of bytes read from the input
     * @throws IOException
     */
	static int deserializeIndex(String cfName, DataInput in, List<ColumnIndexInfo> columnIndexList) throws IOException
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
        String columnName;
        int position;
        int numCols;
        
        TypeInfo typeInfo = DatabaseDescriptor.getTypeInfo(cfName);
        if ( DatabaseDescriptor.getColumnFamilyType(cfName).equals("Super") || DatabaseDescriptor.isNameSortingEnabled(cfName) )
        {
            typeInfo = TypeInfo.STRING;
        }
        
        while(indexIn.available() > 0)
        {            
            ColumnIndexInfo cIndexInfo = ColumnIndexFactory.instance(typeInfo);
        	cIndexInfo = cIndexInfo.deserialize(indexIn);
        	columnIndexList.add(cIndexInfo);
        }

		return totalBytesRead;
	}

    /**
     * Returns the range in which a given column falls in the index
     * @param column The column whose range needs to be found
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
	static List<ColumnRange> getMultiColumnRangesFromNameIndex(List<String> columnNames, List<IndexHelper.ColumnIndexInfo> columnIndexList, int dataSize, int totalNumCols)
	{
		List<ColumnRange> columnRanges = new ArrayList<ColumnRange>();				

        if ( columnIndexList.size() == 0 )
        {
            columnRanges.add( new ColumnRange(0, dataSize, totalNumCols) );
        }
        else
        {
            Map<Long, Boolean> offset = new HashMap<Long, Boolean>();
    		for(String column : columnNames)
    		{
                IndexHelper.ColumnIndexInfo cIndexInfo = new IndexHelper.ColumnNameIndexInfo(column);
    			ColumnRange columnRange = getColumnRangeFromNameIndex(cIndexInfo, columnIndexList, dataSize, totalNumCols);   
                if ( offset.get( columnRange.coordinate().start_ ) == null ) 
                {
                    columnRanges.add(columnRange);
                    offset.put(columnRange.coordinate().start_, true);
                }
    		}
        }

		return columnRanges;
	}
    
    /**
     * Returns the range in which a given column falls in the index. This
     * is used when time range queries are in play. For instance if we are
     * looking for columns in the range [t, t2]
     * @param cIndexInfo the time we are interested in.
     * @param columnIndexList the in-memory representation of the column index
     * @param dataSize the total size of the data
     * @param totalNumCols total number of columns
     * @return an object describing a subrange in which the column is serialized
     */
    static ColumnRange getColumnRangeFromTimeIndex(IndexHelper.TimeRange timeRange, List<IndexHelper.ColumnIndexInfo> columnIndexList, int dataSize, int totalNumCols)
    {
        /* if column indexes were not present for this column family, the handle accordingly */
        if(columnIndexList.size() == 0)
        {
            return new ColumnRange(0, dataSize, totalNumCols);
        }

        /* find the offset for the column */
        int size = columnIndexList.size();
        long start = 0;
        long end = dataSize;
        int numColumns = 0;      
       
        /*
         *  Time indicies are sorted in descending order. So
         *  we need to apply a reverse compartor for the 
         *  binary search.        
        */        
        Comparator<IndexHelper.ColumnIndexInfo> comparator = Collections.reverseOrder(); 
        IndexHelper.ColumnIndexInfo rhs = IndexHelper.ColumnIndexFactory.instance(TypeInfo.LONG);
        rhs.set(timeRange.rhs());
        int index = Collections.binarySearch(columnIndexList, rhs, comparator);
        if ( index < 0 )
        {
            /* We are here which means that the requested column is not an index. */
            index = (++index)*(-1);
        }
        else
        {
            ++index;
        }

        /* 
         * Calculate the starting offset from which we have to read. So
         * we achieve this by performing the probe using the bigger timestamp
         * and then scanning the column position chunks till we reach the
         * lower timestamp in the time range.      
        */
        start = (index == 0) ? 0 : columnIndexList.get(index - 1).position();
        /* add the number of colunms in the first chunk. */
        numColumns += (index ==0) ? columnIndexList.get(0).count() : columnIndexList.get(index - 1).count(); 
        if( index < size )
        {            
            int chunks = columnIndexList.size();
            /* Index info for the lower bound of the time range */
            IndexHelper.ColumnIndexInfo lhs = IndexHelper.ColumnIndexFactory.instance(TypeInfo.LONG);
            lhs.set(timeRange.lhs());
            int i = index + 1;
            for ( ; i < chunks; ++i )
            {
                IndexHelper.ColumnIndexInfo cIndexInfo2 = columnIndexList.get(i);
                if ( cIndexInfo2.compareTo(lhs) < 0 )
                {
                    numColumns += cIndexInfo2.count();
                    break;
                } 
                numColumns += cIndexInfo2.count();
            }
            
            end = columnIndexList.get(i).position();                       
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
    
    public static class ColumnIndexFactory
    {
        public static ColumnIndexInfo instance(TypeInfo typeInfo)
        {
            ColumnIndexInfo cIndexInfo = null;
            switch(typeInfo)
            {
                case STRING:
                    cIndexInfo = new ColumnNameIndexInfo();
                    break;
                    
                case LONG:
                    cIndexInfo = new ColumnTimestampIndexInfo();
                    break;
            }
            return cIndexInfo;
        }    
    }
    
    /**
     * Encapsulates a time range. Queries use 
     * this abstraction for indicating start 
     * and end regions of a time filter.
     * 
     * @author alakshman
     *
     */
    public static class TimeRange
    {
        private long lhs_;
        private long rhs_;
        
        public TimeRange(long lhs, long rhs)
        {
            lhs_ = lhs;
            rhs_ = rhs;
        }
        
        public long lhs()
        {
            return lhs_;
        }
        
        public long rhs()
        {
            return rhs_;
        }
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
        
        Coordinate coordinate()
        {
            return coordinate_;
        }
        
        int count()
        {
            return columnCount_;
        }                
    }

	/**
	 * A helper class to generate indexes while
     * the columns are sorted by name on disk.
	*/
    public static abstract class ColumnIndexInfo implements Comparable<ColumnIndexInfo>
    {
        private long position_;
        private int columnCount_;        
        
        ColumnIndexInfo(long position, int columnCount)
        {
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
        
        int count()
        {
            return columnCount_;
        }
        
        public void count(int count)
        {
            columnCount_ = count;
        }
                
        public abstract void set(Object o);
        public abstract void serialize(DataOutputStream dos) throws IOException;
        public abstract ColumnIndexInfo deserialize(DataInputStream dis) throws IOException;
        
        public int size()
        {
            /* size of long for "position_"  + size of columnCount_ */
            return (8 + 4);
        }
    }

    static class ColumnNameIndexInfo extends ColumnIndexInfo
    {
        private String name_;       
        
        ColumnNameIndexInfo()
        {
            super(0L, 0);
        }
        
        ColumnNameIndexInfo(String name)
        {
            this(name, 0L, 0);
        }
                
        ColumnNameIndexInfo(String name, long position, int columnCount)
        {
            super(position, columnCount);
            name_ = name;
        }
        
        String name()
        {
            return name_;
        }                
        
        public void set(Object o)
        {
            name_ = (String)o;
        }
        
        public int compareTo(ColumnIndexInfo rhs)
        {
            IndexHelper.ColumnNameIndexInfo cIndexInfo = (IndexHelper.ColumnNameIndexInfo)rhs;
            return name_.compareTo(cIndexInfo.name_);
        }
        
        public void serialize(DataOutputStream dos) throws IOException
        {
            dos.writeLong(position()); 
            dos.writeInt(count());
            dos.writeUTF(name_);        
        }
        
        public ColumnNameIndexInfo deserialize(DataInputStream dis) throws IOException
        {
            long position = dis.readLong();
            int columnCount = dis.readInt();            
            String name = dis.readUTF();       
            return new ColumnNameIndexInfo(name, position, columnCount);
        }
        
        public int size()
        {
            int size = super.size();
            /* Size of the name_ as an UTF8 and the actual length as a short for the readUTF. */
            size += FBUtilities.getUTF8Length(name_) + IColumn.UtfPrefix_;
            return size;
        }
    }

    static class ColumnTimestampIndexInfo extends ColumnIndexInfo
    {
        private long timestamp_;
        
        ColumnTimestampIndexInfo()
        {
            super(0L, 0);
        }
        
        ColumnTimestampIndexInfo(long timestamp)
        {
            this(timestamp, 0L, 0);  
        }
        
        ColumnTimestampIndexInfo(long timestamp, long position, int columnCount)
        {
            super(position, columnCount);
            timestamp_ = timestamp;
        }
        
        public long timestamp()
        {
            return timestamp_;
        }
        
        public void set(Object o)
        {
            timestamp_ = (Long)o;
        }
        
        public int compareTo(ColumnIndexInfo rhs)
        {
            ColumnTimestampIndexInfo cIndexInfo = (ColumnTimestampIndexInfo)rhs;
            return Long.valueOf(timestamp_).compareTo(Long.valueOf(cIndexInfo.timestamp_));
        }
        
        public void serialize(DataOutputStream dos) throws IOException
        {
            dos.writeLong(position()); 
            dos.writeInt(count());
            dos.writeLong(timestamp_);        
        }
        
        public ColumnTimestampIndexInfo deserialize(DataInputStream dis) throws IOException
        {
            long position = dis.readLong();
            int columnCount = dis.readInt();
            long timestamp = dis.readLong();        
            return new ColumnTimestampIndexInfo(timestamp, position, columnCount);
        }
        
        public int size()
        {
            int size = super.size();
            /* add the size of the timestamp which is a long */ 
            size += 8;
            return size;
        }
    }
}
