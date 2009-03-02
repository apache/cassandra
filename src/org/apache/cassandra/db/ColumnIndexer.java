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

package org.apache.cassandra.db;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IndexHelper;
import org.apache.cassandra.io.SSTable.KeyPositionInfo;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;


/**
 * Help to create an index for a column family based on size of columns
 * Author : Karthik Ranganathan ( kranganathan@facebook.com )
 */

public class ColumnIndexer
{
	/**
	 * Given a column family this, function creates an in-memory structure that represents the
	 * column index for the column family, and subsequently writes it to disk.
	 * @param columnFamily Column family to create index for
	 * @param dos data output stream
	 * @throws IOException
	 */
    public static void serialize(ColumnFamily columnFamily, DataOutputStream dos) throws IOException
	{
        Collection<IColumn> columns = columnFamily.getAllColumns();
        BloomFilter bf = createColumnBloomFilter(columns);                    
        /* Write out the bloom filter. */
        DataOutputBuffer bufOut = new DataOutputBuffer(); 
        BloomFilter.serializer().serialize(bf, bufOut);
        /* write the length of the serialized bloom filter. */
        dos.writeInt(bufOut.getLength());
        /* write out the serialized bytes. */
        dos.write(bufOut.getData(), 0, bufOut.getLength());
                
        TypeInfo typeInfo = DatabaseDescriptor.getTypeInfo(columnFamily.name());        
        doIndexing(typeInfo, columns, dos);        
	}
    
    /**
     * Create a bloom filter that contains the subcolumns and the columns that
     * make up this Column Family.
     * @param columns columns of the ColumnFamily
     * @return BloomFilter with the summarized information.
     */
    private static BloomFilter createColumnBloomFilter(Collection<IColumn> columns)
    {
        int columnCount = 0;
        for ( IColumn column : columns )
        {
            columnCount += column.getObjectCount();
        }
        
        BloomFilter bf = new BloomFilter(columnCount, 4);
        for ( IColumn column : columns )
        {
            bf.fill(column.name());
            /* If this is SuperColumn type Column Family we need to get the subColumns too. */
            if ( column instanceof SuperColumn )
            {
                Collection<IColumn> subColumns = column.getSubColumns();
                for ( IColumn subColumn : subColumns )
                {
                    bf.fill(subColumn.name());
                }
            }
        }
        return bf;
    }
    
    private static IndexHelper.ColumnIndexInfo getColumnIndexInfo(TypeInfo typeInfo, IColumn column)
    {
        IndexHelper.ColumnIndexInfo cIndexInfo = null;
        
        if ( column instanceof SuperColumn )
        {
            cIndexInfo = IndexHelper.ColumnIndexFactory.instance(TypeInfo.STRING);            
            cIndexInfo.set(column.name());
        }
        else
        {
            cIndexInfo = IndexHelper.ColumnIndexFactory.instance(typeInfo);                        
            switch(typeInfo)
            {
                case STRING:
                    cIndexInfo.set(column.name());                        
                    break;
                    
                case LONG:
                    cIndexInfo.set(column.timestamp());                        
                    break;
            }
        }
        
        return cIndexInfo;
    }

    /**
     * Given the collection of columns in the Column Family,
     * the name index is generated and written into the provided
     * stream
     * @param columns for whom the name index needs to be generated
     * @param bf bloom filter that summarizes the columns that make
     *           up the column family.
     * @param dos stream into which the serialized name index needs
     *            to be written.
     * @throws IOException
     */
    private static void doIndexing(TypeInfo typeInfo, Collection<IColumn> columns, DataOutputStream dos) throws IOException
    {
        /* we are going to write column indexes */
        int numColumns = 0;
        int position = 0;
        int indexSizeInBytes = 0;
        int sizeSummarized = 0;
        
        /*
         * Maintains a list of KeyPositionInfo objects for the columns in this
         * column family. The key is the column name and the position is the
         * relative offset of that column name from the start of the list.
         * We do this so that we don't read all the columns into memory.
        */
        
        List<IndexHelper.ColumnIndexInfo> columnIndexList = new ArrayList<IndexHelper.ColumnIndexInfo>();        
        
        /* column offsets at the right thresholds into the index map. */
        for ( IColumn column : columns )
        {
            /* if we hit the column index size that we have to index after, go ahead and index it */
            if(position - sizeSummarized >= DatabaseDescriptor.getColumnIndexSize())
            {      
                /*
                 * ColumnSort applies only to columns. So in case of 
                 * SuperColumn always use the name indexing scheme for
                 * the SuperColumns. We will fix this later.
                 */
                IndexHelper.ColumnIndexInfo cIndexInfo = getColumnIndexInfo(typeInfo, column);                
                cIndexInfo.position(position);
                cIndexInfo.count(numColumns);                
                columnIndexList.add(cIndexInfo);
                /*
                 * we will be writing this object as a UTF8 string and two ints,
                 * so calculate the size accordingly. Note that we store the string
                 * as UTF-8 encoded, so when we calculate the length, it should be
                 * converted to UTF-8.
                 */
                indexSizeInBytes += cIndexInfo.size();
                sizeSummarized = position;
                numColumns = 0;
            }
            position += column.serializedSize();
            ++numColumns;
        }
        /* write the column index list */
        IndexHelper.serialize(indexSizeInBytes, columnIndexList, dos);
    }
}
