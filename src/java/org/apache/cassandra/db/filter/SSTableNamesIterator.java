package org.apache.cassandra.db.filter;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.io.IOError;
import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;

public class SSTableNamesIterator extends SimpleAbstractColumnIterator implements IColumnIterator
{
    private static Logger logger = LoggerFactory.getLogger(SSTableNamesIterator.class);

    private ColumnFamily cf;
    private Iterator<IColumn> iter;
    public final SortedSet<byte[]> columns;
    public final DecoratedKey decoratedKey;

    public SSTableNamesIterator(SSTableReader ssTable, DecoratedKey key, SortedSet<byte[]> columnNames)
    {
        this (ssTable, null, key, columnNames);
    }

    public SSTableNamesIterator(SSTableReader ssTable, FileDataInput file, DecoratedKey key, SortedSet<byte[]> columnNames)
    {
        boolean closeFileWhenDone = file == null;
        
        try
        {
            assert columnNames != null;

            this.columns = columnNames;
            this.decoratedKey = key;

            // open the sstable file, if we don't have one passed to use from range scan
            if (file == null)
            {
                file = ssTable.getFileDataInput(decoratedKey, DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
                if (file == null)
                    return;
                DecoratedKey keyInDisk = ssTable.getPartitioner().convertFromDiskFormat(FBUtilities.readShortByteArray(file));
                assert keyInDisk.equals(decoratedKey)
                       : String.format("%s != %s in %s", keyInDisk, decoratedKey, file.getPath());
                SSTableReader.readRowSize(file, ssTable.getDescriptor());
            }

            // read the requested columns into `cf`
            /* Read the bloom filter summarizing the columns */
            BloomFilter bf = IndexHelper.defreezeBloomFilter(file);
            List<IndexHelper.IndexInfo> indexList = IndexHelper.deserializeIndex(file);

            // we can stop early if bloom filter says none of the columns actually exist -- but,
            // we can't stop before initializing the cf above, in case there's a relevant tombstone
            cf = ColumnFamily.serializer().deserializeFromSSTableNoColumns(ssTable.makeColumnFamily(), file);

            List<byte[]> filteredColumnNames1 = new ArrayList<byte[]>(columnNames.size());
            for (byte[] name : columnNames)
            {
                if (bf.isPresent(name))
                {
                    filteredColumnNames1.add(name);
                }
            }
            List<byte[]> filteredColumnNames = filteredColumnNames1;
            if (filteredColumnNames.isEmpty())
                return;

            file.readInt(); // column count

            /* get the various column ranges we have to read */
            AbstractType comparator = ssTable.getColumnComparator();
            SortedSet<IndexHelper.IndexInfo> ranges = new TreeSet<IndexHelper.IndexInfo>(IndexHelper.getComparator(comparator));
            for (byte[] name : filteredColumnNames)
            {
                int index = IndexHelper.indexFor(name, indexList, comparator, false);
                if (index == indexList.size())
                    continue;
                IndexHelper.IndexInfo indexInfo = indexList.get(index);
                if (comparator.compare(name, indexInfo.firstName) < 0)
                    continue;
                ranges.add(indexInfo);
            }

            file.mark();
            for (IndexHelper.IndexInfo indexInfo : ranges)
            {
                file.reset();
                long curOffsert = file.skipBytes((int) indexInfo.offset);
                assert curOffsert == indexInfo.offset;
                // TODO only completely deserialize columns we are interested in
                while (file.bytesPastMark() < indexInfo.offset + indexInfo.width)
                {
                    final IColumn column = cf.getColumnSerializer().deserialize(file);
                    // we check vs the original Set, not the filtered List, for efficiency
                    if (columnNames.contains(column.name()))
                    {
                        cf.addColumn(column);
                    }
                }
            }

            // create an iterator view of the columns we read
            iter = cf.getSortedColumns().iterator();
        }
        catch (IOException ioe)
        {
            throw new IOError(ioe);
        }
        finally
        {
            if (closeFileWhenDone && file != null)
            {
                try
                {
                    file.close();
                }
                catch (IOException ioe)
                {
                    logger.warn("error closing " + file.getPath());
                }
            }
        }
    }

    public DecoratedKey getKey()
    {
        return decoratedKey;
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    protected IColumn computeNext()
    {
        if (iter == null || !iter.hasNext())
            return endOfData();
        return iter.next();
    }
}
