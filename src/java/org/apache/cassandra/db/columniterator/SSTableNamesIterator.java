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
package org.apache.cassandra.db.columniterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilySerializer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;

public class SSTableNamesIterator extends SimpleAbstractColumnIterator implements ISSTableColumnIterator
{
    private ColumnFamily cf;
    private final SSTableReader sstable;
    private FileDataInput fileToClose;
    private Iterator<OnDiskAtom> iter;
    public final SortedSet<ByteBuffer> columns;
    public final DecoratedKey key;

    public SSTableNamesIterator(SSTableReader sstable, DecoratedKey key, SortedSet<ByteBuffer> columns)
    {
        assert columns != null;
        this.sstable = sstable;
        this.columns = columns;
        this.key = key;

        RowIndexEntry indexEntry = sstable.getPosition(key, SSTableReader.Operator.EQ);
        if (indexEntry == null)
            return;

        try
        {
            read(sstable, null, indexEntry);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
        finally
        {
            if (fileToClose != null)
                FileUtils.closeQuietly(fileToClose);
        }
    }

    public SSTableNamesIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, SortedSet<ByteBuffer> columns, RowIndexEntry indexEntry)
    {
        assert columns != null;
        this.sstable = sstable;
        this.columns = columns;
        this.key = key;

        try
        {
            read(sstable, file, indexEntry);
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    private FileDataInput createFileDataInput(long position)
    {
        fileToClose = sstable.getFileDataInput(position);
        return fileToClose;
    }

    public SSTableReader getSStable()
    {
        return sstable;
    }

    private void read(SSTableReader sstable, FileDataInput file, RowIndexEntry indexEntry)
    throws IOException
    {
        IFilter bf;
        List<IndexHelper.IndexInfo> indexList;

        // If the entry is not indexed or the index is not promoted, read from the row start
        if (!indexEntry.isIndexed())
        {
            if (file == null)
                file = createFileDataInput(indexEntry.position);
            else
                file.seek(indexEntry.position);

            DecoratedKey keyInDisk = SSTableReader.decodeKey(sstable.partitioner,
                                                             sstable.descriptor,
                                                             ByteBufferUtil.readWithShortLength(file));
            assert keyInDisk.equals(key) : String.format("%s != %s in %s", keyInDisk, key, file.getPath());
            SSTableReader.readRowSize(file, sstable.descriptor);
        }

        if (sstable.descriptor.version.hasPromotedIndexes)
        {
            bf = indexEntry.isIndexed() ? indexEntry.bloomFilter() : null;
            indexList = indexEntry.columnsIndex();
        }
        else
        {
            assert file != null;
            bf = IndexHelper.defreezeBloomFilter(file, sstable.descriptor.version.filterType);
            indexList = IndexHelper.deserializeIndex(file);
        }

        if (!indexEntry.isIndexed())
        {
            // we can stop early if bloom filter says none of the columns actually exist -- but,
            // we can't stop before initializing the cf above, in case there's a relevant tombstone
            ColumnFamilySerializer serializer = ColumnFamily.serializer;
            try
            {
                cf = ColumnFamily.create(sstable.metadata);
                cf.delete(DeletionInfo.serializer().deserializeFromSSTable(file, sstable.descriptor.version));
            }
            catch (Exception e)
            {
                throw new IOException(serializer + " failed to deserialize " + sstable.getColumnFamilyName() + " with " + sstable.metadata + " from " + file, e);
            }
        }
        else
        {
            cf = ColumnFamily.create(sstable.metadata);
            cf.delete(indexEntry.deletionInfo());
        }

        List<OnDiskAtom> result = new ArrayList<OnDiskAtom>();
        List<ByteBuffer> filteredColumnNames = new ArrayList<ByteBuffer>(columns.size());
        for (ByteBuffer name : columns)
        {
            if (bf == null || bf.isPresent(name))
            {
                filteredColumnNames.add(name);
            }
        }
        if (filteredColumnNames.isEmpty())
            return;

        if (indexList.isEmpty())
        {
            readSimpleColumns(file, columns, filteredColumnNames, result);
        }
        else
        {
            long basePosition;
            if (sstable.descriptor.version.hasPromotedIndexes)
            {
                basePosition = indexEntry.position;
            }
            else
            {
                assert file != null;
                file.readInt(); // column count
                basePosition = file.getFilePointer();
            }
            readIndexedColumns(sstable.metadata, file, columns, filteredColumnNames, indexList, basePosition, result);
        }

        // create an iterator view of the columns we read
        iter = result.iterator();
    }

    private void readSimpleColumns(FileDataInput file, SortedSet<ByteBuffer> columnNames, List<ByteBuffer> filteredColumnNames, List<OnDiskAtom> result) throws IOException
    {
        OnDiskAtom.Serializer atomSerializer = cf.getOnDiskSerializer();
        int columns = file.readInt();
        int n = 0;
        for (int i = 0; i < columns; i++)
        {
            OnDiskAtom column = atomSerializer.deserializeFromSSTable(file, sstable.descriptor.version);
            if (column instanceof IColumn)
            {
                if (columnNames.contains(column.name()))
                {
                    result.add(column);
                    if (n++ > filteredColumnNames.size())
                        break;
                }
            }
            else
            {
                result.add(column);
            }
        }
    }

    private void readIndexedColumns(CFMetaData metadata,
                                    FileDataInput file,
                                    SortedSet<ByteBuffer> columnNames,
                                    List<ByteBuffer> filteredColumnNames,
                                    List<IndexHelper.IndexInfo> indexList,
                                    long basePosition,
                                    List<OnDiskAtom> result)
    throws IOException
    {
        /* get the various column ranges we have to read */
        AbstractType<?> comparator = metadata.comparator;
        List<IndexHelper.IndexInfo> ranges = new ArrayList<IndexHelper.IndexInfo>();
        int lastIndexIdx = -1;
        for (ByteBuffer name : filteredColumnNames)
        {
            int index = IndexHelper.indexFor(name, indexList, comparator, false, lastIndexIdx);
            if (index < 0 || index == indexList.size())
                continue;
            IndexHelper.IndexInfo indexInfo = indexList.get(index);
            // Check the index block does contain the column names and that we haven't inserted this block yet.
            if (comparator.compare(name, indexInfo.firstName) < 0 || index == lastIndexIdx)
                continue;
            ranges.add(indexInfo);
            lastIndexIdx = index;
        }

        if (ranges.isEmpty())
            return;

        for (IndexHelper.IndexInfo indexInfo : ranges)
        {
            long positionToSeek = basePosition + indexInfo.offset;

            // With new promoted indexes, our first seek in the data file will happen at that point.
            if (file == null)
                file = createFileDataInput(positionToSeek);

            OnDiskAtom.Serializer atomSerializer = cf.getOnDiskSerializer();
            file.seek(positionToSeek);
            FileMark mark = file.mark();
            // TODO only completely deserialize columns we are interested in
            while (file.bytesPastMark(mark) < indexInfo.width)
            {
                OnDiskAtom column = atomSerializer.deserializeFromSSTable(file, sstable.descriptor.version);
                // we check vs the original Set, not the filtered List, for efficiency
                if (!(column instanceof IColumn) || columnNames.contains(column.name()))
                    result.add(column);
            }
        }
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    protected OnDiskAtom computeNext()
    {
        if (iter == null || !iter.hasNext())
            return endOfData();
        return iter.next();
    }
}
