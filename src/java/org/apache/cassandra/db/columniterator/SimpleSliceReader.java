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

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.utils.ByteBufferUtil;

class SimpleSliceReader extends AbstractIterator<OnDiskAtom> implements OnDiskAtomIterator
{
    private final SSTableReader sstable;
    private final FileDataInput file;
    private final boolean needsClosing;
    private final ByteBuffer finishColumn;
    private final AbstractType<?> comparator;
    private final ColumnFamily emptyColumnFamily;
    private final int columns;
    private int i;
    private FileMark mark;
    private final OnDiskAtom.Serializer atomSerializer;

    public SimpleSliceReader(SSTableReader sstable, RowIndexEntry indexEntry, FileDataInput input, ByteBuffer finishColumn)
    {
        this.sstable = sstable;
        this.finishColumn = finishColumn;
        this.comparator = sstable.metadata.comparator;
        try
        {
            if (input == null)
            {
                this.file = sstable.getFileDataInput(indexEntry.position);
                this.needsClosing = true;
            }
            else
            {
                this.file = input;
                input.seek(indexEntry.position);
                this.needsClosing = false;
            }

            // Skip key and data size
            ByteBufferUtil.skipShortLength(file);
            SSTableReader.readRowSize(file, sstable.descriptor);

            if (!sstable.descriptor.version.hasPromotedIndexes)
            {
                IndexHelper.skipBloomFilter(file);
                IndexHelper.skipIndex(file);
            }

            emptyColumnFamily = ColumnFamily.create(sstable.metadata);
            emptyColumnFamily.delete(DeletionInfo.serializer().deserializeFromSSTable(file, sstable.descriptor.version));
            atomSerializer = emptyColumnFamily.getOnDiskSerializer();
            columns = file.readInt();
            mark = file.mark();
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.descriptor);
        }
    }

    protected OnDiskAtom computeNext()
    {
        if (i++ >= columns)
            return endOfData();

        OnDiskAtom column;
        try
        {
            file.reset(mark);
            column = atomSerializer.deserializeFromSSTable(file, sstable.descriptor.version);
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, file.getPath());
        }
        if (finishColumn.remaining() > 0 && comparator.compare(column.name(), finishColumn) > 0)
            return endOfData();

        mark = file.mark();
        return column;
    }

    public ColumnFamily getColumnFamily()
    {
        return emptyColumnFamily;
    }

    public void close() throws IOException
    {
        if (needsClosing)
            file.close();
    }

    public DecoratedKey getKey()
    {
        throw new UnsupportedOperationException();
    }
}
