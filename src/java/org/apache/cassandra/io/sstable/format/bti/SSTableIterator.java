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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.AbstractSSTableIterator;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.bti.RowIndexReader.IndexInfo;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;

/**
 *  Unfiltered row iterator over a BTI SSTable.
 */
class SSTableIterator extends AbstractSSTableIterator<AbstractRowIndexEntry>
{
    /**
     * The index of the slice being processed.
     */
    private int slice;

    public SSTableIterator(BtiTableReader sstable,
                           FileDataInput file,
                           DecoratedKey key,
                           AbstractRowIndexEntry indexEntry,
                           Slices slices,
                           ColumnFilter columns,
                           FileHandle ifile)
    {
        super(sstable, file, key, indexEntry, slices, columns, ifile);
    }

    protected Reader createReaderInternal(AbstractRowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile, Version version)
    {
        if (indexEntry.isIndexed())
            return new ForwardIndexedReader(indexEntry, file, shouldCloseFile, version);
        else
            return new ForwardReader(file, shouldCloseFile);
    }

    protected int nextSliceIndex()
    {
        int next = slice;
        slice++;
        return next;
    }

    protected boolean hasMoreSlices()
    {
        return slice < slices.size();
    }

    public boolean isReverseOrder()
    {
        return false;
    }

    private class ForwardIndexedReader extends ForwardReader
    {
        private final RowIndexReader indexReader;
        private final long basePosition;
        private final Version version;

        private ForwardIndexedReader(AbstractRowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile, Version version)
        {
            super(file, shouldCloseFile);
            basePosition = indexEntry.position;
            this.version = version;
            indexReader = new RowIndexReader(ifile, (TrieIndexEntry) indexEntry, version);
        }

        @Override
        public void close() throws IOException
        {
            indexReader.close();
            super.close();
        }

        @Override
        public void setForSlice(Slice slice) throws IOException
        {
            super.setForSlice(slice);
            IndexInfo indexInfo = indexReader.separatorFloor(metadata.comparator.asByteComparable(slice.start()));
            assert indexInfo != null;
            long position = basePosition + indexInfo.offset;
            if (file == null || position > file.getFilePointer())
            {
                openMarker = indexInfo.openDeletion;
                seekToPosition(position);
            }
            // Otherwise we are already in the relevant index block, there is no point to go back to its beginning.
        }
    }
}
