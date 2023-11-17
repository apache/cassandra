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
import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.utils.FBUtilities.immutableListWithFilteredNulls;

/**
 * Partition iterator for the BTI format.
 * <p>
 * As the index stores prefixes of keys, the slice returned by the underlying {@link PartitionIndex.IndexPosIterator}
 * may start and end with entries that have the same prefix as the provided bounds, but be in the wrong relationship
 * with them. To filter these out, we start by checking the first item during initialization, and by working one item
 * ahead, so that we can recognize the end of the slice and check the last item before we return it.
 */
class PartitionIterator extends PartitionIndex.IndexPosIterator implements KeyReader
{
    private final PartitionIndex partitionIndex;
    private final IPartitioner partitioner;
    private final PartitionPosition limit;
    private final int exclusiveLimit;
    private final FileHandle dataFile;
    private final FileHandle rowIndexFile;
    private final Version version;

    private FileDataInput dataInput;
    private FileDataInput indexInput;

    private DecoratedKey currentKey;
    private TrieIndexEntry currentEntry;
    private DecoratedKey nextKey;
    private TrieIndexEntry nextEntry;

    static PartitionIterator create(PartitionIndex partitionIndex, IPartitioner partitioner, FileHandle rowIndexFile, FileHandle dataFile,
                                    PartitionPosition left, int inclusiveLeft, PartitionPosition right, int exclusiveRight, Version version) throws IOException
    {
        PartitionIterator partitionIterator = null;
        PartitionIndex partitionIndexCopy = null;
        FileHandle dataFileCopy = null;
        FileHandle rowIndexFileCopy = null;

        try
        {
            partitionIndexCopy = partitionIndex.sharedCopy();
            dataFileCopy = dataFile.sharedCopy();
            rowIndexFileCopy = rowIndexFile.sharedCopy();

            partitionIterator = new PartitionIterator(partitionIndexCopy, partitioner, rowIndexFileCopy, dataFileCopy, left, right, exclusiveRight, version);

            partitionIterator.readNext();
            // Because the index stores prefixes, the first value can be in any relationship with the left bound.
            if (partitionIterator.nextKey != null && !(partitionIterator.nextKey.compareTo(left) > inclusiveLeft))
            {
                partitionIterator.readNext();
            }
            partitionIterator.advance();
            return partitionIterator;
        }
        catch (IOException | RuntimeException ex)
        {
            if (partitionIterator != null)
            {
                partitionIterator.close();
            }
            else
            {
                Throwables.closeNonNullAndAddSuppressed(ex, rowIndexFileCopy, dataFileCopy, partitionIndexCopy);
            }
            throw ex;
        }
    }

    static PartitionIterator create(PartitionIndex partitionIndex, IPartitioner partitioner, FileHandle rowIndexFile, FileHandle dataFile, Version version) throws IOException
    {
        return create(partitionIndex, partitioner, rowIndexFile, dataFile, partitionIndex.firstKey(), -1, partitionIndex.lastKey(), 0, version);
    }

    static PartitionIterator empty(PartitionIndex partitionIndex)
    {
        return new PartitionIterator(partitionIndex.sharedCopy(), null);
    }

    private PartitionIterator(PartitionIndex partitionIndex, IPartitioner partitioner, FileHandle rowIndexFile, FileHandle dataFile,
                              PartitionPosition left, PartitionPosition right, int exclusiveRight, Version version)
    {
        super(partitionIndex, left, right);
        this.partitionIndex = partitionIndex;
        this.partitioner = partitioner;
        this.limit = right;
        this.exclusiveLimit = exclusiveRight;
        this.rowIndexFile = rowIndexFile;
        this.dataFile = dataFile;
        this.version = version;
    }

    private PartitionIterator(PartitionIndex partitionIndex, Version version)
    {
        super(partitionIndex, partitionIndex.firstKey(), partitionIndex.firstKey());
        this.partitionIndex = partitionIndex;
        this.partitioner = null;
        this.limit = partitionIndex.firstKey();
        this.exclusiveLimit = -1;
        this.rowIndexFile = null;
        this.dataFile = null;

        this.currentEntry = null;
        this.currentKey = null;
        this.nextEntry = null;
        this.nextKey = null;
        this.version = version;
    }

    @Override
    public void close()
    {
        Throwable accum = null;
        accum = Throwables.close(accum, immutableListWithFilteredNulls(partitionIndex, dataFile, rowIndexFile));
        accum = Throwables.close(accum, immutableListWithFilteredNulls(dataInput, indexInput));
        accum = Throwables.perform(accum, super::close);
        Throwables.maybeFail(accum);
    }

    public DecoratedKey decoratedKey()
    {
        return currentKey;
    }

    public ByteBuffer key()
    {
        return currentKey.getKey();
    }

    @Override
    public long dataPosition()
    {
        return currentEntry != null ? currentEntry.position : -1;
    }

    @Override
    public long keyPositionForSecondaryIndex()
    {
        return dataPosition();
    }

    public TrieIndexEntry entry()
    {
        return currentEntry;
    }

    @Override
    public boolean advance() throws IOException
    {
        currentKey = nextKey;
        currentEntry = nextEntry;
        if (currentKey != null)
        {
            readNext();
            // if nextKey is null, then currentKey is the last key to be published, therefore check against any limit
            // and suppress the partition if it is beyond the limit
            if (nextKey == null && limit != null && currentKey.compareTo(limit) > exclusiveLimit)
            {   // exclude last partition outside range
                currentKey = null;
                currentEntry = null;
                return false;
            }
            return true;
        }
        return false;
    }

    private void readNext() throws IOException
    {
        long pos = nextIndexPos();
        if (pos != PartitionIndex.NOT_FOUND)
        {
            if (pos >= 0)
            {
                seekIndexInput(pos);
                nextKey = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(indexInput));
                nextEntry = TrieIndexEntry.deserialize(indexInput, indexInput.getFilePointer(), version);
            }
            else
            {
                pos = ~pos;
                seekDataInput(pos);
                nextKey = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(dataInput));
                nextEntry = new TrieIndexEntry(pos);
            }
        }
        else
        {
            nextKey = null;
            nextEntry = null;
        }
    }

    private void seekIndexInput(long pos) throws IOException
    {
        if (indexInput == null)
            indexInput = rowIndexFile.createReader(pos);
        else
            indexInput.seek(pos);
    }

    private void seekDataInput(long pos) throws IOException
    {
        if (dataInput == null)
            dataInput = dataFile.createReader(pos);
        else
            dataInput.seek(pos);
    }

    @Override
    public boolean isExhausted()
    {
        return currentKey == null;
    }

    @Override
    public void reset()
    {
        go(root);
    }

    @Override
    public String toString()
    {
        return String.format("BTI-PartitionIterator(%s)", partitionIndex.getFileHandle().path());
    }
}
