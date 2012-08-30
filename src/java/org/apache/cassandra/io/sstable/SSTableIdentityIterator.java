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
package org.apache.cassandra.io.sstable;

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.ICountableColumnIterator;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.BytesReadTracker;

public class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, ICountableColumnIterator
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIdentityIterator.class);

    private final DecoratedKey key;
    private final DataInput input;
    private final long dataStart;
    public final long dataSize;
    public final IColumnSerializer.Flag flag;

    private final ColumnFamily columnFamily;
    private final int columnCount;
    private final long columnPosition;

    private final OnDiskAtom.Serializer atomSerializer;
    private final Descriptor.Version dataVersion;

    private final BytesReadTracker inputWithTracker; // tracks bytes read

    // Used by lazilyCompactedRow, so that we see the same things when deserializing the first and second time
    private final int expireBefore;

    private final boolean validateColumns;
    private final String filename;

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     * @param dataStart Data for this row starts at this pos.
     * @param dataSize length of row data
     * @throws IOException
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key, long dataStart, long dataSize)
    {
        this(sstable, file, key, dataStart, dataSize, false);
    }

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     * @param dataStart Data for this row starts at this pos.
     * @param dataSize length of row data
     * @param checkData if true, do its best to deserialize and check the coherence of row data
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key, long dataStart, long dataSize, boolean checkData)
    {
        this(sstable.metadata, file, file.getPath(), key, dataStart, dataSize, checkData, sstable, IColumnSerializer.Flag.LOCAL);
    }

    // Must only be used against current file format
    public SSTableIdentityIterator(CFMetaData metadata, DataInput file, String filename, DecoratedKey key, long dataStart, long dataSize, IColumnSerializer.Flag flag)
    {
        this(metadata, file, filename, key, dataStart, dataSize, false, null, flag);
    }

    // sstable may be null *if* checkData is false
    // If it is null, we assume the data is in the current file format
    private SSTableIdentityIterator(CFMetaData metadata,
                                    DataInput input,
                                    String filename,
                                    DecoratedKey key,
                                    long dataStart,
                                    long dataSize,
                                    boolean checkData,
                                    SSTableReader sstable,
                                    IColumnSerializer.Flag flag)
    {
        assert !checkData || (sstable != null);
        this.input = input;
        this.filename = filename;
        this.inputWithTracker = new BytesReadTracker(input);
        this.key = key;
        this.dataStart = dataStart;
        this.dataSize = dataSize;
        this.expireBefore = (int)(System.currentTimeMillis() / 1000);
        this.flag = flag;
        this.validateColumns = checkData;
        this.dataVersion = sstable == null ? Descriptor.Version.CURRENT : sstable.descriptor.version;

        try
        {
            if (input instanceof RandomAccessReader)
            {
                RandomAccessReader file = (RandomAccessReader) input;
                file.seek(this.dataStart);
                if (dataStart + dataSize > file.length())
                    throw new IOException(String.format("dataSize of %s starting at %s would be larger than file %s length %s",
                                          dataSize, dataStart, file.getPath(), file.length()));
                if (checkData && !sstable.descriptor.version.hasPromotedIndexes)
                {
                    try
                    {
                        IndexHelper.defreezeBloomFilter(file, dataSize, sstable.descriptor.version.filterType);
                    }
                    catch (Exception e)
                    {
                        if (e instanceof EOFException)
                            throw (EOFException) e;

                        logger.debug("Invalid bloom filter in {}; will rebuild it", sstable);
                    }

                    try
                    {
                        // deFreeze should have left the file position ready to deserialize index
                        IndexHelper.deserializeIndex(file);
                    }
                    catch (Exception e)
                    {
                        logger.debug("Invalid row summary in {}; will rebuild it", sstable);
                    }
                    file.seek(this.dataStart);
                    inputWithTracker.reset(0);
                }
            }

            if (sstable != null && !sstable.descriptor.version.hasPromotedIndexes)
            {
                IndexHelper.skipBloomFilter(inputWithTracker);
                IndexHelper.skipIndex(inputWithTracker);
            }
            columnFamily = ColumnFamily.create(metadata);
            columnFamily.delete(DeletionInfo.serializer().deserializeFromSSTable(inputWithTracker, dataVersion));
            atomSerializer = columnFamily.getOnDiskSerializer();
            columnCount = inputWithTracker.readInt();
            columnPosition = dataStart + inputWithTracker.getBytesRead();
        }
        catch (IOException e)
        {
            if (sstable != null)
                sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return columnFamily;
    }

    public boolean hasNext()
    {
        return inputWithTracker.getBytesRead() < dataSize;
    }

    public OnDiskAtom next()
    {
        try
        {
            OnDiskAtom atom = atomSerializer.deserializeFromSSTable(inputWithTracker, flag, expireBefore, dataVersion);
            if (validateColumns)
                atom.validateFields(columnFamily.metadata());
            return atom;
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, filename);
        }
        catch (MarshalException me)
        {
            throw new CorruptSSTableException(me, filename);
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        // creator is responsible for closing file when finished
    }

    public String getPath()
    {
        // if input is from file, then return that path, otherwise it's from streaming
        if (input instanceof RandomAccessReader)
        {
            RandomAccessReader file = (RandomAccessReader) input;
            return file.getPath();
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public ColumnFamily getColumnFamilyWithColumns(ISortedColumns.Factory containerFactory) throws IOException
    {
        assert inputWithTracker.getBytesRead() == headerSize();
        ColumnFamily cf = columnFamily.cloneMeShallow(containerFactory, false);
        // since we already read column count, just pass that value and continue deserialization
        columnFamily.serializer.deserializeColumnsFromSSTable(inputWithTracker, cf, columnCount, flag, expireBefore, dataVersion);
        if (validateColumns)
        {
            try
            {
                cf.validateColumnFields();
            }
            catch (MarshalException e)
            {
                throw new RuntimeException("Error validating row " + key, e);
            }
        }
        return cf;
    }

    private long headerSize()
    {
        return columnPosition - dataStart;
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }

    public void reset()
    {
        if (!(input instanceof RandomAccessReader))
            throw new UnsupportedOperationException();

        RandomAccessReader file = (RandomAccessReader) input;
        file.seek(columnPosition);
        inputWithTracker.reset(headerSize());
    }

    public int getColumnCount()
    {
        return columnCount;
    }
}
