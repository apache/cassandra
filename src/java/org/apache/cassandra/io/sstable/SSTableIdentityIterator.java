package org.apache.cassandra.io.sstable;
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

    private final DecoratedKey<?> key;
    private final DataInput input;
    private final long dataStart;
    public final long dataSize;
    public final IColumnSerializer.Flag flag;

    private final ColumnFamily columnFamily;
    private final int columnCount;
    private long columnPosition;

    private BytesReadTracker inputWithTracker; // tracks bytes read

    // Used by lazilyCompactedRow, so that we see the same things when deserializing the first and second time
    private final int expireBefore;

    private final boolean validateColumns;

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     * @param dataStart Data for this row starts at this pos.
     * @param dataSize length of row data
     * @throws IOException
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey<?> key, long dataStart, long dataSize)
    throws IOException
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
     * @throws IOException
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey<?> key, long dataStart, long dataSize, boolean checkData)
    throws IOException
    {
        this(sstable.metadata, file, key, dataStart, dataSize, checkData, sstable, IColumnSerializer.Flag.LOCAL);
    }

    public SSTableIdentityIterator(CFMetaData metadata, DataInput file, DecoratedKey<?> key, long dataStart, long dataSize, IColumnSerializer.Flag flag)
    throws IOException
    {
        this(metadata, file, key, dataStart, dataSize, false, null, flag);
    }

    // sstable may be null *if* deserializeRowHeader is false
    private SSTableIdentityIterator(CFMetaData metadata, DataInput input, DecoratedKey<?> key, long dataStart, long dataSize, boolean checkData, SSTableReader sstable, IColumnSerializer.Flag flag)
    throws IOException
    {
        this.input = input;
        this.inputWithTracker = new BytesReadTracker(input);
        this.key = key;
        this.dataStart = dataStart;
        this.dataSize = dataSize;
        this.expireBefore = (int)(System.currentTimeMillis() / 1000);
        this.flag = flag;
        this.validateColumns = checkData;

        try
        {
            if (input instanceof RandomAccessReader)
            {
                RandomAccessReader file = (RandomAccessReader) input;
                file.seek(this.dataStart);
                if (dataStart + dataSize > file.length())
                    throw new IOException(String.format("dataSize of %s starting at %s would be larger than file %s length %s",
                                          dataSize, dataStart, file.getPath(), file.length()));
                if (checkData)
                {
                    try
                    {
                        IndexHelper.defreezeBloomFilter(file, dataSize, sstable.descriptor.usesOldBloomFilter);
                    }
                    catch (Exception e)
                    {
                        if (e instanceof EOFException)
                            throw (EOFException) e;

                        logger.debug("Invalid bloom filter in {}; will rebuild it", sstable);
                        // deFreeze should have left the file position ready to deserialize index
                    }
                    try
                    {
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

            IndexHelper.skipBloomFilter(inputWithTracker);
            IndexHelper.skipIndex(inputWithTracker);
            columnFamily = ColumnFamily.create(metadata);
            ColumnFamily.serializer().deserializeFromSSTableNoColumns(columnFamily, inputWithTracker);
            columnCount = inputWithTracker.readInt();

            columnPosition = dataStart + inputWithTracker.getBytesRead();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public DecoratedKey<?> getKey()
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

    public IColumn next()
    {
        try
        {
            IColumn column = columnFamily.getColumnSerializer().deserialize(inputWithTracker, flag, expireBefore);
            if (validateColumns)
                column.validateFields(columnFamily.metadata());
            return column;
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        catch (MarshalException e)
        {
            throw new IOError(new IOException("Error validating row " + key, e));
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException
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

    public void echoData(DataOutput out) throws IOException
    {
        if (!(input instanceof RandomAccessReader))
            throw new UnsupportedOperationException();

        ((RandomAccessReader) input).seek(dataStart);
        inputWithTracker.reset(0);
        while (inputWithTracker.getBytesRead() < dataSize)
            out.write(inputWithTracker.readByte());
    }

    public ColumnFamily getColumnFamilyWithColumns() throws IOException
    {
        assert inputWithTracker.getBytesRead() == headerSize();
        ColumnFamily cf = columnFamily.cloneMeShallow(ArrayBackedSortedColumns.factory(), false);
        // since we already read column count, just pass that value and continue deserialization
        ColumnFamily.serializer().deserializeColumns(inputWithTracker, cf, columnCount, flag);
        if (validateColumns)
        {
            try
            {
                cf.validateColumnFields();
            }
            catch (MarshalException e)
            {
                throw new IOException("Error validating row " + key, e);
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
        try
        {
            file.seek(columnPosition);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        inputWithTracker.reset(headerSize());
    }

    public int getColumnCount()
    {
        return columnCount;
    }
}
