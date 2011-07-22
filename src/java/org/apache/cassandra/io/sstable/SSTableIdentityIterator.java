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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOError;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.BytesReadTracker;

public class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, IColumnIterator
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIdentityIterator.class);

    private final DecoratedKey key;
    private final long finishedAt;
    private final DataInput input;
    private final long dataStart;
    public final long dataSize;
    public final boolean fromRemote;

    private final ColumnFamily columnFamily;
    public final int columnCount;
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
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key, long dataStart, long dataSize)
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
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key, long dataStart, long dataSize, boolean checkData)
    throws IOException
    {
        this(sstable.metadata, file, key, dataStart, dataSize, checkData, sstable, false);
    }

    public SSTableIdentityIterator(CFMetaData metadata, DataInput file, DecoratedKey key, long dataStart, long dataSize, boolean fromRemote)
    throws IOException
    {
        this(metadata, file, key, dataStart, dataSize, false, null, fromRemote);
    }

    // sstable may be null *if* deserializeRowHeader is false
    private SSTableIdentityIterator(CFMetaData metadata, DataInput input, DecoratedKey key, long dataStart, long dataSize, boolean checkData, SSTableReader sstable, boolean fromRemote)
    throws IOException
    {
        this.input = input;
        this.inputWithTracker = new BytesReadTracker(input);
        this.key = key;
        this.dataStart = dataStart;
        this.dataSize = dataSize;
        this.expireBefore = (int)(System.currentTimeMillis() / 1000);
        this.fromRemote = fromRemote;
        this.validateColumns = checkData;
        finishedAt = dataStart + dataSize;

        try
        {
            if (input instanceof RandomAccessReader)
            {
                RandomAccessReader file = (RandomAccessReader) input;
                file.seek(this.dataStart);
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
                }
            }

            IndexHelper.skipBloomFilter(inputWithTracker);
            IndexHelper.skipIndex(inputWithTracker);
            columnFamily = ColumnFamily.create(metadata);
            ColumnFamily.serializer().deserializeFromSSTableNoColumns(columnFamily, inputWithTracker);
            columnCount = inputWithTracker.readInt();

            if (input instanceof RandomAccessReader)
            {
                RandomAccessReader file = (RandomAccessReader) input;
                columnPosition = file.getFilePointer();
            }
        }
        catch (IOException e)
        {
            throw new IOError(e);
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
        if (input instanceof RandomAccessReader)
        {
            RandomAccessReader file = (RandomAccessReader) input;
            return file.getFilePointer() < finishedAt;
        }
        else
        {
            return inputWithTracker.getBytesRead() < dataSize;
        }
    }

    public IColumn next()
    {
        try
        {
            IColumn column = columnFamily.getColumnSerializer().deserialize(inputWithTracker, null, fromRemote, expireBefore);
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
        // only effective when input is from file
        if (input instanceof RandomAccessReader)
        {
            RandomAccessReader file = (RandomAccessReader) input;
            file.seek(dataStart);
            while (file.getFilePointer() < finishedAt)
            {
                out.write(file.readByte());
            }
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public ColumnFamily getColumnFamilyWithColumns() throws IOException
    {
        ColumnFamily cf = columnFamily.cloneMeShallow();
        if (input instanceof RandomAccessReader)
        {
            RandomAccessReader file = (RandomAccessReader) input;
            file.seek(columnPosition - 4); // seek to before column count int
            ColumnFamily.serializer().deserializeColumns(inputWithTracker, cf, false, fromRemote);
        }
        else
        {
            // since we already read column count, just pass that value and continue deserialization
            ColumnFamily.serializer().deserializeColumns(inputWithTracker, cf, columnCount, false, fromRemote);
        }
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

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }

    public void reset()
    {
        // only effective when input is from file
        if (input instanceof RandomAccessReader)
        {
            RandomAccessReader file = (RandomAccessReader) input;
            try
            {
                file.seek(columnPosition);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            inputWithTracker.reset();
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
}
