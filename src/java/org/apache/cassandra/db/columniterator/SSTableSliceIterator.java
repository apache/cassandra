package org.apache.cassandra.db.columniterator;
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
import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 *  A Column Iterator over SSTable
 */
public class SSTableSliceIterator implements IColumnIterator
{
    private final FileDataInput fileToClose;
    private IColumnIterator reader;
    private DecoratedKey key;

    public SSTableSliceIterator(SSTableReader sstable, DecoratedKey key, ByteBuffer startColumn, ByteBuffer finishColumn, boolean reversed)
    {
        this.key = key;
        fileToClose = sstable.getFileDataInput(this.key, DatabaseDescriptor.getSlicedReadBufferSizeInKB() * 1024);
        if (fileToClose == null)
            return;

        try
        {
            DecoratedKey keyInDisk = SSTableReader.decodeKey(sstable.partitioner,
                                                             sstable.descriptor,
                                                             ByteBufferUtil.readWithShortLength(fileToClose));
            assert keyInDisk.equals(key)
                   : String.format("%s != %s in %s", keyInDisk, key, fileToClose.getPath());
            SSTableReader.readRowSize(fileToClose, sstable.descriptor);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        reader = createReader(sstable, fileToClose, startColumn, finishColumn, reversed);
    }

    /**
     * An iterator for a slice within an SSTable
     * @param sstable Table for the CFS we are reading from
     * @param file Optional parameter that input is read from.  If null is passed, this class creates an appropriate one automatically.
     * If this class creates, it will close the underlying file when #close() is called.
     * If a caller passes a non-null argument, this class will NOT close the underlying file when the iterator is closed (i.e. the caller is responsible for closing the file)
     * In all cases the caller should explicitly #close() this iterator.
     * @param key The key the requested slice resides under
     * @param startColumn The start of the slice
     * @param finishColumn The end of the slice
     * @param reversed Results are returned in reverse order iff reversed is true.
     */
    public SSTableSliceIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, ByteBuffer startColumn, ByteBuffer finishColumn, boolean reversed)
    {
        this.key = key;
        fileToClose = null;
        reader = createReader(sstable, file, startColumn, finishColumn, reversed);
    }

    private static IColumnIterator createReader(SSTableReader sstable, FileDataInput file, ByteBuffer startColumn, ByteBuffer finishColumn, boolean reversed)
    {
        return startColumn.remaining() == 0 && !reversed
                 ? new SimpleSliceReader(sstable, file, finishColumn)
                 : new IndexedSliceReader(sstable, file, startColumn, finishColumn, reversed);
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return reader == null ? null : reader.getColumnFamily();
    }

    public boolean hasNext()
    {
        return reader != null && reader.hasNext();
    }

    public IColumn next()
    {
        return reader.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException
    {
        if (fileToClose != null)
            fileToClose.close();
    }

}
