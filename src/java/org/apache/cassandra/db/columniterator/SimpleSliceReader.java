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

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;

class SimpleSliceReader extends AbstractIterator<IColumn> implements IColumnIterator
{
    private final FileDataInput file;
    private final ByteBuffer finishColumn;
    private final AbstractType comparator;
    private final ColumnFamily emptyColumnFamily;
    private final int columns;
    private int i;
    private FileMark mark;

    public SimpleSliceReader(SSTableReader sstable, FileDataInput input, ByteBuffer finishColumn)
    {
        this.file = input;
        this.finishColumn = finishColumn;
        comparator = sstable.metadata.comparator;
        try
        {
            IndexHelper.skipBloomFilter(file);
            IndexHelper.skipIndex(file);

            emptyColumnFamily = ColumnFamily.serializer().deserializeFromSSTableNoColumns(ColumnFamily.create(sstable.metadata), file);
            columns = file.readInt();
            mark = file.mark();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    protected IColumn computeNext()
    {
        if (i++ >= columns)
            return endOfData();

        IColumn column;
        try
        {
            file.reset(mark);
            column = emptyColumnFamily.getColumnSerializer().deserialize(file);
        }
        catch (IOException e)
        {
            throw new RuntimeException("error reading " + i + " of " + columns, e);
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
    }

    public DecoratedKey getKey()
    {
        throw new UnsupportedOperationException();
    }
}
