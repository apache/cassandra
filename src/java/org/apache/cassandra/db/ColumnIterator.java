/**
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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SequenceFile.ColumnGroupReader;
import org.apache.cassandra.service.StorageService;
import com.google.common.collect.AbstractIterator;

public interface ColumnIterator extends Iterator<IColumn>
{
    /**
     *  returns the CF of the column being iterated.
     *  The CF is only guaranteed to be available after a call to next() or hasNext().
     */
    public abstract ColumnFamily getColumnFamily();

    /** clean up any open resources */
    public void close() throws IOException;
}

/**
 *  A Column Iterator over SSTable
 */
class SSTableColumnIterator extends AbstractIterator<IColumn> implements ColumnIterator
{
    protected boolean isAscending;
    private String startColumn;
    private DataOutputBuffer outBuf = new DataOutputBuffer();
    private DataInputBuffer inBuf = new DataInputBuffer();
    private int curColumnIndex;
    private ColumnFamily curCF = null;
    private ArrayList<IColumn> curColumns = new ArrayList<IColumn>();
    private ColumnGroupReader reader;

    public SSTableColumnIterator(String filename, String key, String cfName, String startColumn, boolean isAscending)
    throws IOException
    {
        this.isAscending = isAscending;
        SSTable ssTable = new SSTable(filename, StorageService.getPartitioner());
        reader = ssTable.getColumnGroupReader(key, cfName, startColumn, isAscending);
        this.startColumn = startColumn;
        curColumnIndex = isAscending ? 0 : -1;
    }

    private boolean isColumnNeeded(IColumn column)
    {
        if (isAscending)
            return (column.name().compareTo(startColumn) >= 0);
        else
            return (column.name().compareTo(startColumn) <= 0);
    }

    private void getColumnsFromBuffer() throws IOException
    {
        inBuf.reset(outBuf.getData(), outBuf.getLength());
        ColumnFamily columnFamily = ColumnFamily.serializer().deserialize(inBuf);

        if (curCF == null)
            curCF = columnFamily.cloneMeShallow();
        curColumns.clear();
        for (IColumn column : columnFamily.getAllColumns())
            if (isColumnNeeded(column))
                curColumns.add(column);

        if (isAscending)
            curColumnIndex = 0;
        else
            curColumnIndex = curColumns.size() - 1;
    }

    public ColumnFamily getColumnFamily()
    {
        return curCF;
    }

    protected IColumn computeNext()
    {
        while (true)
        {
            if (isAscending)
            {
                if (curColumnIndex < curColumns.size())
                {
                    return curColumns.get(curColumnIndex++);
                }
            }
            else
            {
                if (curColumnIndex >= 0)
                {
                    return curColumns.get(curColumnIndex--);
                }
            }

            try
            {
                if (!reader.getNextBlock(outBuf))
                    return endOfData();
                getColumnsFromBuffer();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() throws IOException
    {
        reader.close();
    }
}
