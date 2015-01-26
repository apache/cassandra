/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.index.PerColumnSecondaryIndex;
import org.apache.cassandra.db.index.PerRowSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SecondaryIndexCellSizeTest
{
    @Test
    public void test64kColumn()
    {
        // a byte buffer more than 64k
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 65);
        buffer.clear();

        //read more than 64k
        for (int i=0; i<1024*64/4 + 1; i++)
            buffer.putInt(0);

        // for read
        buffer.flip();
        Cell cell = new BufferCell(CellNames.simpleDense(ByteBufferUtil.bytes("test")), buffer, 0);

        SecondaryIndexCellSizeTest.MockRowIndex mockRowIndex = new SecondaryIndexCellSizeTest.MockRowIndex();
        SecondaryIndexCellSizeTest.MockColumnIndex mockColumnIndex = new SecondaryIndexCellSizeTest.MockColumnIndex();

        assertTrue(mockRowIndex.validate(cell));
        assertFalse(mockColumnIndex.validate(cell));

        // test less than 64k value
        buffer.flip();
        buffer.clear();
        buffer.putInt(20);
        buffer.flip();

        assertTrue(mockRowIndex.validate(cell));
        assertTrue(mockColumnIndex.validate(cell));
    }

    private class MockRowIndex extends PerRowSecondaryIndex
    {
        public void init()
        {
        }

        public void validateOptions() throws ConfigurationException
        {
        }

        public String getIndexName()
        {
            return null;
        }

        protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
        {
            return null;
        }

        public void forceBlockingFlush()
        {
        }

        public ColumnFamilyStore getIndexCfs()
        {
            return null;
        }

        public void removeIndex(ByteBuffer columnName)
        {
        }

        public void invalidate()
        {
        }

        public void truncateBlocking(long truncatedAt)
        {
        }

        public void index(ByteBuffer rowKey, ColumnFamily cf)
        {
        }

        public void delete(DecoratedKey key, OpOrder.Group opGroup)
        {
        }

        public void index(ByteBuffer rowKey)
        {
        }

        public void reload()
        {
        }

        public boolean indexes(CellName name)
        {
            return true;
        }

        @Override
        public long estimateResultRows() {
            return 0;
        }
    }


    private class MockColumnIndex extends PerColumnSecondaryIndex
    {
        @Override
        public void init()
        {
        }

        @Override
        public void validateOptions() throws ConfigurationException
        {
        }

        @Override
        public String getIndexName()
        {
            return null;
        }

        @Override
        protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
        {
            return null;
        }

        @Override
        public void forceBlockingFlush()
        {
        }

        @Override
        public ColumnFamilyStore getIndexCfs()
        {
            return null;
        }

        @Override
        public void removeIndex(ByteBuffer columnName)
        {
        }

        @Override
        public void invalidate()
        {
        }

        @Override
        public void truncateBlocking(long truncatedAt)
        {
        }

        @Override
        public void delete(ByteBuffer rowKey, Cell col, OpOrder.Group opGroup)
        {
        }

        @Override
        public void deleteForCleanup(ByteBuffer rowKey, Cell col, OpOrder.Group opGroup) {}

        @Override
        public void insert(ByteBuffer rowKey, Cell col, OpOrder.Group opGroup)
        {
        }

        @Override
        public void update(ByteBuffer rowKey, Cell oldCol, Cell col, OpOrder.Group opGroup)
        {
        }

        @Override
        public void reload()
        {
        }

        public boolean indexes(CellName name)
        {
            return true;
        }

        @Override
        public long estimateResultRows() {
            return 0;
        }
    }
}
