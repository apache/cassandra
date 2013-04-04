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
package org.apache.cassandra.db.index;


import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class PerRowSecondaryIndexTest extends SchemaLoader
{

    // test that when index(key) is called on a PRSI index,
    // the data to be indexed can be read using the supplied
    // key. TestIndex.index(key) simply reads the data to be
    // indexed & stashes it in a static variable for inspection
    // in the test.

    @Test
    public void testIndexInsertAndUpdate() throws IOException
    {
        // create a row then test that the configured index instance was able to read the row
        RowMutation rm;
        rm = new RowMutation("PerRowSecondaryIndex", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("indexed")), ByteBufferUtil.bytes("foo"), 1);
        rm.apply();

        ColumnFamily indexedRow = TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);
        assertEquals(ByteBufferUtil.bytes("foo"), indexedRow.getColumn(ByteBufferUtil.bytes("indexed")).value());

        // update the row and verify what was indexed
        rm = new RowMutation("PerRowSecondaryIndex", ByteBufferUtil.bytes("k1"));
        rm.add(new QueryPath("Indexed1", null, ByteBufferUtil.bytes("indexed")), ByteBufferUtil.bytes("bar"), 2);
        rm.apply();

        indexedRow = TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);
        assertEquals(ByteBufferUtil.bytes("bar"), indexedRow.getColumn(ByteBufferUtil.bytes("indexed")).value());
    }

    public static class TestIndex extends PerRowSecondaryIndex
    {
        public static ColumnFamily LAST_INDEXED_ROW;

        @Override
        public void index(ByteBuffer rowKey, ColumnFamily cf)
        {
        }

        @Override
        public void index(ByteBuffer rowKey)
        {
            QueryFilter filter = QueryFilter.getIdentityFilter(DatabaseDescriptor.getPartitioner().decorateKey(rowKey),
                                                               new QueryPath(baseCfs.getColumnFamilyName()));
            LAST_INDEXED_ROW = baseCfs.getColumnFamily(filter);
        }

        @Override
        public void delete(DecoratedKey key)
        {
        }

        @Override
        public void init()
        {
        }

        @Override
        public void reload()
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
        public long getLiveSize()
        {
            return 0;
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
        public void truncate(long truncatedAt)
        {
        }
    }
}
