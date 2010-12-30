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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class CleanupTest extends CleanupHelper
{
    public static final int LOOPS = 800;
    public static final String TABLE1 = "Keyspace1";
    public static final String CF1 = "Indexed1";
    public static final ByteBuffer COLUMN = ByteBuffer.wrap("birthdate".getBytes());
    public static final ByteBuffer VALUE = ByteBuffer.allocate(8);
    static
    {
        VALUE.putLong(20101229);
        VALUE.flip();
    }

    @Test
    public void testCleanup() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open(TABLE1);

        ColumnFamilyStore cfs = table.getColumnFamilyStore(CF1);
        fillCF(cfs, LOOPS);

        assertEquals(cfs.getIndexedColumns().iterator().next(), COLUMN);

        ColumnFamilyStore cfi = cfs.getIndexedColumnFamilyStore(COLUMN);

        assertTrue(cfi.isIndexBuilt());

        IndexExpression expr = new IndexExpression(COLUMN, IndexOperator.EQ, VALUE);
        IndexClause clause = new IndexClause(Arrays.asList(expr), FBUtilities.EMPTY_BYTE_BUFFER, Integer.MAX_VALUE);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        List<Row> rows = table.getColumnFamilyStore(CF1).scan(clause, range, filter);

        assertEquals(LOOPS, rows.size());

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        assertNotNull(tmd);
        assertEquals(0, tmd.getTokenToEndpointMap().size());

        // Since this test has no ring cleanup will remove all
        CompactionManager.instance.performCleanup(cfs);

        // row data should be gone
        rows = cfs.getRangeSlice(null, Util.range("", ""), 1000, new IdentityQueryFilter());
        assertEquals(0, rows.size());

        // not only should it be gone but there should be no data on disk, not even tombstones
        assert cfs.getSSTables().isEmpty();

        // 2ary indexes should result in no results, but
        rows = cfs.scan(clause, range, filter);
        assertEquals(0, rows.size());
    }

    protected void fillCF(ColumnFamilyStore store, int rowsPerSSTable) throws ExecutionException, InterruptedException, IOException
    {
        CompactionManager.instance.disableAutoCompaction();

        for (int i = 0; i < rowsPerSSTable; i++)
        {
            String key = String.valueOf(i);

            // create a row and update the birthdate value, test that the index query fetches the new version
            RowMutation rm;
            rm = new RowMutation(TABLE1, ByteBufferUtil.bytes(key));
            rm.add(new QueryPath(CF1, null, COLUMN), VALUE, System.currentTimeMillis());
            rm.apply();
        }

        store.forceBlockingFlush();        
        store.buildSecondaryIndexes(store.getSSTables(), store.getIndexedColumns());
    }
}
