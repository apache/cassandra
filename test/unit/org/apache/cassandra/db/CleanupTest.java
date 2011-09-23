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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NodeId;
import org.junit.Test;

public class CleanupTest extends CleanupHelper
{
    public static final int LOOPS = 200;
    public static final String TABLE1 = "Keyspace1";
    public static final String CF1 = "Indexed1";
    public static final String CF2 = "Standard1";
    public static final ByteBuffer COLUMN = ByteBufferUtil.bytes("birthdate");
    public static final ByteBuffer VALUE = ByteBuffer.allocate(8);
    static
    {
        VALUE.putLong(20101229);
        VALUE.flip();
    }

    @Test
    public void testCleanup() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        StorageService.instance.initServer(0);

        Table table = Table.open(TABLE1);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CF2);

        List<Row> rows;

        // insert data and verify we get it back w/ range query
        fillCF(cfs, LOOPS);

        // record max timestamps of the sstables pre-cleanup
        List<Long> expectedMaxTimestamps = getMaxTimestampList(cfs);

        rows = cfs.getRangeSlice(null, Util.range("", ""), 1000, new IdentityQueryFilter());
        assertEquals(LOOPS, rows.size());

        // with one token in the ring, owned by the local node, cleanup should be a no-op
        CompactionManager.instance.performCleanup(cfs, new NodeId.OneShotRenewer());

        // ensure max timestamp of the sstables are retained post-cleanup
        assert expectedMaxTimestamps.equals(getMaxTimestampList(cfs));

        // check data is still there
        rows = cfs.getRangeSlice(null, Util.range("", ""), 1000, new IdentityQueryFilter());
        assertEquals(LOOPS, rows.size());
    }

    @Test
    public void testCleanupWithIndexes() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open(TABLE1);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CF1);
        assertEquals(cfs.indexManager.getIndexedColumns().iterator().next(), COLUMN);

        List<Row> rows;

        // insert data and verify we get it back w/ range query
        fillCF(cfs, LOOPS);
        rows = cfs.getRangeSlice(null, Util.range("", ""), 1000, new IdentityQueryFilter());
        assertEquals(LOOPS, rows.size());

        SecondaryIndex index = cfs.indexManager.getIndexForColumn(COLUMN);
        long start = System.currentTimeMillis();
        while (!index.isIndexBuilt(COLUMN) && System.currentTimeMillis() < start + 10000)
            Thread.sleep(10);

        // verify we get it back w/ index query too
        IndexExpression expr = new IndexExpression(COLUMN, IndexOperator.EQ, VALUE);
        IndexClause clause = new IndexClause(Arrays.asList(expr), ByteBufferUtil.EMPTY_BYTE_BUFFER, Integer.MAX_VALUE);
        IFilter filter = new IdentityQueryFilter();
        IPartitioner p = StorageService.getPartitioner();
        Range range = new Range(p.getMinimumToken(), p.getMinimumToken());
        rows = table.getColumnFamilyStore(CF1).search(clause, range, filter);
        assertEquals(LOOPS, rows.size());

        // we don't allow cleanup when the local host has no range to avoid wipping up all data when a node has not join the ring.
        // So to make sure cleanup erase everything here, we give the localhost the tiniest possible range.
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        byte[] tk1 = new byte[1], tk2 = new byte[1];
        tk1[0] = 2;
        tk2[0] = 1;
        tmd.updateNormalToken(new BytesToken(tk1), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(new BytesToken(tk2), InetAddress.getByName("127.0.0.2"));

        CompactionManager.instance.performCleanup(cfs, new NodeId.OneShotRenewer());

        // row data should be gone
        rows = cfs.getRangeSlice(null, Util.range("", ""), 1000, new IdentityQueryFilter());
        assertEquals(0, rows.size());

        // not only should it be gone but there should be no data on disk, not even tombstones
        assert cfs.getSSTables().isEmpty();

        // 2ary indexes should result in no results, too (although tombstones won't be gone until compacted)
        rows = cfs.search(clause, range, filter);
        assertEquals(0, rows.size());
    }

    protected void fillCF(ColumnFamilyStore cfs, int rowsPerSSTable) throws ExecutionException, InterruptedException, IOException
    {
        CompactionManager.instance.disableAutoCompaction();

        for (int i = 0; i < rowsPerSSTable; i++)
        {
            String key = String.valueOf(i);
            // create a row and update the birthdate value, test that the index query fetches the new version
            RowMutation rm;
            rm = new RowMutation(TABLE1, ByteBufferUtil.bytes(key));
            rm.add(new QueryPath(cfs.getColumnFamilyName(), null, COLUMN), VALUE, System.currentTimeMillis());
            rm.applyUnsafe();
        }

        cfs.forceBlockingFlush();
    }

    protected List<Long> getMaxTimestampList(ColumnFamilyStore cfs)
    {
        List<Long> list = new LinkedList<Long>();
        for (SSTableReader sstable : cfs.getSSTables())
            list.add(sstable.getMaxTimestamp());
        return list;
    }
}
