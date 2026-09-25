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
package org.apache.cassandra.db.rows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class PagerBytesVerifyTest extends CQLTester
{
    private static final int ROWS = 200;
    private static final int PAGE_BYTES = 50_000;

    private void pageIt(String label, ColumnFamilyStore cfs)
    {
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(0));
        long nowInSec = FBUtilities.nowInSeconds();

        SinglePartitionReadCommand cmd =
            SinglePartitionReadCommand.create(cfs.metadata(), nowInSec,
                                              ColumnFilter.all(cfs.metadata()),
                                              RowFilter.none(), DataLimits.NONE, key,
                                              new ClusteringIndexSliceFilter(Slices.ALL, false));

        QueryPager pager = cmd.getPager(null, ProtocolVersion.CURRENT);
        int page = 0;
        while (!pager.isExhausted() && page < 3)
        {
            page++;
            int rows = 0;
            int bytes = 0;
            try (ReadExecutionController ec = pager.executionController();
                 PartitionIterator pi = pager.fetchPageInternal(PageSize.inBytes(PAGE_BYTES), ec))
            {
                while (pi.hasNext())
                {
                    try (RowIterator ri = pi.next())
                    {
                        while (ri.hasNext())
                        {
                            Row r = ri.next();
                            rows++;
                            bytes += r.dataSize();
                        }
                    }
                }
            }
            System.out.println("PAGER " + label + " page#" + page + " rowsInPage=" + rows + " bytesInPage=" + bytes
                               + " (budget " + PAGE_BYTES + ")");
        }
    }

    @Test
    public void mapInsertClientPage() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, m map<text,text>, PRIMARY KEY (k,c))");
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < 10; i++)
            m.put("key" + i, String.join("", Collections.nCopies(1000, "v")));
        for (int c = 0; c < ROWS; c++)
            execute("INSERT INTO %s (k, c, m) VALUES (?, ?, ?)", 0, c, m);
        flush();
        pageIt("MAP-INSERT", getCurrentColumnFamilyStore());
    }

    @Test
    public void blobControlClientPage() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b blob, PRIMARY KEY (k,c))");
        byte[] payload = new byte[10_000];
        for (int c = 0; c < ROWS; c++)
            execute("INSERT INTO %s (k, c, b) VALUES (?, ?, ?)", 0, c, java.nio.ByteBuffer.wrap(payload));
        flush();
        pageIt("BLOB-CONTROL", getCurrentColumnFamilyStore());
    }
}
