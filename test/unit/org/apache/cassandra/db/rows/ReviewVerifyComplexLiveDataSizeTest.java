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
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class ReviewVerifyComplexLiveDataSizeTest extends CQLTester
{
    private static final int ROWS = 300;
    private static final int PAGE_BYTES = 50_000;

    private void probe(String label, ColumnFamilyStore cfs)
    {
        long nowInSec = FBUtilities.nowInSeconds();
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(0));
        DataLimits limits = DataLimits.NONE.forPaging(PageSize.inBytes(PAGE_BYTES));

        SinglePartitionReadCommand cmd =
            SinglePartitionReadCommand.create(cfs.metadata(),
                                              nowInSec,
                                              ColumnFilter.all(cfs.metadata()),
                                              RowFilter.none(),
                                              limits,
                                              key,
                                              new ClusteringIndexSliceFilter(Slices.ALL, false));

        int rows = 0;
        long realBytes = 0;
        long countedBytes = 0;
        String firstRowDetail = null;
        try (ReadExecutionController ec = cmd.executionController();
             UnfilteredPartitionIterator pi = cmd.executeLocally(ec))
        {
            while (pi.hasNext())
            {
                try (UnfilteredRowIterator ri = pi.next())
                {
                    while (ri.hasNext())
                    {
                        Unfiltered u = ri.next();
                        if (!u.isRow())
                            continue;
                        Row row = (Row) u;
                        rows++;
                        realBytes += row.dataSize();
                        countedBytes += row.liveDataSize(nowInSec);
                        if (firstRowDetail == null)
                        {
                            StringBuilder sb = new StringBuilder();
                            for (ColumnData cd : row)
                            {
                                sb.append("  col=").append(cd.column().name)
                                  .append(" complex=").append(cd.column().isComplex())
                                  .append(" dataSize=").append(cd.dataSize())
                                  .append(" liveDataSize=").append(cd.liveDataSize(nowInSec));
                                if (cd instanceof ComplexColumnData)
                                    sb.append(" complexDeletion=").append(((ComplexColumnData) cd).complexDeletion())
                                      .append(" isLive=").append(((ComplexColumnData) cd).complexDeletion().isLive());
                                sb.append('\n');
                            }
                            firstRowDetail = sb.toString();
                        }
                    }
                }
            }
        }
        System.out.println("### " + label
                           + " limits=" + limits
                           + " bytesBudget=" + PAGE_BYTES
                           + " rowsInTable=" + ROWS
                           + " rowsReturnedByReplicaRead=" + rows
                           + " actualBytesReturned=" + realBytes
                           + " bytesTheCounterSaw=" + countedBytes);
        System.out.println(firstRowDetail);
    }

    @Test
    public void mapWrittenByInsert() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, m map<text,text>, PRIMARY KEY (k,c))");
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < 10; i++)
            m.put("key" + i, String.join("", Collections.nCopies(1000, "v")));
        for (int c = 0; c < ROWS; c++)
            execute("INSERT INTO %s (k, c, m) VALUES (?, ?, ?)", 0, c, m);
        flush();
        probe("MAP-INSERT(full assignment -> complex deletion)", getCurrentColumnFamilyStore());
    }

    @Test
    public void mapWrittenByAppend() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, m map<text,text>, PRIMARY KEY (k,c))");
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < 10; i++)
            m.put("key" + i, String.join("", Collections.nCopies(1000, "v")));
        for (int c = 0; c < ROWS; c++)
            execute("UPDATE %s SET m = m + ? WHERE k = ? AND c = ?", m, 0, c);
        flush();
        probe("MAP-APPEND(no complex deletion)", getCurrentColumnFamilyStore());
    }

    @Test
    public void simpleBlobControl() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b blob, PRIMARY KEY (k,c))");
        byte[] payload = new byte[10_000];
        for (int c = 0; c < ROWS; c++)
            execute("INSERT INTO %s (k, c, b) VALUES (?, ?, ?)", 0, c, java.nio.ByteBuffer.wrap(payload));
        flush();
        probe("BLOB-CONTROL(simple column)", getCurrentColumnFamilyStore());
    }
}
