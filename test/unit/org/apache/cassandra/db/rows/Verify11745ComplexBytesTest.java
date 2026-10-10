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

import java.util.HashSet;
import java.util.Set;

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

public class Verify11745ComplexBytesTest extends CQLTester
{
    private static final int ROWS = 300;
    private static final int PAGE_BYTES = 50_000;

    @Test
    public void setOverwriteEscapesBytePageLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s set<text>, PRIMARY KEY (k, c))");

        Set<String> s = new HashSet<>();
        for (int i = 0; i < 10; i++)
            s.add("v" + i + "_" + "x".repeat(100));   // ~1 KiB per row

        for (int c = 0; c < ROWS; c++)
            execute("UPDATE %s SET s = ? WHERE k = ? AND c = ?", s, 0, c);
        flush();
        run("SET-OVERWRITE");
    }

    @Test
    public void setAppendIsCountedCorrectly() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s set<text>, PRIMARY KEY (k, c))");

        Set<String> s = new HashSet<>();
        for (int i = 0; i < 10; i++)
            s.add("v" + i + "_" + "x".repeat(100));

        // append does NOT emit a collection tombstone
        for (int c = 0; c < ROWS; c++)
            execute("UPDATE %s SET s = s + ? WHERE k = ? AND c = ?", s, 0, c);
        flush();
        run("SET-APPEND");
    }

    private void run(String label) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(0));
        long nowInSec = FBUtilities.nowInSeconds();
        DataLimits limits = DataLimits.NONE.forPaging(PageSize.inBytes(PAGE_BYTES));

        SinglePartitionReadCommand cmd =
            SinglePartitionReadCommand.create(cfs.metadata(), nowInSec,
                                              ColumnFilter.all(cfs.metadata()),
                                              RowFilter.none(), limits, key,
                                              new ClusteringIndexSliceFilter(Slices.ALL, false));

        int rows = 0, realBytes = 0, countedBytes = 0;
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
                            ComplexColumnData ccd = null;
                            for (ColumnData cd : row)
                                if (cd instanceof ComplexColumnData)
                                    ccd = (ComplexColumnData) cd;
                            firstRowDetail = "complexDeletion=" + (ccd == null ? "n/a" : ccd.complexDeletion())
                                             + " cells=" + (ccd == null ? -1 : ccd.cellsCount())
                                             + " ccd.dataSize=" + (ccd == null ? -1 : ccd.dataSize())
                                             + " ccd.liveDataSize=" + (ccd == null ? -1 : ccd.liveDataSize(nowInSec))
                                             + " row.dataSize=" + row.dataSize()
                                             + " row.liveDataSize=" + row.liveDataSize(nowInSec);
                        }
                    }
                }
            }
        }
        System.out.println("VERIFY-11745 " + label + " firstRow: " + firstRowDetail);
        System.out.println("VERIFY-11745 " + label + " budget=" + PAGE_BYTES
                           + " rowsReturned=" + rows + "/" + ROWS
                           + " realBytes=" + realBytes
                           + " bytesCountedByLimit=" + countedBytes);
    }
}
