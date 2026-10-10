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

public class ByteLimitComplexProofTest extends CQLTester
{
    private static final int ROWS = 200;
    private static final int PAGE_BYTES = 50_000;

    private int[] readWithByteLimit(ColumnFamilyStore cfs, int k)
    {
        DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(k));
        DataLimits limits = DataLimits.NONE.forPaging(PageSize.inBytes(PAGE_BYTES));
        System.out.println("limits = " + limits + " count()=" + limits.count() + " bytes()=" + limits.bytes());

        SinglePartitionReadCommand cmd =
            SinglePartitionReadCommand.create(cfs.metadata(),
                                              FBUtilities.nowInSeconds(),
                                              ColumnFilter.all(cfs.metadata()),
                                              RowFilter.none(),
                                              limits,
                                              key,
                                              new ClusteringIndexSliceFilter(Slices.ALL, false));

        int rows = 0;
        int bytes = 0;
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
                        if (u.isRow())
                        {
                            Row row = (Row) u;
                            rows++;
                            bytes += row.dataSize();
                        }
                    }
                }
            }
        }
        return new int[]{ rows, bytes };
    }

    @Test
    public void nonFrozenCollectionEscapesByteLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, m map<text,text>, PRIMARY KEY (k,c))");

        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < 10; i++)
            m.put("key" + i, String.join("", Collections.nCopies(1000, "v")));

        for (int c = 0; c < ROWS; c++)
            execute("INSERT INTO %s (k, c, m) VALUES (?, ?, ?)", 0, c, m);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        int[] r = readWithByteLimit(cfs, 0);
        System.out.println("MAP(INSERT): rowsReturned=" + r[0] + " actualBytes=" + r[1] + " (budget " + PAGE_BYTES + ", table had " + ROWS + " rows)");
    }

    @Test
    public void nonFrozenCollectionBuiltByAppendIsCounted() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, m map<text,text>, PRIMARY KEY (k,c))");

        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < 10; i++)
            m.put("key" + i, String.join("", Collections.nCopies(1000, "v")));

        // append-style update does NOT emit a collection tombstone
        for (int c = 0; c < ROWS; c++)
            execute("UPDATE %s SET m = m + ? WHERE k = ? AND c = ?", m, 0, c);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        int[] r = readWithByteLimit(cfs, 0);
        System.out.println("MAP(APPEND): rowsReturned=" + r[0] + " actualBytes=" + r[1] + " (budget " + PAGE_BYTES + ", table had " + ROWS + " rows)");
    }

    @Test
    public void simpleBlobColumnIsCounted() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, b blob, PRIMARY KEY (k,c))");

        byte[] payload = new byte[10_000];

        for (int c = 0; c < ROWS; c++)
            execute("INSERT INTO %s (k, c, b) VALUES (?, ?, ?)", 0, c, java.nio.ByteBuffer.wrap(payload));
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        int[] r = readWithByteLimit(cfs, 0);
        System.out.println("BLOB(CONTROL): rowsReturned=" + r[0] + " actualBytes=" + r[1] + " (budget " + PAGE_BYTES + ", table had " + ROWS + " rows)");
    }
}
