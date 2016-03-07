/*
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
 */
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Lists;

import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class CellTest
{
    private static final String KEYSPACE1 = "CellTest";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_COLLECTION = "Collection1";

    private static final CFMetaData cfm = SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1);
    private static final CFMetaData cfm2 = CFMetaData.Builder.create(KEYSPACE1, CF_COLLECTION)
                                                             .addPartitionKey("k", IntegerType.instance)
                                                             .addClusteringColumn("c", IntegerType.instance)
                                                             .addRegularColumn("v", IntegerType.instance)
                                                             .addRegularColumn("m", MapType.getInstance(IntegerType.instance, IntegerType.instance, true))
                                                             .build();

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1), cfm, cfm2);
    }

    @Test
    public void testConflictingTypeEquality()
    {
        boolean[] tf = new boolean[]{ true, false };
        for (boolean lhs : tf)
        {
            for (boolean rhs : tf)
            {
                // don't test equality for both sides native, as this is based on CellName resolution
                if (lhs && rhs)
                    continue;
                Cell a = expiring(cfm, "val", "a", 1, 1);
                Cell b = regular(cfm, "val", "a", 1);
                Assert.assertNotSame(a, b);
                Assert.assertNotSame(b, a);

                a = deleted(cfm, "val", 1, 1);
                Assert.assertNotSame(a, b);
                Assert.assertNotSame(b, a);
            }
        }
    }

    @Test
    public void testExpiringCellReconile()
    {
        // equal
        Assert.assertEquals(0, testExpiring("val", "a", 1, 1, null, null, null, null));

        // newer timestamp
        Assert.assertEquals(-1, testExpiring("val", "a", 2, 1, null, null, 1L, null));
        Assert.assertEquals(-1, testExpiring("val", "a", 2, 1, null, "val", 1L, 2));

        Assert.assertEquals(-1, testExpiring("val", "a", 1, 2, null, null, null, 1));
        Assert.assertEquals(1, testExpiring("val", "a", 1, 2, null, "val", null, 1));

        // newer value
        Assert.assertEquals(-1, testExpiring("val", "b", 2, 1, null, "a", null, null));
        Assert.assertEquals(-1, testExpiring("val", "b", 2, 1, null, "a", null, 2));
    }

    private static ByteBuffer bb(int i)
    {
        return ByteBufferUtil.bytes(i);
    }

    @Test
    public void testComplexCellReconcile()
    {
        ColumnDefinition m = cfm2.getColumnDefinition(new ColumnIdentifier("m", false));
        int now1 = FBUtilities.nowInSeconds();
        long ts1 = now1*1000000;


        Cell r1m1 = BufferCell.live(m, ts1, bb(1), CellPath.create(bb(1)));
        Cell r1m2 = BufferCell.live(m, ts1, bb(2), CellPath.create(bb(2)));
        List<Cell> cells1 = Lists.newArrayList(r1m1, r1m2);

        int now2 = now1 + 1;
        long ts2 = now2*1000000;
        Cell r2m2 = BufferCell.live(m, ts2, bb(1), CellPath.create(bb(2)));
        Cell r2m3 = BufferCell.live(m, ts2, bb(2), CellPath.create(bb(3)));
        Cell r2m4 = BufferCell.live(m, ts2, bb(3), CellPath.create(bb(4)));
        List<Cell> cells2 = Lists.newArrayList(r2m2, r2m3, r2m4);

        RowBuilder builder = new RowBuilder();
        Cells.reconcileComplex(m, cells1.iterator(), cells2.iterator(), DeletionTime.LIVE, builder, now2 + 1);
        Assert.assertEquals(Lists.newArrayList(r1m1, r2m2, r2m3, r2m4), builder.cells);
    }

    private int testExpiring(String n1, String v1, long t1, int et1, String n2, String v2, Long t2, Integer et2)
    {
        if (n2 == null)
            n2 = n1;
        if (v2 == null)
            v2 = v1;
        if (t2 == null)
            t2 = t1;
        if (et2 == null)
            et2 = et1;
        Cell c1 = expiring(cfm, n1, v1, t1, et1);
        Cell c2 = expiring(cfm, n2, v2, t2, et2);

        int now = FBUtilities.nowInSeconds();
        if (Cells.reconcile(c1, c2, now) == c1)
            return Cells.reconcile(c2, c1, now) == c1 ? -1 : 0;
        return Cells.reconcile(c2, c1, now) == c2 ? 1 : 0;
    }

    private Cell regular(CFMetaData cfm, String columnName, String value, long timestamp)
    {
        ColumnDefinition cdef = cfm.getColumnDefinition(ByteBufferUtil.bytes(columnName));
        return BufferCell.live(cdef, timestamp, ByteBufferUtil.bytes(value));
    }

    private Cell expiring(CFMetaData cfm, String columnName, String value, long timestamp, int localExpirationTime)
    {
        ColumnDefinition cdef = cfm.getColumnDefinition(ByteBufferUtil.bytes(columnName));
        return new BufferCell(cdef, timestamp, 1, localExpirationTime, ByteBufferUtil.bytes(value), null);
    }

    private Cell deleted(CFMetaData cfm, String columnName, int localDeletionTime, long timestamp)
    {
        ColumnDefinition cdef = cfm.getColumnDefinition(ByteBufferUtil.bytes(columnName));
        return BufferCell.tombstone(cdef, timestamp, localDeletionTime);
    }
}
