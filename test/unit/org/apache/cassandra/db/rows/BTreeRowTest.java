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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BTreeRowTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testFilterDropsComplexDeletionOfDroppedColumn()
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                           .addClusteringColumn("ck", Int32Type.instance)
                                           .addRegularColumn("m", MapType.getInstance(Int32Type.instance, Int32Type.instance, true))
                                           .build();
        ColumnMetadata m = table.getColumn(ByteBufferUtil.bytes("m"));
        long now = FBUtilities.nowInSeconds();
        DeletionTime complexDeletion = DeletionTime.build(5, now);

        BTreeRow.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(Util.clustering(table.comparator, 1));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(100));
        builder.addComplexDeletion(m, complexDeletion);
        builder.addCell(BufferCell.live(m, 20, ByteBufferUtil.bytes(1), CellPath.create(ByteBufferUtil.bytes(1))));
        Row row = builder.build();

        // Dropped before the complex deletion: nothing is filtered.
        ComplexColumnData filtered = row.filter(ColumnFilter.all(table), table.unbuild().recordColumnDrop(m, 3).build())
                                        .getComplexColumnData(m);
        assertEquals(complexDeletion, filtered.complexDeletion());
        assertEquals(1, filtered.cellsCount());

        // Dropped after the complex deletion but before the cell: like sstable reads, drop the complex deletion.
        filtered = row.filter(ColumnFilter.all(table), table.unbuild().recordColumnDrop(m, 10).build())
                      .getComplexColumnData(m);
        assertEquals(DeletionTime.LIVE, filtered.complexDeletion());
        assertEquals(1, filtered.cellsCount());

        // Dropped after everything: the column is gone.
        assertNull(row.filter(ColumnFilter.all(table), table.unbuild().recordColumnDrop(m, 30).build())
                      .getComplexColumnData(m));
    }
}
