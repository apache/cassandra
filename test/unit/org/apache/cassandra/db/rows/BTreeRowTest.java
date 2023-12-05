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

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class BTreeRowTest
{
    private final TableMetadata metadata = TableMetadata.builder("", "")
                                                        .addPartitionKeyColumn("pk", Int32Type.instance)
                                                        .addClusteringColumn("ck", Int32Type.instance)
                                                        .addRegularColumn("v1", Int32Type.instance)
                                                        .addRegularColumn("v2", Int32Type.instance)
                                                        .build();
    private final ColumnMetadata v2Metadata = metadata.regularAndStaticColumns().columns(false).getSimple(1);
    private final ColumnMetadata v1Metadata = metadata.regularAndStaticColumns().columns(false).getSimple(0);

    private BTreeRow.Builder row(int ck, Cell<?>... columns)
    {
        BTreeRow.Builder builder = new BTreeRow.Builder(true);
        builder.newRow(Util.clustering(metadata.comparator, ck));
        for (Cell<?> cell : columns)
            builder.addCell(cell);
        return builder;
    }

    private Cell<?> cell(ColumnMetadata metadata, int v, long timestamp)
    {
        return new BufferCell(metadata,
                              timestamp,
                              BufferCell.NO_TTL,
                              BufferCell.NO_DELETION_TIME,
                              ByteBufferUtil.bytes(v),
                              null);
    }

    @Test
    public void testRowMinTimespampFromCells()
    {
        int v1CellTimestamp = 1000;
        int v2CellTimestamp = 500;
        int primaryKeyTimestamp = 2000;
        BTreeRow.Builder builder = row(1, cell(v1Metadata, 1, v1CellTimestamp), cell(v2Metadata, 1, v2CellTimestamp));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(primaryKeyTimestamp, FBUtilities.nowInSeconds()));
        Row row = builder.build();
        assertEquals(v2CellTimestamp, row.minTimestamp());
    }

    @Test
    public void testRowMinTimespampFromPrimaryKeyListener()
    {
        int v1CellTimestamp = 1000;
        int v2CellTimestamp = 500;
        int primaryKeyTimestamp = 100;
        BTreeRow.Builder builder = row(2, cell(v1Metadata, 1, v1CellTimestamp), cell(v2Metadata, 1, v2CellTimestamp));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(primaryKeyTimestamp, FBUtilities.nowInSeconds()));
        Row row = builder.build();
        assertEquals(primaryKeyTimestamp, row.minTimestamp());
    }

    @Test
    public void testRowMinTimespampFromDeletion()
    {
        int v1CellTimestamp = 1000;
        int v2CellTimestamp = 500;
        int primaryKeyTimestamp = 100;
        int localDeletionTime = 50;
        BTreeRow.Builder builder = row(3, cell(v1Metadata, 1, v1CellTimestamp), cell(v2Metadata, 1, v2CellTimestamp));
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(primaryKeyTimestamp, FBUtilities.nowInSeconds()));
        builder.addRowDeletion(new Row.Deletion(new DeletionTime(localDeletionTime, FBUtilities.nowInSeconds()), true));
        Row row = builder.build();
        assertEquals(primaryKeyTimestamp, row.minTimestamp());
    }
}