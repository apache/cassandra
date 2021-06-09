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

package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;

/**
 * Holder of the content of a partition, see AbstractBTreePartition.
 * When updating a partition one holder is swapped for another atomically.
 */
public final class BTreePartitionData
{
    public static final BTreePartitionData EMPTY = new BTreePartitionData(RegularAndStaticColumns.NONE, BTree.empty(), DeletionInfo.LIVE, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
    public static final long UNSHARED_HEAP_SIZE = ObjectSizes.measure(EMPTY);


    public final RegularAndStaticColumns columns;
    public final DeletionInfo deletionInfo;
    // the btree of rows
    public final Object[] tree;
    public final Row staticRow;
    public final EncodingStats stats;

    BTreePartitionData(RegularAndStaticColumns columns,
                       Object[] tree,
                       DeletionInfo deletionInfo,
                       Row staticRow,
                       EncodingStats stats)
    {
        this.columns = columns;
        this.tree = tree;
        this.deletionInfo = deletionInfo;
        this.staticRow = staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
        this.stats = stats;
    }
}
