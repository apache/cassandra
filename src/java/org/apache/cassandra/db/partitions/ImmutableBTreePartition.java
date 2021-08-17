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
package org.apache.cassandra.db.partitions;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.*;

public class ImmutableBTreePartition extends AbstractBTreePartition
{

    protected final Holder holder;
    protected final TableMetadata metadata;

    public ImmutableBTreePartition(TableMetadata metadata,
                                   DecoratedKey partitionKey,
                                   RegularAndStaticColumns columns,
                                   Row staticRow,
                                   Object[] tree,
                                   DeletionInfo deletionInfo,
                                   EncodingStats stats)
    {
        super(partitionKey);
        this.metadata = metadata;
        this.holder = new Holder(columns, tree, deletionInfo, staticRow, stats);
    }

    protected ImmutableBTreePartition(TableMetadata metadata,
                                      DecoratedKey partitionKey,
                                      Holder holder)
    {
        super(partitionKey);
        this.metadata = metadata;
        this.holder = holder;
    }

    /**
     * Creates an {@code ImmutableBTreePartition} holding all the data of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     *
     * @param iterator the iterator to gather in memory.
     * @return the created partition.
     */
    public static ImmutableBTreePartition create(UnfilteredRowIterator iterator)
    {
        return create(iterator, 16);
    }

    /**
     * Creates an {@code ImmutableBTreePartition} holding all the data of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     *
     * @param iterator the iterator to gather in memory.
     * @param ordered {@code true} if the iterator will return the rows in order, {@code false} otherwise.
     * @return the created partition.
     */
    public static ImmutableBTreePartition create(UnfilteredRowIterator iterator, boolean ordered)
    {
        return create(iterator, 16, ordered);
    }

    /**
     * Creates an {@code ImmutableBTreePartition} holding all the data of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     *
     * @param iterator the iterator to gather in memory.
     * @param initialRowCapacity sizing hint (in rows) to use for the created partition. It should ideally
     * correspond or be a good estimation of the number or rows in {@code iterator}.
     * @return the created partition.
     */
    public static ImmutableBTreePartition create(UnfilteredRowIterator iterator, int initialRowCapacity)
    {
        return create(iterator, initialRowCapacity, true);
    }

    /**
     * Creates an {@code ImmutableBTreePartition} holding all the data of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     *
     * @param iterator the iterator to gather in memory.
     * @param initialRowCapacity sizing hint (in rows) to use for the created partition. It should ideally
     * correspond or be a good estimation of the number or rows in {@code iterator}.
     * @param ordered {@code true} if the iterator will return the rows in order, {@code false} otherwise.
     * @return the created partition.
     */
    public static ImmutableBTreePartition create(UnfilteredRowIterator iterator, int initialRowCapacity, boolean ordered)
    {
        return new ImmutableBTreePartition(iterator.metadata(), iterator.partitionKey(), build(iterator, initialRowCapacity, ordered));
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    protected Holder holder()
    {
        return holder;
    }

    protected boolean canHaveShadowedData()
    {
        return false;
    }
}
