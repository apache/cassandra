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

package org.apache.cassandra.index.sai.utils;

import java.util.Objects;
import javax.annotation.Nullable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

public class PartitionInfo
{
    public final DecoratedKey key;
    public final Row staticRow;

    // present if it's unfiltered partition iterator
    @Nullable
    public final DeletionTime partitionDeletion;

    // present if it's unfiltered partition iterator
    @Nullable
    public final EncodingStats encodingStats;

    public PartitionInfo(DecoratedKey key, Row staticRow)
    {
        this.key = key;
        this.staticRow = staticRow;
        this.partitionDeletion = null;
        this.encodingStats = null;
    }

    public PartitionInfo(DecoratedKey key, Row staticRow, DeletionTime partitionDeletion, EncodingStats encodingStats)
    {
        this.key = key;
        this.staticRow = staticRow;

        this.partitionDeletion = partitionDeletion;
        this.encodingStats = encodingStats;
    }

    public static <U extends Unfiltered, R extends BaseRowIterator<U>> PartitionInfo create(R baseRowIterator)
    {
        return baseRowIterator instanceof UnfilteredRowIterator
               ? new PartitionInfo(baseRowIterator.partitionKey(), baseRowIterator.staticRow(),
                                   ((UnfilteredRowIterator) baseRowIterator).partitionLevelDeletion(),
                                   ((UnfilteredRowIterator) baseRowIterator).stats())
               : new PartitionInfo(baseRowIterator.partitionKey(), baseRowIterator.staticRow());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionInfo that = (PartitionInfo) o;
        return Objects.equals(key, that.key) && Objects.equals(staticRow, that.staticRow)
               && Objects.equals(partitionDeletion, that.partitionDeletion) && Objects.equals(encodingStats, that.encodingStats);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(key, staticRow, partitionDeletion, encodingStats);
    }
}
