/*
 *
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
 *
 */
package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;

public final class FilteredPartitions extends BasePartitions<RowIterator, BasePartitionIterator<?>> implements PartitionIterator
{
    // wrap basic iterator for transformation
    FilteredPartitions(PartitionIterator input)
    {
        super(input);
    }

    // wrap basic unfiltered iterator for transformation, applying filter as first transformation
    FilteredPartitions(UnfilteredPartitionIterator input, Filter filter)
    {
        super(input);
        add(filter);
    }

    // copy from an UnfilteredPartitions, applying a filter to convert it
    FilteredPartitions(Filter filter, UnfilteredPartitions copyFrom)
    {
        super(copyFrom);
        add(filter);
    }

    /**
     * Filter any RangeTombstoneMarker from the iterator's iterators, transforming it into a PartitionIterator.
     */
    public static FilteredPartitions filter(UnfilteredPartitionIterator iterator, int nowInSecs)
    {
        FilteredPartitions filtered = filter(iterator,
                                             new Filter(nowInSecs,
                                                        iterator.metadata().enforceStrictLiveness()));

        return iterator.isForThrift()
             ? filtered
             : (FilteredPartitions) Transformation.apply(filtered, new EmptyPartitionsDiscarder());
    }

    public static FilteredPartitions filter(UnfilteredPartitionIterator iterator, Filter filter)
    {
        return iterator instanceof UnfilteredPartitions
             ? new FilteredPartitions(filter, (UnfilteredPartitions) iterator)
             : new FilteredPartitions(iterator, filter);
    }
}
