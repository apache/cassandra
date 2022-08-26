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

import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

public final class FilteredRows extends BaseRows<Row, BaseRowIterator<?>> implements RowIterator
{
    FilteredRows(RowIterator input)
    {
        super(input);
    }

    FilteredRows(UnfilteredRowIterator input, Filter filter)
    {
        super(input);
        add(filter);
    }

    FilteredRows(Filter filter, UnfilteredRows input)
    {
        super(input);
        add(filter);
    }

    @Override
    public boolean isEmpty()
    {
        return staticRow().isEmpty() && !hasNext();
    }

    /**
     * Filter any RangeTombstoneMarker from the iterator, transforming it into a RowIterator.
     */
    public static RowIterator filter(UnfilteredRowIterator iterator, long nowInSecs)
    {
        return new Filter(nowInSecs, iterator.metadata().enforceStrictLiveness()).applyToPartition(iterator);
    }
}
