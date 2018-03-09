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

import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.transform.Transformation;

/**
 * Function to skip cells (from an iterator) that are not part of those queried by the user
 * according to the provided {@code ColumnFilter}. See {@link UnfilteredRowIterators#withOnlyQueriedData}
 * for more details.
 */
public class WithOnlyQueriedData<I extends BaseRowIterator<?>> extends Transformation<I>
{
    private final ColumnFilter filter;

    public WithOnlyQueriedData(ColumnFilter filter)
    {
        this.filter = filter;
    }

    @Override
    protected RegularAndStaticColumns applyToPartitionColumns(RegularAndStaticColumns columns)
    {
        return filter.queriedColumns();
    }

    @Override
    protected Row applyToStatic(Row row)
    {
        return row.withOnlyQueriedData(filter);
    }

    @Override
    protected Row applyToRow(Row row)
    {
        return row.withOnlyQueriedData(filter);
    }
};
