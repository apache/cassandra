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
package org.apache.cassandra.db;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A read query that selects a (part of a) range of partitions of a virtual table.
 */
public class VirtualTablePartitionRangeReadQuery extends VirtualTableReadQuery implements PartitionRangeReadQuery
{
    private final DataRange dataRange;

    public static VirtualTablePartitionRangeReadQuery create(TableMetadata metadata,
                                                             int nowInSec,
                                                             ColumnFilter columnFilter,
                                                             RowFilter rowFilter,
                                                             DataLimits limits,
                                                             DataRange dataRange)
    {
        return new VirtualTablePartitionRangeReadQuery(metadata,
                                                       nowInSec,
                                                       columnFilter,
                                                       rowFilter,
                                                       limits,
                                                       dataRange);
    }

    private VirtualTablePartitionRangeReadQuery(TableMetadata metadata,
                                                int nowInSec,
                                                ColumnFilter columnFilter,
                                                RowFilter rowFilter,
                                                DataLimits limits,
                                                DataRange dataRange)
    {
        super(metadata, nowInSec, columnFilter, rowFilter, limits);
        this.dataRange = dataRange;
    }

    @Override
    public DataRange dataRange()
    {
        return dataRange;
    }

    @Override
    public PartitionRangeReadQuery withUpdatedLimit(DataLimits newLimits)
    {
        return new VirtualTablePartitionRangeReadQuery(metadata(),
                                                       nowInSec(),
                                                       columnFilter(),
                                                       rowFilter(),
                                                       newLimits,
                                                       dataRange());
    }

    @Override
    public PartitionRangeReadQuery withUpdatedLimitsAndDataRange(DataLimits newLimits, DataRange newDataRange)
    {
        return new VirtualTablePartitionRangeReadQuery(metadata(),
                                                       nowInSec(),
                                                       columnFilter(),
                                                       rowFilter(),
                                                       newLimits,
                                                       newDataRange);
    }

    @Override
    protected UnfilteredPartitionIterator queryVirtualTable()
    {
        VirtualTable view = VirtualKeyspaceRegistry.instance.getTableNullable(metadata().id);
        return view.select(dataRange, columnFilter());
    }

    @Override
    protected void appendCQLWhereClause(StringBuilder sb)
    {
        if (dataRange.isUnrestricted() && rowFilter().isEmpty())
            return;

        sb.append(" WHERE ");
        // We put the row filter first because the data range can end by "ORDER BY"
        if (!rowFilter().isEmpty())
        {
            sb.append(rowFilter());
            if (!dataRange.isUnrestricted())
                sb.append(" AND ");
        }
        if (!dataRange.isUnrestricted())
            sb.append(dataRange.toCQLString(metadata()));
    }
}
