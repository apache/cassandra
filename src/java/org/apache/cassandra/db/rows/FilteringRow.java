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

import java.util.Iterator;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;

public abstract class FilteringRow extends WrappingRow
{
    public static FilteringRow columnsFilteringRow(final Columns toInclude)
    {
        return new FilteringRow()
        {
            @Override
            protected boolean include(ColumnDefinition column)
            {
                return toInclude.contains(column);
            }
        };
    }

    public static FilteringRow columnsFilteringRow(final ColumnFilter toInclude)
    {
        return new FilteringRow()
        {
            @Override
            protected boolean include(ColumnDefinition column)
            {
                return toInclude.includes(column);
            }

            @Override
            protected boolean include(Cell cell)
            {
                return toInclude.includes(cell);
            }
        };
    }

    public FilteringRow setTo(Row row)
    {
        super.setTo(row);
        return this;
    }

    /**
     * The following functions are meant to be overriden based on needs.
     */
    protected boolean include(Cell cell) { return true; }
    protected boolean include(LivenessInfo info) { return true; }
    protected boolean include(DeletionTime dt) { return true; }
    protected boolean include(ColumnDefinition column) { return true; }
    protected boolean include(ColumnDefinition c, DeletionTime dt) { return true; }

    // Sublcasses that override this should be careful to call the overriden version first, or this might break FilteringRow (i.e. it might not
    // filter what it should).
    @Override
    protected Cell filterCell(Cell cell)
    {
        return include(cell.column()) && include(cell.livenessInfo()) && include(cell) ? cell : null;
    }

    protected DeletionTime filterDeletionTime(DeletionTime deletion)
    {
        return deletion == null || !include(deletion)
             ? DeletionTime.LIVE
             : deletion;
    }

    @Override
    public LivenessInfo primaryKeyLivenessInfo()
    {
        LivenessInfo info = super.primaryKeyLivenessInfo();
        return include(info) ? info : LivenessInfo.NONE;
    }

    @Override
    public DeletionTime deletion()
    {
        DeletionTime deletion = super.deletion();
        return include(deletion) ? deletion : DeletionTime.LIVE;
    }

    @Override
    public Iterator<Cell> getCells(ColumnDefinition c)
    {
        // slightly speed things up if we know we don't care at all about the column
        if (!include(c))
            return null;

        return super.getCells(c);
    }

    @Override
    public DeletionTime getDeletion(ColumnDefinition c)
    {
        if (!include(c))
            return DeletionTime.LIVE;

        DeletionTime dt = super.getDeletion(c);
        return include(c, dt) ? dt : DeletionTime.LIVE;
    }
}
