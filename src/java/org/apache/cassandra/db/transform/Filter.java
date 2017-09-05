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

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.rows.*;

public final class Filter extends Transformation
{
    private final int nowInSec;
    private final boolean enforceStrictLiveness;

    public Filter(int nowInSec, boolean enforceStrictLiveness)
    {
        this.nowInSec = nowInSec;
        this.enforceStrictLiveness = enforceStrictLiveness;
    }

    @Override
    protected RowIterator applyToPartition(BaseRowIterator iterator)
    {
        return iterator instanceof UnfilteredRows
             ? new FilteredRows(this, (UnfilteredRows) iterator)
             : new FilteredRows((UnfilteredRowIterator) iterator, this);
    }

    @Override
    protected Row applyToStatic(Row row)
    {
        if (row.isEmpty())
            return Rows.EMPTY_STATIC_ROW;

        row = row.purge(DeletionPurger.PURGE_ALL, nowInSec, enforceStrictLiveness);
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

    @Override
    protected Row applyToRow(Row row)
    {
        return row.purge(DeletionPurger.PURGE_ALL, nowInSec, enforceStrictLiveness);
    }

    @Override
    protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        return null;
    }
}
