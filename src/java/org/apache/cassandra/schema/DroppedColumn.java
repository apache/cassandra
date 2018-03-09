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
package org.apache.cassandra.schema;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public final class DroppedColumn
{
    public final ColumnMetadata column;
    public final long droppedTime; // drop timestamp, in microseconds, yet with millisecond granularity

    public DroppedColumn(ColumnMetadata column, long droppedTime)
    {
        this.column = column;
        this.droppedTime = droppedTime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof DroppedColumn))
            return false;

        DroppedColumn dc = (DroppedColumn) o;

        return column.equals(dc.column) && droppedTime == dc.droppedTime;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(column, droppedTime);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("column", column).add("droppedTime", droppedTime).toString();
    }
}
