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

package org.apache.cassandra.cql3.selection;

import java.util.List;

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * The {@link ColumnTimestamps} associated to the given set of columns of a row.
 */
interface RowTimestamps
{
    /**
     * Adds an empty timestamp for the specified column.
     *
     * @param index the column index
     */
    void addNoTimestamp(int index);

    /**
     * Adds the timestamp of the specified cell.
     *
     * @param index the column index
     * @param cell the cell to get the timestamp from
     * @param nowInSec the query timestamp in second
     */
    void addTimestamp(int index, Cell<?> cell, long nowInSec);

    /**
     * Returns the timestamp of the specified column.
     *
     * @param index the column index
     * @return the timestamp of the specified column
     */
    ColumnTimestamps get(int index);

    /**
     * A {@code RowTimestamps} that does nothing.
     */
    RowTimestamps NOOP_ROW_TIMESTAMPS = new RowTimestamps()
    {
        @Override
        public void addNoTimestamp(int index)
        {
        }

        @Override
        public void addTimestamp(int index, Cell<?> cell, long nowInSec)
        {
        }

        @Override
        public ColumnTimestamps get(int index)
        {
            return ColumnTimestamps.NO_TIMESTAMP;
        }
    };

    static RowTimestamps newInstance(ColumnTimestamps.TimestampsType type, List<ColumnMetadata> columns)
    {
        final ColumnTimestamps[] array = new ColumnTimestamps[columns.size()];

        for (int i = 0, m = columns.size(); i < m; i++)
            array[i] = ColumnTimestamps.newTimestamps(type, columns.get(i).type);

        return new RowTimestamps()
        {
            @Override
            public void addNoTimestamp(int index)
            {
                array[index].addNoTimestamp();
            }

            @Override
            public void addTimestamp(int index, Cell<?> cell, long nowInSec)
            {
                array[index].addTimestampFrom(cell, nowInSec);
            }

            @Override
            public ColumnTimestamps get(int index)
            {
                return array[index];
            }
        };
    }
}
