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
package org.apache.cassandra.db.filter;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.IColumnContainer;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnCounter
{
    protected int live;
    protected int ignored;

    public void count(IColumn column, IColumnContainer container)
    {
        if (!isLive(column, container))
            ignored++;
        else
            live++;
    }

    protected static boolean isLive(IColumn column, IColumnContainer container)
    {
        return column.isLive() && (!container.deletionInfo().isDeleted(column));
    }

    public int live()
    {
        return live;
    }

    public int ignored()
    {
        return ignored;
    }

    public static class GroupByPrefix extends ColumnCounter
    {
        private final CompositeType type;
        private final int toGroup;
        private ByteBuffer[] last;

        /**
         * A column counter that count only 1 for all the columns sharing a
         * given prefix of the key.
         *
         * @param type the type of the column name. This can be null if {@code
         *             toGroup} is 0, otherwise it should be a composite.
         * @param toGroup the number of composite components on which to group
         *                column. If 0, all columns are grouped, otherwise we group
         *                those for which the {@code toGroup} first component are equals.
         */
        public GroupByPrefix(CompositeType type, int toGroup)
        {
            this.type = type;
            this.toGroup = toGroup;

            assert toGroup == 0 || type != null;
        }

        public void count(IColumn column, IColumnContainer container)
        {
            if (!isLive(column, container))
            {
                ignored++;
                return;
            }

            if (toGroup == 0)
            {
                live = 1;
                return;
            }

            ByteBuffer[] current = type.split(column.name());
            assert current.length >= toGroup;

            if (last != null)
            {
                boolean isSameGroup = true;
                for (int i = 0; i < toGroup; i++)
                {
                    if (ByteBufferUtil.compareUnsigned(last[i], current[i]) != 0)
                    {
                        isSameGroup = false;
                        break;
                    }
                }

                if (isSameGroup)
                    return;
            }

            live++;
            last = current;
        }
    }
}
