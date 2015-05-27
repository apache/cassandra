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

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnCounter
{
    protected int live;
    protected int tombstones;
    protected final long timestamp;

    public ColumnCounter(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public void count(Column column, DeletionInfo.InOrderTester tester)
    {
        // The cell is shadowed by a higher-level deletion, and won't be retained.
        // For the purposes of this counter, we don't care if it's a tombstone or not.
        if (tester.isDeleted(column))
            return;

        if (column.isLive(timestamp))
            live++;
        else
            tombstones++;
    }

    public int live()
    {
        return live;
    }

    public int tombstones()
    {
        return tombstones;
    }

    public ColumnCounter countAll(ColumnFamily container)
    {
        if (container == null)
            return this;

        DeletionInfo.InOrderTester tester = container.inOrderDeletionTester();
        for (Column c : container)
            count(c, tester);
        return this;
    }

    public static class GroupByPrefix extends ColumnCounter
    {
        protected final CompositeType type;
        protected final int toGroup;
        protected ByteBuffer[] previous;
        protected boolean previousGroupIsStatic;

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
        public GroupByPrefix(long timestamp, CompositeType type, int toGroup)
        {
            super(timestamp);
            this.type = type;
            this.toGroup = toGroup;

            assert toGroup == 0 || type != null;
        }

        public void count(Column column, DeletionInfo.InOrderTester tester)
        {
            if (tester.isDeleted(column))
                return;

            if (!column.isLive(timestamp))
            {
                tombstones++;
                return;
            }

            if (toGroup == 0)
            {
                live = 1;
                return;
            }

            ByteBuffer[] current = type.split(column.name());
            assert current.length >= toGroup;

            if (previous == null)
            {
                // Only the first group can be static
                previousGroupIsStatic = CompositeType.isStaticName(column.name());
            }
            else
            {
                boolean isSameGroup = previousGroupIsStatic == CompositeType.isStaticName(column.name());
                if (isSameGroup)
                {
                    for (int i = 0; i < toGroup; i++)
                    {
                        if (ByteBufferUtil.compareUnsigned(previous[i], current[i]) != 0)
                        {
                            isSameGroup = false;
                            break;
                        }
                    }
                }

                if (isSameGroup)
                    return;

                // We want to count the static group as 1 (CQL) row only if it's the only
                // group in the partition. So, since we have already counted it at this point,
                // just don't count the 2nd group if there is one and the first one was static
                if (previousGroupIsStatic)
                {
                    previous = current;
                    previousGroupIsStatic = false;
                    return;
                }
            }

            live++;
            previous = current;
        }
    }

    /**
     * Similar to GroupByPrefix, but designed to handle counting cells in reverse order.
     */
    public static class GroupByPrefixReversed extends GroupByPrefix
    {
        public GroupByPrefixReversed(long timestamp, CompositeType type, int toGroup)
        {
            super(timestamp, type, toGroup);
        }

        @Override
        public void count(Column column, DeletionInfo.InOrderTester tester)
        {
            if (tester.isDeleted(column))
                return;

            if (!column.isLive(timestamp))
            {
                tombstones++;
                return;
            }

            if (toGroup == 0)
            {
                live = 1;
                return;
            }

            ByteBuffer[] current = type.split(column.name());
            assert current.length >= toGroup;

            boolean isStatic = CompositeType.isStaticName(column.name());
            if (previous == null)
            {
                // This is the first group we've seen, and it's static.  In this case we want to return a count of 1,
                // because there are no other live groups.
                previousGroupIsStatic = true;
                previous = current;
                live++;
            }
            else if (isStatic)
            {
                // Ignore statics if we've seen any other statics or any other groups
                return;
            }

            for (int i = 0; i < toGroup; i++)
            {
                if (ByteBufferUtil.compareUnsigned(previous[i], current[i]) != 0)
                {
                    // it's a new group
                    live++;
                    previous = current;
                    return;
                }
            }
        }
    }
}
