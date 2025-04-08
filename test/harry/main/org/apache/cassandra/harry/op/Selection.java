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

package org.apache.cassandra.harry.op;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import accord.utils.Invariants;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.util.BitSet;

public interface Selection
{
    // TODO: allow expressions here
    Collection<ColumnSpec<?>> columns();

    boolean includeTimestamps();

    boolean isWildcard();

    boolean selects(ColumnSpec<?> column);

    boolean selectsAllOf(List<ColumnSpec<?>> subSelection);

    int indexOf(ColumnSpec<?> column);

    static Selection fromBitSet(BitSet bitSet, SchemaSpec schema)
    {
        if (bitSet == MagicConstants.ALL_COLUMNS)
        {
            Map<ColumnSpec<?>, Integer> columns = new HashMap<>();
            for (int i = 0; i < schema.allColumnInSelectOrder.size(); i++)
                columns.put(schema.allColumnInSelectOrder.get(i), i);
            return new Wildcard(columns);
        }
        else
        {
            Invariants.require(schema.allColumnInSelectOrder.size() == bitSet.size());
            Map<ColumnSpec<?>, Integer> columns = new HashMap<>();
            for (int i = 0; i < schema.allColumnInSelectOrder.size(); i++)
            {
                if (bitSet.isSet(i))
                    columns.put(schema.allColumnInSelectOrder.get(i), i);
            }
            // TODO: timestamp
            return new Columns(columns, false);
        }
    }

    class Columns implements Selection
    {
        final Map<ColumnSpec<?>, Integer> columns;
        final boolean includeTimestamp;

        public Columns(Map<ColumnSpec<?>, Integer> columns, boolean includeTimestamp)
        {
            this.columns = columns;
            this.includeTimestamp = includeTimestamp;
        }

        @Override
        public Collection<ColumnSpec<?>> columns()
        {
            return columns.keySet();
        }

        @Override
        public boolean includeTimestamps()
        {
            return includeTimestamp;
        }

        @Override
        public boolean isWildcard()
        {
            return false;
        }

        public boolean selects(ColumnSpec<?> column)
        {
            return columns.containsKey(column);
        }

        public boolean selectsAllOf(List<ColumnSpec<?>> subSelection)
        {
            for (ColumnSpec<?> column : subSelection)
            {
                if (!selects(column))
                    return false;
            }
            return true;
        }

        public int indexOf(ColumnSpec<?> column)
        {
            return columns.get(column);
        }
    }

    class Wildcard extends Columns
    {
        private Wildcard(Map<ColumnSpec<?>, Integer> columns)
        {
            super(columns, false);
        }

        @Override
        public Collection<ColumnSpec<?>> columns()
        {
            return columns.keySet();
        }

        @Override
        public boolean includeTimestamps()
        {
            return false;
        }

        @Override
        public boolean isWildcard()
        {
            return true;
        }
    }
}
