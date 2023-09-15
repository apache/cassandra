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

package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.marshal.AbstractType;

/**
 * A list of values. Each value is stored with its type allowing the list to be compared with another {@code ValueList}
 * having the same elements types.
 */
public final class ValueList extends ForwardingList<ByteBuffer> implements Comparable<ValueList>
{
    /**
     * The empty {@code ValueList} instance used to avoid creating unecessary empty instances.
     */
    private static final ValueList EMPTY = new ValueList(ImmutableList.of(), ImmutableList.of());

    /**
     * The value types.
     */
    private final List<AbstractType<?>> types;

    /**
     * The values.
     */
    private final List<ByteBuffer> values;

    private ValueList(ImmutableList<AbstractType<?>> types, ImmutableList<ByteBuffer> values)
    {
        this.types = types;
        this.values = values;
    }

    @Override
    protected List<ByteBuffer> delegate()
    {
        return values;
    }

    /**
     * Returns an empty {@code ValueList}.
     * @return an empty {@code ValueList}.
     */
    public static ValueList of()
    {
        return EMPTY;
    }

    /**
     * Returns a {@code ValueList} with a single value.
     * @param type the value type
     * @param value the value
     * @return a {@code ValueList} with a single value.
     */
    public static ValueList of(AbstractType<?> type, ByteBuffer value)
    {
        return new ValueList(ImmutableList.of(type), ImmutableList.of(value));
    }

    /**
     * Returns a {@code ValueList} with the specified values.
     * @param types the value types
     * @param values the values
     * @return a {@code ValueList} with the specified values.
     */
    public static ValueList of(List<AbstractType<?>> types, List<ByteBuffer> values)
    {
        return new ValueList(ImmutableList.copyOf(types), ImmutableList.copyOf(values));
    }

    @Override
    public int compareTo(ValueList other)
    {
        if (other == null)
            return 1;

        int minSize = Math.min(this.size(), other.size());

        if (!this.types.subList(0, minSize).equals(other.types.subList(0, minSize)))
            throw new IllegalStateException("Cannot compare 2 lists containing different types");

        List<AbstractType<?>> types = this.size()  == minSize ? other.types : this.types;

        for (int i = 0, m = size(); i < m; i++)
        {
            if (i >= other.size())
                return 1;

            int comparison = this.types.get(i).compare(this.values.get(i), other.values.get(i));

            if (comparison != 0)
                return comparison;
        }
        return other.size() > this.size() ? -1 : 0;
    }
}
