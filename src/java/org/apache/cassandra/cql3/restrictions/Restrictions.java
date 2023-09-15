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

import org.apache.cassandra.schema.ColumnMetadata;

/**
 * Sets of restrictions
 */
public interface Restrictions extends Restriction
{
    /**
     * Checks if the specified column is restricted by an equals at the column level.
     * @return {@code true} if the column is restricted by an equals at the column level, {@code false} otherwise.
     */
    boolean isRestrictedByEquals(ColumnMetadata column);

    /**
     * Checks if the specified column is restricted by an equals or an IN at the column level.
     * @return {@code true} if the column is restricted by an equals or an IN at the column level, {@code false} otherwise.
     */
    boolean isRestrictedByEqualsOrIN(ColumnMetadata column);

    /**
     * Checks if this <code>Restrictions</code> is empty or not.
     *
     * @return <code>true</code> if this <code>Restrictions</code> is empty, <code>false</code> otherwise.
     */
    default boolean isEmpty()
    {
        return size() == 0;
    }

    /**
     * Returns the number of columns that have a restriction.
     *
     * @return the number of columns that have a restriction.
     */
    default int size()
    {
        return columns().size();
    }

    /**
     * Checks if any of the underlying restriction use an {@code IN} operator.
     * @return {@code true} if any of the underlying restriction is an IN, {@code false} otherwise
     */
    default boolean hasIN()
    {
        return false;
    }

    /**
     * Checks if any of the underlying restrictions is a slice.
     * @return {@code true} if any of the underlying restrictions is a slice, {@code false} otherwise
     */
    default boolean hasSlice()
    {
        return false;
    }
}
