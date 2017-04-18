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

import java.util.Set;

import org.apache.cassandra.config.ColumnDefinition;

/**
 * Sets of restrictions
 */
public interface Restrictions extends Restriction
{
    /**
     * Returns the restrictions applied to the specified column.
     *
     * @param columnDef the column definition
     * @return the restrictions applied to the specified column
     */
    Set<Restriction> getRestrictions(ColumnDefinition columnDef);

    /**
     * Checks if this <code>Restrictions</code> is empty or not.
     *
     * @return <code>true</code> if this <code>Restrictions</code> is empty, <code>false</code> otherwise.
     */
    boolean isEmpty();

    /**
     * Returns the number of columns that have a restriction.
     *
     * @return the number of columns that have a restriction.
     */
    public int size();

    /**
     * Checks if any of the underlying restriction is an IN.
     * @return <code>true</code> if any of the underlying restriction is an IN, <code>false</code> otherwise
     */
    public boolean hasIN();

    /**
     * Checks if any of the underlying restrictions is a CONTAINS / CONTAINS KEY restriction.
     * @return <code>true</code> if any of the underlying restrictions is CONTAINS, <code>false</code> otherwise
     */
    public boolean hasContains();
    /**
     * Checks if any of the underlying restrictions is a slice.
     * @return <code>true</code> if any of the underlying restrictions is a slice, <code>false</code> otherwise
     */
    public boolean hasSlice();

    /**
     * Checks if all of the underlying restrictions are EQ or IN restrictions.
     *
     * @return <code>true</code> if all of the underlying restrictions are EQ or IN restrictions,
     * <code>false</code> otherwise
     */
    public boolean hasOnlyEqualityRestrictions();
}
