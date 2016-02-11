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

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.MultiCBuilder;

import org.apache.cassandra.config.ColumnDefinition;

/**
 * Base class for <code>Restriction</code>s
 */
abstract class AbstractRestriction  implements Restriction
{
    @Override
    public  boolean isOnToken()
    {
        return false;
    }

    @Override
    public boolean isMultiColumn()
    {
        return false;
    }

    @Override
    public boolean isSlice()
    {
        return false;
    }

    @Override
    public boolean isEQ()
    {
        return false;
    }

    @Override
    public boolean isLIKE()
    {
        return false;
    }

    @Override
    public boolean isIN()
    {
        return false;
    }

    @Override
    public boolean isContains()
    {
        return false;
    }

    @Override
    public boolean isNotNull()
    {
        return false;
    }

    @Override
    public boolean hasBound(Bound b)
    {
        return true;
    }

    @Override
    public MultiCBuilder appendBoundTo(MultiCBuilder builder, Bound bound, QueryOptions options)
    {
        return appendTo(builder, options);
    }

    @Override
    public boolean isInclusive(Bound b)
    {
        return true;
    }

    /**
     * Reverses the specified bound if the column type is a reversed one.
     *
     * @param columnDefinition the column definition
     * @param bound the bound
     * @return the bound reversed if the column type was a reversed one or the original bound
     */
    protected static Bound reverseBoundIfNeeded(ColumnDefinition columnDefinition, Bound bound)
    {
        return columnDefinition.isReversedType() ? bound.reverse() : bound;
    }
}
