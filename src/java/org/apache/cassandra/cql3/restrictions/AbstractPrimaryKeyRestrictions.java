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
import java.util.*;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Base class for <code>PrimaryKeyRestrictions</code>.
 */
abstract class AbstractPrimaryKeyRestrictions extends AbstractRestriction implements PrimaryKeyRestrictions
{
    /**
     * The composite type.
     */
    protected final ClusteringComparator comparator;

    public AbstractPrimaryKeyRestrictions(ClusteringComparator comparator)
    {
        this.comparator = comparator;
    }

    @Override
    public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException
    {
        return values(options);
    }

    @Override
    public final boolean isEmpty()
    {
        return getColumnDefs().isEmpty();
    }

    @Override
    public final int size()
    {
        return getColumnDefs().size();
    }
}
