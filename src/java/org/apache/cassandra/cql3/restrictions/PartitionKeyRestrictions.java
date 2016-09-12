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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;

/**
 * A set of restrictions on the partition key.
 *
 */
interface PartitionKeyRestrictions extends Restrictions
{
    public PartitionKeyRestrictions mergeWith(Restriction restriction);

    public List<ByteBuffer> values(QueryOptions options);

    public List<ByteBuffer> bounds(Bound b, QueryOptions options);

    /**
     * Checks if the specified bound is set or not.
     * @param b the bound type
     * @return <code>true</code> if the specified bound is set, <code>false</code> otherwise
     */
    public boolean hasBound(Bound b);

    /**
     * Checks if the specified bound is inclusive or not.
     * @param b the bound type
     * @return <code>true</code> if the specified bound is inclusive, <code>false</code> otherwise
     */
    public boolean isInclusive(Bound b);

    /**
     * checks if specified restrictions require filtering
     *
     * @param cfm column family metadata
     * @return <code>true</code> if filtering is required, <code>false</code> otherwise
     */
    public boolean needFiltering(CFMetaData cfm);

    /**
     * Checks if the partition key has unrestricted components.
     *
     * @param cfm column family metadata
     * @return <code>true</code> if the partition key has unrestricted components, <code>false</code> otherwise.
     */
    public boolean hasUnrestrictedPartitionKeyComponents(CFMetaData cfm);
}
