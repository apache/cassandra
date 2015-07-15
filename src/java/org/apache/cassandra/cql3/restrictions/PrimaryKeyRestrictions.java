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
import java.util.NavigableSet;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A set of restrictions on a primary key part (partition key or clustering key).
 *
 */
interface PrimaryKeyRestrictions extends Restriction, Restrictions
{
    @Override
    public PrimaryKeyRestrictions mergeWith(Restriction restriction) throws InvalidRequestException;

    public List<ByteBuffer> values(QueryOptions options) throws InvalidRequestException;

    public NavigableSet<Clustering> valuesAsClustering(QueryOptions options) throws InvalidRequestException;

    public List<ByteBuffer> bounds(Bound b, QueryOptions options) throws InvalidRequestException;

    public NavigableSet<Slice.Bound> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException;
}
