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
package org.apache.cassandra.index;

import java.util.Collection;

import org.apache.cassandra.schema.IndexMetadata;

/**
 * The collection of all Index instances for a base table.
 * The SecondaryIndexManager for a ColumnFamilyStore contains an IndexRegistry
 * (actually it implements this interface at present) and Index implementations
 * register in order to:
 * i) subscribe to the stream of updates being applied to partitions in the base table
 * ii) provide searchers to support queries with the relevant search predicates
 */
public interface IndexRegistry
{
    void registerIndex(Index index);
    void unregisterIndex(Index index);

    Index getIndex(IndexMetadata indexMetadata);
    Collection<Index> listIndexes();
}
