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

package org.apache.cassandra.db.virtual;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.schema.SchemaConstants;

/**
 * Cluster-wide counterparts of {@link SystemViewsKeyspace} tables, so a node-local view can be read for every node
 * from a single coordinator. Each table here carries the same columns as the one it mirrors, prefixed with a
 * {@code node_id} partition key; {@link RemoteToLocalVirtualTable} turns a read of one {@code node_id} into a read
 * of the local table on that node.
 * <p>
 * Only tables listed in {@link #REMOTED_TABLES} are mirrored. The wrapper cannot model every shape - a table needs
 * a single partition key column and no complex or static columns - so tables opt in rather than out.
 */
public class SystemViewsRemoteKeyspace extends RemoteToLocalVirtualKeyspace
{
    private static final ImmutableSet<String> REMOTED_TABLES =
        ImmutableSet.of(CompressionDictionaryAutoTrainingTable.TABLE_NAME);

    public static final SystemViewsRemoteKeyspace instance =
        new SystemViewsRemoteKeyspace(SchemaConstants.VIRTUAL_VIEWS_REMOTE, SystemViewsKeyspace.instance);

    public SystemViewsRemoteKeyspace(String name, VirtualKeyspace wrap)
    {
        super(name, wrap, vt -> REMOTED_TABLES.contains(vt.name()));
    }
}
