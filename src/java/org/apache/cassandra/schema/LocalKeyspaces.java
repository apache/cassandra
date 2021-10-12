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

package org.apache.cassandra.schema;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

import org.apache.cassandra.db.SystemKeyspace;

/**
 * Keeps references to the local system keyspaces ({@link SchemaConstants#LOCAL_SYSTEM_KEYSPACE_NAMES}).
 */
public class LocalKeyspaces
{
    private final ImmutableMap<String, KeyspaceMetadata> localKeyspaces;
    private final ImmutableMap<TableId, TableMetadata> localTablesAndViews;

    public LocalKeyspaces(boolean loadDefinitions)
    {
        if (loadDefinitions)
        {
            List<KeyspaceMetadata> keyspaces = Lists.newArrayList(SchemaKeyspace.metadata(), SystemKeyspace.metadata());
            localKeyspaces = ImmutableMap.copyOf(keyspaces.stream().collect(Collectors.toMap(ksm -> ksm.name, ksm -> ksm)));
            localTablesAndViews = ImmutableMap.copyOf(keyspaces.stream().flatMap(ksm -> Streams.stream(ksm.tablesAndViews()))
                                                               .collect(Collectors.toMap(tm -> tm.id, tm -> tm)));
        }
        else
        {
            localKeyspaces = ImmutableMap.of();
            localTablesAndViews = ImmutableMap.of();
        }
    }

    public Set<KeyspaceMetadata> getAll()
    {
        return ImmutableSet.copyOf(localKeyspaces.values());
    }

    public Set<String> getAllNames()
    {
        return ImmutableSet.copyOf(localKeyspaces.keySet());
    }

    public KeyspaceMetadata get(String name)
    {
        return localKeyspaces.get(name);
    }

    public Set<TableMetadata> getAllTablesAndViews()
    {
        return ImmutableSet.copyOf(localTablesAndViews.values());
    }

    public TableMetadata getTableOrView(TableId tableId)
    {
        return localTablesAndViews.get(tableId);
    }

    public int getAllTablesAndViewsCount()
    {
        return localTablesAndViews.size();
    }
}
