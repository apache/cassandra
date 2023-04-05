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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

public final class VirtualKeyspaceRegistry
{
    public static final VirtualKeyspaceRegistry instance = new VirtualKeyspaceRegistry();

    private final Map<String, VirtualKeyspace> virtualKeyspaces = new ConcurrentHashMap<>();
    private final Map<TableId, VirtualTable> virtualTables = new ConcurrentHashMap<>();

    private VirtualKeyspaceRegistry()
    {
    }

    public void register(VirtualKeyspace keyspace)
    {
        VirtualKeyspace previous = virtualKeyspaces.put(keyspace.name(), keyspace);
        // some tests choose to replace the keyspace, if so make sure to cleanup tables as well
        if (previous != null)
            previous.tables().forEach(t -> virtualTables.remove(t));
        keyspace.tables().forEach(t -> virtualTables.put(t.metadata().id, t));
    }

    @Nullable
    public VirtualKeyspace getKeyspaceNullable(String name)
    {
        return virtualKeyspaces.get(name);
    }

    @Nullable
    public VirtualTable getTableNullable(TableId id)
    {
        return virtualTables.get(id);
    }

    @Nullable
    public KeyspaceMetadata getKeyspaceMetadataNullable(String name)
    {
        VirtualKeyspace keyspace = virtualKeyspaces.get(name);
        return null != keyspace ? keyspace.metadata() : null;
    }

    @Nullable
    public TableMetadata getTableMetadataNullable(TableId id)
    {
        VirtualTable table = virtualTables.get(id);
        return null != table ? table.metadata() : null;
    }

    public Iterable<KeyspaceMetadata> virtualKeyspacesMetadata()
    {
        return Iterables.transform(virtualKeyspaces.values(), VirtualKeyspace::metadata);
    }
}
