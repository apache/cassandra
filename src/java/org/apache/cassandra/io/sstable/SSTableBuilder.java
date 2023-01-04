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

package org.apache.cassandra.io.sstable;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.io.sstable.format.IOOptions;
import org.apache.cassandra.schema.TableMetadataRef;

import static com.google.common.base.Preconditions.checkNotNull;

public class SSTableBuilder<S extends SSTable, B extends SSTableBuilder<S, B>>
{
    public final Descriptor descriptor;

    private Set<Component> components;
    private TableMetadataRef tableMetadataRef;
    private ChunkCache chunkCache = ChunkCache.instance;
    private IOOptions ioOptions = IOOptions.fromDatabaseDescriptor();

    public SSTableBuilder(Descriptor descriptor)
    {
        checkNotNull(descriptor);
        this.descriptor = descriptor;
    }

    public B setComponents(Collection<Component> components)
    {
        if (components != null)
            this.components = ImmutableSet.copyOf(components);
        else
            this.components = null;
        return (B) this;
    }

    public B setTableMetadataRef(TableMetadataRef ref)
    {
        this.tableMetadataRef = ref;
        return (B) this;
    }

    public B setChunkCache(ChunkCache chunkCache)
    {
        this.chunkCache = chunkCache;
        return (B) this;
    }

    public B setIOOptions(IOOptions ioOptions)
    {
        this.ioOptions = ioOptions;
        return (B) this;
    }

    public Descriptor getDescriptor()
    {
        return descriptor;
    }

    public Set<Component> getComponents()
    {
        return components;
    }

    public TableMetadataRef getTableMetadataRef()
    {
        return tableMetadataRef;
    }

    public ChunkCache getChunkCache()
    {
        return chunkCache;
    }

    public IOOptions getIOOptions()
    {
        return ioOptions;
    }
}