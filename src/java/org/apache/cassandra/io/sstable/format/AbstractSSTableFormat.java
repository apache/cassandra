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

package org.apache.cassandra.io.sstable.format;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;

public abstract class AbstractSSTableFormat<R extends SSTableReader, W extends SSTableWriter> implements SSTableFormat<R, W>
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public final String name;
    protected final Map<String, String> options;

    protected AbstractSSTableFormat(String name, Map<String, String> options)
    {
        this.name = Objects.requireNonNull(name);
        this.options = options;
    }

    @Override
    public final String name()
    {
        return name;
    }

    @Override
    public final void deleteOrphanedComponents(Descriptor descriptor, Set<Component> components)
    {
        File dataFile = descriptor.fileFor(Components.DATA);
        if (components.contains(Components.DATA) && dataFile.length() > 0)
            // everything appears to be in order... moving on.
            return;

        // missing the DATA file! all components are orphaned
        logger.warn("[{}] Removing orphans for {}: {}", getClass().getSimpleName(), descriptor, components);
        for (Component component : components)
        {
            File file = descriptor.fileFor(component);
            if (file.exists())
                descriptor.fileFor(component).delete();
        }
    }

    protected final void deleteComponentsOldestFirst(Descriptor desc, List<Component> components)
    {
        logger.info("[{}] Deleting sstable: {}", getClass().getSimpleName(), desc);

        // delete older files first so the overall SSTable timestamp stays the same on partial deletes
        Map<Component, Long> lastModified = new HashMap<>();
        for (Component c : components)
            lastModified.put(c, desc.fileFor(c).lastModified());
        components.sort(Comparator.comparingLong(lastModified::get));

        for (Component component : components)
        {
            logger.trace("[{}] Deleting component {} of {}", getClass().getSimpleName(), component, desc);
            desc.fileFor(component).deleteIfExists();
        }
    }

    @Override
    public final boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractSSTableFormat<?, ?> that = (AbstractSSTableFormat<?, ?>) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(name);
    }

    @Override
    public String toString()
    {
        return name + ":" + options;
    }
}
