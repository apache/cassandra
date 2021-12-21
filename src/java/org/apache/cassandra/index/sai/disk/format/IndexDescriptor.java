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

package org.apache.cassandra.index.sai.disk.format;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * The {@link IndexDescriptor} is an analog of the SSTable {@link Descriptor} and provides version
 * specific information about the on-disk state of a {@link StorageAttachedIndex}.
 *
 * The {@IndexDescriptor} is primarily responsible for maintaining a view of the on-disk state
 * of an index for a specific {@link org.apache.cassandra.io.sstable.SSTable}. It maintains mappings
 * of the current on-disk components and files. It is responsible for opening files for use by
 * writers and readers.
 *
 * It's remaining responsibility is to act as a proxy to the {@link OnDiskFormat} associated with the
 * index {@link Version}.
 */
public class IndexDescriptor
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String TMP_EXTENSION = ".tmp";

    public final Version version;
    public final Descriptor descriptor;
    public final IPartitioner partitioner;
    public final ClusteringComparator clusteringComparator;
    public final PrimaryKey.Factory primaryKeyFactory;
    public final Set<IndexComponent> perSSTableComponents = Sets.newHashSet();
    public final Map<String, Set<IndexComponent>> perIndexComponents = Maps.newHashMap();
    public final Map<IndexComponent, File> onDiskPerSSTableFileMap = Maps.newHashMap();
    public final Map<IndexComponent, File> onDiskPerSSTableTemporaryFileMap = Maps.newHashMap();
    public final Map<Pair<IndexComponent, String>, File> onDiskPerIndexFileMap = Maps.newHashMap();
    public final Map<Pair<IndexComponent, String>, File> onDiskPerIndexTemporaryFileMap = Maps.newHashMap();

    private IndexDescriptor(Version version, Descriptor descriptor, IPartitioner partitioner, ClusteringComparator clusteringComparator)
    {
        this.version = version;
        this.descriptor = descriptor;
        this.partitioner = partitioner;
        this.clusteringComparator = clusteringComparator;
        this.primaryKeyFactory = PrimaryKey.factory(clusteringComparator, version.onDiskFormat().indexFeatureSet());
    }

    public static IndexDescriptor create(Descriptor descriptor, IPartitioner partitioner, ClusteringComparator clusteringComparator)
    {
        return new IndexDescriptor(Version.LATEST, descriptor, partitioner, clusteringComparator);
    }

    public static IndexDescriptor create(SSTableReader sstable)
    {
        for (Version version : Version.ALL)
        {
            IndexDescriptor indexDescriptor = new IndexDescriptor(version,
                                                                  sstable.descriptor,
                                                                  sstable.metadata().partitioner,
                                                                  sstable.metadata().comparator);

            if (version.onDiskFormat().isPerSSTableBuildComplete(indexDescriptor))
            {
                indexDescriptor.registerPerSSTableComponents();
                return indexDescriptor;
            }
        }
        return new IndexDescriptor(Version.LATEST,
                                   sstable.descriptor,
                                   sstable.metadata().partitioner,
                                   sstable.metadata().comparator);
    }

    public boolean hasComponent(IndexComponent indexComponent)
    {
        registerPerSSTableComponents();
        return perSSTableComponents.contains(indexComponent);
    }

    public boolean hasComponent(IndexComponent indexComponent, IndexContext indexContext)
    {
        registerPerIndexComponents(indexContext);
        return perIndexComponents.containsKey(indexContext.getIndexName()) &&
               perIndexComponents.get(indexContext.getIndexName()).contains(indexComponent);
    }

    public String componentName(IndexComponent indexComponent)
    {
        return version.fileNameFormatter().format(indexComponent, null);
    }

    public String componentName(IndexComponent indexComponent, IndexContext indexContext)
    {
        return version.fileNameFormatter().format(indexComponent, indexContext);
    }

    public File fileFor(IndexComponent component)
    {
        return onDiskPerSSTableFileMap.computeIfAbsent(component, c -> createFile(c, null, false));
    }

    public File fileFor(IndexComponent component, IndexContext indexContext)
    {
        return onDiskPerIndexFileMap.computeIfAbsent(Pair.create(component, indexContext.getIndexName()),
                                                     p -> createFile(component, indexContext, false));
    }

    public Set<Component> getLivePerSSTableComponents()
    {
        registerPerSSTableComponents();
        return perSSTableComponents.stream()
                                   .map(c -> new Component(Component.Type.CUSTOM, componentName(c)))
                                   .collect(Collectors.toSet());
    }

    public Set<Component> getLivePerIndexComponents(IndexContext indexContext)
    {
        registerPerIndexComponents(indexContext);
        return perIndexComponents.containsKey(indexContext.getIndexName())
               ? perIndexComponents.get(indexContext.getIndexName())
                                   .stream()
                                   .map(c -> new Component(Component.Type.CUSTOM, componentName(c, indexContext)))
                                                                         .collect(Collectors.toSet())
                                                     : Collections.emptySet();
    }

    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(SSTableReader sstable) throws IOException
    {
        return version.onDiskFormat().newPrimaryKeyMapFactory(this, sstable);
    }

    public SearchableIndex newSearchableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        return version.onDiskFormat().newSearchableIndex(sstableContext, indexContext);
    }

    public PerSSTableWriter newPerSSTableWriter() throws IOException
    {
        return version.onDiskFormat().newPerSSTableWriter(this);
    }

    public PerIndexWriter newPerIndexWriter(StorageAttachedIndex index,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping)
    {
        return version.onDiskFormat().newPerIndexWriter(index, this, tracker, rowMapping);
    }

    public boolean isPerSSTableBuildComplete()
    {
        return version.onDiskFormat().isPerSSTableBuildComplete(this);
    }

    public boolean isPerIndexBuildComplete(IndexContext indexContext)
    {
        return version.onDiskFormat().isPerIndexBuildComplete(this, indexContext);
    }

    public boolean isSSTableEmpty()
    {
        return isPerSSTableBuildComplete() && numberOfComponents() == 1;
    }

    public boolean isIndexEmpty(IndexContext indexContext)
    {
        return isPerIndexBuildComplete(indexContext) && numberOfComponents(indexContext) == 1;
    }

    public long sizeOnDiskOfPerIndexComponents(IndexContext indexContext)
    {
        registerPerIndexComponents(indexContext);
        if (perIndexComponents.containsKey(indexContext.getIndexName()))
            return perIndexComponents.get(indexContext.getIndexName())
                                     .stream()
                                     .map(c -> Pair.create(c, indexContext.getIndexName()))
                                     .map(onDiskPerIndexFileMap::get)
                                     .filter(java.util.Objects::nonNull)
                                     .filter(File::exists)
                                     .mapToLong(File::length)
                                     .sum();
        return 0;
    }

    @VisibleForTesting
    public long sizeOnDiskOfPerIndexComponent(IndexComponent indexComponent, IndexContext indexContext)
    {
        if (perIndexComponents.containsKey(indexContext.getIndexName()))
            return perIndexComponents.get(indexContext.getIndexName())
                                     .stream()
                                     .filter(c -> c == indexComponent)
                                     .map(c -> Pair.create(c, indexContext.getIndexName()))
                                     .map(onDiskPerIndexFileMap::get)
                                     .filter(java.util.Objects::nonNull)
                                     .filter(File::exists)
                                     .mapToLong(File::length)
                                     .sum();
        return 0;
    }

    public boolean validatePerIndexComponents(IndexContext indexContext)
    {
        logger.info("validatePerIndexComponents called for " + indexContext.getIndexName());
        registerPerIndexComponents(indexContext);
        return version.onDiskFormat().validatePerIndexComponents(this, indexContext, false);
    }

    public boolean validatePerIndexComponentsChecksum(IndexContext indexContext)
    {
        registerPerIndexComponents(indexContext);
        return version.onDiskFormat().validatePerIndexComponents(this, indexContext, true);
    }

    public boolean validatePerSSTableComponents()
    {
        registerPerSSTableComponents();
        return version.onDiskFormat().validatePerSSTableComponents(this, false);
    }

    public boolean validatePerSSTableComponentsChecksum()
    {
        registerPerSSTableComponents();
        return version.onDiskFormat().validatePerSSTableComponents(this, true);
    }

    public void deletePerSSTableIndexComponents()
    {
        registerPerSSTableComponents();
        perSSTableComponents.stream()
                            .map(onDiskPerSSTableFileMap::remove)
                            .filter(java.util.Objects::nonNull)
                            .forEach(this::deleteComponent);
        perSSTableComponents.clear();
    }

    public void deleteColumnIndex(IndexContext indexContext)
    {
        registerPerIndexComponents(indexContext);
        if (perIndexComponents.containsKey(indexContext.getIndexName()))
            perIndexComponents.remove(indexContext.getIndexName())
                              .stream()
                              .map(c -> Pair.create(c, indexContext.getIndexName()))
                              .map(onDiskPerIndexFileMap::remove)
                              .filter(java.util.Objects::nonNull)
                              .forEach(this::deleteComponent);
    }

    public void deletePerSSTableTemporaryComponents()
    {
        version.onDiskFormat()
               .perSSTableComponents()
               .stream()
               .map(this::tmpFileFor)
               .filter(File::exists)
               .forEach(this::deleteComponent);
    }

    public void deletePerIndexTemporaryComponents(IndexContext indexContext)
    {
        version.onDiskFormat()
               .perIndexComponents(indexContext)
               .stream()
               .map(c -> tmpFileFor(c, indexContext))
               .filter(File::exists)
               .forEach(this::deleteComponent);
    }

    public void createComponentOnDisk(IndexComponent component) throws IOException
    {
        Files.touch(fileFor(component).toJavaIOFile());
        registerPerSSTableComponent(component);
    }

    public void createComponentOnDisk(IndexComponent component, IndexContext indexContext) throws IOException
    {
        Files.touch(fileFor(component, indexContext).toJavaIOFile());
        registerPerIndexComponent(component, indexContext.getIndexName());
    }

    public IndexInput openPerSSTableInput(IndexComponent indexComponent)
    {
        final File file = fileFor(indexComponent);
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Opening blocking index input for file {} ({})"),
                         file,
                         FBUtilities.prettyPrintMemory(file.length()));

        return IndexFileUtils.instance.openBlockingInput(file);
    }

    public IndexInput openPerIndexInput(IndexComponent indexComponent, IndexContext indexContext)
    {
        return openPerIndexInput(indexComponent, indexContext, false);
    }

    public IndexInput openPerIndexInput(IndexComponent indexComponent, IndexContext indexContext, boolean temporary)
    {
        final File file = temporary ? tmpFileFor(indexComponent, indexContext) : fileFor(indexComponent, indexContext);
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Opening {} blocking index input for file {} ({})"),
                         temporary ? "temporary" : "",
                         file,
                         FBUtilities.prettyPrintMemory(file.length()));

        return IndexFileUtils.instance.openBlockingInput(file);
    }

    public IndexOutputWriter openPerSSTableOutput(IndexComponent component) throws IOException
    {
        return openPerSSTableOutput(component, false, false);
    }

    public IndexOutputWriter openPerSSTableOutput(IndexComponent component, boolean append, boolean temporary) throws IOException
    {
        final File file = temporary ? tmpFileFor(component) : fileFor(component);

        if (logger.isTraceEnabled())
            logger.trace(logMessage("Creating SSTable attached index output for component {} on file {}..."),
                         component,
                         file);

        IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file);

        if (append)
        {
            writer.skipBytes(file.length());
        }

        registerPerSSTableComponent(component);

        return writer;
    }

    public IndexOutputWriter openPerIndexOutput(IndexComponent indexComponent, IndexContext indexContext) throws IOException
    {
        return openPerIndexOutput(indexComponent, indexContext, false, false);
    }

    public IndexOutputWriter openPerIndexOutput(IndexComponent component, IndexContext indexContext, boolean append, boolean temporary) throws IOException
    {
        final File file = temporary ? tmpFileFor(component, indexContext) : fileFor(component, indexContext);

        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Creating {} sstable attached index output for component {} on file {}..."),
                         temporary ? "temporary" : "",
                         component,
                         file);

        IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file);

        if (append)
        {
            writer.skipBytes(file.length());
        }

        if (!temporary)
            registerPerSSTableComponent(component);

        return writer;
    }

    public FileHandle createPerSSTableFileHandle(IndexComponent indexComponent)
    {
        return createPerSSTableFileHandle(indexComponent, false);
    }

    public FileHandle createPerSSTableFileHandle(IndexComponent indexComponent, boolean temporary)
    {
        final File file = temporary ? tmpFileFor(indexComponent) : fileFor(indexComponent);

        if (logger.isTraceEnabled())
        {
            logger.trace(logMessage("Opening {} file handle for {} ({})"),
                         file, FBUtilities.prettyPrintMemory(file.length()));
        }

        try (final FileHandle.Builder builder = new FileHandle.Builder(file).mmapped(true))
        {
            return builder.complete();
        }
    }

    public FileHandle createPerIndexFileHandle(IndexComponent indexComponent, IndexContext indexContext)
    {
        return createPerIndexFileHandle(indexComponent, indexContext, false);
    }

    public FileHandle createPerIndexFileHandle(IndexComponent indexComponent, IndexContext indexContext, boolean temporary)
    {
        final File file = temporary ? tmpFileFor(indexComponent, indexContext)
                                    : fileFor(indexComponent, indexContext);

        if (logger.isTraceEnabled())
        {
            logger.trace(indexContext.logMessage("Opening {} file handle for {} ({})"),
                         temporary ? "temporary" : "", file, FBUtilities.prettyPrintMemory(file.length()));
        }

        try (final FileHandle.Builder builder = new FileHandle.Builder(file).mmapped(true))
        {
            return builder.complete();
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(descriptor, version);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescriptor other = (IndexDescriptor)o;
        return Objects.equal(descriptor, other.descriptor) &&
               Objects.equal(version, other.version);
    }

    @Override
    public String toString()
    {
        return descriptor.toString() + "-SAI";
    }

    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.*] %s",
                             descriptor.ksname,
                             descriptor.cfname,
                             message);
    }

    private void registerPerSSTableComponents()
    {
        version.onDiskFormat()
               .perSSTableComponents()
               .stream()
               .filter(c -> !perSSTableComponents.contains(c) && fileFor(c).exists())
               .forEach(perSSTableComponents::add);
    }

    private void registerPerIndexComponents(IndexContext indexContext)
    {
        Set<IndexComponent> indexComponents = perIndexComponents.computeIfAbsent(indexContext.getIndexName(), k -> Sets.newHashSet());
        version.onDiskFormat()
               .perIndexComponents(indexContext)
               .stream()
               .filter(c -> !indexComponents.contains(c) && fileFor(c, indexContext).exists())
               .forEach(indexComponents::add);
    }

    private int numberOfComponents(IndexContext indexContext)
    {
        return perIndexComponents.containsKey(indexContext.getIndexName()) ? perIndexComponents.get(indexContext.getIndexName()).size() : 0;
    }

    private int numberOfComponents()
    {
        return perSSTableComponents.size();
    }

    private File tmpFileFor(IndexComponent component)
    {
        return onDiskPerSSTableTemporaryFileMap.computeIfAbsent(component,
                                                                c -> createFile(component, null, true));
    }

    private File tmpFileFor(IndexComponent component, IndexContext indexContext)
    {
        return onDiskPerIndexTemporaryFileMap.computeIfAbsent(Pair.create(component, indexContext.getIndexName()),
                                                      c -> createFile(component, indexContext, true));
    }

    private File createFile(IndexComponent component, IndexContext indexContext, boolean temporary)
    {
        Component customComponent = new Component(Component.Type.CUSTOM, componentName(component, indexContext));
        return temporary ? descriptor.tmpFileFor(customComponent) : descriptor.fileFor(customComponent);
    }

    private void deleteComponent(File file)
    {
        logger.debug("Deleting storage attached index component file {}", file);
        try
        {
            IOUtils.deleteFilesIfExist(file.toPath());
        }
        catch (IOException e)
        {
            logger.warn("Unable to delete storage attached index component file {} due to {}.", file, e.getMessage(), e);
        }
    }

    private void registerPerSSTableComponent(IndexComponent indexComponent)
    {
        perSSTableComponents.add(indexComponent);
    }

    private void registerPerIndexComponent(IndexComponent indexComponent, String index)
    {
        perIndexComponents.computeIfAbsent(index, k -> Sets.newHashSet()).add(indexComponent);
    }
}
