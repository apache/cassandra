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
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.oldlucene.EndiannessReverserChecksumIndexInput;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * The `IndexDescriptor` is an analog of the SSTable {@link Descriptor} and provides version
 * specific information about the on-disk state of {@link StorageAttachedIndex}es.
 * <p>
 * The `IndexDescriptor` is primarily responsible for maintaining a view of the on-disk state
 * of the SAI indexes for a specific {@link org.apache.cassandra.io.sstable.SSTable}. It maintains mappings
 * of the current on-disk components and files. It is responsible for opening files for use by
 * writers and readers.
 * <p>
 * Each sstable has per-index components ({@link IndexComponentType}) associated with it, and also components
 * that are shared by all indexes (notably, the components that make up the PrimaryKeyMap).
 * <p>
 * IndexDescriptor's remaining responsibility is to act as a proxy to the {@link OnDiskFormat}
 * associated with the index {@link Version}.
 */
public class IndexDescriptor
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    /*
    TODO: New indexes can be added at any time to existing data. Since CNDB-8756 we keep all new column indexes in the
     same version as the existing per-sstable index components, making sstable upgrade the only way to move to a newer
     version. However, prior to that fix the Version of a column index may not match the Version of the base sstable.
     OnDiskFormat + IndexFeatureSet + IndexDescriptor was not designed with this in mind, leading to some awkwardness,
     notably in IFS where some features are per-sstable (`isRowAware`) and some are per-column (`hasTermsHistogram`).
    */

    public final Descriptor descriptor;
    public final boolean hasClustering;
    private final ComponentsBuildId emptyGroupMarker;
    private final ClusteringComparator comparator;

    // The per-sstable components for this descriptor. This is never `null` in practice, but 1) it's a bit easier to
    // initialize it outsides of the ctor, and 2) it can actually change upon calls to `reload`.
    private IndexComponentsImpl perSSTable;
    private final Map<IndexContext, IndexComponentsImpl> perIndexes = new ConcurrentHashMap<>();

    private IndexDescriptor(Descriptor descriptor, ClusteringComparator comparator)
    {
        this.descriptor = descriptor;
        this.emptyGroupMarker = ComponentsBuildId.of(Version.current(descriptor.ksname), -1);
        this.comparator = comparator;
        this.hasClustering = comparator.size() > 0;
    }

    @VisibleForTesting
    public static IndexDescriptor empty(Descriptor descriptor)
    {
        return empty(descriptor, new ClusteringComparator());
    }

    public static IndexDescriptor empty(Descriptor descriptor, ClusteringComparator comparator)
    {
        IndexDescriptor created = new IndexDescriptor(descriptor, comparator);
        // Some code assumes that you can always at least call `perSSTableComponents()` and not get `null`, so we
        // set it to an empty group here.
        created.perSSTable = created.createEmptyGroup(null);
        return created;
    }

    public static IndexDescriptor load(SSTableReader sstable, Set<IndexContext> indices)
    {
        SSTableIndexComponentsState discovered = IndexComponentDiscovery.instance().discoverComponents(sstable);
        IndexDescriptor descriptor = new IndexDescriptor(sstable.descriptor,
                                                         sstable.metadata().comparator);
        descriptor.initialize(indices, discovered);
        return descriptor;
    }

    private void initialize(Set<IndexContext> indices, SSTableIndexComponentsState discovered)
    {
        this.perSSTable = initializeGroup(null, discovered.perSSTableBuild());
        initializeIndexes(indices, discovered);
    }

    private void initializeIndexes(Set<IndexContext> indices, SSTableIndexComponentsState discovered)
    {
        for (var context : indices)
            perIndexes.put(context, initializeGroup(context, discovered.perIndexBuild(context.getIndexName())));
    }

    private Set<IndexComponentType> expectedComponentsForVersion(Version version, @Nullable IndexContext context)
    {
        return context == null
               ? version.onDiskFormat().perSSTableComponentTypes(hasClustering)
               : version.onDiskFormat().perIndexComponentTypes(context);
    }

    private IndexComponentsImpl initializeGroup(@Nullable IndexContext context, @Nullable ComponentsBuildId buildId)
    {
        IndexComponentsImpl components;
        if (buildId == null)
        {
            // Means there isn't a complete build for this context. We add some empty "group" as a marker.
            components = createEmptyGroup(context);
        }
        else
        {
            components = new IndexComponentsImpl(context, buildId);
            var expectedTypes = expectedComponentsForVersion(buildId.version(), context);
            // Note that the "expected types" are actually a superset of the components we may have. In particular,
            // when a particular index has no data for a particular sstable, the relevant components only have the
            // metadata and completion marker components. So we check what exists.
            expectedTypes.forEach(components::addIfExists);
            components.sealed = true;

            // We'll still track the group if it is incomplete because that's what discovery gave us, and all code know
            // how to handle those, but this will mean some index won't be queriable because of this. Also, we'll only
            // have incomplete groups if either 1) a build failed mid-way, 2) we detected some corrupted components and
            // deleting the completion marker, or 3) we've lost the file, and all of those should be rare, so having
            // a warning here feels appropriate.
            if (!components.isComplete())
            {
                logger.warn("Discovered group of {} for SSTable {} has no completion marker and cannot be used. This will lead to some indexes not being queriable",
                             context == null ? "per-SSTable SAI components" : "per-index SAI components of " + context.getIndexName(), descriptor);
            }
        }
        return components;
    }

    private IndexComponentsImpl createEmptyGroup(@Nullable IndexContext context)
    {
        return new IndexComponentsImpl(context, emptyGroupMarker);
    }

    /**
     * The set of components _expected_ to be written for a newly flushed sstable given the provided set of indices.
     * This includes both per-sstable and per-index components.
     * <p>
     * Please note that the final sstable may not contain all of these components, as some may be empty or not written
     * due to the specific of the flush, but this should be a superset of the components written.
     */
    public static Set<Component> componentsForNewlyFlushedSSTable(Collection<StorageAttachedIndex> indices, Version version, boolean hasClustering)
    {
        ComponentsBuildId buildId = ComponentsBuildId.forNewSSTable(version);
        Set<Component> components = new HashSet<>();
        for (IndexComponentType component : buildId.version().onDiskFormat().perSSTableComponentTypes(hasClustering))
            components.add(customComponentFor(buildId, component, null));

        for (StorageAttachedIndex index : indices)
            addPerIndexComponentsForNewlyFlushedSSTable(components, buildId, index.getIndexContext());
        return components;
    }

    /**
     * The set of per-index components _expected_ to be written for a newly flushed sstable for the provided index.
     * <p>
     * This is a subset of {@link #componentsForNewlyFlushedSSTable(Collection, Version, boolean)} and has the same caveats.
     */
    public static Set<Component> perIndexComponentsForNewlyFlushedSSTable(IndexContext context)
    {
        return addPerIndexComponentsForNewlyFlushedSSTable(new HashSet<>(), ComponentsBuildId.forNewSSTable(context.version()), context);
    }

    private static Set<Component> addPerIndexComponentsForNewlyFlushedSSTable(Set<Component> addTo, ComponentsBuildId buildId, IndexContext context)
    {
        for (IndexComponentType component : buildId.version().onDiskFormat().perIndexComponentTypes(context))
            addTo.add(customComponentFor(buildId, component, context));
        return addTo;
    }

    private static Component customComponentFor(ComponentsBuildId buildId, IndexComponentType componentType, @Nullable IndexContext context)
    {
        return new Component(Component.Type.CUSTOM, buildId.formatAsComponent(componentType, context));
    }

    /**
     * Given the indexes for the sstable this is a descriptor for, reload from disk to check if newer components are
     * available.
     * <p>
     * This method is generally <b>not</b> safe to call concurrently with the other methods that modify the state
     * of {@link IndexDescriptor}, which are {@link #newPerSSTableComponentsForWrite()} and
     * {@link #newPerIndexComponentsForWrite(IndexContext)}. This method is in fact meant for tiered storage use-cases
     * where (post-flush) index building is done on separate dedicated services, and this method allows to reload the
     * result of such external services once it is made available locally.
     *
     * @param sstable the sstable reader to reload index components
     * @param indices The set of indices to should part of the reloaded descriptor.
     * @return this descriptor, for chaining purpose.
     */
    public IndexDescriptor reload(SSTableReader sstable, Set<IndexContext> indices)
    {
        Preconditions.checkArgument(sstable.getDescriptor().equals(this.descriptor));
        SSTableIndexComponentsState discovered = IndexComponentDiscovery.instance().discoverComponents(sstable);

        // We want to make sure the descriptor only has data for the provided `indices` on reload, so we remove any
        // index data that is not in the ones provided. This essentially make sure we don't hold up memory for
        // dropped indexes.
        for (IndexContext context : new HashSet<>(perIndexes.keySet()))
        {
            if (!indices.contains(context))
                perIndexes.remove(context);
        }

        // Then reload data.
        initialize(indices, discovered);
        return this;
    }

    /**
     * Loads per-index components for the provided indexes only when they are currently missing from this descriptor.
     * <p>
     * This method does not modify per-SSTable components and does not replace already tracked per-index groups.
     * It is intended for incremental index discovery flows where callers only need to populate indexes that are
     * not present yet without disturbing existing in-memory references.
     */
    public IndexDescriptor loadIfAbsent(SSTableReader sstable, Set<IndexContext> indices)
    {
        Preconditions.checkArgument(sstable.getDescriptor().equals(this.descriptor));
        SSTableIndexComponentsState discovered = IndexComponentDiscovery.instance().discoverComponents(sstable);
        for (var context : indices)
            perIndexes.computeIfAbsent(context, k -> initializeGroup(context, discovered.perIndexBuild(context.getIndexName())));
        return this;
    }

    /**
     * Returns the version that should be used for new components for the sstable of this descriptor.
     * It is the version of the per-sstable components, which is either the version of the existing per-sstable
     * components if any, or the current version if there is no existing components.
     */
    public Version versionForNewComponents()
    {
        return perSSTableComponents().version();
    }

    public IndexComponents.ForRead perSSTableComponents()
    {
        return perSSTable;
    }

    public IndexComponents.ForRead perIndexComponents(IndexContext context)
    {
        var perIndex = perIndexes.get(context);
        return perIndex == null ? createEmptyGroup(context) : perIndex;
    }

    public Set<IndexContext> includedIndexes()
    {
        return Collections.unmodifiableSet(perIndexes.keySet());
    }

    public IndexComponents.ForWrite newPerSSTableComponentsForWrite()
    {
        return newComponentsForWrite(null, perSSTable);
    }

    public IndexComponents.ForWrite newPerIndexComponentsForWrite(IndexContext context)
    {
        return newComponentsForWrite(context, perIndexes.get(context));
    }

    private IndexComponents.ForWrite newComponentsForWrite(@Nullable IndexContext context, IndexComponentsImpl currentComponents)
    {
        Version version = versionForNewComponents();
        var currentBuildId = currentComponents == null ? null : currentComponents.buildId;
        return new IndexComponentsImpl(context, ComponentsBuildId.forNewBuild(version, currentBuildId, candidateId -> {
            // This checks that there is no existing files on disk we would overwrite by using `candidateId` for our
            // new build.
            IndexComponentsImpl candidate = new IndexComponentsImpl(context, candidateId);
            boolean isUsable = candidate.expectedComponentsForVersion().stream().noneMatch(candidate::componentExistsOnDisk);
            if (!isUsable)
            {
                noSpamLogger.warn(logMessage("Wanted to use generation {} for new build of {} SAI components of {}, but found some existing components on disk for that generation (maybe leftover from an incomplete/corrupted build?); trying next generation"),
                                  candidateId.generation(),
                                  context == null ? "per-SSTable" : "per-index",
                                  descriptor);
            }
            return isUsable;
        }));
    }

    /**
     * Returns true if the per-column index components of the provided sstable have been built and are valid.
     *
     * @param sstable The sstable to check
     * @param context The {@link IndexContext} for the index
     * @return true if the per-column index components have been built and are complete
     */
    public static boolean isIndexBuildCompleteOnDisk(SSTableReader sstable, IndexContext context)
    {
        IndexDescriptor descriptor = IndexDescriptor.load(sstable, Set.of(context));
        return descriptor.perSSTableComponents().isComplete()
               && descriptor.perIndexComponents(context).isComplete();
    }

    public boolean isIndexEmpty(IndexContext context)
    {
        return perSSTableComponents().isComplete() && perIndexComponents(context).isEmpty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(descriptor, perSSTableComponents().version());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescriptor other = (IndexDescriptor)o;
        return Objects.equals(descriptor, other.descriptor) &&
               Objects.equals(perSSTableComponents().version(), other.perSSTableComponents().version());
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

    private class IndexComponentsImpl implements IndexComponents.ForWrite
    {
        private final @Nullable IndexContext context;
        private final ComponentsBuildId buildId;

        private final Map<IndexComponentType, IndexComponentImpl> components = new EnumMap<>(IndexComponentType.class);

        // Mark groups that are being read/have been fully written, and thus should not have new components added.
        // This is just to catch errors where we'd try to add a component after the completion marker was written.
        private volatile boolean sealed;

        private IndexComponentsImpl(@Nullable IndexContext context, ComponentsBuildId buildId)
        {
            this.context = context;
            this.buildId = buildId;
        }

        private boolean componentExistsOnDisk(IndexComponentType component)
        {
            return new IndexComponentImpl(component).file().exists();
        }

        @Override
        public Descriptor descriptor()
        {
            return descriptor;
        }

        @Override
        public IndexDescriptor indexDescriptor()
        {
            return IndexDescriptor.this;
        }

        @Nullable
        @Override
        public IndexContext context()
        {
            return context;
        }

        @Override
        public ComponentsBuildId buildId()
        {
            return buildId;
        }

        @Override
        public boolean has(IndexComponentType component)
        {
            return components.containsKey(component);
        }

        @Override
        public boolean isEmpty()
        {
            return isComplete() && components.size() == 1;
        }

        @Override
        public boolean hasClustering()
        {
            return hasClustering;
        }

        @Override
        public ClusteringComparator comparator()
        {
            return comparator;
        }

        @Override
        public Collection<IndexComponent.ForRead> all()
        {
            return Collections.unmodifiableCollection(components.values());
        }

        @Override
        public boolean isValid(boolean validateChecksum, Consumer<IndexComponent> onInvalidComponent)
        {
            if (isEmpty())
                return true;

            boolean isValid = true;
            for (IndexComponentType expected : expectedComponentsForVersion())
            {
                var component = components.get(expected);
                if (component == null || !component.file().exists())
                {
                    logger.warn(logMessage("Missing index component {} from SSTable {}"), expected, descriptor);
                    isValid = false;
                }
                else if (!onDiskFormat().validateIndexComponent(component, validateChecksum))
                {
                    logger.warn(logMessage("Invalid/corrupted component {} for SSTable {}"), expected, descriptor);
                    onInvalidComponent.accept(component);
                    isValid = false;
                }
            }
            return isValid;
        }

        private void updateParentLink(IndexComponentsImpl update)
        {
            if (isPerIndexGroup())
                perIndexes.put(context, update);
            else
                perSSTable = update;
        }

        @Override
        public void invalidate(SSTable sstable, Tracker tracker)
        {
            // This rewrite the TOC to stop listing the components, which ensures that if the node is restarted,
            // then discovery will use an empty group for that context (like we add at the end of this method).
            sstable.unregisterComponents(allAsCustomComponents(), tracker);

            // Also delete the completion marker, to make it clear the group of components shouldn't be used anymore.
            // Note it's comparatively safe to do so in that the marker is never accessed during reads, so we cannot
            // break ongoing operations here.
            var marker = components.remove(completionMarkerComponent());
            if (marker != null)
                marker.delete();

            // Keeping legacy behavior if immutable components is disabled.
            if (!buildId.version().useImmutableComponentFiles() && CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getBoolean())
                forceDeleteAllComponents();

            // We replace ourselves by an explicitly empty group in the parent.
            updateParentLink(createEmptyGroup(context));
        }

        @Override
        public ForWrite forWrite()
        {
            // The difference between Reader and Writer is just to make code cleaner and make it clear when we read
            // components from when we write/modify them. But this concrete implementatation is both in practice.
            return this;
        }

        @Override
        public IndexComponent.ForRead get(IndexComponentType component)
        {
            IndexComponentImpl info = components.get(component);
            Preconditions.checkNotNull(info, "SSTable %s has no %s component for build %s (context: %s)", descriptor, component, buildId, context);
            return info;
        }

        @Override
        public long liveSizeOnDiskInBytes()
        {
            return components.values().stream().map(IndexComponentImpl::file).mapToLong(File::length).sum();
        }

        public void addIfExists(IndexComponentType component)
        {
            Preconditions.checkArgument(!sealed, "Should not add components for SSTable %s at this point; the completion marker has already been written", descriptor);
            // When a sstable doesn't have any complete group, we use a marker empty one with a generation of -1:
            Preconditions.checkArgument(!buildId.equals(emptyGroupMarker), "Should not be adding component to empty components");
            components.computeIfAbsent(component, type -> {
                var created = new IndexComponentImpl(type);
                return created.file().exists() ? created : null;
            });
        }

        @Override
        public IndexComponent.ForWrite addOrGet(IndexComponentType component)
        {
            Preconditions.checkArgument(!sealed, "Should not add components for SSTable %s at this point; the completion marker has already been written", descriptor);
            // When a sstable doesn't have any complete group, we use a marker empty one with a generation of -1:
            Preconditions.checkArgument(!buildId.equals(emptyGroupMarker), "Should not be adding component to empty components");
            return components.computeIfAbsent(component, IndexComponentImpl::new);
        }

        @Override
        public IndexComponent.ForWrite getForWrite(IndexComponentType component)
        {
            IndexComponentImpl info = components.get(component);
            Preconditions.checkNotNull(info, "SSTable %s has no %s component for build %s (context: %s)", descriptor, component, buildId, context);
            return info;
        }

        @Override
        public File tmpFileFor(String componentName)
        {
            String name = context != null ? String.format("%s_%s_%s", buildId, context.getColumnName(), componentName)
                                          :  String.format("%s_%s", buildId, componentName);
            return descriptor.tmpFileFor(new Component(Component.Type.CUSTOM, name));
        }

        @Override
        public void forceDeleteAllComponents()
        {
            components.values().forEach(IndexComponentImpl::delete);
            components.clear();
        }

        @Override
        public void markComplete() throws IOException
        {
            addOrGet(completionMarkerComponent()).createEmpty();
            sealed = true;
            // Until this call, the group is not attached to the parent. This creates the link.
            updateParentLink(this);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(descriptor, context, buildId);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexComponentsImpl that = (IndexComponentsImpl) o;
            return Objects.equals(descriptor, that.descriptor())
                   && Objects.equals(context, that.context)
                   && Objects.equals(buildId, that.buildId);
        }

        @Override
        public String toString()
        {
            return String.format("%s components for %s (%s): %s",
                                 context == null ? "Per-SSTable" : "Per-Index",
                                 descriptor,
                                 buildId,
                                 components.values());
        }

        private class IndexComponentImpl implements IndexComponent.ForRead, IndexComponent.ForWrite
        {
            private final IndexComponentType component;

            private volatile String filenamePart;
            private volatile File file;

            private IndexComponentImpl(IndexComponentType component)
            {
                this.component = component;
            }

            @Override
            public IndexComponentsImpl parent()
            {
                return IndexComponentsImpl.this;
            }

            @Override
            public IndexComponentType componentType()
            {
                return component;
            }

            @Override
            public ByteOrder byteOrder()
            {
                return buildId.version().onDiskFormat().byteOrderFor(component, context);
            }

            @Override
            public String fileNamePart()
            {
                // Not thread-safe, but not really the end of the world if called multiple time
                if (filenamePart == null)
                    filenamePart = buildId.formatAsComponent(component, context);
                return filenamePart;
            }

            @Override
            public Component asCustomComponent()
            {
                return new Component(Component.Type.CUSTOM, fileNamePart());
            }

            @Override
            public File file()
            {
                // Not thread-safe, but not really the end of the world if called multiple time
                if (file == null)
                    file = descriptor.fileFor(asCustomComponent());
                return file;
            }

            @Override
            public FileHandle createFileHandle()
            {
                try (FileHandle.Builder builder = StorageProvider.instance.fileHandleBuilderFor(this))
                {
                    if (logger.isTraceEnabled())
                    {
                        logger.trace(this.parent().logMessage("Opening file handle for {} ({})"),
                                     file, FBUtilities.prettyPrintMemory(file.length()));
                    }
                    FileHandle.Builder b = builder.order(byteOrder());
                    return b.complete();
                }
            }

            @Override
            public FileHandle createIndexBuildTimeFileHandle()
            {
                try (final FileHandle.Builder builder = StorageProvider.instance.indexBuildTimeFileHandleBuilderFor(this))
                {
                    return builder.order(byteOrder()).complete();
                }
            }

            @Override
            public IndexInput openInput()
            {
                return IndexFileUtils.instance.openBlockingInput(createFileHandle());
            }

            @Override
            public ChecksumIndexInput openCheckSummedInput()
            {
                var indexInput = openInput();
                return checksumIndexInput(indexInput);
            }

            /**
             * Returns a ChecksumIndexInput that reads the indexInput in the correct endianness for the context.
             * These files were written by the Lucene {@link org.apache.lucene.store.DataOutput}. When written by
             * Lucene 7.5, {@link org.apache.lucene.store.DataOutput} wrote the file using big endian formatting.
             * After the upgrade to Lucene 9, the {@link org.apache.lucene.store.DataOutput} writes in little endian
             * formatting.
             *
             * @param indexInput The index input to read
             * @return A ChecksumIndexInput that reads the indexInput in the correct endianness for the context
             */
            private ChecksumIndexInput checksumIndexInput(IndexInput indexInput)
            {
                if (buildId.version() == Version.AA)
                    return new EndiannessReverserChecksumIndexInput(indexInput);
                else
                    return new BufferedChecksumIndexInput(indexInput);
            }

            @Override
            public IndexOutputWriter openOutput(boolean append) throws IOException
            {
                File file = file();

                if (logger.isTraceEnabled())
                    logger.trace(this.parent().logMessage("Creating SSTable attached index output for component {} on file {}..."),
                                 component,
                                 file);

                return IndexFileUtils.instance.openOutput(file, byteOrder(), append, buildId.version());
            }

            @Override
            public void createEmpty() throws IOException
            {
                com.google.common.io.Files.touch(file().toJavaIOFile());
            }

            @Override
            public void delete()
            {
                File file = file();
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

            @Override
            public int hashCode()
            {
                return Objects.hash(this.parent(), component);
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                IndexComponentImpl that = (IndexComponentImpl) o;
                return Objects.equals(this.parent(), that.parent())
                       && component == that.component;
            }

            @Override
            public String toString()
            {
                return file().toString();
            }
        }
    }
}
