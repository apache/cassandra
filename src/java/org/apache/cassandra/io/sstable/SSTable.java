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

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.SharedCloseable;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;

/**
 * This class represents an abstract sstable on disk whose keys and corresponding partitions are stored in
 * a {@link SSTableFormat.Components#DATA} file in order as imposed by {@link DecoratedKey#comparator}.
 */
public abstract class SSTable
{
    public static final int TOMBSTONE_HISTOGRAM_BIN_SIZE = 100;
    public static final int TOMBSTONE_HISTOGRAM_SPOOL_SIZE = 100000;
    public static final int TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS = CassandraRelevantProperties.STREAMING_HISTOGRAM_ROUND_SECONDS.getInt();

    public final Descriptor descriptor;
    protected final Set<Component> components;
    public final boolean compression;

    protected final TableMetadataRef metadata;

    public final ChunkCache chunkCache;
    public final IOOptions ioOptions;

    @Nullable
    private final WeakReference<Owner> owner;

    public SSTable(Builder<?, ?> builder, Owner owner)
    {
        this.owner = new WeakReference<>(owner);
        checkNotNull(builder.descriptor);
        checkNotNull(builder.getComponents());

        this.descriptor = builder.descriptor;
        this.ioOptions = builder.getIOOptions();
        this.components = new CopyOnWriteArraySet<>(builder.getComponents());
        this.compression = components.contains(Components.COMPRESSION_INFO);
        this.metadata = builder.getTableMetadataRef();
        this.chunkCache = builder.getChunkCache();
    }

    public final Optional<Owner> owner()
    {
        if (owner == null)
            return Optional.empty();
        return Optional.ofNullable(owner.get());
    }

    public static void rename(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        components.stream()
                  .filter(c -> !newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .filter(c -> !c.equals(Components.DATA))
                  .forEach(c -> tmpdesc.fileFor(c).move(newdesc.fileFor(c)));

        // do -Data last because -Data present should mean the sstable was completely renamed before crash
        tmpdesc.fileFor(Components.DATA).move(newdesc.fileFor(Components.DATA));

        // rename it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        components.stream()
                  .filter(c -> newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .forEach(c -> tmpdesc.fileFor(c).tryMove(newdesc.fileFor(c)));
    }

    public static void copy(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        components.stream()
                  .filter(c -> !newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .filter(c -> !c.equals(Components.DATA))
                  .forEach(c -> FileUtils.copyWithConfirm(tmpdesc.fileFor(c), newdesc.fileFor(c)));

        // do -Data last because -Data present should mean the sstable was completely copied before crash
        FileUtils.copyWithConfirm(tmpdesc.fileFor(Components.DATA), newdesc.fileFor(Components.DATA));

        // copy it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        components.stream()
                  .filter(c -> newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .forEach(c -> FileUtils.copyWithOutConfirm(tmpdesc.fileFor(c), newdesc.fileFor(c)));
    }

    public static void hardlink(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        components.stream()
                  .filter(c -> !newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .filter(c -> !c.equals(Components.DATA))
                  .forEach(c -> FileUtils.createHardLinkWithConfirm(tmpdesc.fileFor(c), newdesc.fileFor(c)));

        // do -Data last because -Data present should mean the sstable was completely copied before crash
        FileUtils.createHardLinkWithConfirm(tmpdesc.fileFor(Components.DATA), newdesc.fileFor(Components.DATA));

        // copy it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        components.stream()
                  .filter(c -> newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .forEach(c -> FileUtils.createHardLinkWithoutConfirm(tmpdesc.fileFor(c), newdesc.fileFor(c)));
    }

    public abstract DecoratedKey getFirst();

    public abstract DecoratedKey getLast();

    public abstract AbstractBounds<Token> getBounds();

    @VisibleForTesting
    public Set<Component> getComponents()
    {
        return ImmutableSet.copyOf(components);
    }

    /**
     * Returns all SSTable components that should be streamed.
     */
    public Set<Component> getStreamingComponents()
    {
        return components.stream()
                         .filter(c -> c.type.streamable)
                         .collect(Collectors.toSet());
    }

    public TableMetadata metadata()
    {
        return metadata.get();
    }

    public IPartitioner getPartitioner()
    {
        return metadata().partitioner;
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return getPartitioner().decorateKey(key);
    }

    public String getFilename()
    {
        return descriptor.fileFor(Components.DATA).absolutePath();
    }

    public String getColumnFamilyName()
    {
        return descriptor.cfname;
    }

    public String getKeyspaceName()
    {
        return descriptor.ksname;
    }

    public List<String> getAllFilePaths()
    {
        List<String> ret = new ArrayList<>(components.size());
        for (Component component : components)
            ret.add(descriptor.fileFor(component).absolutePath());
        return ret;
    }

    /**
     * The method sets fields for this sstable representation on the provided {@link Builder}. The method is intended
     * to be called from the overloaded {@code unbuildTo} method in subclasses.
     *
     * @param builder    the builder on which the fields should be set
     * @param sharedCopy whether the {@link SharedCloseable} resources should be passed as shared copies or directly;
     *                   note that the method will overwrite the fields representing {@link SharedCloseable} only if
     *                   they are not set in the builder yet (the relevant fields in the builder are {@code null}).
     *                   Although {@link SSTable} does not keep any references to resources, the parameters is added
     *                   for the possible future fields and for consistency with the overloaded implementations in
     *                   subclasses
     * @return the same instance of builder as provided
     */
    protected final <B extends Builder<?, B>> B unbuildTo(B builder, boolean sharedCopy)
    {
        return builder.setTableMetadataRef(metadata)
                      .setComponents(components)
                      .setChunkCache(chunkCache)
                      .setIOOptions(ioOptions);
    }

    /**
     * Parse a sstable filename into both a {@link Descriptor} and {@code Component} object.
     *
     * @param file the filename to parse.
     * @return a pair of the {@code Descriptor} and {@code Component} corresponding to {@code file} if it corresponds to
     * a valid and supported sstable filename, {@code null} otherwise. Note that components of an unknown type will be
     * returned as CUSTOM ones.
     */
    public static Pair<Descriptor, Component> tryComponentFromFilename(File file)
    {
        try
        {
            return Descriptor.fromFileWithComponent(file);
        }
        catch (Throwable e)
        {
            return null;
        }
    }

    /**
     * Parse a sstable filename into both a {@link Descriptor} and {@code Component} object.
     *
     * @param file     the filename to parse.
     * @param keyspace The keyspace name of the file.
     * @param table    The table name of the file.
     * @return a pair of the {@code Descriptor} and {@code Component} corresponding to {@code file} if it corresponds to
     * a valid and supported sstable filename, {@code null} otherwise. Note that components of an unknown type will be
     * returned as CUSTOM ones.
     */
    public static Pair<Descriptor, Component> tryComponentFromFilename(File file, String keyspace, String table)
    {
        try
        {
            return Descriptor.fromFileWithComponent(file, keyspace, table);
        }
        catch (Throwable e)
        {
            return null;
        }
    }

    /**
     * Parse a sstable filename into a {@link Descriptor} object.
     * <p>
     * Note that this method ignores the component part of the filename; if this is not what you want, use
     * {@link #tryComponentFromFilename} instead.
     *
     * @param file the filename to parse.
     * @return the {@code Descriptor} corresponding to {@code file} if it corresponds to a valid and supported sstable
     * filename, {@code null} otherwise.
     */
    public static Descriptor tryDescriptorFromFile(File file)
    {
        try
        {
            return Descriptor.fromFile(file);
        }
        catch (Throwable e)
        {
            return null;
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s:%s(path='%s')", getClass().getSimpleName(), descriptor.version.format.name(), getFilename());
    }

    public static void validateRepairedMetadata(long repairedAt, TimeUUID pendingRepair, boolean isTransient)
    {
        Preconditions.checkArgument((pendingRepair == NO_PENDING_REPAIR) || (repairedAt == UNREPAIRED_SSTABLE),
                                    "pendingRepair cannot be set on a repaired sstable");
        Preconditions.checkArgument(!isTransient || (pendingRepair != NO_PENDING_REPAIR),
                                    "isTransient can only be true for sstables pending repair");
    }

    /**
     * Registers new custom components. Used by custom compaction strategies.
     * Adding a component for the second time is a no-op.
     * Don't remove this - this method is a part of the public API, intended for use by custom compaction strategies.
     *
     * @param newComponents collection of components to be added
     */
    public synchronized void addComponents(Collection<Component> newComponents)
    {
        Collection<Component> componentsToAdd = Collections2.filter(newComponents, Predicates.not(Predicates.in(components)));
        TOCComponent.appendTOC(descriptor, componentsToAdd);
        components.addAll(componentsToAdd);
    }

    /**
     * Registers new custom components into sstable and update size tracking
     * @param newComponents collection of components to be added
     * @param tracker used to update on-disk size metrics
     */
    public synchronized void registerComponents(Collection<Component> newComponents, Tracker tracker)
    {
        Collection<Component> componentsToAdd = new HashSet<>(Collections2.filter(newComponents, x -> !components.contains(x)));
        TOCComponent.appendTOC(descriptor, componentsToAdd);
        components.addAll(componentsToAdd);

        for (Component component : componentsToAdd)
        {
            File file = descriptor.fileFor(component);
            if (file.exists())
                tracker.updateLiveDiskSpaceUsed(file.length());
        }
    }

    /**
     * Unregisters custom components from sstable and update size tracking
     * @param removeComponents collection of components to be remove
     * @param tracker used to update on-disk size metrics
     */
    public synchronized void unregisterComponents(Collection<Component> removeComponents, Tracker tracker)
    {
        Collection<Component> componentsToRemove = new HashSet<>(Collections2.filter(removeComponents, components::contains));
        components.removeAll(componentsToRemove);
        TOCComponent.rewriteTOC(descriptor, components);

        for (Component component : componentsToRemove)
        {
            File file = descriptor.fileFor(component);
            if (file.exists())
                tracker.updateLiveDiskSpaceUsed(-file.length());
        }
    }

    public interface Owner
    {
        Double getCrcCheckChance();

        OpOrder.Barrier newReadOrderingBarrier();

        TableMetrics getMetrics();
    }

    /**
     * A builder of this sstable representation. It should be extended for each implementation with the specific fields.
     *
     * @param <S> type of the sstable representation to be build with this builder
     * @param <B> type of this builder
     */
    public static class Builder<S extends SSTable, B extends Builder<S, B>>
    {
        public final Descriptor descriptor;

        private Set<Component> components;
        private TableMetadataRef tableMetadataRef;
        private ChunkCache chunkCache = ChunkCache.instance;
        private IOOptions ioOptions = IOOptions.fromDatabaseDescriptor();

        public Builder(Descriptor descriptor)
        {
            checkNotNull(descriptor);
            this.descriptor = descriptor;
        }

        public B setComponents(Collection<Component> components)
        {
            if (components != null)
            {
                components.forEach(c -> Preconditions.checkState(c.isValidFor(descriptor), "Invalid component type for sstable format " + descriptor.version.format.name()));
                this.components = ImmutableSet.copyOf(components);
            }
            else
            {
                this.components = null;
            }
            return (B) this;
        }

        public B addComponents(Collection<Component> components)
        {
            if (components == null || components.isEmpty())
                return (B) this;

            if (this.components == null)
                return setComponents(components);

            return setComponents(Sets.union(this.components, ImmutableSet.copyOf(components)));
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
}
