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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.guardrails.Guardrails;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Transactional;

/**
 * This is the API all table writers must implement.
 *
 * TableWriter.create() is the primary way to create a writer for a particular format.
 * The format information is part of the Descriptor.
 */
public abstract class SSTableWriter extends SSTable implements Transactional
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    protected long repairedAt;
    protected UUID pendingRepair;
    protected boolean isTransient;
    protected long maxDataAge = -1;
    protected final long keyCount;
    protected final MetadataCollector metadataCollector;
    protected final SerializationHeader header;
    protected final TransactionalProxy txnProxy = txnProxy();
    protected final Collection<SSTableFlushObserver> observers;

    protected abstract TransactionalProxy txnProxy();

    // due to lack of multiple inheritance, we use an inner class to proxy our Transactional implementation details
    protected abstract class TransactionalProxy extends AbstractTransactional
    {
        // should be set during doPrepare()
        protected SSTableReader finalReader;
        protected boolean openResult;
    }

    protected SSTableWriter(Descriptor descriptor,
                            Set<Component> components,
                            long keyCount,
                            long repairedAt,
                            UUID pendingRepair,
                            boolean isTransient,
                            TableMetadataRef metadata,
                            MetadataCollector metadataCollector,
                            SerializationHeader header,
                            Collection<SSTableFlushObserver> observers)
    {
        super(descriptor, components, metadata, DatabaseDescriptor.getDiskOptimizationStrategy());
        this.keyCount = keyCount;
        this.repairedAt = repairedAt;
        this.pendingRepair = pendingRepair;
        this.isTransient = isTransient;
        this.metadataCollector = metadataCollector;
        this.header = header;
        this.observers = observers == null ? Collections.emptySet() : observers;
    }

    private static Set<Component> indexComponents(Collection<Index.Group> indexGroups)
    {
        if (indexGroups == null)
            return Collections.emptySet();

        Set<Component> components = new HashSet<>();
        for (Index.Group group : indexGroups)
        {
            components.addAll(group.getComponents());
        }

        return components;
    }

    public static SSTableWriter create(Descriptor descriptor,
                                       Long keyCount,
                                       Long repairedAt,
                                       UUID pendingRepair,
                                       boolean isTransient,
                                       TableMetadataRef metadata,
                                       MetadataCollector metadataCollector,
                                       SerializationHeader header,
                                       Collection<Index.Group> indexGroups,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        Factory writerFactory = descriptor.getFormat().getWriterFactory();
        return writerFactory.open(descriptor, keyCount, repairedAt, pendingRepair, isTransient, metadata, metadataCollector, header, observers(descriptor, indexGroups, lifecycleNewTracker, metadata.get()), lifecycleNewTracker,
                                  indexComponents(indexGroups));
    }

    public static SSTableWriter create(Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       UUID pendingRepair,
                                       boolean isTransient,
                                       int sstableLevel,
                                       SerializationHeader header,
                                       Collection<Index.Group> indexGroups,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        TableMetadataRef metadata = Schema.instance.getTableMetadataRef(descriptor);
        return create(metadata, descriptor, keyCount, repairedAt, pendingRepair, isTransient, sstableLevel, header, indexGroups, lifecycleNewTracker);
    }

    public static SSTableWriter create(TableMetadataRef metadata,
                                       Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       UUID pendingRepair,
                                       boolean isTransient,
                                       int sstableLevel,
                                       SerializationHeader header,
                                       Collection<Index.Group> indexGroups,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        MetadataCollector collector = new MetadataCollector(metadata.get().comparator).sstableLevel(sstableLevel);
        return create(descriptor, keyCount, repairedAt, pendingRepair, isTransient, metadata, collector, header, indexGroups, lifecycleNewTracker);
    }

    @VisibleForTesting
    public static SSTableWriter create(Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       UUID pendingRepair,
                                       boolean isTransient,
                                       SerializationHeader header,
                                       Collection<Index.Group> indexGroups,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        return create(descriptor, keyCount, repairedAt, pendingRepair, isTransient, 0, header, indexGroups, lifecycleNewTracker);
    }

    /**
     * BigTable SSTable components. Should be moved to BigTableWriter but is left here for painless upstream merges.
     */
    public static Set<Component> bigTableComponents(TableMetadata metadata, Collection<Component> indexComponents)
    {
        Set<Component> components = new HashSet<Component>(Arrays.asList(Component.DATA,
                Component.PRIMARY_INDEX,
                Component.STATS,
                Component.SUMMARY,
                Component.TOC,
                Component.DIGEST));

        if (metadata.params.bloomFilterFpChance < 1.0)
            components.add(Component.FILTER);

        if (metadata.params.compression.isEnabled())
        {
            components.add(Component.COMPRESSION_INFO);
        }
        else
        {
            // it would feel safer to actually add this component later in maybeWriteDigest(),
            // but the components are unmodifiable after construction
            components.add(Component.CRC);
        }

        components.addAll(indexComponents);

        return components;
    }

    private static Collection<SSTableFlushObserver> observers(Descriptor descriptor,
                                                              Collection<Index.Group> indexGroups,
                                                              LifecycleNewTracker tracker,
                                                              TableMetadata metadata)
    {
        if (indexGroups == null)
            return Collections.emptyList();

        List<SSTableFlushObserver> observers = new ArrayList<>(indexGroups.size());
        for (Index.Group group : indexGroups)
        {
            SSTableFlushObserver observer = group.getFlushObserver(descriptor, tracker, metadata);
            if (observer != null)
            {
                observer.begin();
                observers.add(observer);
            }
        }

        return ImmutableList.copyOf(observers);
    }

    public abstract void mark();

    /**
     * Appends partition data to this writer.
     *
     * @param iterator the partition to write
     * @return the created index entry if something was written, that is if {@code iterator}
     * wasn't empty, {@code null} otherwise.
     *
     * @throws FSWriteError if a write to the dataFile fails
     */
    public abstract RowIndexEntry append(UnfilteredRowIterator iterator);

    public abstract long getFilePointer();

    public abstract long getOnDiskFilePointer();

    public long getEstimatedOnDiskBytesWritten()
    {
        return getOnDiskFilePointer();
    }

    public abstract void resetAndTruncate();

    public SSTableWriter setRepairedAt(long repairedAt)
    {
        if (repairedAt > 0)
            this.repairedAt = repairedAt;
        return this;
    }

    public SSTableWriter setMaxDataAge(long maxDataAge)
    {
        this.maxDataAge = maxDataAge;
        return this;
    }

    public SSTableWriter setOpenResult(boolean openResult)
    {
        txnProxy.openResult = openResult;
        return this;
    }

    /**
     * Open the resultant SSTableReader before it has been fully written
     */
    public abstract boolean openEarly(Consumer<SSTableReader> callWhenReady);

    /**
     * Open the resultant SSTableReader once it has been fully written, but before the
     * _set_ of tables that are being written together as one atomic operation are all ready
     */
    public abstract SSTableReader openFinalEarly();

    public SSTableReader finish(long repairedAt, long maxDataAge, boolean openResult)
    {
        if (repairedAt > 0)
            this.repairedAt = repairedAt;
        this.maxDataAge = maxDataAge;
        return finish(openResult);
    }

    public SSTableReader finish(boolean openResult)
    {
        setOpenResult(openResult);
        txnProxy.finish();
        observers.forEach(SSTableFlushObserver::complete);
        return finished();
    }

    /**
     * Open the resultant SSTableReader once it has been fully written, and all related state
     * is ready to be finalised including other sstables being written involved in the same operation
     */
    public SSTableReader finished()
    {
        return txnProxy.finalReader;
    }

    // finalise our state on disk, including renaming
    public final void prepareToCommit()
    {
        txnProxy.prepareToCommit();
    }

    public final Throwable commit(Throwable accumulate)
    {
        try
        {
            return txnProxy.commit(accumulate);
        }
        finally
        {
            observers.forEach(SSTableFlushObserver::complete);
        }
    }

    public final Throwable abort(Throwable accumulate)
    {
        try
        {
            return txnProxy.abort(accumulate);
        }
        finally
        {
            observers.forEach(observer -> observer.abort(accumulate));
        }
    }

    public final void close()
    {
        txnProxy.close();
    }

    public final void abort()
    {
        try
        {
            txnProxy.abort();
        }
        finally
        {
            observers.forEach(observer -> observer.abort(null));
        }
    }

    protected Map<MetadataType, MetadataComponent> finalizeMetadata()
    {
        return metadataCollector.finalizeMetadata(getPartitioner().getClass().getCanonicalName(),
                                                  metadata().params.bloomFilterFpChance,
                                                  repairedAt,
                                                  pendingRepair,
                                                  isTransient,
                                                  header);
    }

    protected StatsMetadata statsMetadata()
    {
        return (StatsMetadata) finalizeMetadata().get(MetadataType.STATS);
    }

    public void releaseMetadataOverhead()
    {
        metadataCollector.release();
    }

    public static void rename(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        for (Component component : Sets.difference(components, Sets.newHashSet(Component.DATA, Component.SUMMARY)))
        {
            FileUtils.renameWithConfirm(tmpdesc.filenameFor(component), newdesc.filenameFor(component));
        }

        // do -Data last because -Data present should mean the sstable was completely renamed before crash
        FileUtils.renameWithConfirm(tmpdesc.filenameFor(Component.DATA), newdesc.filenameFor(Component.DATA));

        // rename it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        if (components.contains(Component.SUMMARY))
            FileUtils.renameWithOutConfirm(tmpdesc.filenameFor(Component.SUMMARY), newdesc.filenameFor(Component.SUMMARY));
    }

    public static void copy(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        for (Component component : Sets.difference(components, Sets.newHashSet(Component.DATA, Component.SUMMARY)))
        {
            FileUtils.copyWithConfirm(tmpdesc.filenameFor(component), newdesc.filenameFor(component));
        }

        // do -Data last because -Data present should mean the sstable was completely copied before crash
        FileUtils.copyWithConfirm(tmpdesc.filenameFor(Component.DATA), newdesc.filenameFor(Component.DATA));

        // copy it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        if (components.contains(Component.SUMMARY))
            FileUtils.copyWithOutConfirm(tmpdesc.filenameFor(Component.SUMMARY), newdesc.filenameFor(Component.SUMMARY));
    }

    public static void hardlink(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        for (Component component : Sets.difference(components, Sets.newHashSet(Component.DATA, Component.SUMMARY)))
        {
            FileUtils.createHardLinkWithConfirm(tmpdesc.filenameFor(component), newdesc.filenameFor(component));
        }

        // do -Data last because -Data present should mean the sstable was completely copied before crash
        FileUtils.createHardLinkWithConfirm(tmpdesc.filenameFor(Component.DATA), newdesc.filenameFor(Component.DATA));

        // copy it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        if (components.contains(Component.SUMMARY))
            FileUtils.createHardLinkWithoutConfirm(tmpdesc.filenameFor(Component.SUMMARY), newdesc.filenameFor(Component.SUMMARY));
    }

    public interface SSTableSizeParameters
    {
        long partitionCount();
        long partitionKeySize();
        long dataSize();
    }

    public static void guardCollectionSize(UnfilteredRowIterator partition, Unfiltered unfiltered)
    {
        if (!unfiltered.isRow() || SchemaConstants.isInternalKeyspace(partition.metadata().keyspace))
            return;

        if (!Guardrails.collectionSize.enabled() && !Guardrails.itemsPerCollection.enabled())
            return;

        Row row = (Row) unfiltered;
        for (ColumnMetadata column : row.columns())
        {
            if (!column.type.isCollection() || !column.type.isMultiCell())
                continue;

            ComplexColumnData cells = row.getComplexColumnData(column);
            if (cells == null)
                continue;

            ComplexColumnData liveCells = cells.purge(DeletionPurger.PURGE_ALL, FBUtilities.nowInSeconds());
            if (liveCells == null)
                continue;

            int cellsSize = liveCells.dataSize();
            int cellsCount = liveCells.cellsCount();

            if (!Guardrails.collectionSize.triggersOn(cellsSize) &&
                !Guardrails.itemsPerCollection.triggersOn(cellsCount))
                continue;

            TableMetadata metadata = partition.metadata();
            ByteBuffer key = partition.partitionKey().getKey();
            String keyString = metadata.primaryKeyAsCQLLiteral(key, row.clustering());
            String msg = String.format("%s in row %s in table %s",
                                       column.name.toString(),
                                       keyString,
                                       metadata);
            Guardrails.collectionSize.guard(cellsSize, msg, true);
            Guardrails.itemsPerCollection.guard(cellsCount, msg, true);
        }
    }

    public static abstract class Factory
    {
        public abstract long estimateSize(SSTableSizeParameters parameters);

        public abstract SSTableWriter open(Descriptor descriptor,
                                           long keyCount,
                                           long repairedAt,
                                           UUID pendingRepair,
                                           boolean isTransient,
                                           TableMetadataRef metadata,
                                           MetadataCollector metadataCollector,
                                           SerializationHeader header,
                                           Collection<SSTableFlushObserver> observers,
                                           LifecycleNewTracker lifecycleNewTracker,
                                           Set<Component> indexComponents);
    }

    protected void maybeLogLargePartitionWarning(DecoratedKey key, long rowSize)
    {
        if (SchemaConstants.isInternalKeyspace(metadata().keyspace))
            return;

        if (Guardrails.partitionSize.triggersOn(rowSize))
        {
            String keyString = metadata().partitionKeyAsCQLLiteral(key.getKey());
            Guardrails.partitionSize.guard(rowSize, String.format("%s in %s", keyString, metadata), true);
        }

        if (rowSize > DatabaseDescriptor.getCompactionLargePartitionWarningThreshold())
        {
            String keyString = metadata().partitionKeyType.getString(key.getKey());
            logger.warn("Writing large partition {}/{}:{} ({}) to sstable {}", metadata.keyspace, metadata.name, keyString, FBUtilities.prettyPrintMemory(rowSize), getFilename());
        }
    }
}
