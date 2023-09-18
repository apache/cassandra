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

import java.io.IOError;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.SSTableZeroCopyWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.MmappedRegionsCache;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Transactional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A root class for a writer implementation. A writer must be created by passing an implementation-specific
 * {@link Builder}, a {@link LifecycleNewTracker} and {@link SSTable.Owner} instances. Implementing classes should
 * not extend that list and all the additional properties should be included in the builder.
 */
public abstract class SSTableWriter extends SSTable implements Transactional
{
    private final static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    protected long repairedAt;
    protected TimeUUID pendingRepair;
    protected boolean isTransient;
    protected long maxDataAge = -1;
    protected final long keyCount;
    protected final MetadataCollector metadataCollector;
    protected final SerializationHeader header;
    protected final List<SSTableFlushObserver> observers;
    protected final MmappedRegionsCache mmappedRegionsCache;
    protected final TransactionalProxy txnProxy = txnProxy();
    protected final LifecycleNewTracker lifecycleNewTracker;
    protected DecoratedKey first;
    protected DecoratedKey last;

    /**
     * The implementing method should return an instance of {@link TransactionalProxy} initialized with a list of all
     * transactional resources included in this writer.
     */
    protected abstract TransactionalProxy txnProxy();

    protected SSTableWriter(Builder<?, ?> builder, LifecycleNewTracker lifecycleNewTracker, SSTable.Owner owner)
    {
        super(builder, owner);
        checkNotNull(builder.getIndexGroups());
        checkNotNull(builder.getMetadataCollector());
        checkNotNull(builder.getSerializationHeader());

        this.keyCount = builder.getKeyCount();
        this.repairedAt = builder.getRepairedAt();
        this.pendingRepair = builder.getPendingRepair();
        this.isTransient = builder.isTransientSSTable();
        this.metadataCollector = builder.getMetadataCollector();
        this.header = builder.getSerializationHeader();
        this.mmappedRegionsCache = builder.getMmappedRegionsCache();
        this.lifecycleNewTracker = lifecycleNewTracker;

        // We need to ensure that no sstable components exist before the lifecycle transaction starts tracking it.
        // Otherwise, it means that we either want to overwrite some existing sstable, which is not allowed, or some
        // sstable files were created before the sstable is registered in the lifecycle transaction, which may lead
        // to a race such that the sstable is listed as completed due to the lack of the transaction file before
        // anything is actually written to it.
        Set<Component> existingComponents = Sets.filter(components, c -> descriptor.fileFor(c).exists());
        assert existingComponents.isEmpty() : String.format("Cannot create a new SSTable in directory %s as component files %s already exist there",
                                                            descriptor.directory,
                                                            existingComponents);

        lifecycleNewTracker.trackNew(this);

        try
        {
            ArrayList<SSTableFlushObserver> observers = new ArrayList<>();
            this.observers = Collections.unmodifiableList(observers);
            for (Index.Group group : builder.getIndexGroups())
            {
                SSTableFlushObserver observer = group.getFlushObserver(descriptor, lifecycleNewTracker, metadata.getLocal());
                if (observer != null)
                {
                    observer.begin();
                    observers.add(observer);
                }
            }
        }
        catch (RuntimeException | IOError ex)
        {
            handleConstructionFailure(ex);
            throw ex;
        }
    }

    /**
     * Constructors of subclasses, if they open any resources, should wrap that in a try-catch block and call this
     * method in the 'catch' section after closing any resources opened in the constructor. This method would remove
     * the sstable from the transaction and delete the orphaned components, if any were created during the construction.
     * The caught exception should be then rethrown so the {@link Builder} can handle it and close any resources opened
     * implicitly by the builder.
     * <p>
     * See {@link SortedTableWriter#SortedTableWriter(SortedTableWriter.Builder, LifecycleNewTracker, Owner)} as of CASSANDRA-18737.
     *
     * @param ex the exception thrown during the construction
     */
    protected void handleConstructionFailure(Throwable ex)
    {
        logger.warn("Failed to open " + descriptor + " for writing", ex);
        for (int i = observers.size()-1; i >= 0; i--)
            observers.get(i).abort(ex);
        descriptor.getFormat().deleteOrphanedComponents(descriptor, components);
        lifecycleNewTracker.untrackNew(this);
    }

    @Override
    public DecoratedKey getFirst()
    {
        return first;
    }

    @Override
    public DecoratedKey getLast()
    {
        return last;
    }

    @Override
    public AbstractBounds<Token> getBounds()
    {
        return (first != null && last != null) ? AbstractBounds.bounds(first.getToken(), true, last.getToken(), true)
                                               : null;
    }

    public abstract void mark();

    /**
     * Appends partition data to this writer.
     *
     * @param iterator the partition to write
     * @return the created index entry if something was written, that is if {@code iterator}
     * wasn't empty, {@code null} otherwise.
     *
     * @throws FSWriteError if writing to the dataFile fails
     */
    public abstract AbstractRowIndexEntry append(UnfilteredRowIterator iterator);

    /**
     * Returns a position in the uncompressed data - for uncompressed files it is the same as {@link #getOnDiskFilePointer()}
     * but for compressed files it returns a position in the data rather than a position in the file on disk.
     */
    public abstract long getFilePointer();

    /**
     * Returns a position in the (compressed) data file on disk. See {@link #getFilePointer()}
     */
    public abstract long getOnDiskFilePointer();

    /**
     * Returns the amount of data already written to disk that may not be accurate (for example, the position after
     * the recently flushed chunk).
     */
    public long getEstimatedOnDiskBytesWritten()
    {
        return getOnDiskFilePointer();
    }

    /**
     * Reset the data file to the marked position (see {@link #mark()}) and truncate the rest of the file.
     */
    public abstract void resetAndTruncate();

    public void setRepairedAt(long repairedAt)
    {
        if (repairedAt > 0)
            this.repairedAt = repairedAt;
    }

    public void setMaxDataAge(long maxDataAge)
    {
        this.maxDataAge = maxDataAge;
    }

    public SSTableWriter setTokenSpaceCoverage(double rangeSpanned)
    {
        metadataCollector.tokenSpaceCoverage(rangeSpanned);
        return this;
    }

    public void setOpenResult(boolean openResult)
    {
        txnProxy.openResult = openResult;
    }

    /**
     * Open the resultant SSTableReader before it has been fully written.
     * <p>
     * The passed consumer will be called when the necessary data has been flushed to disk/cache. This may never happen
     * (e.g. if the table was finished before the flushes materialized, or if this call returns false e.g. if a table
     * was already prepared but hasn't reached readiness yet).
     * <p>
     * Uses callback instead of future because preparation and callback happen on the same thread.
     */

    public abstract void openEarly(Consumer<SSTableReader> doWhenReady);

    /**
     * Open the resultant SSTableReader once it has been fully written, but before the
     * _set_ of tables that are being written together as one atomic operation are all ready
     */
    public abstract SSTableReader openFinalEarly();

    protected abstract SSTableReader openFinal(SSTableReader.OpenReason openReason);

    public SSTableReader finish(boolean openResult)
    {
        this.setOpenResult(openResult);
        observers.forEach(SSTableFlushObserver::complete);
        txnProxy.finish();
        return finished();
    }

    /**
     * Open the resultant SSTableReader once it has been fully written, and all related state
     * is ready to be finalised including other sstables being written involved in the same operation
     */
    public SSTableReader finished()
    {
        txnProxy.finalReaderAccessed = true;
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
            observers.forEach(SSTableFlushObserver::complete);
        }
        catch (Throwable t)
        {
            // Return without advancing to COMMITTED, which will trigger abort() when the Transactional closes...
            return Throwables.merge(accumulate, t);
        }

        return txnProxy.commit(accumulate);
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
                                                  header,
                                                  first.retainable().getKey(),
                                                  last.retainable().getKey());
    }

    protected StatsMetadata statsMetadata()
    {
        return (StatsMetadata) finalizeMetadata().get(MetadataType.STATS);
    }

    public void releaseMetadataOverhead()
    {
        metadataCollector.release();
    }

    /**
     * Parameters for calculating the expected size of an SSTable. Exposed on memtable flush sets (i.e. collected
     * subsets of a memtable that will be written to sstables).
     */
    public interface SSTableSizeParameters
    {
        long partitionCount();
        long partitionKeysSize();
        long dataSize();
    }

    // due to lack of multiple inheritance, we use an inner class to proxy our Transactional implementation details
    protected class TransactionalProxy extends AbstractTransactional
    {
        // should be set during doPrepare()
        private final Supplier<ImmutableList<Transactional>> transactionals;

        private SSTableReader finalReader;
        private boolean openResult;
        private boolean finalReaderAccessed;

        public TransactionalProxy(Supplier<ImmutableList<Transactional>> transactionals)
        {
            this.transactionals = transactionals;
        }

        // finalise our state on disk, including renaming
        protected void doPrepare()
        {
            transactionals.get().forEach(Transactional::prepareToCommit);
            new StatsComponent(finalizeMetadata()).save(descriptor);

            // save the table of components
            TOCComponent.appendTOC(descriptor, components);

            if (openResult)
                finalReader = openFinal(SSTableReader.OpenReason.NORMAL);
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            for (Transactional t : transactionals.get().reverse())
                accumulate = t.commit(accumulate);

            return accumulate;
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            for (Transactional t : transactionals.get())
                accumulate = t.abort(accumulate);

            if (!finalReaderAccessed && finalReader != null)
            {
                accumulate = Throwables.perform(accumulate, () -> finalReader.selfRef().release());
                finalReader = null;
                finalReaderAccessed = false;
            }

            return accumulate;
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = super.doPostCleanup(accumulate);
            accumulate = Throwables.close(accumulate, mmappedRegionsCache);
            return accumulate;
        }
    }

    /**
     * A builder of this sstable writer. It should be extended for each implementation with the specific fields.
     *
     * An implementation should open all the resources when {@link #build(LifecycleNewTracker, Owner)} and pass them
     * in builder fields to the writer, so that the writer can access them via getters.
     *
     * @param <W> type of the sstable writer to be build with this builder
     * @param <B> type of this builder
     */
    public abstract static class Builder<W extends SSTableWriter, B extends Builder<W, B>> extends SSTable.Builder<W, B>
    {
        private MetadataCollector metadataCollector;
        private long keyCount;
        private long repairedAt;
        private TimeUUID pendingRepair;
        private boolean transientSSTable;
        private SerializationHeader serializationHeader;
        private List<Index.Group> indexGroups;

        public B setMetadataCollector(MetadataCollector metadataCollector)
        {
            this.metadataCollector = metadataCollector;
            return (B) this;
        }

        public B setKeyCount(long keyCount)
        {
            this.keyCount = keyCount;
            return (B) this;
        }

        public B setRepairedAt(long repairedAt)
        {
            this.repairedAt = repairedAt;
            return (B) this;
        }

        public B setPendingRepair(TimeUUID pendingRepair)
        {
            this.pendingRepair = pendingRepair;
            return (B) this;
        }

        public B setTransientSSTable(boolean transientSSTable)
        {
            this.transientSSTable = transientSSTable;
            return (B) this;
        }

        public B setSerializationHeader(SerializationHeader serializationHeader)
        {
            this.serializationHeader = serializationHeader;
            return (B) this;
        }

        public B addDefaultComponents(Collection<Index.Group> indexGroups)
        {
            checkNotNull(getTableMetadataRef());

            addComponents(ImmutableSet.of(Components.DATA, Components.STATS, Components.DIGEST, Components.TOC));

            if (getTableMetadataRef().getLocal().params.compression.isEnabled())
            {
                addComponents(ImmutableSet.of(Components.COMPRESSION_INFO));
            }
            else
            {
                // it would feel safer to actually add this component later in maybeWriteDigest(),
                // but the components are unmodifiable after construction
                addComponents(ImmutableSet.of(Components.CRC));
            }

            if (!indexGroups.isEmpty())
                addComponents(indexComponents(indexGroups));

            return (B) this;
        }

        private static Set<Component> indexComponents(Collection<Index.Group> indexGroups)
        {
            Set<Component> components = new HashSet<>();
            for (Index.Group group : indexGroups)
            {
                components.addAll(group.getComponents());
            }

            return components;
        }

        public B setSecondaryIndexGroups(Collection<Index.Group> indexGroups)
        {
            checkNotNull(indexGroups);
            this.indexGroups = ImmutableList.copyOf(indexGroups);
            return (B) this;
        }

        public MetadataCollector getMetadataCollector()
        {
            return metadataCollector;
        }

        public long getKeyCount()
        {
            return keyCount;
        }

        public long getRepairedAt()
        {
            return repairedAt;
        }

        public TimeUUID getPendingRepair()
        {
            return pendingRepair;
        }

        public boolean isTransientSSTable()
        {
            return transientSSTable;
        }

        public SerializationHeader getSerializationHeader()
        {
            return serializationHeader;
        }

        public List<Index.Group> getIndexGroups()
        {
            return indexGroups == null ? Collections.emptyList() : indexGroups;
        }

        public abstract MmappedRegionsCache getMmappedRegionsCache();

        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        public W build(LifecycleNewTracker lifecycleNewTracker, Owner owner)
        {
            checkNotNull(getComponents());

            validateRepairedMetadata(getRepairedAt(), getPendingRepair(), isTransientSSTable());

            return buildInternal(lifecycleNewTracker, owner);
        }

        protected abstract W buildInternal(LifecycleNewTracker lifecycleNewTracker, Owner owner);

        public SSTableZeroCopyWriter createZeroCopyWriter(LifecycleNewTracker lifecycleNewTracker, Owner owner)
        {
            return new SSTableZeroCopyWriter(this, lifecycleNewTracker, owner);
        }
    }
}
