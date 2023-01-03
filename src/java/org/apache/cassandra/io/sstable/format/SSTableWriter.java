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

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Transactional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is the API all table writers must implement.
 *
 * TableWriter.create() is the primary way to create a writer for a particular format.
 * The format information is part of the Descriptor.
 */
public abstract class SSTableWriter<RIE extends AbstractRowIndexEntry> extends SSTable implements Transactional
{
    protected long repairedAt;
    protected TimeUUID pendingRepair;
    protected boolean isTransient;
    protected long maxDataAge = -1;
    protected final long keyCount;
    protected final MetadataCollector metadataCollector;
    protected final SerializationHeader header;
    protected final TransactionalProxy txnProxy = txnProxy();
    protected final Collection<SSTableFlushObserver> observers;

    protected abstract TransactionalProxy txnProxy();

    protected SSTableWriter(SSTableWriterBuilder<?, ?> builder, LifecycleNewTracker lifecycleNewTracker)
    {
        super(builder);
        checkNotNull(builder.getFlushObservers());
        checkNotNull(builder.getMetadataCollector());
        checkNotNull(builder.getSerializationHeader());

        this.keyCount = builder.getKeyCount();
        this.repairedAt = builder.getRepairedAt();
        this.pendingRepair = builder.getPendingRepair();
        this.isTransient = builder.isTransientSSTable();
        this.metadataCollector = builder.getMetadataCollector();
        this.header = builder.getSerializationHeader();
        this.observers = builder.getFlushObservers();

        lifecycleNewTracker.trackNew(this);
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
    public abstract RIE append(UnfilteredRowIterator iterator);

    public abstract long getFilePointer();

    public abstract long getOnDiskFilePointer();

    public long getEstimatedOnDiskBytesWritten()
    {
        return getOnDiskFilePointer();
    }

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

    public void setOpenResult(boolean openResult)
    {
        txnProxy.openResult = openResult;
    }

    /**
     * Open the resultant SSTableReader before it has been fully written
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
            return txnProxy.commit(accumulate);
        }
        finally
        {
            observers.forEach(SSTableFlushObserver::complete);
        }
    }

    public final Throwable abort(Throwable accumulate)
    {
        return txnProxy.abort(accumulate);
    }

    public final void close()
    {
        txnProxy.close();
    }

    public final void abort()
    {
        txnProxy.abort();
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

    public interface Factory<W extends SSTableWriter<?>, B extends SSTableWriterBuilder<W, B>>
    {
        long estimateSize(SSTableSizeParameters parameters);

        B builder(Descriptor descriptor);
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
    }
}