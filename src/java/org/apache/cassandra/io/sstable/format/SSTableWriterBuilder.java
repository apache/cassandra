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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableBuilder;
import org.apache.cassandra.io.sstable.SSTableZeroCopyWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.TimeUUID;

public abstract class SSTableWriterBuilder<W extends SSTableWriter<?>, B extends SSTableWriterBuilder<W, B>> extends SSTableBuilder<W, B>
{
    private MetadataCollector metadataCollector;
    private long keyCount;
    private long repairedAt;
    private TimeUUID pendingRepair;
    private boolean transientSSTable;
    private SerializationHeader serializationHeader;
    private Collection<SSTableFlushObserver> flushObservers;

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

    public B setFlushObservers(Collection<SSTableFlushObserver> flushObservers)
    {
        this.flushObservers = ImmutableList.copyOf(flushObservers);
        return (B) this;
    }

    public B addDefaultComponents()
    {
        Preconditions.checkNotNull(getTableMetadataRef());

        Set<Component> components = new HashSet<>(descriptor.getFormat().writeComponents());
        if (this.getComponents() != null)
            components.addAll(this.getComponents());

        if (FilterComponent.shouldUseBloomFilter(getTableMetadataRef().getLocal().params.bloomFilterFpChance))
        {
            components.add(Component.FILTER);
        }

        if (getTableMetadataRef().getLocal().params.compression.isEnabled())
        {
            components.add(Component.COMPRESSION_INFO);
        }
        else
        {
            // it would feel safer to actually add this component later in maybeWriteDigest(),
            // but the components are unmodifiable after construction
            components.add(Component.CRC);
        }

        setComponents(components);

        return (B) this;
    }

    public B addFlushObserversForSecondaryIndexes(Collection<Index> indexes, OperationType operationType)
    {
        if (indexes == null)
            return (B) this;

        Collection<SSTableFlushObserver> current = this.flushObservers != null ? this.flushObservers : Collections.emptyList();
        List<SSTableFlushObserver> observers = new ArrayList<>(indexes.size() + current.size());
        observers.addAll(current);

        for (Index index : indexes)
        {
            SSTableFlushObserver observer = index.getFlushObserver(descriptor, operationType);
            if (observer != null)
            {
                observer.begin();
                observers.add(observer);
            }
        }

        return setFlushObservers(observers);
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

    public Collection<SSTableFlushObserver> getFlushObservers()
    {
        return flushObservers;
    }

    public SSTableWriterBuilder(Descriptor descriptor)
    {
        super(descriptor);
    }

    public W build(LifecycleNewTracker lifecycleNewTracker)
    {
        Preconditions.checkNotNull(getComponents());

        SSTable.validateRepairedMetadata(getRepairedAt(), getPendingRepair(), isTransientSSTable());

        return buildInternal(lifecycleNewTracker);
    }

    protected abstract W buildInternal(LifecycleNewTracker lifecycleNewTracker);

    public SSTableZeroCopyWriter createZeroCopyWriter(LifecycleNewTracker lifecycleNewTracker)
    {
        return new SSTableZeroCopyWriter(this, lifecycleNewTracker);
    }
}