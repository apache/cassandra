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
package org.apache.cassandra.index.sasi;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.PerSSTableIndexWriter;
import org.apache.cassandra.index.sasi.plan.QueryPlan;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class SASIIndex implements Index, INotificationConsumer
{
    private static class SASIIndexBuildingSupport implements IndexBuildingSupport
    {
        public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs,
                                                       Set<Index> indexes,
                                                       Collection<SSTableReader> sstablesToRebuild)
        {
            NavigableMap<SSTableReader, Map<ColumnDefinition, ColumnIndex>> sstables = new TreeMap<>((a, b) -> {
                return Integer.compare(a.descriptor.generation, b.descriptor.generation);
            });

            indexes.stream()
                   .filter((i) -> i instanceof SASIIndex)
                   .forEach((i) -> {
                       SASIIndex sasi = (SASIIndex) i;
                       sstablesToRebuild.stream()
                                        .filter((sstable) -> !sasi.index.hasSSTable(sstable))
                                        .forEach((sstable) -> {
                                            Map<ColumnDefinition, ColumnIndex> toBuild = sstables.get(sstable);
                                            if (toBuild == null)
                                                sstables.put(sstable, (toBuild = new HashMap<>()));

                                            toBuild.put(sasi.index.getDefinition(), sasi.index);
                                        });
                   });

            return new SASIIndexBuilder(cfs, sstables);
        }
    }

    private static final SASIIndexBuildingSupport INDEX_BUILDER_SUPPORT = new SASIIndexBuildingSupport();

    private final ColumnFamilyStore baseCfs;
    private final IndexMetadata config;
    private final ColumnIndex index;

    public SASIIndex(ColumnFamilyStore baseCfs, IndexMetadata config)
    {
        this.baseCfs = baseCfs;
        this.config = config;

        ColumnDefinition column = CassandraIndex.parseTarget(baseCfs.metadata, config).left;
        this.index = new ColumnIndex(baseCfs.metadata.getKeyValidator(), column, config);

        baseCfs.getTracker().subscribe(this);
    }

    public static Map<String, String> validateOptions(Map<String, String> options)
    {
        return Collections.emptyMap();
    }

    public void register(IndexRegistry registry)
    {
        registry.registerIndex(this);
    }

    public IndexMetadata getIndexMetadata()
    {
        return config;
    }

    public Callable<?> getInitializationTask()
    {
        return null;
    }

    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
    {
        return null;
    }

    public Callable<?> getBlockingFlushTask()
    {
        return null; // SASI indexes are flushed along side memtable
    }

    public Callable<?> getInvalidateTask()
    {
        return getTruncateTask(FBUtilities.timestampMicros());
    }

    public Callable<?> getTruncateTask(long truncatedAt)
    {
        return () -> {
            index.dropData(truncatedAt);
            return null;
        };
    }

    public boolean shouldBuildBlocking()
    {
        return true;
    }

    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    public boolean indexes(PartitionColumns columns)
    {
        return columns.contains(index.getDefinition());
    }

    public boolean dependsOn(ColumnDefinition column)
    {
        return index.getDefinition().compareTo(column) == 0;
    }

    public boolean supportsExpression(ColumnDefinition column, Operator operator)
    {
        return dependsOn(column);
    }

    public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        return filter.withoutExpressions();
    }

    public long getEstimatedResultRows()
    {
        // this is temporary (until proper QueryPlan is integrated into Cassandra)
        // and allows us to priority SASI indexes if any in the query since they
        // are going to be more efficient, to query and intersect, than built-in indexes.
        return Long.MIN_VALUE;
    }

    public void validate(PartitionUpdate update) throws InvalidRequestException
    {}

    public Indexer indexerFor(DecoratedKey key, PartitionColumns columns, int nowInSec, OpOrder.Group opGroup, IndexTransaction.Type transactionType)
    {
        return new Indexer()
        {
            public void begin()
            {}

            public void partitionDelete(DeletionTime deletionTime)
            {}

            public void rangeTombstone(RangeTombstone tombstone)
            {}

            public void insertRow(Row row)
            {
                if (isNewData())
                    adjustMemtableSize(index.index(key, row), opGroup);
            }

            public void updateRow(Row oldRow, Row newRow)
            {
                insertRow(newRow);
            }

            public void removeRow(Row row)
            {}

            public void finish()
            {}

            // we are only interested in the data from Memtable
            // everything else is going to be handled by SSTableWriter observers
            private boolean isNewData()
            {
                return transactionType == IndexTransaction.Type.UPDATE;
            }

            public void adjustMemtableSize(long additionalSpace, OpOrder.Group opGroup)
            {
                baseCfs.getTracker().getView().getCurrentMemtable().getAllocator().onHeap().allocate(additionalSpace, opGroup);
            }
        };
    }

    public Searcher searcherFor(ReadCommand command) throws InvalidRequestException
    {
        CFMetaData config = command.metadata();
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(config.cfId);
        return controller -> new QueryPlan(cfs, command, DatabaseDescriptor.getRangeRpcTimeout()).execute(controller);
    }

    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, OperationType opType)
    {
        return newWriter(baseCfs.metadata.getKeyValidator(), descriptor, Collections.singletonMap(index.getDefinition(), index), opType);
    }

    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command)
    {
        return (partitionIterator, readCommand) -> partitionIterator;
    }

    public IndexBuildingSupport getBuildTaskSupport()
    {
        return INDEX_BUILDER_SUPPORT;
    }

    public void handleNotification(INotification notification, Object sender)
    {
        // unfortunately, we can only check the type of notification via instanceof :(
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification notice = (SSTableAddedNotification) notification;
            index.update(Collections.<SSTableReader>emptyList(), Iterables.toList(notice.added));
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification notice = (SSTableListChangedNotification) notification;
            index.update(notice.removed, notice.added);
        }
        else if (notification instanceof MemtableRenewedNotification)
        {
            index.switchMemtable();
        }
    }

    public ColumnIndex getIndex()
    {
        return index;
    }

    protected static PerSSTableIndexWriter newWriter(AbstractType<?> keyValidator,
                                                     Descriptor descriptor,
                                                     Map<ColumnDefinition, ColumnIndex> indexes,
                                                     OperationType opType)
    {
        return new PerSSTableIndexWriter(keyValidator, descriptor, opType, indexes);
    }
}
