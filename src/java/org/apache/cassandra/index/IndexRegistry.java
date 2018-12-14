/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * The collection of all Index instances for a base table.
 * The SecondaryIndexManager for a ColumnFamilyStore contains an IndexRegistry
 * (actually it implements this interface at present) and Index implementations
 * register in order to:
 * i) subscribe to the stream of updates being applied to partitions in the base table
 * ii) provide searchers to support queries with the relevant search predicates
 */
public interface IndexRegistry
{
    /**
     * An empty {@code IndexRegistry}
     */
    public static final IndexRegistry EMPTY = new IndexRegistry()
    {
        @Override
        public void unregisterIndex(Index index)
        {
        }

        @Override
        public void registerIndex(Index index)
        {
        }

        @Override
        public Collection<Index> listIndexes()
        {
            return Collections.emptyList();
        }

        @Override
        public Index getIndex(IndexMetadata indexMetadata)
        {
            return null;
        }

        @Override
        public Optional<Index> getBestIndexFor(RowFilter.Expression expression)
        {
            return Optional.empty();
        }

        @Override
        public void validate(PartitionUpdate update)
        {
        }
    };

    /**
     * An {@code IndexRegistry} intended for use when Cassandra is initialized in client or tool mode.
     * Contains a single stub {@code Index} which possesses no actual indexing or searching capabilities
     * but enables query validation and preparation to succeed. Useful for tools which need to prepare
     * CQL statements without instantiating the whole ColumnFamilyStore infrastructure.
     */
    public static final IndexRegistry NON_DAEMON = new IndexRegistry()
    {
        Index index = new Index()
        {
            public Callable<?> getInitializationTask()
            {
                return null;
            }

            public IndexMetadata getIndexMetadata()
            {
                return null;
            }

            public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
            {
                return null;
            }

            public void register(IndexRegistry registry)
            {

            }

            public Optional<ColumnFamilyStore> getBackingTable()
            {
                return Optional.empty();
            }

            public Callable<?> getBlockingFlushTask()
            {
                return null;
            }

            public Callable<?> getInvalidateTask()
            {
                return null;
            }

            public Callable<?> getTruncateTask(long truncatedAt)
            {
                return null;
            }

            public boolean shouldBuildBlocking()
            {
                return false;
            }

            public boolean dependsOn(ColumnMetadata column)
            {
                return false;
            }

            public boolean supportsExpression(ColumnMetadata column, Operator operator)
            {
                return true;
            }

            public AbstractType<?> customExpressionValueType()
            {
                return BytesType.instance;
            }

            public RowFilter getPostIndexQueryFilter(RowFilter filter)
            {
                return null;
            }

            public long getEstimatedResultRows()
            {
                return 0;
            }

            public void validate(PartitionUpdate update) throws InvalidRequestException
            {
            }

            public Indexer indexerFor(DecoratedKey key, RegularAndStaticColumns columns, int nowInSec, WriteContext ctx, IndexTransaction.Type transactionType)
            {
                return null;
            }

            public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command)
            {
                return null;
            }

            public Searcher searcherFor(ReadCommand command)
            {
                return null;
            }
        };

        public void registerIndex(Index index)
        {
        }

        public void unregisterIndex(Index index)
        {
        }

        public Index getIndex(IndexMetadata indexMetadata)
        {
            return index;
        }

        public Collection<Index> listIndexes()
        {
            return Collections.singletonList(index);
        }

        public Optional<Index> getBestIndexFor(RowFilter.Expression expression)
        {
            return Optional.empty();
        }

        public void validate(PartitionUpdate update)
        {

        }
    };

    void registerIndex(Index index);
    void unregisterIndex(Index index);

    Index getIndex(IndexMetadata indexMetadata);
    Collection<Index> listIndexes();

    Optional<Index> getBestIndexFor(RowFilter.Expression expression);

    /**
     * Called at write time to ensure that values present in the update
     * are valid according to the rules of all registered indexes which
     * will process it. The partition key as well as the clustering and
     * cell values for each row in the update may be checked by index
     * implementations
     *
     * @param update PartitionUpdate containing the values to be validated by registered Index implementations
     */
    void validate(PartitionUpdate update);

    /**
     * Returns the {@code IndexRegistry} associated to the specified table.
     *
     * @param table the table metadata
     * @return the {@code IndexRegistry} associated to the specified table
     */
    public static IndexRegistry obtain(TableMetadata table)
    {
        if (!DatabaseDescriptor.isDaemonInitialized())
            return NON_DAEMON;

        return table.isVirtual() ? EMPTY : Keyspace.openAndGetStore(table).indexManager;
    }
}
