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

package org.apache.cassandra.db.virtual;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A virtual table for querying partition keys of SSTables in a specific keyspace.
 *
 * <p>This table is implemented as a virtual table in Cassandra, meaning it does not
 * store data persistently on disk but instead derives its data from live metadata.
 *
 * <p>The CQL equivalent of this virtual table is:
 * <pre>
 * CREATE TABLE system_views.partition_key_statistics (
 *     keyspace_name TEXT,
 *     table_name TEXT,
 *     token_value INT,
 *     key TEXT,
 *     size_estimate COUNTER,
 *     sstables COUNTER,
 *     PRIMARY KEY ((keyspace_name, table_name), token_value, key)
 * );
 * </pre>
 *
 * <p>Note:
 * <ul>
 *     <li>The `size_estimate` and `sstables` columns represent aggregate information about SSTable sizes and counts.</li>
 *     <li>Range queries across multiple tables and updates are not supported as this is a read-only table.</li>
 * </ul>
 */
public class PartitionKeyStatsTable implements VirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionKeyStatsTable.class);
    public static final String NAME = "partition_key_statistics";

    private static final String TABLE_READ_ONLY_ERROR = "The specified table is read-only.";
    private static final String UNSUPPORTED_RANGE_QUERY_ERROR = "Range queries are not supported. Please provide both a keyspace and a table name.";
    private static final String REVERSED_QUERY_ERROR = "Reversed queries are not supported.";
    private static final String KEYSPACE_NOT_EXIST_ERROR = "The keyspace '%s' does not exist.";
    private static final String TABLE_NOT_EXIST_ERROR = "The table '%s' does not exist in the keyspace '%s'.";
    private static final String KEY_ONLY_EQUALS_ERROR = "The 'key' column can only be used in an equality query for this virtual table.";
    private static final String KEY_NOT_WITHIN_BOUNDS_ERROR = "The specified 'key' is not within the provided token value bounds.";
    private static final String PARTITIONER_NOT_SUPPORTED = "Partitioner '%s' for table '%s' in keyspace '%s' is not supported.";

    private static final String COLUMN_KEYSPACE_NAME = "keyspace_name";
    private static final String COLUMN_TABLE_NAME = "table_name";
    private static final String COLUMN_TOKEN_VALUE = "token_value";
    private static final String COLUMN_KEY = "key";
    private static final String COLUMN_SIZE_ESTIMATE = "size_estimate";
    private static final String COLUMN_SSTABLES = "sstables";

    private final TableMetadata metadata;
    private final ColumnMetadata sizeEstimateColumn;
    private final ColumnMetadata sstablesColumn;

    @VisibleForTesting
    final CopyOnWriteArrayList<Consumer<DecoratedKey>> readListener = new CopyOnWriteArrayList<>();

    public PartitionKeyStatsTable(String keyspace)
    {
        this.metadata = TableMetadata.builder(keyspace, NAME)
                                     .kind(TableMetadata.Kind.VIRTUAL)
                                     .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance)))
                                     .addPartitionKeyColumn(COLUMN_KEYSPACE_NAME, UTF8Type.instance)
                                     .addPartitionKeyColumn(COLUMN_TABLE_NAME, UTF8Type.instance)
                                     .addClusteringColumn(COLUMN_TOKEN_VALUE, IntegerType.instance)
                                     .addClusteringColumn(COLUMN_KEY, UTF8Type.instance)
                                     .addRegularColumn(COLUMN_SIZE_ESTIMATE, CounterColumnType.instance)
                                     .addRegularColumn(COLUMN_SSTABLES, CounterColumnType.instance)
                                     .build();
        sizeEstimateColumn = metadata.regularColumns().getSimple(0);
        sstablesColumn = metadata.regularColumns().getSimple(1);
    }

    @Override
    public UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter, RowFilter rowFilter)
    {
        if (clusteringIndexFilter.isReversed())
            throw new InvalidRequestException(REVERSED_QUERY_ERROR);

        ByteBuffer[] key = ((CompositeType) this.metadata.partitionKeyType).split(partitionKey.getKey());
        String keyspace = UTF8Type.instance.getString(key[0]);
        String table = UTF8Type.instance.getString(key[1]);

        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        if (ksm == null)
            throw invalidRequest(KEYSPACE_NOT_EXIST_ERROR, keyspace);

        TableMetadata metadata = ksm.getTableOrViewNullable(table);
        if (metadata == null)
            throw invalidRequest(TABLE_NOT_EXIST_ERROR, table, keyspace);

        if (!metadata.partitioner.supportsSplitting())
            throw invalidRequest(PARTITIONER_NOT_SUPPORTED, metadata.partitioner.getClass().getName(), table, keyspace);

        AbstractBounds<PartitionPosition> range = getBounds(metadata, clusteringIndexFilter, rowFilter);
        return new SingletonUnfilteredPartitionIterator(select(partitionKey, metadata, clusteringIndexFilter, range));
    }

    private List<SSTableReader> getSStables(TableMetadata metadata, AbstractBounds<PartitionPosition> range)
    {
        return Lists.newArrayList(ColumnFamilyStore.getIfExists(metadata).getTracker().getView().liveSSTablesInBounds(range.left, range.right));
    }

    private UnfilteredRowIterator select(DecoratedKey partitionKey, TableMetadata metadata, ClusteringIndexFilter clusteringIndexFilter, AbstractBounds<PartitionPosition> range)
    {
        List<SSTableReader> sstables = getSStables(metadata, range);
        if (sstables.isEmpty())
            return UnfilteredRowIterators.noRowsIterator(metadata, partitionKey, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, false);

        List<UnfilteredRowIterator> sstableIterators = Lists.newArrayList();
        for (SSTableReader sstable : sstables)
            sstableIterators.add(getSStableRowIterator(metadata, partitionKey, sstable, clusteringIndexFilter, range));

        return UnfilteredRowIterators.merge(sstableIterators);
    }

    private UnfilteredRowIterator getSStableRowIterator(TableMetadata target, DecoratedKey partitionKey, SSTableReader sstable, ClusteringIndexFilter filter, AbstractBounds<PartitionPosition> range)
    {
        final KeyReader reader;
        try
        {
            // ignore warning on try-with-resources, the reader will be closed on endOfData or close
            reader = sstable.keyReader(range.left);
        }
        catch (IOException e)
        {
            logger.error("Error generating keyReader for SSTable: {}", sstable, e);
            throw new RuntimeException(e);
        }

        return new AbstractUnfilteredRowIterator(metadata, partitionKey, DeletionTime.LIVE,
                                                 metadata.regularAndStaticColumns(), Rows.EMPTY_STATIC_ROW,
                                                 false, EncodingStats.NO_STATS)
        {
            public Unfiltered endOfData()
            {
                reader.close();
                return super.endOfData();
            }

            public void close()
            {
                reader.close();
            }

            private Row buildRow(Clustering<?> clustering, long size)
            {
                Row.Builder row = BTreeRow.sortedBuilder();
                row.newRow(clustering);
                row.addCell(cell(sizeEstimateColumn, CounterContext.instance().createUpdate(size)));
                row.addCell(cell(sstablesColumn, CounterContext.instance().createUpdate(1)));
                return row.build();
            }

            @Override
            protected Unfiltered computeNext()
            {
                while (!reader.isExhausted())
                {
                    DecoratedKey key = target.partitioner.decorateKey(reader.key());

                    for (Consumer<DecoratedKey> listener : readListener)
                        listener.accept(key);

                    // Store the reader's current data position to calculate size later
                    long lastPosition = reader.dataPosition();
                    try
                    {
                        // Advance the reader to the next key for the next iteration. Also by moving to next key
                        // we move the dataPosition to the start of the next key for calculating size
                        reader.advance();
                    }
                    catch (IOException e)
                    {
                        logger.error("Error advancing reader for SSTable: {}", sstable, e);
                        return endOfData();
                    }

                    // Calculate the size of the current key. If EOF use the length of the file
                    long current = reader.dataPosition() == -1 ? sstable.uncompressedLength() : reader.dataPosition();
                    long size = current - lastPosition;

                    String keyString = target.partitionKeyType.getString(key.getKey());

                    // Check if the current key is outside the queried range; if so, stop
                    if (range.right.compareTo(key) < 0)
                        return endOfData();

                    // Convert the token to a string and create a clustering object
                    String tokenString = key.getToken().toString();
                    Clustering<?> clustering = Clustering.make(
                    IntegerType.instance.decompose(new BigInteger(tokenString)),
                    UTF8Type.instance.decompose(keyString)
                    );

                    // Check if the current clustering matches the filter; if so, return the row
                    if (filter.selects(clustering))
                        return buildRow(clustering, size);
                }
                return endOfData();
            }
        };
    }

    /**
     * This converts the clustering token/key into the partition level token/key for the target table. Also provides an
     * optimization from RowFilter when a `key` is specified with or without the clustering `token` being set.
     */
    private AbstractBounds<PartitionPosition> getBounds(TableMetadata target, ClusteringIndexFilter clusteringIndexFilter, RowFilter rowFilter)
    {
        Slices s = clusteringIndexFilter.getSlices(target);
        Token startToken = target.partitioner.getMinimumToken();
        Token endToken = target.partitioner.getMaximumTokenForSplitting();
        BigInteger startTokenValue = new BigInteger(endToken.getTokenValue().toString(), 10);
        BigInteger endTokenValue = new BigInteger(startToken.getTokenValue().toString(), 10);

        // find min/max token values from the clustering key
        for (int i = 0; i < s.size(); i++)
        {
            Slice slice = s.get(i);
            if (!slice.start().isEmpty())
            {
                startTokenValue = startTokenValue.min(IntegerType.instance.compose(slice.start().bufferAt(0)));
                startToken = target.partitioner.getTokenFactory().fromString(startTokenValue.toString());
            }
            if (!slice.end().isEmpty())
            {
                endTokenValue = endTokenValue.max(IntegerType.instance.compose(slice.end().bufferAt(0)));
                endToken = target.partitioner.getTokenFactory().fromString(endTokenValue.toString());
            }
        }

        // override min/max of token if the `key` is specified
        for (RowFilter.Expression expression : rowFilter.getExpressions())
        {
            if (expression.column().name.toString().equals(COLUMN_KEY))
            {
                if (expression.operator() != Operator.EQ)
                    throw new InvalidRequestException(KEY_ONLY_EQUALS_ERROR);

                String keyString = UTF8Type.instance.compose(expression.getIndexValue());
                ByteBuffer keyAsBB;
                try
                {
                    keyAsBB = target.partitionKeyType.fromString(keyString);
                }
                catch (MarshalException ex)
                {
                    throw new InvalidRequestException(ex.getMessage());
                }
                DecoratedKey decoratedKey = target.partitioner.decorateKey(keyAsBB);

                if (!DataRange.forKeyRange(new Range<>(startToken.minKeyBound(), endToken.maxKeyBound())).contains(decoratedKey.getToken().minKeyBound()))
                    throw new InvalidRequestException(KEY_NOT_WITHIN_BOUNDS_ERROR);

                return Bounds.bounds(decoratedKey, true, decoratedKey, true);
            }
        }
        return Bounds.bounds(startToken.minKeyBound(), true, endToken.maxKeyBound(), true);
    }

    private static Cell<?> cell(ColumnMetadata column, ByteBuffer value)
    {
        return BufferCell.live(column, 1L, value);
    }

    @Override
    public TableMetadata metadata()
    {
        return this.metadata;
    }

    @Override
    public UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter, RowFilter rowFilter)
    {
        throw new InvalidRequestException(UNSUPPORTED_RANGE_QUERY_ERROR);
    }

    @Override
    public void truncate()
    {
        throw new InvalidRequestException(TABLE_READ_ONLY_ERROR);
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        throw new InvalidRequestException(TABLE_READ_ONLY_ERROR);
    }
}
