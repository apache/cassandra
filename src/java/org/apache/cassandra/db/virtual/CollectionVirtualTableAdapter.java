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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.virtual.model.Column;
import org.apache.cassandra.db.virtual.walker.RowWalker;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.rows.Cell.NO_DELETION_TIME;
import static org.apache.cassandra.utils.FBUtilities.camelToSnake;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

/**
 * This is a virtual table that iteratively builds rows using a data set provided by internal collection.
 * Some metric views might be too large to fit in memory, for example, virtual tables that contain metrics
 * for all the keyspaces registered in the cluster. Such a technique is also facilitates keeping the low
 * memory footprint of the virtual tables in general.
 * <p>
 * It doesn't require the input data set to be sorted, but it does require that the partition keys are
 * provided in the order of the partitioner of the table metadata.
 */
public class CollectionVirtualTableAdapter<R> implements VirtualTable
{
    private static final Pattern ONLY_ALPHABET_PATTERN = Pattern.compile("[^a-zA-Z1-9]");
    private static final List<Pair<String, String>> knownAbbreviations = Arrays.asList(Pair.create("CAS", "Cas"),
                                                                                       Pair.create("CIDR", "Cidr"));

    /** The map of the supported converters for the column types. */
    private static final Map<Class<?>, ? extends AbstractType<?>> converters =
        ImmutableMap.<Class<?>, AbstractType<?>>builder()
                    .put(String.class, UTF8Type.instance)
                    .put(Integer.class, Int32Type.instance)
                    .put(Integer.TYPE, Int32Type.instance)
                    .put(Long.class, LongType.instance)
                    .put(Long.TYPE, LongType.instance)
                    .put(Float.class, FloatType.instance)
                    .put(Float.TYPE, FloatType.instance)
                    .put(Double.class, DoubleType.instance)
                    .put(Double.TYPE, DoubleType.instance)
                    .put(Boolean.class, BooleanType.instance)
                    .put(Boolean.TYPE, BooleanType.instance)
                    .put(Byte.class, ByteType.instance)
                    .put(Byte.TYPE, ByteType.instance)
                    .put(Short.class, ShortType.instance)
                    .put(Short.TYPE, ShortType.instance)
                    .put(UUID.class, UUIDType.instance)
                    .build();

    /**
     * The map is used to avoid getting column metadata for each regular column for each row.
     */
    private final ConcurrentHashMap<String, ColumnMetadata> columnMetas = new ConcurrentHashMap<>();
    private final RowWalker<R> walker;
    private final Iterable<R> data;
    private final Function<DecoratedKey, R> decorateKeyToRowExtractor;
    private final TableMetadata metadata;

    private CollectionVirtualTableAdapter(String keySpaceName,
                                          String tableName,
                                          String description,
                                          RowWalker<R> walker,
                                          Iterable<R> data)
    {
        this(keySpaceName, tableName, description, walker, data, null);
    }

    private CollectionVirtualTableAdapter(String keySpaceName,
                                          String tableName,
                                          String description,
                                          RowWalker<R> walker,
                                          Iterable<R> data,
                                          Function<DecoratedKey, R> keyToRowExtractor)
    {
        this.walker = walker;
        this.data = data;
        this.metadata = buildMetadata(keySpaceName, tableName, description, walker);
        this.decorateKeyToRowExtractor = keyToRowExtractor;
    }

    public static <C, R> CollectionVirtualTableAdapter<R> create(String keySpaceName,
                                                                 String rawTableName,
                                                                 String description,
                                                                 RowWalker<R> walker,
                                                                 Iterable<C> container,
                                                                 Function<C, R> rowFunc)
    {
        return new CollectionVirtualTableAdapter<>(keySpaceName,
                                                   virtualTableNameStyle(rawTableName),
                                                   description,
                                                   walker,
                                                   () -> StreamSupport.stream(container.spliterator(), false)
                                                                      .map(rowFunc).iterator());
    }

    public static <K, C, R> CollectionVirtualTableAdapter<R> createSinglePartitionedKeyFiltered(String keySpaceName,
                                                                                                String rawTableName,
                                                                                                String description,
                                                                                                RowWalker<R> walker,
                                                                                                Map<K, C> map,
                                                                                                Predicate<K> mapKeyFilter,
                                                                                                BiFunction<K, C, R> rowConverter)
    {
        return createSinglePartitioned(keySpaceName, rawTableName, description, walker, map, mapKeyFilter,
                                       Objects::nonNull, rowConverter);
    }

    public static <K, C, R> CollectionVirtualTableAdapter<R> createSinglePartitionedValueFiltered(String keySpaceName,
                                                                                                  String rawTableName,
                                                                                                  String description,
                                                                                                  RowWalker<R> walker,
                                                                                                  Map<K, C> map,
                                                                                                  Predicate<C> mapValueFilter,
                                                                                                  BiFunction<K, C, R> rowConverter)
    {
        return createSinglePartitioned(keySpaceName, rawTableName, description, walker, map, key -> true,
                                       mapValueFilter, rowConverter);
    }

    private static <K, C, R> CollectionVirtualTableAdapter<R> createSinglePartitioned(String keySpaceName,
                                                                                      String rawTableName,
                                                                                      String description,
                                                                                      RowWalker<R> walker,
                                                                                      Map<K, C> map,
                                                                                      Predicate<K> mapKeyFilter,
                                                                                      Predicate<C> mapValueFilter,
                                                                                      BiFunction<K, C, R> rowConverter)
    {
        assert walker.count(Column.Type.PARTITION_KEY) == 1 : "Partition key must be a single column";
        assert walker.count(Column.Type.CLUSTERING) == 0 : "Clustering columns are not supported";

        AtomicReference<Class<?>> partitionKeyClass = new AtomicReference<>();
        walker.visitMeta(new RowWalker.MetadataVisitor()
        {
            @Override
            public <T> void accept(Column.Type type, String columnName, Class<T> clazz)
            {
                if (type == Column.Type.PARTITION_KEY)
                    partitionKeyClass.set(clazz);
            }
        });

        return new CollectionVirtualTableAdapter<>(keySpaceName,
                                                   virtualTableNameStyle(rawTableName),
                                                   description,
                                                   walker,
                                                   () -> map.entrySet()
                                                            .stream()
                                                            .filter(e -> mapKeyFilter.test(e.getKey()))
                                                            .filter(e -> mapValueFilter.test(e.getValue()))
                                                            .map(e -> rowConverter.apply(e.getKey(), e.getValue()))
                                                            .iterator(),
                                                   decoratedKey ->
                                                   {
                                                       K partitionKey = compose(converters.get(partitionKeyClass.get()),
                                                                                decoratedKey.getKey());
                                                       boolean keyRequired = mapKeyFilter.test(partitionKey);
                                                       if (!keyRequired)
                                                           return null;

                                                       C value = map.get(partitionKey);
                                                       return mapValueFilter.test(value) ? rowConverter.apply(
                                                           partitionKey, value) : null;
                                                   });
    }

    public static String virtualTableNameStyle(String camel)
    {
        // Process sub names in the full metrics group name separately and then join them.
        // For example: "ClientRequest.Write-EACH_QUORUM" will be converted to "client_request_write_each_quorum".
        String[] subNames = ONLY_ALPHABET_PATTERN.matcher(camel).replaceAll(".").split("\\.");
        return Arrays.stream(subNames)
                     .map(CollectionVirtualTableAdapter::camelToSnakeWithAbbreviations)
                     .reduce((a, b) -> a + '_' + b)
                     .orElseThrow(() -> new IllegalArgumentException("Invalid table name: " + camel));
    }

    private static String camelToSnakeWithAbbreviations(String camel)
    {
        Pattern pattern = Pattern.compile("^[A-Z1-9_]+$");
        // Contains only uppercase letters, numbers and underscores, so it's already snake case.
        if (pattern.matcher(camel).matches())
            return toLowerCaseLocalized(camel);

        // Some special cases must be handled manually.
        String modifiedCamel = camel;
        for (Pair<String, String> replacement : knownAbbreviations)
            modifiedCamel = modifiedCamel.replace(replacement.left, replacement.right);

        return camelToSnake(modifiedCamel);
    }

    private TableMetadata buildMetadata(String keyspaceName, String tableName, String description, RowWalker<R> walker)
    {
        TableMetadata.Builder builder = TableMetadata.builder(keyspaceName, tableName)
                                                     .comment(description)
                                                     .kind(TableMetadata.Kind.VIRTUAL);

        List<AbstractType<?>> partitionKeyTypes = new ArrayList<>(walker.count(Column.Type.PARTITION_KEY));
        walker.visitMeta(new RowWalker.MetadataVisitor()
        {
            @Override
            public <T> void accept(Column.Type type, String columnName, Class<T> clazz)
            {
                switch (type)
                {
                    case PARTITION_KEY:
                        partitionKeyTypes.add(converters.get(clazz));
                        builder.addPartitionKeyColumn(columnName, converters.get(clazz));
                        break;
                    case CLUSTERING:
                        builder.addClusteringColumn(columnName, converters.get(clazz));
                        break;
                    case REGULAR:
                        builder.addRegularColumn(columnName, converters.get(clazz));
                        break;
                    default:
                        throw new IllegalStateException("Unknown column type: " + type);
                }
            }
        });

        if (partitionKeyTypes.size() == 1)
            builder.partitioner(new LocalPartitioner(partitionKeyTypes.get(0)));
        else if (partitionKeyTypes.size() > 1)
            builder.partitioner(new LocalPartitioner(CompositeType.getInstance(partitionKeyTypes)));

        return builder.build();
    }

    @Override
    public UnfilteredPartitionIterator select(DecoratedKey partitionKey,
                                              ClusteringIndexFilter clusteringFilter,
                                              ColumnFilter columnFilter)
    {
        if (!data.iterator().hasNext())
            return EmptyIterators.unfilteredPartition(metadata);

        NavigableMap<Clustering<?>, Row> rows = new TreeMap<>(metadata.comparator);
        Stream<CollectionRow> stream;
        if (decorateKeyToRowExtractor == null)
        {
            // The benchmark shows that if we continuously read the data from the virtual table e.g. by metric name,
            // then the parallel stream is slightly faster to get the first result, but for continuous reads it gives us
            // a higher GC pressure. The sequential stream is slightly slower to get the first result, but it has the
            // same throughput as the parallel stream, and it gives us less GC pressure.
            // See the details in the benchmark: https://gist.github.com/Mmuzaf/80c73b7f9441ff21f6d22efe5746541a
            stream = StreamSupport.stream(data.spliterator(), false)
                                  .map(row -> makeRow(row, columnFilter))
                                  .filter(cr -> partitionKey.equals(cr.key.get()))
                                  .filter(cr -> clusteringFilter.selects(cr.clustering));
        }
        else
        {
            R row = decorateKeyToRowExtractor.apply(partitionKey);
            if (row == null)
                return EmptyIterators.unfilteredPartition(metadata);
            stream = Stream.of(makeRow(row, columnFilter));
        }

        // If there are no clustering columns, we've found a unique partition that matches the partition key,
        // so we can stop the stream without looping through all the rows.
        if (walker.count(Column.Type.CLUSTERING) == 0)
            stream.findFirst().ifPresent(cr -> rows.put(cr.clustering, cr.rowSup.get()));
        else
            stream.forEach(cr -> rows.put(cr.clustering, cr.rowSup.get()));

        return new SingletonUnfilteredPartitionIterator(new DataRowUnfilteredIterator(partitionKey,
                                                                                      clusteringFilter,
                                                                                      columnFilter,
                                                                                      rows));
    }

    @Override
    public UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter)
    {
        return createPartitionIterator(metadata, new AbstractIterator<>()
        {
            private final Iterator<? extends UnfilteredRowIterator> partitions = buildDataRangeIterator(dataRange,
                                                                                                        columnFilter);

            @Override
            protected UnfilteredRowIterator computeNext()
            {
                return partitions.hasNext() ? partitions.next() : endOfData();
            }

            private Iterator<? extends UnfilteredRowIterator> buildDataRangeIterator(DataRange dataRange,
                                                                                     ColumnFilter columnFilter)
            {
                NavigableMap<DecoratedKey, NavigableMap<Clustering<?>, Row>> partitionMap = new ConcurrentSkipListMap<>(DecoratedKey.comparator);
                StreamSupport.stream(data.spliterator(), true)
                             .map(row -> makeRow(row, columnFilter))
                             .filter(cr -> dataRange.keyRange().contains(cr.key.get()))
                             .forEach(cr -> partitionMap.computeIfAbsent(cr.key.get(),
                                                                         key -> new TreeMap<>(metadata.comparator))
                                                        .put(cr.clustering, cr.rowSup.get()));

                return partitionMap.entrySet().stream().map(
                    e -> new DataRowUnfilteredIterator(e.getKey(), dataRange.clusteringIndexFilter(e.getKey()), columnFilter,
                                                       e.getValue())).iterator();
            }
        });
    }

    private class DataRowUnfilteredIterator extends AbstractUnfilteredRowIterator
    {
        private final Iterator<Row> rows;

        public DataRowUnfilteredIterator(DecoratedKey partitionKey,
                                         ClusteringIndexFilter indexFilter,
                                         ColumnFilter columnFilter,
                                         NavigableMap<Clustering<?>, Row> data)
        {
            super(CollectionVirtualTableAdapter.this.metadata,
                  partitionKey,
                  DeletionTime.LIVE,
                  columnFilter.queriedColumns(),
                  Rows.EMPTY_STATIC_ROW,
                  indexFilter.isReversed(),
                  EncodingStats.NO_STATS);
            this.rows = indexFilter.isReversed() ? data.descendingMap().values().iterator() : data.values().iterator();
        }

        @Override
        protected Unfiltered computeNext()
        {
            return rows.hasNext() ? rows.next() : endOfData();
        }
    }

    private CollectionRow makeRow(R row, ColumnFilter columnFilter)
    {
        assert metadata.partitionKeyColumns().size() == walker.count(Column.Type.PARTITION_KEY) :
            "Invalid number of partition key columns";
        assert metadata.clusteringColumns().size() == walker.count(Column.Type.CLUSTERING) :
            "Invalid number of clustering columns";

        Map<Column.Type, Object[]> fiterable = new EnumMap<>(Column.Type.class);
        fiterable.put(Column.Type.PARTITION_KEY, new Object[metadata.partitionKeyColumns().size()]);
        if (walker.count(Column.Type.CLUSTERING) > 0)
            fiterable.put(Column.Type.CLUSTERING, new Object[metadata.clusteringColumns().size()]);

        Map<ColumnMetadata, Supplier<?>> cells = new HashMap<>();

        walker.visitRow(row, new RowWalker.RowMetadataVisitor()
        {
            private int pIdx, cIdx = 0;

            @Override
            public <T> void accept(Column.Type type, String columnName, Class<T> clazz, Supplier<T> value)
            {
                switch (type)
                {
                    case PARTITION_KEY:
                        fiterable.get(type)[pIdx++] = value.get();
                        break;
                    case CLUSTERING:
                        fiterable.get(type)[cIdx++] = value.get();
                        break;
                    case REGULAR:
                    {
                        if (columnFilter.equals(ColumnFilter.NONE))
                            break;

                        // Push down the column filter to the walker, so we don't have to process the value if it's not queried
                        ColumnMetadata cm = columnMetas.computeIfAbsent(columnName, name -> metadata.getColumn(ByteBufferUtil.bytes(name)));
                        if (columnFilter.queriedColumns().contains(cm))
                            cells.put(cm, value);

                        break;
                    }
                    default:
                        throw new IllegalStateException("Unknown column type: " + type);
                }
            }
        });

        return new CollectionRow(() -> makeRowKey(metadata, fiterable.get(Column.Type.PARTITION_KEY)),
                                 makeRowClustering(metadata, fiterable.get(Column.Type.CLUSTERING)),
                                 clustering ->
                                 {
                                     Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
                                     rowBuilder.newRow(clustering);
                                     cells.forEach((column, value) -> {
                                         Object valueObj = value.get();
                                         if (valueObj == null)
                                             return;
                                         rowBuilder.addCell(BufferCell.live(column, NO_DELETION_TIME,
                                                                            decompose(column.type, valueObj)));
                                     });
                                     return rowBuilder.build();
                                 });
    }

    private static class CollectionRow
    {
        private final Supplier<DecoratedKey> key;
        private final Clustering<?> clustering;
        private final Supplier<Row> rowSup;

        public CollectionRow(Supplier<DecoratedKey> key, Clustering<?> clustering, Function<Clustering<?>, Row> rowSup)
        {
            this.key = new ValueHolder<>(key);
            this.clustering = clustering;
            this.rowSup = new ValueHolder<>(() -> rowSup.apply(clustering));
        }

        private static class ValueHolder<T> implements Supplier<T>
        {
            private final Supplier<T> delegate;
            private volatile T value;

            public ValueHolder(Supplier<T> delegate)
            {
                this.delegate = delegate;
            }

            @Override
            public T get()
            {
                if (value == null)
                    value = delegate.get();

                return value;
            }
        }
    }

    private static Clustering<?> makeRowClustering(TableMetadata metadata, Object... clusteringValues)
    {
        if (clusteringValues == null || clusteringValues.length == 0)
            return Clustering.EMPTY;

        ByteBuffer[] clusteringByteBuffers = new ByteBuffer[clusteringValues.length];
        for (int i = 0; i < clusteringValues.length; i++)
            clusteringByteBuffers[i] = decompose(metadata.clusteringColumns().get(i).type, clusteringValues[i]);
        return Clustering.make(clusteringByteBuffers);
    }

    /**
     * @param table              the table metadata
     * @param partitionKeyValues the partition key values
     * @return the decorated key
     */
    private static DecoratedKey makeRowKey(TableMetadata table, Object... partitionKeyValues)
    {
        ByteBuffer key;
        if (partitionKeyValues.length > 1)
            key = ((CompositeType) table.partitionKeyType).decompose(partitionKeyValues);
        else
            key = decompose(table.partitionKeyType, partitionKeyValues[0]);
        return table.partitioner.decorateKey(key);
    }

    private static UnfilteredPartitionIterator createPartitionIterator(TableMetadata metadata,
                                                                       Iterator<UnfilteredRowIterator> partitions)
    {
        return new AbstractUnfilteredPartitionIterator()
        {
            public UnfilteredRowIterator next()
            {
                return partitions.next();
            }

            public boolean hasNext()
            {
                return partitions.hasNext();
            }

            public TableMetadata metadata()
            {
                return metadata;
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T> ByteBuffer decompose(AbstractType<?> type, T value)
    {
        return ((AbstractType<T>) type).decompose(value);
    }

    @SuppressWarnings("unchecked")
    private static <T> T compose(AbstractType<?> type, ByteBuffer value)
    {
        return (T) type.compose(value);
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        throw new InvalidRequestException("Modification is not supported by table " + metadata);
    }

    @Override
    public void truncate()
    {
        throw new InvalidRequestException("Truncate is not supported by table " + metadata);
    }
}