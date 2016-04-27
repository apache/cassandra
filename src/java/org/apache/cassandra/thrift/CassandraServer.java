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
package org.apache.cassandra.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.google.common.base.Joiner;
import com.google.common.collect.*;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.*;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.thrift.TException;

public class CassandraServer implements Cassandra.Iface
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraServer.class);

    private final static int COUNT_PAGE_SIZE = 1024;

    private final static List<ColumnOrSuperColumn> EMPTY_COLUMNS = Collections.emptyList();

    /*
     * RequestScheduler to perform the scheduling of incoming requests
     */
    private final IRequestScheduler requestScheduler;

    public CassandraServer()
    {
        requestScheduler = DatabaseDescriptor.getRequestScheduler();
        registerMetrics();
    }

    public ThriftClientState state()
    {
        return ThriftSessionManager.instance.currentSession();
    }

    protected PartitionIterator read(List<SinglePartitionReadCommand> commands, org.apache.cassandra.db.ConsistencyLevel consistency_level, ClientState cState)
    throws org.apache.cassandra.exceptions.InvalidRequestException, UnavailableException, TimedOutException
    {
        try
        {
            schedule(DatabaseDescriptor.getReadRpcTimeout());
            try
            {
                return StorageProxy.read(new SinglePartitionReadCommand.Group(commands, DataLimits.NONE), consistency_level, cState);
            }
            finally
            {
                release();
            }
        }
        catch (RequestExecutionException e)
        {
            throw ThriftConversion.rethrow(e);
        }
    }

    public List<ColumnOrSuperColumn> thriftifyColumns(CFMetaData metadata, Iterator<LegacyLayout.LegacyCell> cells)
    {
        ArrayList<ColumnOrSuperColumn> thriftColumns = new ArrayList<>();
        while (cells.hasNext())
        {
            LegacyLayout.LegacyCell cell = cells.next();
            thriftColumns.add(thriftifyColumnWithName(metadata, cell, cell.name.encode(metadata)));
        }
        return thriftColumns;
    }

    private ColumnOrSuperColumn thriftifyColumnWithName(CFMetaData metadata, LegacyLayout.LegacyCell cell, ByteBuffer newName)
    {
        if (cell.isCounter())
            return new ColumnOrSuperColumn().setCounter_column(thriftifySubCounter(metadata, cell).setName(newName));
        else
            return new ColumnOrSuperColumn().setColumn(thriftifySubColumn(cell, newName));
    }

    private Column thriftifySubColumn(CFMetaData metadata, LegacyLayout.LegacyCell cell)
    {
        return thriftifySubColumn(cell, cell.name.encode(metadata));
    }

    private Column thriftifySubColumn(LegacyLayout.LegacyCell cell, ByteBuffer name)
    {
        assert !cell.isCounter();

        Column thrift_column = new Column(name).setValue(cell.value).setTimestamp(cell.timestamp);
        if (cell.isExpiring())
            thrift_column.setTtl(cell.ttl);
        return thrift_column;
    }

    private List<Column> thriftifyColumnsAsColumns(CFMetaData metadata, Iterator<LegacyLayout.LegacyCell> cells)
    {
        List<Column> thriftColumns = new ArrayList<>();
        while (cells.hasNext())
            thriftColumns.add(thriftifySubColumn(metadata, cells.next()));
        return thriftColumns;
    }

    private CounterColumn thriftifySubCounter(CFMetaData metadata, LegacyLayout.LegacyCell cell)
    {
        assert cell.isCounter();
        return new CounterColumn(cell.name.encode(metadata), CounterContext.instance().total(cell.value));
    }

    private List<ColumnOrSuperColumn> thriftifySuperColumns(CFMetaData metadata,
                                                            Iterator<LegacyLayout.LegacyCell> cells,
                                                            boolean subcolumnsOnly,
                                                            boolean isCounterCF,
                                                            boolean reversed)
    {
        if (subcolumnsOnly)
        {
            ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<>();
            while (cells.hasNext())
            {
                LegacyLayout.LegacyCell cell = cells.next();
                thriftSuperColumns.add(thriftifyColumnWithName(metadata, cell, cell.name.superColumnSubName()));
            }
            // Generally, cells come reversed if the query is reverse. However, this is not the case within a super column because
            // internally a super column is a map within a row and those are never returned reversed.
            if (reversed)
                Collections.reverse(thriftSuperColumns);
            return thriftSuperColumns;
        }
        else
        {
            if (isCounterCF)
                return thriftifyCounterSuperColumns(metadata, cells, reversed);
            else
                return thriftifySuperColumns(cells, reversed);
        }
    }

    private List<ColumnOrSuperColumn> thriftifySuperColumns(Iterator<LegacyLayout.LegacyCell> cells, boolean reversed)
    {
        ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<>();
        SuperColumn current = null;
        while (cells.hasNext())
        {
            LegacyLayout.LegacyCell cell = cells.next();
            ByteBuffer scName = cell.name.superColumnName();
            if (current == null || !scName.equals(current.bufferForName()))
            {
                // Generally, cells come reversed if the query is reverse. However, this is not the case within a super column because
                // internally a super column is a map within a row and those are never returned reversed.
                if (current != null && reversed)
                    Collections.reverse(current.columns);

                current = new SuperColumn(scName, new ArrayList<>());
                thriftSuperColumns.add(new ColumnOrSuperColumn().setSuper_column(current));
            }
            current.getColumns().add(thriftifySubColumn(cell, cell.name.superColumnSubName()));
        }

        if (current != null && reversed)
            Collections.reverse(current.columns);

        return thriftSuperColumns;
    }

    private List<ColumnOrSuperColumn> thriftifyCounterSuperColumns(CFMetaData metadata, Iterator<LegacyLayout.LegacyCell> cells, boolean reversed)
    {
        ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<>();
        CounterSuperColumn current = null;
        while (cells.hasNext())
        {
            LegacyLayout.LegacyCell cell = cells.next();
            ByteBuffer scName = cell.name.superColumnName();
            if (current == null || !scName.equals(current.bufferForName()))
            {
                // Generally, cells come reversed if the query is reverse. However, this is not the case within a super column because
                // internally a super column is a map within a row and those are never returned reversed.
                if (current != null && reversed)
                    Collections.reverse(current.columns);

                current = new CounterSuperColumn(scName, new ArrayList<>());
                thriftSuperColumns.add(new ColumnOrSuperColumn().setCounter_super_column(current));
            }
            current.getColumns().add(thriftifySubCounter(metadata, cell).setName(cell.name.superColumnSubName()));
        }
        return thriftSuperColumns;
    }

    private List<ColumnOrSuperColumn> thriftifyPartition(RowIterator partition, boolean subcolumnsOnly, boolean reversed, int cellLimit)
    {
        if (partition.isEmpty())
            return EMPTY_COLUMNS;

        Iterator<LegacyLayout.LegacyCell> cells = LegacyLayout.fromRowIterator(partition).right;
        List<ColumnOrSuperColumn> result;
        if (partition.metadata().isSuper())
        {
            boolean isCounterCF = partition.metadata().isCounter();
            result = thriftifySuperColumns(partition.metadata(), cells, subcolumnsOnly, isCounterCF, reversed);
        }
        else
        {
            result = thriftifyColumns(partition.metadata(), cells);
        }

        // Thrift count cells, but internally we only count them at "row" boundaries, which means that if the limit stops in the middle
        // of an internal row we'll include a few additional cells. So trim it here.
        return result.size() > cellLimit
             ? result.subList(0, cellLimit)
             : result;
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> getSlice(List<SinglePartitionReadCommand> commands, boolean subColumnsOnly, int cellLimit, org.apache.cassandra.db.ConsistencyLevel consistency_level, ClientState cState)
    throws org.apache.cassandra.exceptions.InvalidRequestException, UnavailableException, TimedOutException
    {
        try (PartitionIterator results = read(commands, consistency_level, cState))
        {
            Map<ByteBuffer, List<ColumnOrSuperColumn>> columnFamiliesMap = new HashMap<>();
            while (results.hasNext())
            {
                try (RowIterator iter = results.next())
                {
                    List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyPartition(iter, subColumnsOnly, iter.isReverseOrder(), cellLimit);
                    columnFamiliesMap.put(iter.partitionKey().getKey(), thriftifiedColumns);
                }
            }
            return columnFamiliesMap;
        }
    }

    public List<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("key", ByteBufferUtil.bytesToHex(key),
                                                                  "column_parent", column_parent.toString(),
                                                                  "predicate", predicate.toString(),
                                                                  "consistency_level", consistency_level.name());
            Tracing.instance.begin("get_slice", traceParameters);
        }
        else
        {
            logger.trace("get_slice");
        }

        try
        {
            ClientState cState = state();
            String keyspace = cState.getKeyspace();
            state().hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
            List<ColumnOrSuperColumn> result = getSliceInternal(keyspace, key, column_parent, FBUtilities.nowInSeconds(), predicate, consistency_level, cState);
            return result == null ? Collections.<ColumnOrSuperColumn>emptyList() : result;
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    private List<ColumnOrSuperColumn> getSliceInternal(String keyspace,
                                                       ByteBuffer key,
                                                       ColumnParent column_parent,
                                                       int nowInSec,
                                                       SlicePredicate predicate,
                                                       ConsistencyLevel consistency_level,
                                                       ClientState cState)
    throws org.apache.cassandra.exceptions.InvalidRequestException, UnavailableException, TimedOutException
    {
        return multigetSliceInternal(keyspace, Collections.singletonList(key), column_parent, nowInSec, predicate, consistency_level, cState).get(key);
    }

    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            List<String> keysList = Lists.newArrayList();
            for (ByteBuffer key : keys)
                keysList.add(ByteBufferUtil.bytesToHex(key));
            Map<String, String> traceParameters = ImmutableMap.of("keys", keysList.toString(),
                                                                  "column_parent", column_parent.toString(),
                                                                  "predicate", predicate.toString(),
                                                                  "consistency_level", consistency_level.name());
            Tracing.instance.begin("multiget_slice", traceParameters);
        }
        else
        {
            logger.trace("multiget_slice");
        }

        try
        {
            ClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
            return multigetSliceInternal(keyspace, keys, column_parent, FBUtilities.nowInSeconds(), predicate, consistency_level, cState);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    private ClusteringIndexFilter toInternalFilter(CFMetaData metadata, ColumnParent parent, SliceRange range)
    {
        if (metadata.isSuper() && parent.isSetSuper_column())
            return new ClusteringIndexNamesFilter(FBUtilities.singleton(new Clustering(parent.bufferForSuper_column()), metadata.comparator), range.reversed);
        else
            return new ClusteringIndexSliceFilter(makeSlices(metadata, range), range.reversed);
    }

    private Slices makeSlices(CFMetaData metadata, SliceRange range)
    {
        // Note that in thrift, the bounds are reversed if the query is reversed, but not internally.
        ByteBuffer start = range.reversed ? range.finish : range.start;
        ByteBuffer finish = range.reversed ? range.start : range.finish;
        return Slices.with(metadata.comparator, Slice.make(LegacyLayout.decodeBound(metadata, start, true).bound, LegacyLayout.decodeBound(metadata, finish, false).bound));
    }

    private ClusteringIndexFilter toInternalFilter(CFMetaData metadata, ColumnParent parent, SlicePredicate predicate)
    throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        try
        {
            if (predicate.column_names != null)
            {
                if (metadata.isSuper())
                {
                    if (parent.isSetSuper_column())
                    {
                        return new ClusteringIndexNamesFilter(FBUtilities.singleton(new Clustering(parent.bufferForSuper_column()), metadata.comparator), false);
                    }
                    else
                    {
                        NavigableSet<Clustering> clusterings = new TreeSet<>(metadata.comparator);
                        for (ByteBuffer bb : predicate.column_names)
                            clusterings.add(new Clustering(bb));
                        return new ClusteringIndexNamesFilter(clusterings, false);
                    }
                }
                else
                {
                    NavigableSet<Clustering> clusterings = new TreeSet<>(metadata.comparator);
                    for (ByteBuffer bb : predicate.column_names)
                    {
                        LegacyLayout.LegacyCellName name = LegacyLayout.decodeCellName(metadata, parent.bufferForSuper_column(), bb);

                        if (!name.clustering.equals(Clustering.STATIC_CLUSTERING))
                            clusterings.add(name.clustering);
                    }

                    // clusterings cannot include STATIC_CLUSTERING, so if the names filter is for static columns, clusterings
                    // will be empty.  However, by requesting the static columns in our ColumnFilter, this will still work.
                    return new ClusteringIndexNamesFilter(clusterings, false);
                }
            }
            else
            {
                return toInternalFilter(metadata, parent, predicate.slice_range);
            }
        }
        catch (UnknownColumnException e)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException(e.getMessage());
        }
    }

    private ColumnFilter makeColumnFilter(CFMetaData metadata, ColumnParent parent, SliceRange range)
    {
        if (metadata.isSuper() && parent.isSetSuper_column())
        {
            // We want a slice of the dynamic columns
            ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
            ColumnDefinition def = metadata.compactValueColumn();
            ByteBuffer start = range.reversed ? range.finish : range.start;
            ByteBuffer finish = range.reversed ? range.start : range.finish;
            builder.slice(def, start.hasRemaining() ? CellPath.create(start) : CellPath.BOTTOM, finish.hasRemaining() ? CellPath.create(finish) : CellPath.TOP);

            // We also want to add any staticly defined column if it's within the range
            AbstractType<?> cmp = metadata.thriftColumnNameType();
            for (ColumnDefinition column : metadata.partitionColumns())
            {
                if (CompactTables.isSuperColumnMapColumn(column))
                    continue;

                ByteBuffer name = column.name.bytes;
                if (cmp.compare(name, start) < 0 || cmp.compare(finish, name) > 0)
                    continue;

                builder.add(column);
            }
            return builder.build();
        }
        return makeColumnFilter(metadata, makeSlices(metadata, range));
    }

    private ColumnFilter makeColumnFilter(CFMetaData metadata, Slices slices)
    {
        PartitionColumns columns = metadata.partitionColumns();
        if (metadata.isStaticCompactTable() && !columns.statics.isEmpty())
        {
            PartitionColumns.Builder builder = PartitionColumns.builder();
            builder.addAll(columns.regulars);
            // We only want to include the static columns that are selected by the slices
            for (ColumnDefinition def : columns.statics)
            {
                if (slices.selects(new Clustering(def.name.bytes)))
                    builder.add(def);
            }
            columns = builder.build();
        }
        return ColumnFilter.selection(columns);
    }

    private ColumnFilter makeColumnFilter(CFMetaData metadata, ColumnParent parent, SlicePredicate predicate)
    throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        try
        {
            if (predicate.column_names != null)
            {
                if (metadata.isSuper())
                {
                    if (parent.isSetSuper_column())
                    {
                        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
                        ColumnDefinition dynamicDef = metadata.compactValueColumn();
                        for (ByteBuffer bb : predicate.column_names)
                        {
                            ColumnDefinition staticDef = metadata.getColumnDefinition(bb);
                            if (staticDef == null)
                                builder.select(dynamicDef, CellPath.create(bb));
                            else
                                builder.add(staticDef);
                        }
                        return builder.build();
                    }
                    else
                    {
                        return ColumnFilter.all(metadata);
                    }
                }
                else
                {
                    PartitionColumns.Builder builder = PartitionColumns.builder();
                    for (ByteBuffer bb : predicate.column_names)
                    {
                        LegacyLayout.LegacyCellName name = LegacyLayout.decodeCellName(metadata, parent.bufferForSuper_column(), bb);
                        builder.add(name.column);
                    }

                    if (metadata.isStaticCompactTable())
                        builder.add(metadata.compactValueColumn());

                    return ColumnFilter.selection(builder.build());
                }
            }
            else
            {
                return makeColumnFilter(metadata, parent, predicate.slice_range);
            }
        }
        catch (UnknownColumnException e)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException(e.getMessage());
        }
    }

    private DataLimits getLimits(int partitionLimit, boolean countSuperColumns, SlicePredicate predicate)
    {
        int cellsPerPartition = predicate.slice_range == null ? Integer.MAX_VALUE : predicate.slice_range.count;
        return getLimits(partitionLimit, countSuperColumns, cellsPerPartition);
    }

    private DataLimits getLimits(int partitionLimit, boolean countSuperColumns, int perPartitionCount)
    {
        return countSuperColumns
             ? DataLimits.superColumnCountingLimits(partitionLimit, perPartitionCount)
             : DataLimits.thriftLimits(partitionLimit, perPartitionCount);
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetSliceInternal(String keyspace,
                                                                             List<ByteBuffer> keys,
                                                                             ColumnParent column_parent,
                                                                             int nowInSec,
                                                                             SlicePredicate predicate,
                                                                             ConsistencyLevel consistency_level,
                                                                             ClientState cState)
    throws org.apache.cassandra.exceptions.InvalidRequestException, UnavailableException, TimedOutException
    {
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, predicate);

        org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
        consistencyLevel.validateForRead(keyspace);

        List<SinglePartitionReadCommand> commands = new ArrayList<>(keys.size());
        ColumnFilter columnFilter = makeColumnFilter(metadata, column_parent, predicate);
        ClusteringIndexFilter filter = toInternalFilter(metadata, column_parent, predicate);
        DataLimits limits = getLimits(1, metadata.isSuper() && !column_parent.isSetSuper_column(), predicate);

        for (ByteBuffer key: keys)
        {
            ThriftValidation.validateKey(metadata, key);
            DecoratedKey dk = metadata.decorateKey(key);
            commands.add(SinglePartitionReadCommand.create(true, metadata, nowInSec, columnFilter, RowFilter.NONE, limits, dk, filter));
        }

        return getSlice(commands, column_parent.isSetSuper_column(), limits.perPartitionCount(), consistencyLevel, cState);
    }

    public ColumnOrSuperColumn get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("key", ByteBufferUtil.bytesToHex(key),
                                                                  "column_path", column_path.toString(),
                                                                  "consistency_level", consistency_level.name());
            Tracing.instance.begin("get", traceParameters);
        }
        else
        {
            logger.trace("get");
        }

        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_path.column_family, Permission.SELECT);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_path.column_family);
            ThriftValidation.validateColumnPath(metadata, column_path);
            org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
            consistencyLevel.validateForRead(keyspace);

            ThriftValidation.validateKey(metadata, key);

            ColumnFilter columns;
            ClusteringIndexFilter filter;
            if (metadata.isSuper())
            {
                if (column_path.column == null)
                {
                    // Selects a full super column
                    columns = ColumnFilter.all(metadata);
                }
                else
                {
                    // Selects a single column within a super column
                    ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
                    ColumnDefinition staticDef = metadata.getColumnDefinition(column_path.column);
                    ColumnDefinition dynamicDef = metadata.compactValueColumn();

                    if (staticDef != null)
                        builder.add(staticDef);
                    // Note that even if there is a staticDef, we still query the dynamicDef since we can't guarantee the static one hasn't
                    // been created after data has been inserted for that definition
                    builder.select(dynamicDef, CellPath.create(column_path.column));
                    columns = builder.build();
                }
                filter = new ClusteringIndexNamesFilter(FBUtilities.singleton(new Clustering(column_path.super_column), metadata.comparator),
                                                  false);
            }
            else
            {
                LegacyLayout.LegacyCellName cellname = LegacyLayout.decodeCellName(metadata, column_path.super_column, column_path.column);
                if (cellname.clustering == Clustering.STATIC_CLUSTERING)
                {
                    // Same as above: even if we're querying a static column, we still query the equivalent dynamic column and value as some
                    // values might have been created post creation of the column (ThriftResultMerger then ensures we get only one result).
                    ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
                    builder.add(cellname.column);
                    builder.add(metadata.compactValueColumn());
                    columns = builder.build();
                    filter = new ClusteringIndexNamesFilter(FBUtilities.singleton(new Clustering(column_path.column), metadata.comparator), false);
                }
                else
                {
                    columns = ColumnFilter.selection(PartitionColumns.of(cellname.column));
                    filter = new ClusteringIndexNamesFilter(FBUtilities.singleton(cellname.clustering, metadata.comparator), false);
                }
            }

            DecoratedKey dk = metadata.decorateKey(key);
            SinglePartitionReadCommand command = SinglePartitionReadCommand.create(true, metadata, FBUtilities.nowInSeconds(), columns, RowFilter.NONE, DataLimits.NONE, dk, filter);

            try (RowIterator result = PartitionIterators.getOnlyElement(read(Arrays.asList(command), consistencyLevel, cState), command))
            {
                if (!result.hasNext())
                    throw new NotFoundException();

                List<ColumnOrSuperColumn> tcolumns = thriftifyPartition(result, metadata.isSuper() && column_path.column != null, result.isReverseOrder(), 1);
                if (tcolumns.isEmpty())
                    throw new NotFoundException();
                assert tcolumns.size() == 1;
                return tcolumns.get(0);
            }
        }
        catch (UnknownColumnException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    public int get_count(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("key", ByteBufferUtil.bytesToHex(key),
                                                                  "column_parent", column_parent.toString(),
                                                                  "predicate", predicate.toString(),
                                                                  "consistency_level", consistency_level.name());
            Tracing.instance.begin("get_count", traceParameters);
        }
        else
        {
            logger.trace("get_count");
        }

        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
            Keyspace keyspaceName = Keyspace.open(keyspace);
            ColumnFamilyStore cfs = keyspaceName.getColumnFamilyStore(column_parent.column_family);
            int nowInSec = FBUtilities.nowInSeconds();

            if (predicate.column_names != null)
                return getSliceInternal(keyspace, key, column_parent, nowInSec, predicate, consistency_level, cState).size();

            int pageSize;
            // request by page if this is a large row
            if (cfs.getMeanColumns() > 0)
            {
                int averageColumnSize = (int) (cfs.metric.meanPartitionSize.getValue() / cfs.getMeanColumns());
                pageSize = Math.min(COUNT_PAGE_SIZE, 4 * 1024 * 1024 / averageColumnSize);
                pageSize = Math.max(2, pageSize);
                logger.trace("average row column size is {}; using pageSize of {}", averageColumnSize, pageSize);
            }
            else
            {
                pageSize = COUNT_PAGE_SIZE;
            }

            SliceRange sliceRange = predicate.slice_range == null
                                  ? new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE)
                                  : predicate.slice_range;

            ColumnFilter columnFilter;
            ClusteringIndexFilter filter;
            CFMetaData metadata = cfs.metadata;
            if (metadata.isSuper() && !column_parent.isSetSuper_column())
            {
                // If we count on a super column table without having set the super column name, we're in fact interested by the count of super columns
                columnFilter = ColumnFilter.all(metadata);
                filter = new ClusteringIndexSliceFilter(makeSlices(metadata, sliceRange), sliceRange.reversed);
            }
            else
            {
                columnFilter = makeColumnFilter(metadata, column_parent, sliceRange);
                filter = toInternalFilter(metadata, column_parent, sliceRange);
            }

            DataLimits limits = getLimits(1, metadata.isSuper() && !column_parent.isSetSuper_column(), predicate);
            DecoratedKey dk = metadata.decorateKey(key);

            return QueryPagers.countPaged(metadata,
                                          dk,
                                          columnFilter,
                                          filter,
                                          limits,
                                          ThriftConversion.fromThrift(consistency_level),
                                          cState,
                                          pageSize,
                                          nowInSec,
                                          true);
        }
        catch (IllegalArgumentException e)
        {
            // CASSANDRA-5701
            throw new InvalidRequestException(e.getMessage());
        }
        catch (RequestExecutionException e)
        {
            throw ThriftConversion.rethrow(e);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    public Map<ByteBuffer, Integer> multiget_count(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            List<String> keysList = Lists.newArrayList();
            for (ByteBuffer key : keys)
            {
                keysList.add(ByteBufferUtil.bytesToHex(key));
            }
            Map<String, String> traceParameters = ImmutableMap.of("keys", keysList.toString(),
                                                                  "column_parent", column_parent.toString(),
                                                                  "predicate", predicate.toString(),
                                                                  "consistency_level", consistency_level.name());
            Tracing.instance.begin("multiget_count", traceParameters);
        }
        else
        {
            logger.trace("multiget_count");
        }

        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);

            Map<ByteBuffer, Integer> counts = new HashMap<>();
            Map<ByteBuffer, List<ColumnOrSuperColumn>> columnFamiliesMap = multigetSliceInternal(keyspace,
                                                                                                 keys,
                                                                                                 column_parent,
                                                                                                 FBUtilities.nowInSeconds(),
                                                                                                 predicate,
                                                                                                 consistency_level,
                                                                                                 cState);

            for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> cf : columnFamiliesMap.entrySet())
                counts.put(cf.getKey(), cf.getValue().size());
            return counts;
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    private Cell cellFromColumn(CFMetaData metadata, LegacyLayout.LegacyCellName name, Column column)
    {
        CellPath path = name.collectionElement == null ? null : CellPath.create(name.collectionElement);
        return column.ttl == 0
             ? BufferCell.live(metadata, name.column, column.timestamp, column.value, path)
             : BufferCell.expiring(name.column, column.timestamp, column.ttl, FBUtilities.nowInSeconds(), column.value, path);
    }

    private void internal_insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level)
    throws RequestValidationException, UnavailableException, TimedOutException
    {
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.MODIFY);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, false);
        if (metadata.isView())
            throw new org.apache.cassandra.exceptions.InvalidRequestException("Cannot modify Materialized Views directly");

        ThriftValidation.validateKey(metadata, key);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        // SuperColumn field is usually optional, but not when we're inserting
        if (metadata.isSuper() && column_parent.super_column == null)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("missing mandatory super column name for super CF " + column_parent.column_family);
        }
        ThriftValidation.validateColumnNames(metadata, column_parent, Collections.singletonList(column.name));
        ThriftValidation.validateColumnData(metadata, column_parent.super_column, column);

        org.apache.cassandra.db.Mutation mutation;
        try
        {
            LegacyLayout.LegacyCellName name = LegacyLayout.decodeCellName(metadata, column_parent.super_column, column.name);
            Cell cell = cellFromColumn(metadata, name, column);
            PartitionUpdate update = PartitionUpdate.singleRowUpdate(metadata, key, BTreeRow.singleCellRow(name.clustering, cell));

            // Indexed column values cannot be larger than 64K.  See CASSANDRA-3057/4240 for more details
            Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfName).indexManager.validate(update);

            mutation = new org.apache.cassandra.db.Mutation(update);
        }
        catch (MarshalException|UnknownColumnException e)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException(e.getMessage());
        }
        doInsert(consistency_level, Collections.singletonList(mutation));
    }

    public void insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("key", ByteBufferUtil.bytesToHex(key),
                                                                  "column_parent", column_parent.toString(),
                                                                  "column", column.toString(),
                                                                  "consistency_level", consistency_level.name());
            Tracing.instance.begin("insert", traceParameters);
        }
        else
        {
            logger.trace("insert");
        }

        try
        {
            internal_insert(key, column_parent, column, consistency_level);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    public CASResult cas(ByteBuffer key,
                         String column_family,
                         List<Column> expected,
                         List<Column> updates,
                         ConsistencyLevel serial_consistency_level,
                         ConsistencyLevel commit_consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();
            builder.put("key", ByteBufferUtil.bytesToHex(key));
            builder.put("column_family", column_family);
            builder.put("old", expected.toString());
            builder.put("updates", updates.toString());
            builder.put("consistency_level", commit_consistency_level.name());
            builder.put("serial_consistency_level", serial_consistency_level.name());
            Map<String,String> traceParameters = builder.build();

            Tracing.instance.begin("cas", traceParameters);
        }
        else
        {
            logger.trace("cas");
        }

        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_family, Permission.MODIFY);
            // CAS updates can be used to simulate a get request, so should require Permission.SELECT.
            cState.hasColumnFamilyAccess(keyspace, column_family, Permission.SELECT);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_family, false);
            if (metadata.isView())
                throw new org.apache.cassandra.exceptions.InvalidRequestException("Cannot modify Materialized Views directly");

            ThriftValidation.validateKey(metadata, key);
            if (metadata.isSuper())
                throw new org.apache.cassandra.exceptions.InvalidRequestException("CAS does not support supercolumns");

            Iterable<ByteBuffer> names = Iterables.transform(updates, column -> column.name);
            ThriftValidation.validateColumnNames(metadata, new ColumnParent(column_family), names);
            for (Column column : updates)
                ThriftValidation.validateColumnData(metadata, null, column);

            DecoratedKey dk = metadata.decorateKey(key);
            int nowInSec = FBUtilities.nowInSeconds();

            PartitionUpdate partitionUpdates = PartitionUpdate.fromIterator(LegacyLayout.toRowIterator(metadata, dk, toLegacyCells(metadata, updates, nowInSec).iterator(), nowInSec));
            // Indexed column values cannot be larger than 64K.  See CASSANDRA-3057/4240 for more details
            Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfName).indexManager.validate(partitionUpdates);

            schedule(DatabaseDescriptor.getWriteRpcTimeout());
            try (RowIterator result = StorageProxy.cas(cState.getKeyspace(),
                                                       column_family,
                                                       dk,
                                                       new ThriftCASRequest(toLegacyCells(metadata, expected, nowInSec), partitionUpdates, nowInSec),
                                                       ThriftConversion.fromThrift(serial_consistency_level),
                                                       ThriftConversion.fromThrift(commit_consistency_level),
                                                       cState))
            {
                return result == null
                     ? new CASResult(true)
                     : new CASResult(false).setCurrent_values(thriftifyColumnsAsColumns(metadata, LegacyLayout.fromRowIterator(result).right));
            }
        }
        catch (UnknownColumnException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
        catch (RequestTimeoutException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (RequestExecutionException e)
        {
            throw ThriftConversion.rethrow(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    private LegacyLayout.LegacyCell toLegacyCell(CFMetaData metadata, Column column, int nowInSec) throws UnknownColumnException
    {
        return toLegacyCell(metadata, null, column, nowInSec);
    }

    private LegacyLayout.LegacyCell toLegacyCell(CFMetaData metadata, ByteBuffer superColumnName, Column column, int nowInSec)
    throws UnknownColumnException
    {
        return column.ttl > 0
             ? LegacyLayout.LegacyCell.expiring(metadata, superColumnName, column.name, column.value, column.timestamp, column.ttl, nowInSec)
             : LegacyLayout.LegacyCell.regular(metadata, superColumnName, column.name, column.value, column.timestamp);
    }

    private LegacyLayout.LegacyCell toLegacyDeletion(CFMetaData metadata, ByteBuffer name, long timestamp, int nowInSec)
    throws UnknownColumnException
    {
        return toLegacyDeletion(metadata, null, name, timestamp, nowInSec);
    }

    private LegacyLayout.LegacyCell toLegacyDeletion(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, long timestamp, int nowInSec)
    throws UnknownColumnException
    {
        return LegacyLayout.LegacyCell.tombstone(metadata, superColumnName, name, timestamp, nowInSec);
    }

    private LegacyLayout.LegacyCell toCounterLegacyCell(CFMetaData metadata, CounterColumn column)
    throws UnknownColumnException
    {
        return toCounterLegacyCell(metadata, null, column);
    }

    private LegacyLayout.LegacyCell toCounterLegacyCell(CFMetaData metadata, ByteBuffer superColumnName, CounterColumn column)
    throws UnknownColumnException
    {
        return LegacyLayout.LegacyCell.counter(metadata, superColumnName, column.name, column.value);
    }

    private void sortAndMerge(CFMetaData metadata, List<LegacyLayout.LegacyCell> cells, int nowInSec)
    {
        Collections.sort(cells, LegacyLayout.legacyCellComparator(metadata));

        // After sorting, if we have multiple cells for the same "cellname", we want to merge those together.
        Comparator<LegacyLayout.LegacyCellName> comparator = LegacyLayout.legacyCellNameComparator(metadata, false);

        int previous = 0; // The last element that was set
        for (int current = 1; current < cells.size(); current++)
        {
            LegacyLayout.LegacyCell pc = cells.get(previous);
            LegacyLayout.LegacyCell cc = cells.get(current);

            // There is really only 2 possible comparison: < 0 or == 0 since we've sorted already
            int cmp = comparator.compare(pc.name, cc.name);
            if (cmp == 0)
            {
                // current and previous are the same cell. Merge current into previous
                // (and so previous + 1 will be "free").
                Conflicts.Resolution res;
                if (metadata.isCounter())
                {
                    res = Conflicts.resolveCounter(pc.timestamp, pc.isLive(nowInSec), pc.value,
                                                   cc.timestamp, cc.isLive(nowInSec), cc.value);

                }
                else
                {
                    res = Conflicts.resolveRegular(pc.timestamp, pc.isLive(nowInSec), pc.localDeletionTime, pc.value,
                                                   cc.timestamp, cc.isLive(nowInSec), cc.localDeletionTime, cc.value);
                }

                switch (res)
                {
                    case LEFT_WINS:
                        // The previous cell wins, we'll just ignore current
                        break;
                    case RIGHT_WINS:
                        cells.set(previous, cc);
                        break;
                    case MERGE:
                        assert metadata.isCounter();
                        ByteBuffer merged = Conflicts.mergeCounterValues(pc.value, cc.value);
                        cells.set(previous, LegacyLayout.LegacyCell.counter(pc.name, merged));
                        break;
                }
            }
            else
            {
                // cell.get(previous) < cells.get(current), so move current just after previous if needs be
                ++previous;
                if (previous != current)
                    cells.set(previous, cc);
            }
        }

        // The last element we want is previous, so trim anything after that
        for (int i = cells.size() - 1; i > previous; i--)
            cells.remove(i);
    }

    private List<LegacyLayout.LegacyCell> toLegacyCells(CFMetaData metadata, List<Column> columns, int nowInSec)
    throws UnknownColumnException
    {
        List<LegacyLayout.LegacyCell> cells = new ArrayList<>(columns.size());
        for (Column column : columns)
            cells.add(toLegacyCell(metadata, column, nowInSec));

        sortAndMerge(metadata, cells, nowInSec);
        return cells;
    }

    private List<IMutation> createMutationList(ConsistencyLevel consistency_level,
                                               Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map,
                                               boolean allowCounterMutations)
    throws RequestValidationException, InvalidRequestException
    {
        List<IMutation> mutations = new ArrayList<>();
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();
        int nowInSec = FBUtilities.nowInSeconds();

        for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> mutationEntry: mutation_map.entrySet())
        {
            ByteBuffer key = mutationEntry.getKey();

            // We need to separate mutation for standard cf and counter cf (that will be encapsulated in a
            // CounterMutation) because it doesn't follow the same code path
            org.apache.cassandra.db.Mutation standardMutation = null;
            org.apache.cassandra.db.Mutation counterMutation = null;

            Map<String, List<Mutation>> columnFamilyToMutations = mutationEntry.getValue();
            for (Map.Entry<String, List<Mutation>> columnFamilyMutations : columnFamilyToMutations.entrySet())
            {
                String cfName = columnFamilyMutations.getKey();
                List<Mutation> muts = columnFamilyMutations.getValue();

                cState.hasColumnFamilyAccess(keyspace, cfName, Permission.MODIFY);

                CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, cfName);
                if (metadata.isView())
                    throw new org.apache.cassandra.exceptions.InvalidRequestException("Cannot modify Materialized Views directly");

                ThriftValidation.validateKey(metadata, key);
                if (metadata.isCounter())
                    ThriftConversion.fromThrift(consistency_level).validateCounterForWrite(metadata);

                LegacyLayout.LegacyDeletionInfo delInfo = LegacyLayout.LegacyDeletionInfo.live();
                List<LegacyLayout.LegacyCell> cells = new ArrayList<>();
                for (Mutation m : muts)
                {
                    ThriftValidation.validateMutation(metadata, m);

                    if (m.deletion != null)
                    {
                        deleteColumnOrSuperColumn(delInfo, cells, metadata, m.deletion, nowInSec);
                    }
                    if (m.column_or_supercolumn != null)
                    {
                        addColumnOrSuperColumn(cells, metadata, m.column_or_supercolumn, nowInSec);
                    }
                }

                sortAndMerge(metadata, cells, nowInSec);
                DecoratedKey dk = metadata.decorateKey(key);
                PartitionUpdate update = PartitionUpdate.fromIterator(LegacyLayout.toUnfilteredRowIterator(metadata, dk, delInfo, cells.iterator()));

                // Indexed column values cannot be larger than 64K.  See CASSANDRA-3057/4240 for more details
                Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfName).indexManager.validate(update);

                org.apache.cassandra.db.Mutation mutation;
                if (metadata.isCounter())
                {
                    counterMutation = counterMutation == null ? new org.apache.cassandra.db.Mutation(keyspace, dk) : counterMutation;
                    mutation = counterMutation;
                }
                else
                {
                    standardMutation = standardMutation == null ? new org.apache.cassandra.db.Mutation(keyspace, dk) : standardMutation;
                    mutation = standardMutation;
                }
                mutation.add(update);
            }
            if (standardMutation != null && !standardMutation.isEmpty())
                mutations.add(standardMutation);

            if (counterMutation != null && !counterMutation.isEmpty())
            {
                if (allowCounterMutations)
                    mutations.add(new CounterMutation(counterMutation, ThriftConversion.fromThrift(consistency_level)));
                else
                    throw new org.apache.cassandra.exceptions.InvalidRequestException("Counter mutations are not allowed in atomic batches");
            }
        }

        return mutations;
    }

    private void addColumnOrSuperColumn(List<LegacyLayout.LegacyCell> cells, CFMetaData cfm, ColumnOrSuperColumn cosc, int nowInSec)
    throws InvalidRequestException
    {
        try
        {
            if (cosc.super_column != null)
            {
                for (Column column : cosc.super_column.columns)
                    cells.add(toLegacyCell(cfm, cosc.super_column.name, column, nowInSec));
            }
            else if (cosc.column != null)
            {
                cells.add(toLegacyCell(cfm, cosc.column, nowInSec));
            }
            else if (cosc.counter_super_column != null)
            {
                for (CounterColumn column : cosc.counter_super_column.columns)
                    cells.add(toCounterLegacyCell(cfm, cosc.counter_super_column.name, column));
            }
            else // cosc.counter_column != null
            {
                cells.add(toCounterLegacyCell(cfm, cosc.counter_column));
            }
        }
        catch (UnknownColumnException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    private void addRange(CFMetaData cfm, LegacyLayout.LegacyDeletionInfo delInfo, Slice.Bound start, Slice.Bound end, long timestamp, int nowInSec)
    {
        delInfo.add(cfm, new RangeTombstone(Slice.make(start, end), new DeletionTime(timestamp, nowInSec)));
    }

    private void deleteColumnOrSuperColumn(LegacyLayout.LegacyDeletionInfo delInfo, List<LegacyLayout.LegacyCell> cells, CFMetaData cfm, Deletion del, int nowInSec)
    throws InvalidRequestException
    {
        if (del.predicate != null && del.predicate.column_names != null)
        {
            for (ByteBuffer c : del.predicate.column_names)
            {
                try
                {
                    if (del.super_column == null && cfm.isSuper())
                        addRange(cfm, delInfo, Slice.Bound.inclusiveStartOf(c), Slice.Bound.inclusiveEndOf(c), del.timestamp, nowInSec);
                    else if (del.super_column != null)
                        cells.add(toLegacyDeletion(cfm, del.super_column, c, del.timestamp, nowInSec));
                    else
                        cells.add(toLegacyDeletion(cfm, c, del.timestamp, nowInSec));
                }
                catch (UnknownColumnException e)
                {
                    throw new InvalidRequestException(e.getMessage());
                }
            }
        }
        else if (del.predicate != null && del.predicate.slice_range != null)
        {
            if (del.super_column == null)
            {
                LegacyLayout.LegacyBound start = LegacyLayout.decodeBound(cfm, del.predicate.getSlice_range().start, true);
                LegacyLayout.LegacyBound end = LegacyLayout.decodeBound(cfm, del.predicate.getSlice_range().finish, false);
                delInfo.add(cfm, new LegacyLayout.LegacyRangeTombstone(start, end, new DeletionTime(del.timestamp, nowInSec)));
            }
            else
            {
                // Since we use a map for subcolumns, we would need range tombstone for collections to support this.
                // And while we may want those some day, this require a bit of additional work. And since super columns
                // are basically deprecated since a long time, and range tombstone on them has been only very recently
                // added so that no thrift driver actually supports it to the best of my knowledge, it's likely ok to
                // discontinue support for this. If it turns out that this is blocking the update of someone, we can
                // decide then if we want to tackle the addition of range tombstone for collections then.
                throw new InvalidRequestException("Cannot delete a range of subcolumns in a super column");
            }
        }
        else
        {
            if (del.super_column != null)
                addRange(cfm, delInfo, Slice.Bound.inclusiveStartOf(del.super_column), Slice.Bound.inclusiveEndOf(del.super_column), del.timestamp, nowInSec);
            else
                delInfo.add(new DeletionTime(del.timestamp, nowInSec));
        }
    }

    public void batch_mutate(Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = Maps.newLinkedHashMap();
            for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> mutationEntry : mutation_map.entrySet())
            {
                traceParameters.put(ByteBufferUtil.bytesToHex(mutationEntry.getKey()),
                                    Joiner.on(";").withKeyValueSeparator(":").join(mutationEntry.getValue()));
            }
            traceParameters.put("consistency_level", consistency_level.name());
            Tracing.instance.begin("batch_mutate", traceParameters);
        }
        else
        {
            logger.trace("batch_mutate");
        }

        try
        {
            doInsert(consistency_level, createMutationList(consistency_level, mutation_map, true));
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    public void atomic_batch_mutate(Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = Maps.newLinkedHashMap();
            for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> mutationEntry : mutation_map.entrySet())
            {
                traceParameters.put(ByteBufferUtil.bytesToHex(mutationEntry.getKey()),
                                    Joiner.on(";").withKeyValueSeparator(":").join(mutationEntry.getValue()));
            }
            traceParameters.put("consistency_level", consistency_level.name());
            Tracing.instance.begin("atomic_batch_mutate", traceParameters);
        }
        else
        {
            logger.trace("atomic_batch_mutate");
        }

        try
        {
            doInsert(consistency_level, createMutationList(consistency_level, mutation_map, false), true);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    private void internal_remove(ByteBuffer key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level, boolean isCommutativeOp)
    throws RequestValidationException, UnavailableException, TimedOutException
    {
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, column_path.column_family, Permission.MODIFY);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_path.column_family, isCommutativeOp);
        if (metadata.isView())
            throw new org.apache.cassandra.exceptions.InvalidRequestException("Cannot modify Materialized Views directly");

        ThriftValidation.validateKey(metadata, key);
        ThriftValidation.validateColumnPathOrParent(metadata, column_path);
        if (isCommutativeOp)
            ThriftConversion.fromThrift(consistency_level).validateCounterForWrite(metadata);

        DecoratedKey dk = metadata.decorateKey(key);

        int nowInSec = FBUtilities.nowInSeconds();
        PartitionUpdate update;
        if (column_path.super_column == null && column_path.column == null)
        {
            update = PartitionUpdate.fullPartitionDelete(metadata, dk, timestamp, nowInSec);
        }
        else if (column_path.super_column != null && column_path.column == null)
        {
            Row row = BTreeRow.emptyDeletedRow(new Clustering(column_path.super_column), Row.Deletion.regular(new DeletionTime(timestamp, nowInSec)));
            update = PartitionUpdate.singleRowUpdate(metadata, dk, row);
        }
        else
        {
            try
            {
                LegacyLayout.LegacyCellName name = LegacyLayout.decodeCellName(metadata, column_path.super_column, column_path.column);
                CellPath path = name.collectionElement == null ? null : CellPath.create(name.collectionElement);
                Cell cell = BufferCell.tombstone(name.column, timestamp, nowInSec, path);
                update = PartitionUpdate.singleRowUpdate(metadata, dk, BTreeRow.singleCellRow(name.clustering, cell));
            }
            catch (UnknownColumnException e)
            {
                throw new org.apache.cassandra.exceptions.InvalidRequestException(e.getMessage());
            }
        }

        org.apache.cassandra.db.Mutation mutation = new org.apache.cassandra.db.Mutation(update);

        if (isCommutativeOp)
            doInsert(consistency_level, Collections.singletonList(new CounterMutation(mutation, ThriftConversion.fromThrift(consistency_level))));
        else
            doInsert(consistency_level, Collections.singletonList(mutation));
    }

    public void remove(ByteBuffer key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("key", ByteBufferUtil.bytesToHex(key),
                                                                  "column_path", column_path.toString(),
                                                                  "timestamp", timestamp + "",
                                                                  "consistency_level", consistency_level.name());
            Tracing.instance.begin("remove", traceParameters);
        }
        else
        {
            logger.trace("remove");
        }

        try
        {
            internal_remove(key, column_path, timestamp, consistency_level, false);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    private void doInsert(ConsistencyLevel consistency_level, List<? extends IMutation> mutations)
    throws UnavailableException, TimedOutException, org.apache.cassandra.exceptions.InvalidRequestException
    {
        doInsert(consistency_level, mutations, false);
    }

    private void doInsert(ConsistencyLevel consistency_level, List<? extends IMutation> mutations, boolean mutateAtomically)
    throws UnavailableException, TimedOutException, org.apache.cassandra.exceptions.InvalidRequestException
    {
        org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
        consistencyLevel.validateForWrite(state().getKeyspace());
        if (mutations.isEmpty())
            return;

        long timeout = Long.MAX_VALUE;
        for (IMutation m : mutations)
            timeout = Longs.min(timeout, m.getTimeout());

        schedule(timeout);
        try
        {
            StorageProxy.mutateWithTriggers(mutations, consistencyLevel, mutateAtomically);
        }
        catch (RequestExecutionException e)
        {
            ThriftConversion.rethrow(e);
        }
        finally
        {
            release();
        }
    }

    private void validateLogin() throws InvalidRequestException
    {
        try
        {
            state().validateLogin();
        }
        catch (UnauthorizedException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    public KsDef describe_keyspace(String keyspaceName) throws NotFoundException, InvalidRequestException
    {
        validateLogin();

        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspaceName);
        if (ksm == null)
            throw new NotFoundException();

        return ThriftConversion.toThrift(ksm);
    }

    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of(
                    "column_parent", column_parent.toString(),
                    "predicate", predicate.toString(),
                    "range", range.toString(),
                    "consistency_level", consistency_level.name());
            Tracing.instance.begin("get_range_slices", traceParameters);
        }
        else
        {
            logger.trace("range_slice");
        }

        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
            ThriftValidation.validateColumnParent(metadata, column_parent);
            ThriftValidation.validatePredicate(metadata, column_parent, predicate);
            ThriftValidation.validateKeyRange(metadata, column_parent.super_column, range);

            org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
            consistencyLevel.validateForRead(keyspace);

            IPartitioner p = metadata.partitioner;
            AbstractBounds<PartitionPosition> bounds;
            if (range.start_key == null)
            {
                Token.TokenFactory tokenFactory = p.getTokenFactory();
                Token left = tokenFactory.fromString(range.start_token);
                Token right = tokenFactory.fromString(range.end_token);
                bounds = Range.makeRowRange(left, right);
            }
            else
            {
                PartitionPosition end = range.end_key == null
                                ? p.getTokenFactory().fromString(range.end_token).maxKeyBound()
                                : PartitionPosition.ForKey.get(range.end_key, p);
                bounds = new Bounds<>(PartitionPosition.ForKey.get(range.start_key, p), end);
            }
            int nowInSec = FBUtilities.nowInSeconds();
            schedule(DatabaseDescriptor.getRangeRpcTimeout());
            try
            {
                ColumnFilter columns = makeColumnFilter(metadata, column_parent, predicate);
                ClusteringIndexFilter filter = toInternalFilter(metadata, column_parent, predicate);
                DataLimits limits = getLimits(range.count, metadata.isSuper() && !column_parent.isSetSuper_column(), predicate);
                PartitionRangeReadCommand cmd = new PartitionRangeReadCommand(false,
                                                                              0,
                                                                              true,
                                                                              metadata,
                                                                              nowInSec,
                                                                              columns,
                                                                              ThriftConversion.rowFilterFromThrift(metadata, range.row_filter),
                                                                              limits,
                                                                              new DataRange(bounds, filter),
                                                                              Optional.empty());
                try (PartitionIterator results = StorageProxy.getRangeSlice(cmd, consistencyLevel))
                {
                    assert results != null;
                    return thriftifyKeySlices(results, column_parent, limits.perPartitionCount());
                }
            }
            finally
            {
                release();
            }
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (RequestExecutionException e)
        {
            throw ThriftConversion.rethrow(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    public List<KeySlice> get_paged_slice(String column_family, KeyRange range, ByteBuffer start_column, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("column_family", column_family,
                                                                  "range", range.toString(),
                                                                  "start_column", ByteBufferUtil.bytesToHex(start_column),
                                                                  "consistency_level", consistency_level.name());
            Tracing.instance.begin("get_paged_slice", traceParameters);
        }
        else
        {
            logger.trace("get_paged_slice");
        }

        try
        {

            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_family, Permission.SELECT);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_family);
            ThriftValidation.validateKeyRange(metadata, null, range);

            org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
            consistencyLevel.validateForRead(keyspace);

            IPartitioner p = metadata.partitioner;
            AbstractBounds<PartitionPosition> bounds;
            if (range.start_key == null)
            {
                // (token, key) is unsupported, assume (token, token)
                Token.TokenFactory tokenFactory = p.getTokenFactory();
                Token left = tokenFactory.fromString(range.start_token);
                Token right = tokenFactory.fromString(range.end_token);
                bounds = Range.makeRowRange(left, right);
            }
            else
            {
                PartitionPosition end = range.end_key == null
                                ? p.getTokenFactory().fromString(range.end_token).maxKeyBound()
                                : PartitionPosition.ForKey.get(range.end_key, p);
                bounds = new Bounds<>(PartitionPosition.ForKey.get(range.start_key, p), end);
            }

            if (range.row_filter != null && !range.row_filter.isEmpty())
                throw new InvalidRequestException("Cross-row paging is not supported along with index clauses");

            int nowInSec = FBUtilities.nowInSeconds();
            schedule(DatabaseDescriptor.getRangeRpcTimeout());
            try
            {
                ClusteringIndexFilter filter = new ClusteringIndexSliceFilter(Slices.ALL, false);
                DataLimits limits = getLimits(range.count, true, Integer.MAX_VALUE);
                Clustering pageFrom = metadata.isSuper()
                                    ? new Clustering(start_column)
                                    : LegacyLayout.decodeCellName(metadata, start_column).clustering;
                PartitionRangeReadCommand cmd = new PartitionRangeReadCommand(false,
                                                                              0,
                                                                              true,
                                                                              metadata,
                                                                              nowInSec,
                                                                              ColumnFilter.all(metadata),
                                                                              RowFilter.NONE,
                                                                              limits,
                                                                              new DataRange(bounds, filter).forPaging(bounds, metadata.comparator, pageFrom, true),
                                                                              Optional.empty());
                try (PartitionIterator results = StorageProxy.getRangeSlice(cmd, consistencyLevel))
                {
                    return thriftifyKeySlices(results, new ColumnParent(column_family), limits.perPartitionCount());
                }
            }
            catch (UnknownColumnException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
            finally
            {
                release();
            }
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (RequestExecutionException e)
        {
            throw ThriftConversion.rethrow(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    private List<KeySlice> thriftifyKeySlices(PartitionIterator results, ColumnParent column_parent, int cellLimit)
    {
        try (PartitionIterator iter = results)
        {
            List<KeySlice> keySlices = new ArrayList<>();
            while (iter.hasNext())
            {
                try (RowIterator partition = iter.next())
                {
                    List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyPartition(partition, column_parent.super_column != null, partition.isReverseOrder(), cellLimit);
                    keySlices.add(new KeySlice(partition.partitionKey().getKey(), thriftifiedColumns));
                }
            }

            return keySlices;
        }
    }

    public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("column_parent", column_parent.toString(),
                                                                  "index_clause", index_clause.toString(),
                                                                  "slice_predicate", column_predicate.toString(),
                                                                  "consistency_level", consistency_level.name());
            Tracing.instance.begin("get_indexed_slices", traceParameters);
        }
        else
        {
            logger.trace("scan");
        }

        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, false);
            ThriftValidation.validateColumnParent(metadata, column_parent);
            ThriftValidation.validatePredicate(metadata, column_parent, column_predicate);
            ThriftValidation.validateIndexClauses(metadata, index_clause);
            org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
            consistencyLevel.validateForRead(keyspace);

            IPartitioner p = metadata.partitioner;
            AbstractBounds<PartitionPosition> bounds = new Bounds<>(PartitionPosition.ForKey.get(index_clause.start_key, p),
                                                                    p.getMinimumToken().minKeyBound());

            int nowInSec = FBUtilities.nowInSeconds();
            ColumnFilter columns = makeColumnFilter(metadata, column_parent, column_predicate);
            ClusteringIndexFilter filter = toInternalFilter(metadata, column_parent, column_predicate);
            DataLimits limits = getLimits(index_clause.count, metadata.isSuper() && !column_parent.isSetSuper_column(), column_predicate);
            PartitionRangeReadCommand cmd = new PartitionRangeReadCommand(false,
                                                                          0,
                                                                          true,
                                                                          metadata,
                                                                          nowInSec,
                                                                          columns,
                                                                          ThriftConversion.rowFilterFromThrift(metadata, index_clause.expressions),
                                                                          limits,
                                                                          new DataRange(bounds, filter),
                                                                          Optional.empty());
            // If there's a secondary index that the command can use, have it validate
            // the request parameters. Note that as a side effect, if a viable Index is
            // identified by the CFS's index manager, it will be cached in the command
            // and serialized during distribution to replicas in order to avoid performing
            // further lookups.
            cmd.maybeValidateIndex();

            try (PartitionIterator results = StorageProxy.getRangeSlice(cmd, consistencyLevel))
            {
                return thriftifyKeySlices(results, column_parent, limits.perPartitionCount());
            }
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (RequestExecutionException e)
        {
            throw ThriftConversion.rethrow(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    public List<KsDef> describe_keyspaces() throws TException, InvalidRequestException
    {
        validateLogin();

        Set<String> keyspaces = Schema.instance.getKeyspaces();
        List<KsDef> ksset = new ArrayList<>(keyspaces.size());
        for (String ks : keyspaces)
        {
            try
            {
                ksset.add(describe_keyspace(ks));
            }
            catch (NotFoundException nfe)
            {
                logger.info("Failed to find metadata for keyspace '{}'. Continuing... ", ks);
            }
        }
        return ksset;
    }

    public String describe_cluster_name() throws TException
    {
        return DatabaseDescriptor.getClusterName();
    }

    public String describe_version() throws TException
    {
        return cassandraConstants.VERSION;
    }

    public List<TokenRange> describe_ring(String keyspace) throws InvalidRequestException
    {
        try
        {
            return StorageService.instance.describeRing(keyspace);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    @Override
    public List<TokenRange> describe_local_ring(String keyspace) throws InvalidRequestException, TException
    {
        try
        {
            return StorageService.instance.describeLocalRing(keyspace);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public Map<String, String> describe_token_map() throws InvalidRequestException
    {
        return StorageService.instance.getTokenToEndpointMap();
    }

    public String describe_partitioner() throws TException
    {
        return StorageService.instance.getPartitionerName();
    }

    public String describe_snitch() throws TException
    {
        if (DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch)
            return ((DynamicEndpointSnitch)DatabaseDescriptor.getEndpointSnitch()).subsnitch.getClass().getName();
        return DatabaseDescriptor.getEndpointSnitch().getClass().getName();
    }

    @Deprecated
    public List<String> describe_splits(String cfName, String start_token, String end_token, int keys_per_split)
    throws TException, InvalidRequestException
    {
        List<CfSplit> splits = describe_splits_ex(cfName, start_token, end_token, keys_per_split);
        List<String> result = new ArrayList<>(splits.size() + 1);

        result.add(splits.get(0).getStart_token());
        for (CfSplit cfSplit : splits)
            result.add(cfSplit.getEnd_token());

        return result;
    }

    public List<CfSplit> describe_splits_ex(String cfName, String start_token, String end_token, int keys_per_split)
    throws InvalidRequestException, TException
    {
        try
        {
            Token.TokenFactory tf = StorageService.instance.getTokenFactory();
            Range<Token> tr = new Range<Token>(tf.fromString(start_token), tf.fromString(end_token));
            List<Pair<Range<Token>, Long>> splits =
                    StorageService.instance.getSplits(state().getKeyspace(), cfName, tr, keys_per_split);
            List<CfSplit> result = new ArrayList<>(splits.size());
            for (Pair<Range<Token>, Long> split : splits)
                result.add(new CfSplit(split.left.left.toString(), split.left.right.toString(), split.right));
            return result;
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public void login(AuthenticationRequest auth_request) throws TException
    {
        try
        {
            state().login(DatabaseDescriptor.getAuthenticator().legacyAuthenticate(auth_request.getCredentials()));
        }
        catch (org.apache.cassandra.exceptions.AuthenticationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    /**
     * Schedule the current thread for access to the required services
     */
    private void schedule(long timeoutMS) throws UnavailableException
    {
        try
        {
            requestScheduler.queue(Thread.currentThread(), state().getSchedulingValue(), timeoutMS);
        }
        catch (TimeoutException e)
        {
            throw new UnavailableException();
        }
    }

    /**
     * Release count for the used up resources
     */
    private void release()
    {
        requestScheduler.release();
    }

    public String system_add_column_family(CfDef cf_def) throws TException
    {
        logger.trace("add_column_family");

        try
        {
            ClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasKeyspaceAccess(keyspace, Permission.CREATE);
            cf_def.unsetId(); // explicitly ignore any id set by client (Hector likes to set zero)
            CFMetaData cfm = ThriftConversion.fromThrift(cf_def);
            cfm.params.compaction.validate();

            if (!cfm.getTriggers().isEmpty())
                state().ensureIsSuper("Only superusers are allowed to add triggers.");

            MigrationManager.announceNewColumnFamily(cfm);
            return Schema.instance.getVersion().toString();
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public String system_drop_column_family(String column_family)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.trace("drop_column_family");

        ThriftClientState cState = state();

        try
        {
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_family, Permission.DROP);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_family);
            if (metadata.isView())
                throw new org.apache.cassandra.exceptions.InvalidRequestException("Cannot drop Materialized Views from Thrift");

            MigrationManager.announceColumnFamilyDrop(keyspace, column_family);
            return Schema.instance.getVersion().toString();
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public String system_add_keyspace(KsDef ks_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.trace("add_keyspace");

        try
        {
            ThriftValidation.validateKeyspaceNotSystem(ks_def.name);
            state().hasAllKeyspacesAccess(Permission.CREATE);
            ThriftValidation.validateKeyspaceNotYetExisting(ks_def.name);

            // generate a meaningful error if the user setup keyspace and/or column definition incorrectly
            for (CfDef cf : ks_def.cf_defs)
            {
                if (!cf.getKeyspace().equals(ks_def.getName()))
                {
                    throw new InvalidRequestException("CfDef (" + cf.getName() +") had a keyspace definition that did not match KsDef");
                }
            }

            Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(ks_def.cf_defs.size());
            for (CfDef cf_def : ks_def.cf_defs)
            {
                cf_def.unsetId(); // explicitly ignore any id set by client (same as system_add_column_family)
                CFMetaData cfm = ThriftConversion.fromThrift(cf_def);

                if (!cfm.getTriggers().isEmpty())
                    state().ensureIsSuper("Only superusers are allowed to add triggers.");

                cfDefs.add(cfm);
            }
            MigrationManager.announceNewKeyspace(ThriftConversion.fromThrift(ks_def, cfDefs.toArray(new CFMetaData[cfDefs.size()])));
            return Schema.instance.getVersion().toString();
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public String system_drop_keyspace(String keyspace)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.trace("drop_keyspace");

        try
        {
            ThriftValidation.validateKeyspaceNotSystem(keyspace);
            state().hasKeyspaceAccess(keyspace, Permission.DROP);

            MigrationManager.announceKeyspaceDrop(keyspace);
            return Schema.instance.getVersion().toString();
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    /** update an existing keyspace, but do not allow column family modifications.
     * @throws SchemaDisagreementException
     */
    public String system_update_keyspace(KsDef ks_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.trace("update_keyspace");

        try
        {
            ThriftValidation.validateKeyspaceNotSystem(ks_def.name);
            state().hasKeyspaceAccess(ks_def.name, Permission.ALTER);
            ThriftValidation.validateKeyspace(ks_def.name);
            if (ks_def.getCf_defs() != null && ks_def.getCf_defs().size() > 0)
                throw new InvalidRequestException("Keyspace update must not contain any table definitions.");

            MigrationManager.announceKeyspaceUpdate(ThriftConversion.fromThrift(ks_def));
            return Schema.instance.getVersion().toString();
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public String system_update_column_family(CfDef cf_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.trace("update_column_family");

        try
        {
            if (cf_def.keyspace == null || cf_def.name == null)
                throw new InvalidRequestException("Keyspace and CF name must be set.");

            state().hasColumnFamilyAccess(cf_def.keyspace, cf_def.name, Permission.ALTER);
            CFMetaData oldCfm = Schema.instance.getCFMetaData(cf_def.keyspace, cf_def.name);

            if (oldCfm == null)
                throw new InvalidRequestException("Could not find table definition to modify.");

            if (oldCfm.isView())
                throw new InvalidRequestException("Cannot modify Materialized View table " + oldCfm.cfName + " as it may break the schema. You should use cqlsh to modify Materialized View tables instead.");
            if (!Iterables.isEmpty(View.findAll(cf_def.keyspace, cf_def.name)))
                throw new InvalidRequestException("Cannot modify table with Materialized View " + oldCfm.cfName + " as it may break the schema. You should use cqlsh to modify tables with Materialized Views instead.");

            if (!oldCfm.isThriftCompatible())
                throw new InvalidRequestException("Cannot modify CQL3 table " + oldCfm.cfName + " as it may break the schema. You should use cqlsh to modify CQL3 tables instead.");

            CFMetaData cfm = ThriftConversion.fromThriftForUpdate(cf_def, oldCfm);
            cfm.params.compaction.validate();

            if (!oldCfm.getTriggers().equals(cfm.getTriggers()))
                state().ensureIsSuper("Only superusers are allowed to add or remove triggers.");

            MigrationManager.announceColumnFamilyUpdate(cfm);
            return Schema.instance.getVersion().toString();
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public void truncate(String cfname) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        ClientState cState = state();

        try
        {
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, cfname, Permission.MODIFY);
            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, cfname, false);
            if (metadata.isView())
                throw new org.apache.cassandra.exceptions.InvalidRequestException("Cannot truncate Materialized Views");

            if (startSessionIfRequested())
            {
                Tracing.instance.begin("truncate", ImmutableMap.of("cf", cfname, "ks", keyspace));
            }
            else
            {
                logger.trace("truncating {}.{}", cState.getKeyspace(), cfname);
            }

            schedule(DatabaseDescriptor.getTruncateRpcTimeout());
            try
            {
                StorageProxy.truncateBlocking(cState.getKeyspace(), cfname);
            }
            finally
            {
                release();
            }
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (org.apache.cassandra.exceptions.UnavailableException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw (UnavailableException) new UnavailableException().initCause(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    public void set_keyspace(String keyspace) throws InvalidRequestException, TException
    {
        try
        {
            state().setKeyspace(keyspace);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public Map<String, List<String>> describe_schema_versions() throws TException, InvalidRequestException
    {
        logger.trace("checking schema agreement");
        return StorageProxy.describeSchemaVersions();
    }

    // counter methods

    public void add(ByteBuffer key, ColumnParent column_parent, CounterColumn column, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("column_parent", column_parent.toString(),
                                                                  "column", column.toString(),
                                                                  "consistency_level", consistency_level.name());
            Tracing.instance.begin("add", traceParameters);
        }
        else
        {
            logger.trace("add");
        }

        try
        {
            ClientState cState = state();
            String keyspace = cState.getKeyspace();

            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.MODIFY);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, true);
            if (metadata.isView())
                throw new org.apache.cassandra.exceptions.InvalidRequestException("Cannot modify Materialized Views directly");

            ThriftValidation.validateKey(metadata, key);
            ThriftConversion.fromThrift(consistency_level).validateCounterForWrite(metadata);
            ThriftValidation.validateColumnParent(metadata, column_parent);
            // SuperColumn field is usually optional, but not when we're adding
            if (metadata.isSuper() && column_parent.super_column == null)
                throw new InvalidRequestException("missing mandatory super column name for super CF " + column_parent.column_family);

            ThriftValidation.validateColumnNames(metadata, column_parent, Arrays.asList(column.name));

            try
            {
                LegacyLayout.LegacyCellName name = LegacyLayout.decodeCellName(metadata, column_parent.super_column, column.name);

                // See UpdateParameters.addCounter() for more details on this
                ByteBuffer value = CounterContext.instance().createLocal(column.value);
                CellPath path = name.collectionElement == null ? null : CellPath.create(name.collectionElement);
                Cell cell = BufferCell.live(metadata, name.column, FBUtilities.timestampMicros(), value, path);

                PartitionUpdate update = PartitionUpdate.singleRowUpdate(metadata, key, BTreeRow.singleCellRow(name.clustering, cell));

                org.apache.cassandra.db.Mutation mutation = new org.apache.cassandra.db.Mutation(update);
                doInsert(consistency_level, Arrays.asList(new CounterMutation(mutation, ThriftConversion.fromThrift(consistency_level))));
            }
            catch (MarshalException|UnknownColumnException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    public void remove_counter(ByteBuffer key, ColumnPath path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("key", ByteBufferUtil.bytesToHex(key),
                                                                  "column_path", path.toString(),
                                                                  "consistency_level", consistency_level.name());
            Tracing.instance.begin("remove_counter", traceParameters);
        }
        else
        {
            logger.trace("remove_counter");
        }

        try
        {
            internal_remove(key, path, FBUtilities.timestampMicros(), consistency_level, true);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    private static String uncompress(ByteBuffer query, Compression compression) throws InvalidRequestException
    {
        String queryString = null;

        // Decompress the query string.
        try
        {
            switch (compression)
            {
                case GZIP:
                    DataOutputBuffer decompressed = new DataOutputBuffer();
                    byte[] outBuffer = new byte[1024], inBuffer = new byte[1024];

                    Inflater decompressor = new Inflater();

                    int lenRead = 0;
                    while (true)
                    {
                        if (decompressor.needsInput())
                            lenRead = query.remaining() < 1024 ? query.remaining() : 1024;
                        query.get(inBuffer, 0, lenRead);
                        decompressor.setInput(inBuffer, 0, lenRead);

                        int lenWrite = 0;
                        while ((lenWrite = decompressor.inflate(outBuffer)) != 0)
                            decompressed.write(outBuffer, 0, lenWrite);

                        if (decompressor.finished())
                            break;
                    }

                    decompressor.end();

                    queryString = new String(decompressed.getData(), 0, decompressed.getLength(), StandardCharsets.UTF_8);
                    break;
                case NONE:
                    try
                    {
                        queryString = ByteBufferUtil.string(query);
                    }
                    catch (CharacterCodingException ex)
                    {
                        throw new InvalidRequestException(ex.getMessage());
                    }
                    break;
            }
        }
        catch (DataFormatException e)
        {
            throw new InvalidRequestException("Error deflating query string.");
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
        return queryString;
    }

    public CqlResult execute_cql_query(ByteBuffer query, Compression compression) throws TException
    {
        throw new InvalidRequestException("CQL2 has been removed in Cassandra 3.0. Please use CQL3 instead");
    }

    public CqlResult execute_cql3_query(ByteBuffer query, Compression compression, ConsistencyLevel cLevel) throws TException
    {
        try
        {
            String queryString = uncompress(query, compression);
            if (startSessionIfRequested())
            {
                Tracing.instance.begin("execute_cql3_query",
                                       ImmutableMap.of("query", queryString,
                                                       "consistency_level", cLevel.name()));
            }
            else
            {
                logger.trace("execute_cql3_query");
            }

            ThriftClientState cState = state();
            return ClientState.getCQLQueryHandler().process(queryString,
                                                            cState.getQueryState(),
                                                            QueryOptions.fromThrift(ThriftConversion.fromThrift(cLevel),
                                                            Collections.<ByteBuffer>emptyList()),
                                                            null).toThriftResult();
        }
        catch (RequestExecutionException e)
        {
            throw ThriftConversion.rethrow(e);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    public CqlPreparedResult prepare_cql_query(ByteBuffer query, Compression compression) throws TException
    {
        throw new InvalidRequestException("CQL2 has been removed in Cassandra 3.0. Please use CQL3 instead");
    }

    public CqlPreparedResult prepare_cql3_query(ByteBuffer query, Compression compression) throws TException
    {
        logger.trace("prepare_cql3_query");

        String queryString = uncompress(query, compression);
        ThriftClientState cState = state();

        try
        {
            cState.validateLogin();
            return ClientState.getCQLQueryHandler().prepare(queryString, cState.getQueryState(), null).toThriftPreparedResult();
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public CqlResult execute_prepared_cql_query(int itemId, List<ByteBuffer> bindVariables) throws TException
    {
        throw new InvalidRequestException("CQL2 has been removed in Cassandra 3.0. Please use CQL3 instead");
    }

    public CqlResult execute_prepared_cql3_query(int itemId, List<ByteBuffer> bindVariables, ConsistencyLevel cLevel) throws TException
    {
        if (startSessionIfRequested())
        {
            // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
            Tracing.instance.begin("execute_prepared_cql3_query", ImmutableMap.of("consistency_level", cLevel.name()));
        }
        else
        {
            logger.trace("execute_prepared_cql3_query");
        }

        try
        {
            ThriftClientState cState = state();
            ParsedStatement.Prepared prepared = ClientState.getCQLQueryHandler().getPreparedForThrift(itemId);

            if (prepared == null)
                throw new InvalidRequestException(String.format("Prepared query with ID %d not found" +
                                                                " (either the query was not prepared on this host (maybe the host has been restarted?)" +
                                                                " or you have prepared too many queries and it has been evicted from the internal cache)",
                                                                itemId));
            logger.trace("Retrieved prepared statement #{} with {} bind markers", itemId, prepared.statement.getBoundTerms());

            return ClientState.getCQLQueryHandler().processPrepared(prepared.statement,
                                                                    cState.getQueryState(),
                                                                    QueryOptions.fromThrift(ThriftConversion.fromThrift(cLevel), bindVariables),
                                                                    null).toThriftResult();
        }
        catch (RequestExecutionException e)
        {
            throw ThriftConversion.rethrow(e);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    @Override
    public List<ColumnOrSuperColumn> get_multi_slice(MultiSliceRequest request)
            throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("key", ByteBufferUtil.bytesToHex(request.key),
                                                                  "column_parent", request.column_parent.toString(),
                                                                  "consistency_level", request.consistency_level.name(),
                                                                  "count", String.valueOf(request.count),
                                                                  "column_slices", request.column_slices.toString());
            Tracing.instance.begin("get_multi_slice", traceParameters);
        }
        else
        {
            logger.trace("get_multi_slice");
        }
        try 
        {
            ClientState cState = state();
            String keyspace = cState.getKeyspace();
            state().hasColumnFamilyAccess(keyspace, request.getColumn_parent().column_family, Permission.SELECT);
            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, request.getColumn_parent().column_family);
            if (metadata.isSuper())
                throw new org.apache.cassandra.exceptions.InvalidRequestException("get_multi_slice does not support super columns");
            ThriftValidation.validateColumnParent(metadata, request.getColumn_parent());
            org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(request.getConsistency_level());
            consistencyLevel.validateForRead(keyspace);

            Slices.Builder builder = new Slices.Builder(metadata.comparator, request.getColumn_slices().size());
            for (int i = 0 ; i < request.getColumn_slices().size() ; i++)
            {
                fixOptionalSliceParameters(request.getColumn_slices().get(i));
                Slice.Bound start = LegacyLayout.decodeBound(metadata, request.getColumn_slices().get(i).start, true).bound;
                Slice.Bound finish = LegacyLayout.decodeBound(metadata, request.getColumn_slices().get(i).finish, false).bound;

                int compare = metadata.comparator.compare(start, finish);
                if (!request.reversed && compare > 0)
                    throw new InvalidRequestException(String.format("Column slice at index %d had start greater than finish", i));
                else if (request.reversed && compare < 0)
                    throw new InvalidRequestException(String.format("Reversed column slice at index %d had start less than finish", i));

                builder.add(request.reversed ? Slice.make(finish, start) : Slice.make(start, finish));
            }

            Slices slices = builder.build();
            ColumnFilter columns = makeColumnFilter(metadata, slices);
            ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slices, request.reversed);
            DataLimits limits = getLimits(1, false, request.count);

            ThriftValidation.validateKey(metadata, request.key);
            DecoratedKey dk = metadata.decorateKey(request.key);
            SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(true, metadata, FBUtilities.nowInSeconds(), columns, RowFilter.NONE, limits, dk, filter);
            return getSlice(Collections.<SinglePartitionReadCommand>singletonList(cmd),
                            false,
                            limits.perPartitionCount(),
                            consistencyLevel,
                            cState).entrySet().iterator().next().getValue();
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        } 
        finally 
        {
            Tracing.instance.stopSession();
        }
    }

    /**
     * Set the to start-of end-of value of "" for start and finish.
     * @param columnSlice
     */
    private static void fixOptionalSliceParameters(org.apache.cassandra.thrift.ColumnSlice columnSlice) {
        if (!columnSlice.isSetStart())
            columnSlice.setStart(new byte[0]);
        if (!columnSlice.isSetFinish())
            columnSlice.setFinish(new byte[0]);
    }

    /*
     * No-op since 3.0.
     */
    public void set_cql_version(String version)
    {
    }

    public ByteBuffer trace_next_query() throws TException
    {
        UUID sessionId = UUIDGen.getTimeUUID();
        state().getQueryState().prepareTracingSession(sessionId);
        return TimeUUIDType.instance.decompose(sessionId);
    }

    private boolean startSessionIfRequested()
    {
        if (state().getQueryState().traceNextQuery())
        {
            state().getQueryState().createTracingSession();
            return true;
        }
        return false;
    }

    private void registerMetrics()
    {
        ClientMetrics.instance.addCounter("connectedThriftClients", new Callable<Integer>()
        {
            @Override
            public Integer call() throws Exception
            {
                return ThriftSessionManager.instance.getConnectedClients();
            }
        });
    }

    private static class ThriftCASRequest implements CASRequest
    {
        private final CFMetaData metadata;
        private final DecoratedKey key;
        private final List<LegacyLayout.LegacyCell> expected;
        private final PartitionUpdate updates;
        private final int nowInSec;

        private ThriftCASRequest(List<LegacyLayout.LegacyCell> expected, PartitionUpdate updates, int nowInSec)
        {
            this.metadata = updates.metadata();
            this.key = updates.partitionKey();
            this.expected = expected;
            this.updates = updates;
            this.nowInSec = nowInSec;
        }

        public SinglePartitionReadCommand readCommand(int nowInSec)
        {
            if (expected.isEmpty())
            {
                // We want to know if the partition exists, so just fetch a single cell.
                ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(Slices.ALL, false);
                DataLimits limits = DataLimits.thriftLimits(1, 1);
                return new SinglePartitionReadCommand(false, 0, true, metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.NONE, limits, key, filter);
            }

            // Gather the clustering for the expected values and query those.
            BTreeSet.Builder<Clustering> clusterings = BTreeSet.builder(metadata.comparator);
            FilteredPartition expectedPartition =
                FilteredPartition.create(LegacyLayout.toRowIterator(metadata, key, expected.iterator(), nowInSec));

            for (Row row : expectedPartition)
                clusterings.add(row.clustering());

            PartitionColumns columns = expectedPartition.staticRow().isEmpty()
                                     ? metadata.partitionColumns().withoutStatics()
                                     : metadata.partitionColumns();
            ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings.build(), false);
            return SinglePartitionReadCommand.create(true, metadata, nowInSec, ColumnFilter.selection(columns), RowFilter.NONE, DataLimits.NONE, key, filter);
        }

        public boolean appliesTo(FilteredPartition current)
        {
            if (expected.isEmpty())
                return current.isEmpty();
            else if (current.isEmpty())
                return false;

            // Push the expected results through ThriftResultsMerger to translate any static
            // columns into clusterings. The current partition is retrieved in the same so
            // unless they're both handled the same, they won't match.
            FilteredPartition expectedPartition =
                FilteredPartition.create(
                    UnfilteredRowIterators.filter(
                        ThriftResultsMerger.maybeWrap(expectedToUnfilteredRowIterator(), nowInSec), nowInSec));

            // Check that for everything we expected, the fetched values exists and correspond.
            for (Row e : expectedPartition)
            {
                Row c = current.getRow(e.clustering());
                if (c == null)
                    return false;

                SearchIterator<ColumnDefinition, ColumnData> searchIter = c.searchIterator();
                for (ColumnData expectedData : e)
                {
                    ColumnDefinition column = expectedData.column();
                    ColumnData currentData = searchIter.next(column);
                    if (currentData == null)
                        return false;

                    if (column.isSimple())
                    {
                        if (!((Cell)currentData).value().equals(((Cell)expectedData).value()))
                            return false;
                    }
                    else
                    {
                        ComplexColumnData currentComplexData = (ComplexColumnData)currentData;
                        for (Cell expectedCell : (ComplexColumnData)expectedData)
                        {
                            Cell currentCell = currentComplexData.getCell(expectedCell.path());
                            if (currentCell == null || !currentCell.value().equals(expectedCell.value()))
                                return false;
                        }
                    }
                }
            }
            return true;
        }

        public PartitionUpdate makeUpdates(FilteredPartition current)
        {
            return updates;
        }

        private UnfilteredRowIterator expectedToUnfilteredRowIterator()
        {
            return LegacyLayout.toUnfilteredRowIterator(metadata, key, LegacyLayout.LegacyDeletionInfo.live(), expected.iterator());
        }
    }
}
