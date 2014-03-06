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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql.CQLStatement;
import org.apache.cassandra.cql.QueryProcessor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.CASConditions;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SemanticVersion;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.thrift.TException;

public class CassandraServer implements Cassandra.Iface
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraServer.class);

    private final static int COUNT_PAGE_SIZE = 1024;

    private final static List<ColumnOrSuperColumn> EMPTY_COLUMNS = Collections.emptyList();

    private volatile boolean loggedCQL2Warning = false;

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

    protected Map<DecoratedKey, ColumnFamily> readColumnFamily(List<ReadCommand> commands, org.apache.cassandra.db.ConsistencyLevel consistency_level)
    throws org.apache.cassandra.exceptions.InvalidRequestException, UnavailableException, TimedOutException
    {
        // TODO - Support multiple column families per row, right now row only contains 1 column family
        Map<DecoratedKey, ColumnFamily> columnFamilyKeyMap = new HashMap<DecoratedKey, ColumnFamily>();

        List<Row> rows = null;
        try
        {
            schedule(DatabaseDescriptor.getReadRpcTimeout());
            try
            {
                rows = StorageProxy.read(commands, consistency_level);
            }
            finally
            {
                release();
            }
        }
        catch (RequestExecutionException e)
        {
            ThriftConversion.rethrow(e);
        }

        for (Row row: rows)
        {
            columnFamilyKeyMap.put(row.key, row.cf);
        }
        return columnFamilyKeyMap;
    }

    public List<ColumnOrSuperColumn> thriftifyColumns(Collection<org.apache.cassandra.db.Column> columns, boolean reverseOrder, long now)
    {
        ArrayList<ColumnOrSuperColumn> thriftColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
        for (org.apache.cassandra.db.Column column : columns)
        {
            if (column.isMarkedForDelete(now))
                continue;

            thriftColumns.add(thriftifyColumnWithName(column, column.name()));
        }

        // we have to do the reversing here, since internally we pass results around in ColumnFamily
        // objects, which always sort their columns in the "natural" order
        // TODO this is inconvenient for direct users of StorageProxy
        if (reverseOrder)
            Collections.reverse(thriftColumns);
        return thriftColumns;
    }

    private ColumnOrSuperColumn thriftifyColumnWithName(org.apache.cassandra.db.Column column, ByteBuffer newName)
    {
        if (column instanceof org.apache.cassandra.db.CounterColumn)
            return new ColumnOrSuperColumn().setCounter_column(thriftifySubCounter(column).setName(newName));
        else
            return new ColumnOrSuperColumn().setColumn(thriftifySubColumn(column).setName(newName));
    }

    private Column thriftifySubColumn(org.apache.cassandra.db.Column column)
    {
        assert !(column instanceof org.apache.cassandra.db.CounterColumn);

        Column thrift_column = new Column(column.name()).setValue(column.value()).setTimestamp(column.timestamp());
        if (column instanceof ExpiringColumn)
        {
            thrift_column.setTtl(((ExpiringColumn) column).getTimeToLive());
        }
        return thrift_column;
    }

    private List<Column> thriftifyColumnsAsColumns(Collection<org.apache.cassandra.db.Column> columns, long now)
    {
        List<Column> thriftColumns = new ArrayList<Column>(columns.size());
        for (org.apache.cassandra.db.Column column : columns)
        {
            if (column.isMarkedForDelete(now))
                continue;

            thriftColumns.add(thriftifySubColumn(column));
        }
        return thriftColumns;
    }

    private CounterColumn thriftifySubCounter(org.apache.cassandra.db.Column column)
    {
        assert column instanceof org.apache.cassandra.db.CounterColumn;
        return new CounterColumn(column.name(), CounterContext.instance().total(column.value()));
    }

    private List<ColumnOrSuperColumn> thriftifySuperColumns(Collection<org.apache.cassandra.db.Column> columns,
                                                            boolean reverseOrder,
                                                            long now,
                                                            boolean subcolumnsOnly,
                                                            boolean isCounterCF)
    {
        if (subcolumnsOnly)
        {
            ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
            for (org.apache.cassandra.db.Column column : columns)
            {
                if (column.isMarkedForDelete(now))
                    continue;

                thriftSuperColumns.add(thriftifyColumnWithName(column, SuperColumns.subName(column.name())));
            }
            if (reverseOrder)
                Collections.reverse(thriftSuperColumns);
            return thriftSuperColumns;
        }
        else
        {
            if (isCounterCF)
                return thriftifyCounterSuperColumns(columns, reverseOrder, now);
            else
                return thriftifySuperColumns(columns, reverseOrder, now);
        }
    }

    private List<ColumnOrSuperColumn> thriftifySuperColumns(Collection<org.apache.cassandra.db.Column> columns, boolean reverseOrder, long now)
    {
        ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
        SuperColumn current = null;
        for (org.apache.cassandra.db.Column column : columns)
        {
            if (column.isMarkedForDelete(now))
                continue;

            ByteBuffer scName = SuperColumns.scName(column.name());
            if (current == null || !scName.equals(current.bufferForName()))
            {
                current = new SuperColumn(scName, new ArrayList<Column>());
                thriftSuperColumns.add(new ColumnOrSuperColumn().setSuper_column(current));
            }
            current.getColumns().add(thriftifySubColumn(column).setName(SuperColumns.subName(column.name())));
        }

        if (reverseOrder)
            Collections.reverse(thriftSuperColumns);

        return thriftSuperColumns;
    }

    private List<ColumnOrSuperColumn> thriftifyCounterSuperColumns(Collection<org.apache.cassandra.db.Column> columns, boolean reverseOrder, long now)
    {
        ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
        CounterSuperColumn current = null;
        for (org.apache.cassandra.db.Column column : columns)
        {
            if (column.isMarkedForDelete(now))
                continue;

            ByteBuffer scName = SuperColumns.scName(column.name());
            if (current == null || !scName.equals(current.bufferForName()))
            {
                current = new CounterSuperColumn(scName, new ArrayList<CounterColumn>());
                thriftSuperColumns.add(new ColumnOrSuperColumn().setCounter_super_column(current));
            }
            current.getColumns().add(thriftifySubCounter(column).setName(SuperColumns.subName(column.name())));
        }

        if (reverseOrder)
            Collections.reverse(thriftSuperColumns);

        return thriftSuperColumns;
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> getSlice(List<ReadCommand> commands, boolean subColumnsOnly, org.apache.cassandra.db.ConsistencyLevel consistency_level)
    throws org.apache.cassandra.exceptions.InvalidRequestException, UnavailableException, TimedOutException
    {
        Map<DecoratedKey, ColumnFamily> columnFamilies = readColumnFamily(commands, consistency_level);
        Map<ByteBuffer, List<ColumnOrSuperColumn>> columnFamiliesMap = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
        for (ReadCommand command: commands)
        {
            ColumnFamily cf = columnFamilies.get(StorageService.getPartitioner().decorateKey(command.key));
            boolean reverseOrder = command instanceof SliceFromReadCommand && ((SliceFromReadCommand)command).filter.reversed;
            List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyColumnFamily(cf, subColumnsOnly, reverseOrder, command.timestamp);
            columnFamiliesMap.put(command.key, thriftifiedColumns);
        }

        return columnFamiliesMap;
    }

    private List<ColumnOrSuperColumn> thriftifyColumnFamily(ColumnFamily cf, boolean subcolumnsOnly, boolean reverseOrder, long now)
    {
        if (cf == null || cf.getColumnCount() == 0)
            return EMPTY_COLUMNS;

        if (cf.metadata().isSuper())
        {
            boolean isCounterCF = cf.metadata().getDefaultValidator().isCommutative();
            return thriftifySuperColumns(cf.getSortedColumns(), reverseOrder, now, subcolumnsOnly, isCounterCF);
        }
        else
        {
            return thriftifyColumns(cf.getSortedColumns(), reverseOrder, now);
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
            logger.debug("get_slice");
        }

        try
        {
            ClientState cState = state();
            String keyspace = cState.getKeyspace();
            state().hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
            return getSliceInternal(keyspace, key, column_parent, System.currentTimeMillis(), predicate, consistency_level);
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
                                                       long timestamp,
                                                       SlicePredicate predicate,
                                                       ConsistencyLevel consistency_level)
    throws org.apache.cassandra.exceptions.InvalidRequestException, UnavailableException, TimedOutException
    {
        return multigetSliceInternal(keyspace, Collections.singletonList(key), column_parent, timestamp, predicate, consistency_level).get(key);
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
            logger.debug("multiget_slice");
        }

        try
        {
            ClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
            return multigetSliceInternal(keyspace, keys, column_parent, System.currentTimeMillis(), predicate, consistency_level);
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

    private SliceQueryFilter toInternalFilter(CFMetaData metadata, ColumnParent parent, SliceRange range)
    {
        SliceQueryFilter filter = new SliceQueryFilter(range.start, range.finish, range.reversed, range.count);
        if (metadata.isSuper())
            filter = SuperColumns.fromSCSliceFilter((CompositeType)metadata.comparator, parent.bufferForSuper_column(), filter);
        return filter;
    }

    private IDiskAtomFilter toInternalFilter(CFMetaData metadata, ColumnParent parent, SlicePredicate predicate)
    {
        IDiskAtomFilter filter;
        if (predicate.column_names != null)
        {
            if (metadata.isSuper())
            {
                CompositeType type = (CompositeType)metadata.comparator;
                SortedSet s = new TreeSet<ByteBuffer>(parent.isSetSuper_column() ? type.types.get(1) : type.types.get(0));
                s.addAll(predicate.column_names);
                filter = SuperColumns.fromSCNamesFilter(type, parent.bufferForSuper_column(), new NamesQueryFilter(s));
            }
            else
            {
                SortedSet s = new TreeSet<ByteBuffer>(metadata.comparator);
                s.addAll(predicate.column_names);
                filter = new NamesQueryFilter(s);
            }
        }
        else
        {
            filter = toInternalFilter(metadata, parent, predicate.slice_range);
        }
        return filter;
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetSliceInternal(String keyspace,
                                                                             List<ByteBuffer> keys,
                                                                             ColumnParent column_parent,
                                                                             long timestamp,
                                                                             SlicePredicate predicate,
                                                                             ConsistencyLevel consistency_level)
    throws org.apache.cassandra.exceptions.InvalidRequestException, UnavailableException, TimedOutException
    {
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, predicate);

        org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
        consistencyLevel.validateForRead(keyspace);

        List<ReadCommand> commands = new ArrayList<ReadCommand>(keys.size());
        IDiskAtomFilter filter = toInternalFilter(metadata, column_parent, predicate);

        for (ByteBuffer key: keys)
        {
            ThriftValidation.validateKey(metadata, key);
            // Note that we should not share a slice filter amongst the command, due to SliceQueryFilter not  being immutable
            // due to its columnCounter used by the lastCounted() method (also see SelectStatement.getSliceCommands)
            commands.add(ReadCommand.create(keyspace, key, column_parent.getColumn_family(), timestamp, filter.cloneShallow()));
        }

        return getSlice(commands, column_parent.isSetSuper_column(), consistencyLevel);
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
            logger.debug("get");
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

            IDiskAtomFilter filter;
            if (metadata.isSuper())
            {
                CompositeType type = (CompositeType)metadata.comparator;
                SortedSet names = new TreeSet<ByteBuffer>(column_path.column == null ? type.types.get(0) : type.types.get(1));
                names.add(column_path.column == null ? column_path.super_column : column_path.column);
                filter = SuperColumns.fromSCNamesFilter(type, column_path.column == null ? null : column_path.bufferForSuper_column(), new NamesQueryFilter(names));
            }
            else
            {
                SortedSet<ByteBuffer> names = new TreeSet<ByteBuffer>(metadata.comparator);
                names.add(column_path.column);
                filter = new NamesQueryFilter(names);
            }

            long now = System.currentTimeMillis();
            ReadCommand command = ReadCommand.create(keyspace, key, column_path.column_family, now, filter);

            Map<DecoratedKey, ColumnFamily> cfamilies = readColumnFamily(Arrays.asList(command), consistencyLevel);

            ColumnFamily cf = cfamilies.get(StorageService.getPartitioner().decorateKey(command.key));

            if (cf == null)
                throw new NotFoundException();
            List<ColumnOrSuperColumn> tcolumns = thriftifyColumnFamily(cf, metadata.isSuper() && column_path.column != null, false, now);
            if (tcolumns.isEmpty())
                throw new NotFoundException();
            assert tcolumns.size() == 1;
            return tcolumns.get(0);
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
            logger.debug("get_count");
        }

        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
            Keyspace keyspaceName = Keyspace.open(keyspace);
            ColumnFamilyStore cfs = keyspaceName.getColumnFamilyStore(column_parent.column_family);
            long timestamp = System.currentTimeMillis();

            if (predicate.column_names != null)
                return getSliceInternal(keyspace, key, column_parent, timestamp, predicate, consistency_level).size();

            int pageSize;
            // request by page if this is a large row
            if (cfs.getMeanColumns() > 0)
            {
                int averageColumnSize = (int) (cfs.getMeanRowSize() / cfs.getMeanColumns());
                pageSize = Math.min(COUNT_PAGE_SIZE, DatabaseDescriptor.getInMemoryCompactionLimit() / averageColumnSize);
                pageSize = Math.max(2, pageSize);
                logger.debug("average row column size is {}; using pageSize of {}", averageColumnSize, pageSize);
            }
            else
            {
                pageSize = COUNT_PAGE_SIZE;
            }

            SliceRange sliceRange = predicate.slice_range == null
                                  ? new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE)
                                  : predicate.slice_range;
            SliceQueryFilter filter = toInternalFilter(cfs.metadata, column_parent, sliceRange);

            return QueryPagers.countPaged(keyspace,
                                          column_parent.column_family,
                                          key,
                                          filter,
                                          ThriftConversion.fromThrift(consistency_level),
                                          pageSize,
                                          timestamp);
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

    private static ByteBuffer getName(ColumnOrSuperColumn cosc)
    {
        return cosc.isSetSuper_column() ? cosc.super_column.name :
                   (cosc.isSetColumn() ? cosc.column.name :
                       (cosc.isSetCounter_column() ? cosc.counter_column.name : cosc.counter_super_column.name));
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
            logger.debug("multiget_count");
        }

        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);

            Map<ByteBuffer, Integer> counts = new HashMap<ByteBuffer, Integer>();
            Map<ByteBuffer, List<ColumnOrSuperColumn>> columnFamiliesMap = multigetSliceInternal(keyspace,
                                                                                                 keys,
                                                                                                 column_parent,
                                                                                                 System.currentTimeMillis(),
                                                                                                 predicate,
                                                                                                 consistency_level);

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

    private void internal_insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level)
    throws RequestValidationException, UnavailableException, TimedOutException
    {
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.MODIFY);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, false);
        ThriftValidation.validateKey(metadata, key);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        // SuperColumn field is usually optional, but not when we're inserting
        if (metadata.cfType == ColumnFamilyType.Super && column_parent.super_column == null)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException("missing mandatory super column name for super CF " + column_parent.column_family);
        }
        ThriftValidation.validateColumnNames(metadata, column_parent, Arrays.asList(column.name));
        ThriftValidation.validateColumnData(metadata, column, column_parent.super_column != null);

        RowMutation rm;
        try
        {
            ByteBuffer name = column.name;
            if (metadata.isSuper())
                name = CompositeType.build(column_parent.super_column, name);

            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cState.getKeyspace(), column_parent.column_family);
            cf.addColumn(name, column.value, column.timestamp, column.ttl);
            rm = new RowMutation(cState.getKeyspace(), key, cf);
        }
        catch (MarshalException e)
        {
            throw new org.apache.cassandra.exceptions.InvalidRequestException(e.getMessage());
        }
        doInsert(consistency_level, Arrays.asList(rm));
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
            logger.debug("insert");
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
            Map<String, String> traceParameters = ImmutableMap.of("key", ByteBufferUtil.bytesToHex(key),
                                                                  "column_family", column_family,
                                                                  "old", expected.toString(),
                                                                  "updates", updates.toString());
            Tracing.instance.begin("cas", traceParameters);
        }
        else
        {
            logger.debug("cas");
        }

        try
        {
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_family, Permission.MODIFY);
            // CAS updates can be used to simulate a get request, so should require Permission.SELECT.
            cState.hasColumnFamilyAccess(keyspace, column_family, Permission.SELECT);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_family, false);
            ThriftValidation.validateKey(metadata, key);
            if (metadata.cfType == ColumnFamilyType.Super)
                throw new org.apache.cassandra.exceptions.InvalidRequestException("CAS does not support supercolumns");

            Iterable<ByteBuffer> names = Iterables.transform(updates, new Function<Column, ByteBuffer>()
            {
                public ByteBuffer apply(Column column)
                {
                    return column.name;
                }
            });
            ThriftValidation.validateColumnNames(metadata, new ColumnParent(column_family), names);
            for (Column column : updates)
                ThriftValidation.validateColumnData(metadata, column, false);

            CFMetaData cfm = Schema.instance.getCFMetaData(cState.getKeyspace(), column_family);
            UnsortedColumns cfUpdates = UnsortedColumns.factory.create(cfm);
            for (Column column : updates)
                cfUpdates.addColumn(column.name, column.value, column.timestamp);

            ColumnFamily cfExpected;
            if (expected.isEmpty())
            {
                cfExpected = null;
            }
            else
            {
                cfExpected = TreeMapBackedSortedColumns.factory.create(cfm);
                for (Column column : expected)
                    cfExpected.addColumn(column.name, column.value, column.timestamp);
            }

            schedule(DatabaseDescriptor.getWriteRpcTimeout());
            ColumnFamily result = StorageProxy.cas(cState.getKeyspace(),
                                                   column_family,
                                                   key,
                                                   new ThriftCASConditions(cfExpected),
                                                   cfUpdates,
                                                   ThriftConversion.fromThrift(serial_consistency_level),
                                                   ThriftConversion.fromThrift(commit_consistency_level));
            return result == null
                 ? new CASResult(true)
                 : new CASResult(false).setCurrent_values(thriftifyColumnsAsColumns(result.getSortedColumns(), System.currentTimeMillis()));
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

    private List<IMutation> createMutationList(ConsistencyLevel consistency_level,
                                               Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map,
                                               boolean allowCounterMutations)
    throws RequestValidationException
    {
        List<IMutation> rowMutations = new ArrayList<IMutation>();
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();

        for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> mutationEntry: mutation_map.entrySet())
        {
            ByteBuffer key = mutationEntry.getKey();

            // We need to separate row mutation for standard cf and counter cf (that will be encapsulated in a
            // CounterMutation) because it doesn't follow the same code path
            RowMutation rmStandard = null;
            RowMutation rmCounter = null;

            Map<String, List<Mutation>> columnFamilyToMutations = mutationEntry.getValue();
            for (Map.Entry<String, List<Mutation>> columnFamilyMutations : columnFamilyToMutations.entrySet())
            {
                String cfName = columnFamilyMutations.getKey();

                cState.hasColumnFamilyAccess(keyspace, cfName, Permission.MODIFY);

                CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, cfName);
                ThriftValidation.validateKey(metadata, key);

                RowMutation rm;
                if (metadata.getDefaultValidator().isCommutative())
                {
                    ThriftConversion.fromThrift(consistency_level).validateCounterForWrite(metadata);
                    rmCounter = rmCounter == null ? new RowMutation(keyspace, key) : rmCounter;
                    rm = rmCounter;
                }
                else
                {
                    rmStandard = rmStandard == null ? new RowMutation(keyspace, key) : rmStandard;
                    rm = rmStandard;
                }

                for (Mutation mutation : columnFamilyMutations.getValue())
                {
                    ThriftValidation.validateMutation(metadata, mutation);

                    if (mutation.deletion != null)
                    {
                        deleteColumnOrSuperColumn(rm, cfName, mutation.deletion);
                    }
                    if (mutation.column_or_supercolumn != null)
                    {
                        addColumnOrSuperColumn(rm, cfName, mutation.column_or_supercolumn);
                    }
                }
            }
            if (rmStandard != null && !rmStandard.isEmpty())
                rowMutations.add(rmStandard);

            if (rmCounter != null && !rmCounter.isEmpty())
            {
                if (allowCounterMutations)
                    rowMutations.add(new CounterMutation(rmCounter, ThriftConversion.fromThrift(consistency_level)));
                else
                    throw new org.apache.cassandra.exceptions.InvalidRequestException("Counter mutations are not allowed in atomic batches");
            }
        }

        return rowMutations;
    }

    private void addColumnOrSuperColumn(RowMutation rm, String cfName, ColumnOrSuperColumn cosc)
    {
        if (cosc.super_column != null)
        {
            for (Column column : cosc.super_column.columns)
            {
                rm.add(cfName, CompositeType.build(cosc.super_column.name, column.name), column.value, column.timestamp, column.ttl);
            }
        }
        else if (cosc.column != null)
        {
            rm.add(cfName, cosc.column.name, cosc.column.value, cosc.column.timestamp, cosc.column.ttl);
        }
        else if (cosc.counter_super_column != null)
        {
            for (CounterColumn column : cosc.counter_super_column.columns)
            {
                rm.addCounter(cfName, CompositeType.build(cosc.counter_super_column.name, column.name), column.value);
            }
        }
        else // cosc.counter_column != null
        {
            rm.addCounter(cfName, cosc.counter_column.name, cosc.counter_column.value);
        }
    }

    private void deleteColumnOrSuperColumn(RowMutation rm, String cfName, Deletion del)
    {
        if (del.predicate != null && del.predicate.column_names != null)
        {
            for (ByteBuffer c : del.predicate.column_names)
            {
                if (del.super_column == null && Schema.instance.getColumnFamilyType(rm.getKeyspaceName(), cfName) == ColumnFamilyType.Super)
                    rm.deleteRange(cfName, SuperColumns.startOf(c), SuperColumns.endOf(c), del.timestamp);
                else if (del.super_column != null)
                    rm.delete(cfName, CompositeType.build(del.super_column, c), del.timestamp);
                else
                    rm.delete(cfName, c, del.timestamp);
            }
        }
        else if (del.predicate != null && del.predicate.slice_range != null)
        {
            if (del.super_column == null && Schema.instance.getColumnFamilyType(rm.getKeyspaceName(), cfName) == ColumnFamilyType.Super)
                rm.deleteRange(cfName,
                               SuperColumns.startOf(del.predicate.getSlice_range().start),
                               SuperColumns.startOf(del.predicate.getSlice_range().finish),
                               del.timestamp);
            else if (del.super_column != null)
                rm.deleteRange(cfName,
                               CompositeType.build(del.super_column, del.predicate.getSlice_range().start),
                               CompositeType.build(del.super_column, del.predicate.getSlice_range().finish),
                               del.timestamp);
            else
                rm.deleteRange(cfName, del.predicate.getSlice_range().start, del.predicate.getSlice_range().finish, del.timestamp);
        }
        else
        {
            if (del.super_column != null)
                rm.deleteRange(cfName, SuperColumns.startOf(del.super_column), SuperColumns.endOf(del.super_column), del.timestamp);
            else
                rm.delete(cfName, del.timestamp);
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
            logger.debug("batch_mutate");
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
            logger.debug("atomic_batch_mutate");
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
        ThriftValidation.validateKey(metadata, key);
        ThriftValidation.validateColumnPathOrParent(metadata, column_path);
        if (isCommutativeOp)
            ThriftConversion.fromThrift(consistency_level).validateCounterForWrite(metadata);

        RowMutation rm = new RowMutation(keyspace, key);
        if (column_path.super_column == null && column_path.column == null)
            rm.delete(column_path.column_family, timestamp);
        else if (column_path.super_column == null)
            rm.delete(column_path.column_family, column_path.column, timestamp);
        else if (column_path.column == null)
            rm.deleteRange(column_path.column_family, SuperColumns.startOf(column_path.super_column), SuperColumns.endOf(column_path.super_column), timestamp);
        else
            rm.delete(column_path.column_family, CompositeType.build(column_path.super_column, column_path.column), timestamp);

        if (isCommutativeOp)
            doInsert(consistency_level, Arrays.asList(new CounterMutation(rm, ThriftConversion.fromThrift(consistency_level))));
        else
            doInsert(consistency_level, Arrays.asList(rm));
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
            logger.debug("remove");
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

        schedule(DatabaseDescriptor.getWriteRpcTimeout());
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

        KSMetaData ksm = Schema.instance.getKSMetaData(keyspaceName);
        if (ksm == null)
            throw new NotFoundException();

        return ksm.toThrift();
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
            logger.debug("range_slice");
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

            List<Row> rows = null;

            IPartitioner<?> p = StorageService.getPartitioner();
            AbstractBounds<RowPosition> bounds;
            if (range.start_key == null)
            {
                Token.TokenFactory<?> tokenFactory = p.getTokenFactory();
                Token left = tokenFactory.fromString(range.start_token);
                Token right = tokenFactory.fromString(range.end_token);
                bounds = Range.makeRowRange(left, right, p);
            }
            else
            {
                RowPosition end = range.end_key == null
                                ? p.getTokenFactory().fromString(range.end_token).maxKeyBound(p)
                                : RowPosition.forKey(range.end_key, p);
                bounds = new Bounds<RowPosition>(RowPosition.forKey(range.start_key, p), end);
            }
            long now = System.currentTimeMillis();
            schedule(DatabaseDescriptor.getRangeRpcTimeout());
            try
            {
                IDiskAtomFilter filter = ThriftValidation.asIFilter(predicate, metadata, column_parent.super_column);
                rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace,
                                                                        column_parent.column_family,
                                                                        now,
                                                                        filter,
                                                                        bounds,
                                                                        range.row_filter,
                                                                        range.count),
                                                  consistencyLevel);
            }
            finally
            {
                release();
            }
            assert rows != null;

            return thriftifyKeySlices(rows, column_parent, predicate, now);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (ReadTimeoutException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (org.apache.cassandra.exceptions.UnavailableException e)
        {
            throw ThriftConversion.toThrift(e);
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
            logger.debug("get_paged_slice");
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

            SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(start_column, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, -1));

            IPartitioner p = StorageService.getPartitioner();
            AbstractBounds<RowPosition> bounds;
            if (range.start_key == null)
            {
                // (token, key) is unsupported, assume (token, token)
                Token.TokenFactory tokenFactory = p.getTokenFactory();
                Token left = tokenFactory.fromString(range.start_token);
                Token right = tokenFactory.fromString(range.end_token);
                bounds = Range.makeRowRange(left, right, p);
            }
            else
            {
                RowPosition end = range.end_key == null
                                ? p.getTokenFactory().fromString(range.end_token).maxKeyBound(p)
                                : RowPosition.forKey(range.end_key, p);
                bounds = new Bounds<RowPosition>(RowPosition.forKey(range.start_key, p), end);
            }

            if (range.row_filter != null && !range.row_filter.isEmpty())
                throw new InvalidRequestException("Cross-row paging is not supported along with index clauses");

            List<Row> rows;
            long now = System.currentTimeMillis();
            schedule(DatabaseDescriptor.getRangeRpcTimeout());
            try
            {
                IDiskAtomFilter filter = ThriftValidation.asIFilter(predicate, metadata, null);
                rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace, column_family, now, filter, bounds, null, range.count, true, true), consistencyLevel);
            }
            finally
            {
                release();
            }
            assert rows != null;

            return thriftifyKeySlices(rows, new ColumnParent(column_family), predicate, now);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (ReadTimeoutException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (org.apache.cassandra.exceptions.UnavailableException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    private List<KeySlice> thriftifyKeySlices(List<Row> rows, ColumnParent column_parent, SlicePredicate predicate, long now)
    {
        List<KeySlice> keySlices = new ArrayList<KeySlice>(rows.size());
        boolean reversed = predicate.slice_range != null && predicate.slice_range.reversed;
        for (Row row : rows)
        {
            List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyColumnFamily(row.cf, column_parent.super_column != null, reversed, now);
            keySlices.add(new KeySlice(row.key.key, thriftifiedColumns));
        }

        return keySlices;
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
            logger.debug("scan");
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

            IPartitioner p = StorageService.getPartitioner();
            AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(RowPosition.forKey(index_clause.start_key, p),
                                                                         p.getMinimumToken().minKeyBound());

            IDiskAtomFilter filter = ThriftValidation.asIFilter(column_predicate, metadata, column_parent.super_column);
            long now = System.currentTimeMillis();
            RangeSliceCommand command = new RangeSliceCommand(keyspace,
                                                              column_parent.column_family,
                                                              now,
                                                              filter,
                                                              bounds,
                                                              index_clause.expressions,
                                                              index_clause.count);

            List<Row> rows = StorageProxy.getRangeSlice(command, consistencyLevel);
            return thriftifyKeySlices(rows, column_parent, column_predicate, now);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (ReadTimeoutException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (org.apache.cassandra.exceptions.UnavailableException e)
        {
            throw ThriftConversion.toThrift(e);
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
        List<KsDef> ksset = new ArrayList<KsDef>(keyspaces.size());
        for (String ks : keyspaces)
        {
            try
            {
                ksset.add(describe_keyspace(ks));
            }
            catch (NotFoundException nfe)
            {
                logger.info("Failed to find metadata for keyspace '" + ks + "'. Continuing... ");
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
        return StorageService.getPartitioner().getClass().getName();
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
        List<String> result = new ArrayList<String>(splits.size() + 1);

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
            Token.TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
            Range<Token> tr = new Range<Token>(tf.fromString(start_token), tf.fromString(end_token));
            List<Pair<Range<Token>, Long>> splits =
                    StorageService.instance.getSplits(state().getKeyspace(), cfName, tr, keys_per_split, Schema.instance.getCFMetaData(state().getKeyspace(), cfName));
            List<CfSplit> result = new ArrayList<CfSplit>(splits.size());
            for (Pair<Range<Token>, Long> split : splits)
                result.add(new CfSplit(split.left.left.toString(), split.left.right.toString(), split.right));
            return result;
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public void login(AuthenticationRequest auth_request) throws AuthenticationException, AuthorizationException, TException
    {
        try
        {
            AuthenticatedUser user = DatabaseDescriptor.getAuthenticator().authenticate(auth_request.getCredentials());
            state().login(user);
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

    public String system_add_column_family(CfDef cf_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("add_column_family");

        try
        {
            ClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasKeyspaceAccess(keyspace, Permission.CREATE);
            cf_def.unsetId(); // explicitly ignore any id set by client (Hector likes to set zero)
            CFMetaData cfm = CFMetaData.fromThrift(cf_def);
            CFMetaData.validateCompactionOptions(cfm.compactionStrategyClass, cfm.compactionStrategyOptions);
            cfm.addDefaultIndexNames();

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
        logger.debug("drop_column_family");

        ThriftClientState cState = state();

        try
        {
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_family, Permission.DROP);
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
        logger.debug("add_keyspace");

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
                CFMetaData cfm = CFMetaData.fromThrift(cf_def);
                cfm.addDefaultIndexNames();

                if (!cfm.getTriggers().isEmpty())
                    state().ensureIsSuper("Only superusers are allowed to add triggers.");

                cfDefs.add(cfm);
            }
            MigrationManager.announceNewKeyspace(KSMetaData.fromThrift(ks_def, cfDefs.toArray(new CFMetaData[cfDefs.size()])));
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
        logger.debug("drop_keyspace");

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
        logger.debug("update_keyspace");

        try
        {
            ThriftValidation.validateKeyspaceNotSystem(ks_def.name);
            state().hasKeyspaceAccess(ks_def.name, Permission.ALTER);
            ThriftValidation.validateKeyspace(ks_def.name);
            if (ks_def.getCf_defs() != null && ks_def.getCf_defs().size() > 0)
                throw new InvalidRequestException("Keyspace update must not contain any column family definitions.");

            MigrationManager.announceKeyspaceUpdate(KSMetaData.fromThrift(ks_def));
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
        logger.debug("update_column_family");

        try
        {
            if (cf_def.keyspace == null || cf_def.name == null)
                throw new InvalidRequestException("Keyspace and CF name must be set.");

            state().hasColumnFamilyAccess(cf_def.keyspace, cf_def.name, Permission.ALTER);
            CFMetaData oldCfm = Schema.instance.getCFMetaData(cf_def.keyspace, cf_def.name);

            if (oldCfm == null)
                throw new InvalidRequestException("Could not find column family definition to modify.");

            if (!oldCfm.isThriftCompatible())
                throw new InvalidRequestException("Cannot modify CQL3 table " + oldCfm.cfName + " as it may break the schema. You should use cqlsh to modify CQL3 tables instead.");

            CFMetaData.applyImplicitDefaults(cf_def);
            CFMetaData cfm = CFMetaData.fromThrift(cf_def);
            CFMetaData.validateCompactionOptions(cfm.compactionStrategyClass, cfm.compactionStrategyOptions);
            cfm.addDefaultIndexNames();

            if (!oldCfm.getTriggers().equals(cfm.getTriggers()))
                state().ensureIsSuper("Only superusers are allowed to add or remove triggers.");

            MigrationManager.announceColumnFamilyUpdate(cfm, true);
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

            if (startSessionIfRequested())
            {
                Tracing.instance.begin("truncate", ImmutableMap.of("cf", cfname, "ks", keyspace));
            }
            else
            {
                logger.debug("truncating {}.{}", cState.getKeyspace(), cfname);
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
        logger.debug("checking schema agreement");
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
            logger.debug("add");
        }

        try
        {
            ClientState cState = state();
            String keyspace = cState.getKeyspace();

            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.MODIFY);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, true);
            ThriftValidation.validateKey(metadata, key);
            ThriftConversion.fromThrift(consistency_level).validateCounterForWrite(metadata);
            ThriftValidation.validateColumnParent(metadata, column_parent);
            // SuperColumn field is usually optional, but not when we're adding
            if (metadata.cfType == ColumnFamilyType.Super && column_parent.super_column == null)
                throw new InvalidRequestException("missing mandatory super column name for super CF " + column_parent.column_family);

            ThriftValidation.validateColumnNames(metadata, column_parent, Arrays.asList(column.name));

            RowMutation rm = new RowMutation(keyspace, key);
            try
            {
                if (metadata.isSuper())
                    rm.addCounter(column_parent.column_family, CompositeType.build(column_parent.super_column, column.name), column.value);
                else
                    rm.addCounter(column_parent.column_family, column.name, column.value);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
            doInsert(consistency_level, Arrays.asList(new CounterMutation(rm, ThriftConversion.fromThrift(consistency_level))));
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
            logger.debug("remove_counter");
        }

        try
        {
            internal_remove(key, path, System.currentTimeMillis(), consistency_level, true);
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

                    queryString = new String(decompressed.getData(), 0, decompressed.size(), "UTF-8");
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
        catch (UnsupportedEncodingException e)
        {
            throw new InvalidRequestException("Unknown query string encoding.");
        }
        return queryString;
    }

    private void validateCQLVersion(int major) throws InvalidRequestException
    {
        /*
         * The rules are:
         *   - If no version are set, we don't validate anything. The reason is
         *     that 1) old CQL2 client might not have called set_cql_version
         *     and 2) some client may have removed the set_cql_version for CQL3
         *     when updating to 1.2.0. A CQL3 client upgrading from pre-1.2
         *     shouldn't be in that case however since set_cql_version uses to
         *     be mandatory (for CQL3).
         *   - Otherwise, checks the major matches whatever was set.
         */
        SemanticVersion versionSet = state().getCQLVersion();
        if (versionSet == null)
            return;

        if (versionSet.major != major)
            throw new InvalidRequestException(
                "Cannot execute/prepare CQL" + major + " statement since the CQL has been set to CQL" + versionSet.major
              + "(This might mean your client hasn't been upgraded correctly to use the new CQL3 methods introduced in Cassandra 1.2+).");
    }

    public CqlResult execute_cql_query(ByteBuffer query, Compression compression)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        validateCQLVersion(2);
        maybeLogCQL2Warning();

        try
        {
            String queryString = uncompress(query, compression);
            if (startSessionIfRequested())
            {
                Tracing.instance.begin("execute_cql_query",
                                       ImmutableMap.of("query", queryString));
            }
            else
            {
                logger.debug("execute_cql_query");
            }

            return QueryProcessor.process(queryString, state());
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

    public CqlResult execute_cql3_query(ByteBuffer query, Compression compression, ConsistencyLevel cLevel)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        validateCQLVersion(3);
        try
        {
            String queryString = uncompress(query, compression);
            if (startSessionIfRequested())
            {
                Tracing.instance.begin("execute_cql3_query",
                                       ImmutableMap.of("query", queryString));
            }
            else
            {
                logger.debug("execute_cql3_query");
            }

            ThriftClientState cState = state();
            return cState.getCQLQueryHandler().process(queryString, cState.getQueryState(), new QueryOptions(ThriftConversion.fromThrift(cLevel), Collections.<ByteBuffer>emptyList())).toThriftResult();
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

    public CqlPreparedResult prepare_cql_query(ByteBuffer query, Compression compression)
    throws InvalidRequestException, TException
    {
        if (logger.isDebugEnabled())
            logger.debug("prepare_cql_query");

        validateCQLVersion(2);
        maybeLogCQL2Warning();

        String queryString = uncompress(query, compression);
        ThriftClientState cState = state();

        try
        {
            cState.validateLogin();
            return QueryProcessor.prepare(queryString, cState);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public CqlPreparedResult prepare_cql3_query(ByteBuffer query, Compression compression)
    throws InvalidRequestException, TException
    {
        if (logger.isDebugEnabled())
            logger.debug("prepare_cql3_query");

        validateCQLVersion(3);

        String queryString = uncompress(query, compression);
        ThriftClientState cState = state();

        try
        {
            cState.validateLogin();
            return cState.getCQLQueryHandler().prepare(queryString, cState.getQueryState()).toThriftPreparedResult();
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
    }

    public CqlResult execute_prepared_cql_query(int itemId, List<ByteBuffer> bindVariables)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        validateCQLVersion(2);
        maybeLogCQL2Warning();

        if (startSessionIfRequested())
        {
            // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
            Tracing.instance.begin("execute_prepared_cql_query", Collections.<String, String>emptyMap());
        }
        else
        {
            logger.debug("execute_prepared_cql_query");
        }

        try
        {
            ThriftClientState cState = state();
            CQLStatement statement = cState.getPrepared().get(itemId);

            if (statement == null)
                throw new InvalidRequestException(String.format("Prepared query with ID %d not found", itemId));
            logger.trace("Retrieved prepared statement #{} with {} bind markers", itemId, statement.boundTerms);

            return QueryProcessor.processPrepared(statement, cState, bindVariables);
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

    public CqlResult execute_prepared_cql3_query(int itemId, List<ByteBuffer> bindVariables, ConsistencyLevel cLevel)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        validateCQLVersion(3);

        if (startSessionIfRequested())
        {
            // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
            Tracing.instance.begin("execute_prepared_cql3_query", Collections.<String, String>emptyMap());
        }
        else
        {
            logger.debug("execute_prepared_cql3_query");
        }

        try
        {
            ThriftClientState cState = state();
            org.apache.cassandra.cql3.CQLStatement statement = cState.getCQLQueryHandler().getPreparedForThrift(itemId);

            if (statement == null)
                throw new InvalidRequestException(String.format("Prepared query with ID %d not found" +
                                                                " (either the query was not prepared on this host (maybe the host has been restarted?)" +
                                                                " or you have prepared too many queries and it has been evicted from the internal cache)",
                                                                itemId));
            logger.trace("Retrieved prepared statement #{} with {} bind markers", itemId, statement.getBoundTerms());

            return cState.getCQLQueryHandler().processPrepared(statement,
                                                               cState.getQueryState(),
                                                               new QueryOptions(ThriftConversion.fromThrift(cLevel), bindVariables)).toThriftResult();
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

    /*
     * Deprecated, but if a client sets CQL2, it is a no-op for compatibility sake.
     * If it sets CQL3 however, we throw an IRE because this mean the client
     * hasn't been updated for Cassandra 1.2 and should start using the new
     * execute_cql3_query, etc... and there is no point no warning it early.
     */
    public void set_cql_version(String version) throws InvalidRequestException
    {
        try
        {
            state().setCQLVersion(version);
        }
        catch (org.apache.cassandra.exceptions.InvalidRequestException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    private void maybeLogCQL2Warning()
    {
        if (!loggedCQL2Warning)
        {
            logger.warn("CQL2 has been deprecated since Cassandra 2.0, and will be removed entirely in version 2.2."
                        + " Please switch to CQL3 before then.");
            loggedCQL2Warning = true;
        }
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

    private static class ThriftCASConditions implements CASConditions
    {
        private final ColumnFamily expected;

        private ThriftCASConditions(ColumnFamily expected)
        {
            this.expected = expected;
        }

        public IDiskAtomFilter readFilter()
        {
            return expected == null || expected.isEmpty()
                 ? new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1)
                 : new NamesQueryFilter(ImmutableSortedSet.copyOf(expected.getComparator(), expected.getColumnNames()));
        }

        public boolean appliesTo(ColumnFamily current)
        {
            long now = System.currentTimeMillis();

            if (!hasLiveColumns(expected, now))
                return !hasLiveColumns(current, now);
            else if (!hasLiveColumns(current, now))
                return false;

            // current has been built from expected, so we know that it can't have columns
            // that excepted don't have. So we just check that for each columns in expected:
            //   - if it is a tombstone, whether current has no column or a tombstone;
            //   - otherwise, that current has a live column with the same value.
            for (org.apache.cassandra.db.Column e : expected)
            {
                org.apache.cassandra.db.Column c = current.getColumn(e.name());
                if (e.isLive(now))
                {
                    if (c == null || !c.isLive(now) || !c.value().equals(e.value()))
                        return false;
                }
                else
                {
                    if (c != null && c.isLive(now))
                        return false;
                }
            }
            return true;
        }

        private static boolean hasLiveColumns(ColumnFamily cf, long now)
        {
            return cf != null && !cf.hasOnlyTombstones(now);
        }

        @Override
        public String toString()
        {
            return expected.toString();
        }
    }
}
