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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql.CQLStatement;
import org.apache.cassandra.cql.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.IFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.service.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.thrift.TException;

public class CassandraServer implements Cassandra.Iface
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraServer.class);

    private final static int COUNT_PAGE_SIZE = 1024;

    private final static List<ColumnOrSuperColumn> EMPTY_COLUMNS = Collections.emptyList();
    private final static List<Column> EMPTY_SUBCOLUMNS = Collections.emptyList();
    private final static List<CounterColumn> EMPTY_COUNTER_SUBCOLUMNS = Collections.emptyList();

    // thread local state containing session information
    public final ThreadLocal<ClientState> clientState = new ThreadLocal<ClientState>()
    {
        @Override
        public ClientState initialValue()
        {
            return new ClientState();
        }
    };

    /*
     * RequestScheduler to perform the scheduling of incoming requests
     */
    private final IRequestScheduler requestScheduler;

    public CassandraServer()
    {
        requestScheduler = DatabaseDescriptor.getRequestScheduler();
    }

    public ClientState state()
    {
        SocketAddress remoteSocket = SocketSessionManagementService.remoteSocket.get();
        if (remoteSocket == null)
            return clientState.get();

        ClientState cState = SocketSessionManagementService.instance.get(remoteSocket);
        if (cState == null)
        {
            cState = new ClientState();
            SocketSessionManagementService.instance.put(remoteSocket, cState);
        }
        return cState;
    }

    protected Map<DecoratedKey, ColumnFamily> readColumnFamily(List<ReadCommand> commands, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        // TODO - Support multiple column families per row, right now row only contains 1 column family
        Map<DecoratedKey, ColumnFamily> columnFamilyKeyMap = new HashMap<DecoratedKey, ColumnFamily>();

        List<Row> rows;
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
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        for (Row row: rows)
        {
            columnFamilyKeyMap.put(row.key, row.cf);
        }
        return columnFamilyKeyMap;
    }

    public List<Column> thriftifySubColumns(Collection<IColumn> columns)
    {
        if (columns == null || columns.isEmpty())
        {
            return EMPTY_SUBCOLUMNS;
        }

        ArrayList<Column> thriftColumns = new ArrayList<Column>(columns.size());
        for (IColumn column : columns)
        {
            if (column.isMarkedForDelete())
            {
                continue;
            }
            Column thrift_column = new Column(column.name()).setValue(column.value()).setTimestamp(column.timestamp());
            if (column instanceof ExpiringColumn)
            {
                thrift_column.setTtl(((ExpiringColumn) column).getTimeToLive());
            }
            thriftColumns.add(thrift_column);
        }

        return thriftColumns;
    }

    public List<CounterColumn> thriftifyCounterSubColumns(Collection<IColumn> columns)
    {
        if (columns == null || columns.isEmpty())
        {
            return EMPTY_COUNTER_SUBCOLUMNS;
        }

        ArrayList<CounterColumn> thriftColumns = new ArrayList<CounterColumn>(columns.size());
        for (IColumn column : columns)
        {
            if (column.isMarkedForDelete())
            {
                continue;
            }
            assert column instanceof org.apache.cassandra.db.CounterColumn;
            CounterColumn thrift_column = new CounterColumn(column.name(), CounterContext.instance().total(column.value()));
            thriftColumns.add(thrift_column);
        }

        return thriftColumns;
    }

    public List<ColumnOrSuperColumn> thriftifyColumns(Collection<IColumn> columns, boolean reverseOrder)
    {
        ArrayList<ColumnOrSuperColumn> thriftColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
        for (IColumn column : columns)
        {
            if (column.isMarkedForDelete())
            {
                continue;
            }
            if (column instanceof org.apache.cassandra.db.CounterColumn)
            {
                CounterColumn thrift_column = new CounterColumn(column.name(), CounterContext.instance().total(column.value()));
                thriftColumns.add(new ColumnOrSuperColumn().setCounter_column(thrift_column));
            }
            else
            {
                Column thrift_column = new Column(column.name()).setValue(column.value()).setTimestamp(column.timestamp());
                if (column instanceof ExpiringColumn)
                {
                    thrift_column.setTtl(((ExpiringColumn) column).getTimeToLive());
                }
                thriftColumns.add(new ColumnOrSuperColumn().setColumn(thrift_column));
            }
        }

        // we have to do the reversing here, since internally we pass results around in ColumnFamily
        // objects, which always sort their columns in the "natural" order
        // TODO this is inconvenient for direct users of StorageProxy
        if (reverseOrder)
            Collections.reverse(thriftColumns);
        return thriftColumns;
    }

    private List<ColumnOrSuperColumn> thriftifySuperColumns(Collection<IColumn> columns, boolean reverseOrder, boolean isCounterCF)
    {
        if (isCounterCF)
            return thriftifyCounterSuperColumns(columns, reverseOrder);
        else
            return thriftifySuperColumns(columns, reverseOrder);
    }

    private List<ColumnOrSuperColumn> thriftifySuperColumns(Collection<IColumn> columns, boolean reverseOrder)
    {
        ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
        for (IColumn column : columns)
        {
            List<Column> subcolumns = thriftifySubColumns(column.getSubColumns());
            if (subcolumns.isEmpty())
            {
                continue;
            }
            SuperColumn superColumn = new SuperColumn(column.name(), subcolumns);
            thriftSuperColumns.add(new ColumnOrSuperColumn().setSuper_column(superColumn));
        }

        if (reverseOrder)
            Collections.reverse(thriftSuperColumns);

        return thriftSuperColumns;
    }

    private List<ColumnOrSuperColumn> thriftifyCounterSuperColumns(Collection<IColumn> columns, boolean reverseOrder)
    {
        ArrayList<ColumnOrSuperColumn> thriftSuperColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
        for (IColumn column : columns)
        {
            List<CounterColumn> subcolumns = thriftifyCounterSubColumns(column.getSubColumns());
            if (subcolumns.isEmpty())
            {
                continue;
            }
            CounterSuperColumn superColumn = new CounterSuperColumn(column.name(), subcolumns);
            thriftSuperColumns.add(new ColumnOrSuperColumn().setCounter_super_column(superColumn));
        }

        if (reverseOrder)
            Collections.reverse(thriftSuperColumns);

        return thriftSuperColumns;
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> getSlice(List<ReadCommand> commands, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        Map<DecoratedKey, ColumnFamily> columnFamilies = readColumnFamily(commands, consistency_level);
        Map<ByteBuffer, List<ColumnOrSuperColumn>> columnFamiliesMap = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
        for (ReadCommand command: commands)
        {
            ColumnFamily cf = columnFamilies.get(StorageService.getPartitioner().decorateKey(command.key));
            boolean reverseOrder = command instanceof SliceFromReadCommand && ((SliceFromReadCommand)command).filter.reversed;
            List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyColumnFamily(cf, command.queryPath.superColumnName != null, reverseOrder);
            columnFamiliesMap.put(command.key, thriftifiedColumns);
        }

        return columnFamiliesMap;
    }

    private List<ColumnOrSuperColumn> thriftifyColumnFamily(ColumnFamily cf, boolean subcolumnsOnly, boolean reverseOrder)
    {
        if (cf == null || cf.isEmpty())
            return EMPTY_COLUMNS;
        if (subcolumnsOnly)
        {
            IColumn column = cf.iterator().next();
            Collection<IColumn> subcolumns = column.getSubColumns();
            if (subcolumns == null || subcolumns.isEmpty())
                return EMPTY_COLUMNS;
            else
                return thriftifyColumns(subcolumns, reverseOrder);
        }
        if (cf.isSuper())
        {
            boolean isCounterCF = cf.metadata().getDefaultValidator().isCommutative();
            return thriftifySuperColumns(cf.getSortedColumns(), reverseOrder, isCounterCF);
        }
        else
        {
            return thriftifyColumns(cf.getSortedColumns(), reverseOrder);
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
            Tracing.instance().begin("get_slice", traceParameters);
        }
        else
        {
            logger.debug("get_slice");
        }

        try
        {
            state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
            return multigetSliceInternal(state().getKeyspace(), Collections.singletonList(key), column_parent,
                    predicate, consistency_level).get(key);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
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
            Tracing.instance().begin("multiget_slice", traceParameters);
        }
        else
        {
            logger.debug("multiget_slice");
        }

        try
        {
            state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
            return multigetSliceInternal(state().getKeyspace(), keys, column_parent, predicate, consistency_level);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetSliceInternal(String keyspace, List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, predicate);
        ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

        List<ReadCommand> commands = new ArrayList<ReadCommand>(keys.size());
        if (predicate.column_names != null)
        {
            for (ByteBuffer key: keys)
            {
                ThriftValidation.validateKey(metadata, key);
                commands.add(new SliceByNamesReadCommand(keyspace, key, column_parent, predicate.column_names));
            }
        }
        else
        {
            SliceRange range = predicate.slice_range;
            for (ByteBuffer key: keys)
            {
                ThriftValidation.validateKey(metadata, key);
                commands.add(new SliceFromReadCommand(keyspace, key, column_parent, range.start, range.finish, range.reversed, range.count));
            }
        }

        return getSlice(commands, consistency_level);
    }

    private ColumnOrSuperColumn internal_get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException
    {
        ClientState cState = state();
        cState.hasColumnFamilyAccess(column_path.column_family, Permission.READ);
        String keyspace = cState.getKeyspace();

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_path.column_family);
        ThriftValidation.validateColumnPath(metadata, column_path);
        ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

        QueryPath path = new QueryPath(column_path.column_family, column_path.column == null ? null : column_path.super_column);
        List<ByteBuffer> nameAsList = Arrays.asList(column_path.column == null ? column_path.super_column : column_path.column);
        ThriftValidation.validateKey(metadata, key);
        ReadCommand command = new SliceByNamesReadCommand(keyspace, key, path, nameAsList);

        Map<DecoratedKey, ColumnFamily> cfamilies = readColumnFamily(Arrays.asList(command), consistency_level);

        ColumnFamily cf = cfamilies.get(StorageService.getPartitioner().decorateKey(command.key));

        if (cf == null)
            throw new NotFoundException();
        List<ColumnOrSuperColumn> tcolumns = thriftifyColumnFamily(cf, command.queryPath.superColumnName != null, false);
        if (tcolumns.isEmpty())
            throw new NotFoundException();
        assert tcolumns.size() == 1;
        return tcolumns.get(0);
    }

    public ColumnOrSuperColumn get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException
    {

        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("key", ByteBufferUtil.bytesToHex(key),
                    "column_path", column_path.toString(),
                    "consistency_level", consistency_level.name());
            Tracing.instance().begin("get", traceParameters);
        }
        else
        {
            logger.debug("get");
        }

        try
        {
            return internal_get(key, column_path, consistency_level);
        }
        finally
        {
            Tracing.instance().stopSession();
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
            Tracing.instance().begin("get_count", traceParameters);
        }
        else
        {
            logger.debug("get_count");
        }

        try
        {
            ClientState cState = state();
            cState.hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
            Table table = Table.open(cState.getKeyspace());
            ColumnFamilyStore cfs = table.getColumnFamilyStore(column_parent.column_family);

            if (predicate.column_names != null)
                return get_slice(key, column_parent, predicate, consistency_level).size();

            int pageSize;
            // request by page if this is a large row
            if (cfs.getMeanColumns() > 0)
            {
                int averageColumnSize = (int) (cfs.getMeanRowSize() / cfs.getMeanColumns());
                pageSize = Math.min(COUNT_PAGE_SIZE,
                        DatabaseDescriptor.getInMemoryCompactionLimit() / averageColumnSize);
                pageSize = Math.max(2, pageSize);
                logger.debug("average row column size is {}; using pageSize of {}", averageColumnSize, pageSize);
            }
            else
            {
                pageSize = COUNT_PAGE_SIZE;
            }

            int totalCount = 0;
            List<ColumnOrSuperColumn> columns;

            if (predicate.slice_range == null)
            {
                predicate.slice_range = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                        false,
                        Integer.MAX_VALUE);
            }

            int requestedCount = predicate.slice_range.count;
            int pages = 0;
            while (true)
            {
                predicate.slice_range.count = Math.min(pageSize, requestedCount);
                columns = get_slice(key, column_parent, predicate, consistency_level);
                if (columns.isEmpty())
                    break;

                ByteBuffer firstName = getName(columns.get(0));
                int newColumns = pages == 0 || !firstName.equals(predicate.slice_range.start) ? columns.size()
                        : columns.size() - 1;

                totalCount += newColumns;
                requestedCount -= newColumns;
                pages++;
                // We're done if either:
                // - We've querying the number of columns requested by the user
                // - The last page wasn't full
                if (requestedCount == 0 || columns.size() < predicate.slice_range.count)
                    break;
                else
                    predicate.slice_range.start = getName(columns.get(columns.size() - 1));
            }

            return totalCount;
        }
        finally
        {
            Tracing.instance().stopSession();
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
            Tracing.instance().begin("multiget_count", traceParameters);
        }
        else
        {
            logger.debug("multiget_count");
        }

        try
        {
            ClientState cState = state();
            cState.hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
            String keyspace = cState.getKeyspace();

            Map<ByteBuffer, Integer> counts = new HashMap<ByteBuffer, Integer>();
            Map<ByteBuffer, List<ColumnOrSuperColumn>> columnFamiliesMap = multigetSliceInternal(keyspace, keys,
                    column_parent, predicate, consistency_level);

            for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> cf : columnFamiliesMap.entrySet())
            {
                counts.put(cf.getKey(), cf.getValue().size());
            }
            return counts;
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    private void internal_insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        ClientState cState = state();
        cState.hasColumnFamilyAccess(column_parent.column_family, Permission.WRITE);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(cState.getKeyspace(), column_parent.column_family, false);
        ThriftValidation.validateKey(metadata, key);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        // SuperColumn field is usually optional, but not when we're inserting
        if (metadata.cfType == ColumnFamilyType.Super && column_parent.super_column == null)
        {
            throw new InvalidRequestException("missing mandatory super column name for super CF " + column_parent.column_family);
        }
        ThriftValidation.validateColumnNames(metadata, column_parent, Arrays.asList(column.name));
        ThriftValidation.validateColumnData(metadata, column, column_parent.super_column != null);

        RowMutation rm = new RowMutation(cState.getKeyspace(), key);
        try
        {
            rm.add(new QueryPath(column_parent.column_family, column_parent.super_column, column.name), column.value, column.timestamp, column.ttl);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
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
            Tracing.instance().begin("insert", traceParameters);
        }
        else
        {
            logger.debug("insert");
        }

        try
        {
            internal_insert(key, column_parent, column, consistency_level);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    private void internal_batch_mutate(Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        List<String> cfamsSeen = new ArrayList<String>();
        List<IMutation> rowMutations = new ArrayList<IMutation>();
        ClientState cState = state();
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

                // Avoid unneeded authorizations
                if (!(cfamsSeen.contains(cfName)))
                {
                    cState.hasColumnFamilyAccess(cfName, Permission.WRITE);
                    cfamsSeen.add(cfName);
                }

                CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, cfName);
                ThriftValidation.validateKey(metadata, key);

                RowMutation rm;
                if (metadata.getDefaultValidator().isCommutative())
                {
                    ThriftValidation.validateCommutativeForWrite(metadata, consistency_level);
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
                        rm.deleteColumnOrSuperColumn(cfName, mutation.deletion);
                    }
                    if (mutation.column_or_supercolumn != null)
                    {
                        rm.addColumnOrSuperColumn(cfName, mutation.column_or_supercolumn);
                    }
                }
            }
            if (rmStandard != null && !rmStandard.isEmpty())
                rowMutations.add(rmStandard);
            if (rmCounter != null && !rmCounter.isEmpty())
                rowMutations.add(new org.apache.cassandra.db.CounterMutation(rmCounter, consistency_level));
        }

        doInsert(consistency_level, rowMutations);
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
            Tracing.instance().begin("batch_mutate", traceParameters);
        }
        else
        {
            logger.debug("batch_mutate");
        }

        try
        {
            internal_batch_mutate(mutation_map, consistency_level);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    private void internal_remove(ByteBuffer key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level, boolean isCommutativeOp)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        ClientState cState = state();
        cState.hasColumnFamilyAccess(column_path.column_family, Permission.WRITE);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(cState.getKeyspace(), column_path.column_family, isCommutativeOp);
        ThriftValidation.validateKey(metadata, key);
        ThriftValidation.validateColumnPathOrParent(metadata, column_path);
        if (isCommutativeOp)
            ThriftValidation.validateCommutativeForWrite(metadata, consistency_level);

        RowMutation rm = new RowMutation(cState.getKeyspace(), key);
        rm.delete(new QueryPath(column_path), timestamp);

        if (isCommutativeOp)
            doInsert(consistency_level, Arrays.asList(new CounterMutation(rm, consistency_level)));
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
                    "timestamp", timestamp+"",
                    "consistency_level", consistency_level.name());
            Tracing.instance().begin("remove", traceParameters);
        }
        else
        {
            logger.debug("remove");
        }

        try
        {
            internal_remove(key, column_path, timestamp, consistency_level, false);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    private void doInsert(ConsistencyLevel consistency_level, List<? extends IMutation> mutations) throws UnavailableException, TimedOutException, InvalidRequestException
    {
        ThriftValidation.validateConsistencyLevel(state().getKeyspace(), consistency_level, RequestType.WRITE);
        if (mutations.isEmpty())
            return;

        schedule(DatabaseDescriptor.getWriteRpcTimeout());
        try
        {
            StorageProxy.mutate(mutations, consistency_level);
        }
        finally
        {
            release();
        }
    }

    public KsDef describe_keyspace(String table) throws NotFoundException, InvalidRequestException
    {
        state().hasKeyspaceSchemaAccess(Permission.READ);

        KSMetaData ksm = Schema.instance.getTableDefinition(table);
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
            Tracing.instance().begin("get_range_slices", traceParameters);
        }
        else
        {
            logger.debug("range_slice");
        }

        try
        {

            String keyspace = null;
            CFMetaData metadata = null;

            ClientState cState = state();
            keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(column_parent.column_family, Permission.READ);

            metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
            ThriftValidation.validateColumnParent(metadata, column_parent);
            ThriftValidation.validatePredicate(metadata, column_parent, predicate);
            ThriftValidation.validateKeyRange(metadata, column_parent.super_column, range);
            ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

            List<Row> rows = null;
            try
            {
                IPartitioner p = StorageService.getPartitioner();
                AbstractBounds<RowPosition> bounds;
                if (range.start_key == null)
                {
                    Token.TokenFactory tokenFactory = p.getTokenFactory();
                    Token left = tokenFactory.fromString(range.start_token);
                    Token right = tokenFactory.fromString(range.end_token);
                    bounds = Range.makeRowRange(left, right, p);
                }
                else
                {
                    bounds = new Bounds<RowPosition>(RowPosition.forKey(range.start_key, p), RowPosition.forKey(
                            range.end_key, p));
                }
                schedule(DatabaseDescriptor.getRangeRpcTimeout());
                try
                {
                    IFilter filter = ThriftValidation.asIFilter(predicate,
                            metadata.getComparatorFor(column_parent.super_column));
                    rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace, column_parent, filter, bounds,
                            range.row_filter, range.count), consistency_level);
                }
                finally
                {
                    release();
                }
                assert rows != null;
            }
            catch (TimeoutException e)
            {
                logger.debug("... timed out");
                throw new TimedOutException();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            return thriftifyKeySlices(rows, column_parent, predicate);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    public List<KeySlice> get_paged_slice(String column_family, KeyRange range, ByteBuffer start_column, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of(
                    "column_family", column_family,
                    "range", range.toString(),
                    "start_column", ByteBufferUtil.bytesToHex(start_column),
                    "consistency_level", consistency_level.name());
            Tracing.instance().begin("get_paged_slice", traceParameters);
        }
        else
        {
            logger.debug("get_paged_slice");
        }

        try
        {

            ClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(column_family, Permission.READ);

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_family);
            ThriftValidation.validateKeyRange(metadata, null, range);
            ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

            SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(start_column,
                    ByteBufferUtil.EMPTY_BYTE_BUFFER, false, -1));

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
                RowPosition end = range.end_key == null ? p.getTokenFactory().fromString(range.end_token)
                        .maxKeyBound(p)
                        : RowPosition.forKey(range.end_key, p);
                bounds = new Bounds<RowPosition>(RowPosition.forKey(range.start_key, p), end);
            }

            List<Row> rows;
            try
            {
                schedule(DatabaseDescriptor.getRangeRpcTimeout());
                try
                {
                    IFilter filter = ThriftValidation.asIFilter(predicate, metadata.comparator);
                    rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace, column_family, null, filter,
                            bounds, range.row_filter, range.count, true, true), consistency_level);
                }
                finally
                {
                    release();
                }
                assert rows != null;
            }
            catch (TimeoutException e)
            {
                logger.debug("... timed out");
                throw new TimedOutException();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            return thriftifyKeySlices(rows, new ColumnParent(column_family), predicate);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    private List<KeySlice> thriftifyKeySlices(List<Row> rows, ColumnParent column_parent, SlicePredicate predicate)
    {
        List<KeySlice> keySlices = new ArrayList<KeySlice>(rows.size());
        boolean reversed = predicate.slice_range != null && predicate.slice_range.reversed;
        for (Row row : rows)
        {
            List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyColumnFamily(row.cf, column_parent.super_column != null, reversed);
            keySlices.add(new KeySlice(row.key.key, thriftifiedColumns));
        }

        return keySlices;
    }

    public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of(
                    "column_parent", column_parent.toString(),
                    "index_clause", index_clause.toString(),
                    "slice_predicate", column_predicate.toString(),
                    "consistency_level", consistency_level.name());
            Tracing.instance().begin("get_indexed_slices", traceParameters);
        }
        else
        {
            logger.debug("scan");
        }

        try
        {

            ClientState cState = state();
            cState.hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
            String keyspace = cState.getKeyspace();
            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, false);
            ThriftValidation.validateColumnParent(metadata, column_parent);
            ThriftValidation.validatePredicate(metadata, column_parent, column_predicate);
            ThriftValidation.validateIndexClauses(metadata, index_clause);
            ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

            IPartitioner p = StorageService.getPartitioner();
            AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(RowPosition.forKey(index_clause.start_key, p),
                    p.getMinimumToken().minKeyBound());

            IFilter filter = ThriftValidation.asIFilter(column_predicate,
                    metadata.getComparatorFor(column_parent.super_column));
            RangeSliceCommand command = new RangeSliceCommand(keyspace,
                    column_parent.column_family,
                    null,
                    filter,
                    bounds,
                    index_clause.expressions,
                    index_clause.count);

            List<Row> rows;
            try
            {
                rows = StorageProxy.getRangeSlice(command, consistency_level);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            catch (TimeoutException e)
            {
                logger.debug("... timed out");
                throw new TimedOutException();
            }

            return thriftifyKeySlices(rows, column_parent, column_predicate);

        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    public List<KsDef> describe_keyspaces() throws TException, InvalidRequestException
    {
        state().hasKeyspaceSchemaAccess(Permission.READ);

        Set<String> keyspaces = Schema.instance.getTables();
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
        return Constants.VERSION;
    }

    public List<TokenRange> describe_ring(String keyspace)throws InvalidRequestException
    {
        return StorageService.instance.describeRing(keyspace);
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

    public List<String> describe_splits(String cfName, String start_token, String end_token, int keys_per_split)
    throws TException, InvalidRequestException
    {
        // TODO: add keyspace authorization call post CASSANDRA-1425
        Token.TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
        List<Token> tokens = StorageService.instance.getSplits(state().getKeyspace(), cfName, new Range<Token>(tf.fromString(start_token), tf.fromString(end_token)), keys_per_split);
        List<String> splits = new ArrayList<String>(tokens.size());
        for (Token token : tokens)
        {
            splits.add(tf.toString(token));
        }
        return splits;
    }

    public void login(AuthenticationRequest auth_request) throws AuthenticationException, AuthorizationException, TException
    {
         state().login(auth_request.getCredentials());
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
        state().hasColumnFamilySchemaAccess(Permission.WRITE);

        try
        {
            cf_def.unsetId(); // explicitly ignore any id set by client (Hector likes to set zero)
            CFMetaData cfm = CFMetaData.fromThrift(cf_def);
            if (cfm.getBloomFilterFpChance() == null)
                cfm.bloomFilterFpChance(CFMetaData.DEFAULT_BF_FP_CHANCE);
            cfm.addDefaultIndexNames();
            MigrationManager.announceNewColumnFamily(cfm);
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public String system_drop_column_family(String column_family)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("drop_column_family");

        ClientState cState = state();
        cState.hasColumnFamilySchemaAccess(Permission.WRITE);

        try
        {
            MigrationManager.announceColumnFamilyDrop(cState.getKeyspace(), column_family);
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public String system_add_keyspace(KsDef ks_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("add_keyspace");
        ThriftValidation.validateKeyspaceNotSystem(ks_def.name);
        state().hasKeyspaceSchemaAccess(Permission.WRITE);
        ThriftValidation.validateKeyspaceNotYetExisting(ks_def.name);

        // generate a meaningful error if the user setup keyspace and/or column definition incorrectly
        for (CfDef cf : ks_def.cf_defs)
        {
            if (!cf.getKeyspace().equals(ks_def.getName()))
            {
                throw new InvalidRequestException("CfDef (" + cf.getName() +") had a keyspace definition that did not match KsDef");
            }
        }

        try
        {
            Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(ks_def.cf_defs.size());
            for (CfDef cf_def : ks_def.cf_defs)
            {
                cf_def.unsetId(); // explicitly ignore any id set by client (same as system_add_column_family)
                CFMetaData cfm = CFMetaData.fromThrift(cf_def);
                cfm.addDefaultIndexNames();
                cfDefs.add(cfm);
            }
            MigrationManager.announceNewKeyspace(KSMetaData.fromThrift(ks_def, cfDefs.toArray(new CFMetaData[cfDefs.size()])));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public String system_drop_keyspace(String keyspace)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("drop_keyspace");
        ThriftValidation.validateKeyspaceNotSystem(keyspace);
        state().hasKeyspaceSchemaAccess(Permission.WRITE);

        try
        {
            MigrationManager.announceKeyspaceDrop(keyspace);
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    /** update an existing keyspace, but do not allow column family modifications.
     * @throws SchemaDisagreementException
     */
    public String system_update_keyspace(KsDef ks_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("update_keyspace");
        ThriftValidation.validateKeyspaceNotSystem(ks_def.name);
        state().hasKeyspaceSchemaAccess(Permission.WRITE);
        ThriftValidation.validateTable(ks_def.name);
        if (ks_def.getCf_defs() != null && ks_def.getCf_defs().size() > 0)
            throw new InvalidRequestException("Keyspace update must not contain any column family definitions.");

        try
        {
            MigrationManager.announceKeyspaceUpdate(KSMetaData.fromThrift(ks_def));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public String system_update_column_family(CfDef cf_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("update_column_family");
        state().hasColumnFamilySchemaAccess(Permission.WRITE);
        if (cf_def.keyspace == null || cf_def.name == null)
            throw new InvalidRequestException("Keyspace and CF name must be set.");
        CFMetaData oldCfm = Schema.instance.getCFMetaData(cf_def.keyspace, cf_def.name);
        if (oldCfm == null)
            throw new InvalidRequestException("Could not find column family definition to modify.");

        try
        {
            CFMetaData.applyImplicitDefaults(cf_def);
            CFMetaData cfm = CFMetaData.fromThrift(cf_def);
            cfm.addDefaultIndexNames();
            MigrationManager.announceColumnFamilyUpdate(cfm);
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public void truncate(String cfname) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        ClientState cState = state();
        cState.hasColumnFamilyAccess(cfname, Permission.WRITE);

        if (startSessionIfRequested())
        {
            Tracing.instance().begin("truncate", ImmutableMap.of("cf", cfname, "ks", cState.getKeyspace()));
        }
        else
        {
            logger.debug("truncating {}.{}", cState.getKeyspace(), cfname);
        }

        try
        {
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
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw (UnavailableException) new UnavailableException().initCause(e);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    public void set_keyspace(String keyspace) throws InvalidRequestException, TException
    {
        ThriftValidation.validateTable(keyspace);

        state().setKeyspace(keyspace);
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
            Map<String, String> traceParameters = ImmutableMap.of(
                    "column_parent", column_parent.toString(),
                    "column", column.toString(),
                    "consistency_level", consistency_level.name());
            Tracing.instance().begin("add", traceParameters);
        }
        else
        {
            logger.debug("add");
        }

        try
        {
            ClientState cState = state();
            cState.hasColumnFamilyAccess(column_parent.column_family, Permission.WRITE);
            String keyspace = cState.getKeyspace();

            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, true);
            ThriftValidation.validateKey(metadata, key);
            ThriftValidation.validateCommutativeForWrite(metadata, consistency_level);
            ThriftValidation.validateColumnParent(metadata, column_parent);
            // SuperColumn field is usually optional, but not when we're adding
            if (metadata.cfType == ColumnFamilyType.Super && column_parent.super_column == null)
            {
                throw new InvalidRequestException("missing mandatory super column name for super CF "
                        + column_parent.column_family);
            }
            ThriftValidation.validateColumnNames(metadata, column_parent, Arrays.asList(column.name));

            RowMutation rm = new RowMutation(keyspace, key);
            try
            {
                rm.addCounter(new QueryPath(column_parent.column_family, column_parent.super_column, column.name),
                        column.value);
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
            doInsert(consistency_level, Arrays.asList(new CounterMutation(rm, consistency_level)));
        }
        finally
        {
            Tracing.instance().stopSession();
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
            Tracing.instance().begin("remove_counter", traceParameters);
        }
        else
        {
            logger.debug("remove_counter");
        }

        try
        {
            internal_remove(key, path, System.currentTimeMillis(), consistency_level, true);
        }
        finally
        {
            Tracing.instance().stopSession();
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
                        while ((lenWrite = decompressor.inflate(outBuffer)) !=0)
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

    public CqlResult execute_cql_query(ByteBuffer query, Compression compression)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        try
        {
            String queryString = uncompress(query, compression);
            if (startSessionIfRequested())
            {
                Tracing.instance().begin("execute_cql_query",
                                         ImmutableMap.of("query", queryString));
            }
            else
            {
                logger.debug("execute_cql_query");
            }

            ClientState cState = state();
            if (cState.getCQLVersion().major == 2)
                return QueryProcessor.process(queryString, state());
            else
                return org.apache.cassandra.cql3.QueryProcessor.process(queryString, cState).toThriftResult();
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    public CqlPreparedResult prepare_cql_query(ByteBuffer query, Compression compression)
    throws InvalidRequestException, TException
    {
        if (logger.isDebugEnabled())
            logger.debug("prepare_cql_query");

        String queryString = uncompress(query,compression);

        ClientState cState = state();
        if (cState.getCQLVersion().major == 2)
            return QueryProcessor.prepare(queryString, cState);
        else
            return org.apache.cassandra.cql3.QueryProcessor.prepare(queryString, cState).toThriftPreparedResult();
    }

    public CqlResult execute_prepared_cql_query(int itemId, List<ByteBuffer> bindVariables)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        if (startSessionIfRequested())
        {
            // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
            Tracing.instance().begin("execute_prepared_cql_query", Collections.<String, String>emptyMap());
        }
        else
        {
            logger.debug("execute_prepared_cql_query");
        }

        try
        {
            ClientState cState = state();
            if (cState.getCQLVersion().major == 2)
            {
                CQLStatement statement = cState.getPrepared().get(itemId);

                if (statement == null)
                    throw new InvalidRequestException(String.format("Prepared query with ID %d not found", itemId));
                logger.trace("Retrieved prepared statement #{} with {} bind markers", itemId, statement.boundTerms);

                return QueryProcessor.processPrepared(statement, cState, bindVariables);
            }
            else
            {
                org.apache.cassandra.cql3.CQLStatement statement = cState.getCQL3Prepared().get(itemId);

                if (statement == null)
                    throw new InvalidRequestException(String.format("Prepared query with ID %d not found", itemId));
                logger.trace("Retrieved prepared statement #{} with {} bind markers", itemId,
                        statement.getBoundsTerms());

                return org.apache.cassandra.cql3.QueryProcessor.processPrepared(statement, cState, bindVariables)
                        .toThriftResult();
            }
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    public void set_cql_version(String version) throws InvalidRequestException
    {
        logger.debug("set_cql_version: " + version);

        state().setCQLVersion(version);
    }

    public ByteBuffer trace_next_query() throws TException
    {
        UUID sessionId = UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress());
        state().prepareTracingSession(sessionId);
        return TimeUUIDType.instance.decompose(sessionId);
    }

    private boolean startSessionIfRequested()
    {
        if (state().traceNextQuery())
        {
            state().createSession();
            return true;
        }
        return false;
    }

    // main method moved to CassandraDaemon
}
