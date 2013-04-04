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
import java.util.concurrent.TimeoutException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql.CQLStatement;
import org.apache.cassandra.cql.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.service.*;
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
    private final static List<Column> EMPTY_SUBCOLUMNS = Collections.emptyList();
    private final static List<CounterColumn> EMPTY_COUNTER_SUBCOLUMNS = Collections.emptyList();

    /*
     * RequestScheduler to perform the scheduling of incoming requests
     */
    private final IRequestScheduler requestScheduler;

    public CassandraServer()
    {
        requestScheduler = DatabaseDescriptor.getRequestScheduler();
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

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> getSlice(List<ReadCommand> commands, org.apache.cassandra.db.ConsistencyLevel consistency_level)
    throws org.apache.cassandra.exceptions.InvalidRequestException, UnavailableException, TimedOutException
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
            ClientState cState = state();
            String keyspace = cState.getKeyspace();
            state().hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
            return multigetSliceInternal(keyspace, Collections.singletonList(key), column_parent, predicate, consistency_level).get(key);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
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
                keysList.add(ByteBufferUtil.bytesToHex(key));
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
            ClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
            return multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetSliceInternal(String keyspace, List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws org.apache.cassandra.exceptions.InvalidRequestException, UnavailableException, TimedOutException
    {
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, predicate);

        org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
        consistencyLevel.validateForRead(keyspace);

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

        return getSlice(commands, consistencyLevel);
    }

    private ColumnOrSuperColumn internal_get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws RequestValidationException, NotFoundException, UnavailableException, TimedOutException
    {
        ThriftClientState cState = state();
        String keyspace = cState.getKeyspace();
        cState.hasColumnFamilyAccess(keyspace, column_path.column_family, Permission.SELECT);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_path.column_family);
        ThriftValidation.validateColumnPath(metadata, column_path);
        org.apache.cassandra.db.ConsistencyLevel consistencyLevel = ThriftConversion.fromThrift(consistency_level);
        consistencyLevel.validateForRead(keyspace);

        QueryPath path = new QueryPath(column_path.column_family, column_path.column == null ? null : column_path.super_column);
        List<ByteBuffer> nameAsList = Arrays.asList(column_path.column == null ? column_path.super_column : column_path.column);
        ThriftValidation.validateKey(metadata, key);
        ReadCommand command = new SliceByNamesReadCommand(keyspace, key, path, nameAsList);

        Map<DecoratedKey, ColumnFamily> cfamilies = readColumnFamily(Arrays.asList(command), consistencyLevel);

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
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
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
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);
            Table table = Table.open(keyspace);
            ColumnFamilyStore cfs = table.getColumnFamilyStore(column_parent.column_family);

            if (predicate.column_names != null)
                return get_slice(key, column_parent, predicate, consistency_level).size();

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

            int totalCount = 0;
            List<ColumnOrSuperColumn> columns;

            if (predicate.slice_range == null)
            {
                predicate.slice_range = new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                       false,
                                                       Integer.MAX_VALUE);
            }

            final int requestedCount = predicate.slice_range.count;
            int remaining = requestedCount;
            int pages = 0;
            while (true)
            {
                predicate.slice_range.count = Math.min(pageSize, Math.max(2, remaining)); // fetch at least two columns
                columns = get_slice(key, column_parent, predicate, consistency_level);
                if (columns.isEmpty())
                    break;

                ByteBuffer firstName = getName(columns.get(0));
                int newColumns = pages == 0 || !firstName.equals(predicate.slice_range.start) ? columns.size() : columns.size() - 1;

                totalCount += newColumns;
                // if we over-counted, just return original limit
                if (totalCount > requestedCount)
                    return requestedCount;
                remaining -= newColumns;
                pages++;
                // We're done if either:
                // - We've querying the number of columns requested by the user
                // - last fetched page only contains the column we already fetched
                if (remaining == 0 || ((columns.size() == 1) && (firstName.equals(predicate.slice_range.start))))
                    break;
                else
                    predicate.slice_range.start = getName(columns.get(columns.size() - 1));
            }

            return totalCount;
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
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
            ThriftClientState cState = state();
            String keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);

            Map<ByteBuffer, Integer> counts = new HashMap<ByteBuffer, Integer>();
            Map<ByteBuffer, List<ColumnOrSuperColumn>> columnFamiliesMap = multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level);

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
            Tracing.instance().stopSession();
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

        RowMutation rm = new RowMutation(cState.getKeyspace(), key);
        try
        {
            rm.add(new QueryPath(column_parent.column_family, column_parent.super_column, column.name), column.value, column.timestamp, column.ttl);
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
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance().stopSession();
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
            {
                if (allowCounterMutations)
                    rowMutations.add(new CounterMutation(rmCounter, ThriftConversion.fromThrift(consistency_level)));
                else
                    throw new org.apache.cassandra.exceptions.InvalidRequestException("Counter mutations are not allowed in atomic batches");
            }
        }

        return rowMutations;
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
            doInsert(consistency_level, createMutationList(consistency_level, mutation_map, true));
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance().stopSession();
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
            Tracing.instance().begin("atomic_batch_mutate", traceParameters);
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
            Tracing.instance().stopSession();
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
        rm.delete(new QueryPath(column_path), timestamp);

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
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance().stopSession();
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
            if (mutateAtomically)
                StorageProxy.mutateAtomically((List<RowMutation>) mutations, consistencyLevel);
            else
                StorageProxy.mutate(mutations, consistencyLevel);
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

    public KsDef describe_keyspace(String table) throws NotFoundException, InvalidRequestException
    {
        validateLogin();

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

            ThriftClientState cState = state();
            keyspace = cState.getKeyspace();
            cState.hasColumnFamilyAccess(keyspace, column_parent.column_family, Permission.SELECT);

            metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
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
            schedule(DatabaseDescriptor.getRangeRpcTimeout());
            try
            {
                IDiskAtomFilter filter = ThriftValidation.asIFilter(predicate, metadata.getComparatorFor(column_parent.super_column));
                rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace, column_parent, filter, bounds,
                                                                        range.row_filter, range.count), consistencyLevel);
            }
            finally
            {
                release();
            }
            assert rows != null;

            return thriftifyKeySlices(rows, column_parent, predicate);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (ReadTimeoutException e)
        {
            logger.debug("... timed out");
            throw ThriftConversion.toThrift(e);
        }
        catch (org.apache.cassandra.exceptions.UnavailableException e)
        {
            throw ThriftConversion.toThrift(e);
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
            Map<String, String> traceParameters = ImmutableMap.of("column_family", column_family,
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

            List<Row> rows;
            schedule(DatabaseDescriptor.getRangeRpcTimeout());
            try
            {
                IDiskAtomFilter filter = ThriftValidation.asIFilter(predicate, metadata.comparator);
                rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace, column_family, null, filter,
                                                                        bounds, range.row_filter, range.count, true, true), consistencyLevel);
            }
            finally
            {
                release();
            }
            assert rows != null;

            return thriftifyKeySlices(rows, new ColumnParent(column_family), predicate);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (ReadTimeoutException e)
        {
            logger.debug("... timed out");
            throw ThriftConversion.toThrift(e);
        }
        catch (org.apache.cassandra.exceptions.UnavailableException e)
        {
            throw ThriftConversion.toThrift(e);
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

    public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        if (startSessionIfRequested())
        {
            Map<String, String> traceParameters = ImmutableMap.of("column_parent", column_parent.toString(),
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

            IDiskAtomFilter filter = ThriftValidation.asIFilter(column_predicate, metadata.getComparatorFor(column_parent.super_column));
            RangeSliceCommand command = new RangeSliceCommand(keyspace,
                                                              column_parent.column_family,
                                                              null,
                                                              filter,
                                                              bounds,
                                                              index_clause.expressions,
                                                              index_clause.count);

            List<Row> rows = StorageProxy.getRangeSlice(command, consistencyLevel);
            return thriftifyKeySlices(rows, column_parent, column_predicate);
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        catch (ReadTimeoutException e)
        {
            logger.debug("... timed out");
            throw ThriftConversion.toThrift(e);
        }
        catch (org.apache.cassandra.exceptions.UnavailableException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    public List<KsDef> describe_keyspaces() throws TException, InvalidRequestException
    {
        validateLogin();

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
                    StorageService.instance.getSplits(state().getKeyspace(), cfName, tr, keys_per_split);
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
            state().login(auth_request.getCredentials());
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
            CFMetaData.validateCompactionOptions(cfm.compactionStrategyClass, cfm.compactionStrategyOptions, false);

            cfm.addDefaultIndexNames();
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
            ThriftValidation.validateTable(ks_def.name);
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
            CFMetaData oldCfm = Schema.instance.getCFMetaData(cf_def.keyspace, cf_def.name);
            if (oldCfm == null)
                throw new InvalidRequestException("Could not find column family definition to modify.");

            state().hasColumnFamilyAccess(cf_def.keyspace, cf_def.name, Permission.ALTER);

            CFMetaData.applyImplicitDefaults(cf_def);
            CFMetaData cfm = CFMetaData.fromThrift(cf_def);
            CFMetaData.validateCompactionOptions(cfm.compactionStrategyClass, cfm.compactionStrategyOptions, false);
            cfm.addDefaultIndexNames();
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

            if (startSessionIfRequested())
            {
                Tracing.instance().begin("truncate", ImmutableMap.of("cf", cfname, "ks", keyspace));
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
            Tracing.instance().begin("add", traceParameters);
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
                rm.addCounter(new QueryPath(column_parent.column_family, column_parent.super_column, column.name), column.value);
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
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
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

            return QueryProcessor.process(queryString, state());
        }
        catch (RequestExecutionException e)
        {
            ThriftConversion.rethrow(e);
            return null;
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance().stopSession();
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
                Tracing.instance().begin("execute_cql3_query",
                                         ImmutableMap.of("query", queryString));
            }
            else
            {
                logger.debug("execute_cql3_query");
            }

            ThriftClientState cState = state();
            return org.apache.cassandra.cql3.QueryProcessor.process(queryString, ThriftConversion.fromThrift(cLevel), cState.getQueryState()).toThriftResult();
        }
        catch (RequestExecutionException e)
        {
            ThriftConversion.rethrow(e);
            return null;
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
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

        validateCQLVersion(2);

        try
        {
            ThriftClientState cState = state();
            String queryString = uncompress(query,compression);
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

        try
        {
            ThriftClientState cState = state();
            String queryString = uncompress(query,compression);
            return org.apache.cassandra.cql3.QueryProcessor.prepare(queryString, cState, true).toThriftPreparedResult();
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
            ThriftClientState cState = state();
            CQLStatement statement = cState.getPrepared().get(itemId);

            if (statement == null)
                throw new InvalidRequestException(String.format("Prepared query with ID %d not found", itemId));
            logger.trace("Retrieved prepared statement #{} with {} bind markers", itemId, statement.boundTerms);

            return QueryProcessor.processPrepared(statement, cState, bindVariables);
        }
        catch (RequestExecutionException e)
        {
            ThriftConversion.rethrow(e);
            return null;
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    public CqlResult execute_prepared_cql3_query(int itemId, List<ByteBuffer> bindVariables, ConsistencyLevel cLevel)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        validateCQLVersion(3);

        if (startSessionIfRequested())
        {
            // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
            Tracing.instance().begin("execute_prepared_cql3_query", Collections.<String, String>emptyMap());
        }
        else
        {
            logger.debug("execute_prepared_cql3_query");
        }

        try
        {
            ThriftClientState cState = state();
            org.apache.cassandra.cql3.CQLStatement statement = org.apache.cassandra.cql3.QueryProcessor.getPrepared(itemId);

            if (statement == null)
                throw new InvalidRequestException(String.format("Prepared query with ID %d not found" +
                                                                " (either the query was not prepared on this host (maybe the host has been restarted?)" +
                                                                " or you have prepared more than %d queries and queries %d has been evicted from the internal cache)",
                                                                itemId, org.apache.cassandra.cql3.QueryProcessor.MAX_CACHE_PREPARED, itemId));
            logger.trace("Retrieved prepared statement #{} with {} bind markers", itemId, statement.getBoundsTerms());

            return org.apache.cassandra.cql3.QueryProcessor.processPrepared(statement, ThriftConversion.fromThrift(cLevel), cState.getQueryState(), bindVariables).toThriftResult();
        }
        catch (RequestExecutionException e)
        {
            ThriftConversion.rethrow(e);
            return null;
        }
        catch (RequestValidationException e)
        {
            throw ThriftConversion.toThrift(e);
        }
        finally
        {
            Tracing.instance().stopSession();
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

    // main method moved to CassandraDaemon
}
