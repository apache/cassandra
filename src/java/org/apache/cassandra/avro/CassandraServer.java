package org.apache.cassandra.avro;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.clock.AbstractReconciler;
import org.apache.cassandra.db.clock.TimestampReconciler;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.migration.AddKeyspace;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.avro.AvroRecordFactory.*;
import static org.apache.cassandra.avro.ErrorFactory.*;

public class CassandraServer implements Cassandra {
    private static Logger logger = LoggerFactory.getLogger(CassandraServer.class);

    private final static GenericArray<Column> EMPTY_SUBCOLUMNS = new GenericData.Array<Column>(0, Schema.createArray(Column.SCHEMA$));
    private final static GenericArray<ColumnOrSuperColumn> EMPTY_COLUMNS = new GenericData.Array<ColumnOrSuperColumn>(0, Schema.createArray(ColumnOrSuperColumn.SCHEMA$));
    private final static Utf8 API_VERSION = new Utf8("0.0.0");
    
    // CfDef default values
    private final static String D_CF_CFTYPE = "Standard";
    private final static String D_CF_CFCLOCKTYPE = "Timestamp";
    private final static String D_CF_COMPTYPE = "BytesType";
    private final static String D_CF_SUBCOMPTYPE = "";
    
    private ThreadLocal<AccessLevel> loginDone = new ThreadLocal<AccessLevel>()
    {
        @Override
        protected AccessLevel initialValue()
        {
            return AccessLevel.NONE;
        }
    };
    
    // Session keyspace.
    private ThreadLocal<String> curKeyspace = new ThreadLocal<String>();

    /*
     * An associated Id for scheduling the requests
     */
    private ThreadLocal<String> requestSchedulerId = new ThreadLocal<String>();

    /*
     * RequestScheduler to perform the scheduling of incoming requests
     */
    private final IRequestScheduler requestScheduler;

    public CassandraServer()
    {
        requestScheduler = DatabaseDescriptor.getRequestScheduler();
    }

    @Override
    public ColumnOrSuperColumn get(ByteBuffer key, ColumnPath columnPath, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, NotFoundException, UnavailableException, TimedOutException {
        if (logger.isDebugEnabled())
            logger.debug("get");

        AvroValidation.validateColumnPath(curKeyspace.get(), columnPath);
        
        // FIXME: This is repetitive.
        byte[] column, super_column;
        column = columnPath.column == null ? null : columnPath.column.array();
        super_column = columnPath.super_column == null ? null : columnPath.super_column.array();
        
        QueryPath path = new QueryPath(columnPath.column_family.toString(), column == null ? null : super_column);
        List<byte[]> nameAsList = Arrays.asList(column == null ? super_column : column);
        AvroValidation.validateKey(key.array());
        ReadCommand command = new SliceByNamesReadCommand(curKeyspace.get(), key.array(), path, nameAsList);
        
        Map<DecoratedKey<?>, ColumnFamily> cfamilies = readColumnFamily(Arrays.asList(command), consistencyLevel);
        ColumnFamily cf = cfamilies.get(StorageService.getPartitioner().decorateKey(command.key));
        
        if (cf == null)
            throw newNotFoundException();
        
        GenericArray<ColumnOrSuperColumn> avroColumns = avronateColumnFamily(cf, command.queryPath.superColumnName != null, false);
        
        if (avroColumns.size() == 0)
            throw newNotFoundException();
        
        assert avroColumns.size() == 1;
        return avroColumns.iterator().next();
    }
    
    protected Map<DecoratedKey<?>, ColumnFamily> readColumnFamily(List<ReadCommand> commands, ConsistencyLevel consistency)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        // TODO - Support multiple column families per row, right now row only contains 1 column family
        Map<DecoratedKey<?>, ColumnFamily> columnFamilyKeyMap = new HashMap<DecoratedKey<?>, ColumnFamily>();
        
        if (consistency == ConsistencyLevel.ZERO)
            throw newInvalidRequestException("Consistency level zero may not be applied to read operations");
        
        List<Row> rows;
        try
        {
            schedule();
            rows = StorageProxy.readProtocol(commands, thriftConsistencyLevel(consistency));
        }
        catch (TimeoutException e) 
        {
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        // FIXME: This suckage brought to you by StorageService and StorageProxy
        // which throw Thrift exceptions directly.
        catch (org.apache.cassandra.thrift.UnavailableException e)
        {
            throw new UnavailableException();
        }
        finally
        {
            release();
        }

        for (Row row: rows)
        {
            columnFamilyKeyMap.put(row.key, row.cf);
        }
        
        return columnFamilyKeyMap;
    }
    
    // Don't playa hate, avronate.
    private GenericArray<Column> avronateSubColumns(Collection<IColumn> columns)
    {
        if (columns == null || columns.isEmpty())
            return EMPTY_SUBCOLUMNS;
        
        GenericData.Array<Column> avroColumns = new GenericData.Array<Column>(columns.size(), Column.SCHEMA$);

        for (IColumn column : columns)
        {
            if (column.isMarkedForDelete())
                continue;
            
            Column avroColumn = newColumn(column.name(), column.value(), avronateIClock(column.clock()));
            avroColumns.add(avroColumn);
        }
        
        return avroColumns;
    }

    private static Clock avronateIClock(IClock clock)
    {
        return newClock(((org.apache.cassandra.db.TimestampClock) clock).timestamp());
    }

    private GenericArray<ColumnOrSuperColumn> avronateColumns(Collection<IColumn> columns, boolean reverseOrder)
    {
        ArrayList<ColumnOrSuperColumn> avroColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
        for (IColumn column : columns)
        {
            if (column.isMarkedForDelete())
                continue;
            
            Column avroColumn = newColumn(column.name(), column.value(), avronateIClock(column.clock()));
            
            if (column instanceof ExpiringColumn)
                avroColumn.ttl = ((ExpiringColumn)column).getTimeToLive();
            
            avroColumns.add(newColumnOrSuperColumn(avroColumn));
        }
        
        if (reverseOrder)
            Collections.reverse(avroColumns);
        
        // FIXME: update for AVRO-540 when upgrading to Avro 1.4.0
        GenericArray<ColumnOrSuperColumn> avroArray = new GenericData.Array<ColumnOrSuperColumn>(avroColumns.size(), Schema.createArray(ColumnOrSuperColumn.SCHEMA$));
        for (ColumnOrSuperColumn cosc : avroColumns)
            avroArray.add(cosc);
        
        return avroArray;
    }
    
    private GenericArray<ColumnOrSuperColumn> avronateSuperColumns(Collection<IColumn> columns, boolean reverseOrder)
    {
        ArrayList<ColumnOrSuperColumn> avroSuperColumns = new ArrayList<ColumnOrSuperColumn>(columns.size());
        for (IColumn column: columns)
        {
            GenericArray<Column> subColumns = avronateSubColumns(column.getSubColumns());
            if (subColumns.size() == 0)
                continue;
            SuperColumn superColumn = newSuperColumn(column.name(), subColumns);
            avroSuperColumns.add(newColumnOrSuperColumn(superColumn));
        }
        
        if (reverseOrder)
            Collections.reverse(avroSuperColumns);

        // FIXME: update for AVRO-540 when upgrading to Avro 1.4.0
        GenericArray<ColumnOrSuperColumn> avroArray = new GenericData.Array<ColumnOrSuperColumn>(avroSuperColumns.size(), Schema.createArray(ColumnOrSuperColumn.SCHEMA$));
        for (ColumnOrSuperColumn cosc : avroSuperColumns)
            avroArray.add(cosc);
        
        return avroArray;
    }
    
    private GenericArray<ColumnOrSuperColumn> avronateColumnFamily(ColumnFamily cf, boolean subColumnsOnly, boolean reverseOrder)
    {
        if (cf == null || cf.getColumnsMap().size() == 0)
            return EMPTY_COLUMNS;
        
        if (subColumnsOnly)
        {
            IColumn column = cf.getColumnsMap().values().iterator().next();
            Collection<IColumn> subColumns = column.getSubColumns();
            if (subColumns == null || subColumns.isEmpty())
                return EMPTY_COLUMNS;
            else
                return avronateColumns(subColumns, reverseOrder);
        }
        
        if (cf.isSuper())
            return avronateSuperColumns(cf.getSortedColumns(), reverseOrder);
        else
            return avronateColumns(cf.getSortedColumns(), reverseOrder);
    }
    
    @Override
    public GenericArray<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent columnParent,
            SlicePredicate predicate, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_slice");
        
        Schema bytesArray = Schema.createArray(Schema.parse("{\"type\": \"bytes\"}"));
        GenericArray<ByteBuffer> keys = new GenericData.Array<ByteBuffer>(1, bytesArray);
        keys.add(key);
        
        return multigetSliceInternal(curKeyspace.get(), keys, columnParent, predicate, consistencyLevel).iterator().next().columns;
    }
    
    private GenericArray<CoscsMapEntry> multigetSliceInternal(String keyspace, GenericArray<ByteBuffer> keys,
            ColumnParent columnParent, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        AvroValidation.validateColumnParent(keyspace, columnParent);
        AvroValidation.validatePredicate(keyspace, columnParent, predicate);
        
        byte[] superName = columnParent.super_column == null ? null : columnParent.super_column.array();
        QueryPath queryPath = new QueryPath(columnParent.column_family.toString(), superName);

        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        if (predicate.column_names != null)
        {
            for (ByteBuffer key : keys)
            {
                AvroValidation.validateKey(key);
                
                // FIXME: Copying the collection for the sake of SliceByNamesReadCommands
                Collection<byte[]> column_names = new ArrayList<byte[]>();
                for (ByteBuffer name : predicate.column_names)
                    column_names.add(name.array());
                
                commands.add(new SliceByNamesReadCommand(keyspace, key.array(), queryPath, column_names));
            }
        }
        else
        {
            SliceRange range = predicate.slice_range;
            for (ByteBuffer key : keys)
            {
                AvroValidation.validateKey(key);
                commands.add(new SliceFromReadCommand(keyspace, key.array(), queryPath, range.start.array(), range.finish.array(), range.reversed, range.count));
            }
        }
        
        return getSlice(commands, consistencyLevel);
    }
    
    private GenericArray<CoscsMapEntry> getSlice(List<ReadCommand> commands, ConsistencyLevel consistencyLevel)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        Map<DecoratedKey<?>, ColumnFamily> columnFamilies = readColumnFamily(commands, consistencyLevel);
        Schema sch = Schema.createArray(CoscsMapEntry.SCHEMA$);
        GenericArray<CoscsMapEntry> columnFamiliesList = new GenericData.Array<CoscsMapEntry>(commands.size(), sch);
        
        for (ReadCommand cmd : commands)
        {
            ColumnFamily cf = columnFamilies.get(StorageService.getPartitioner().decorateKey(cmd.key));
            boolean reverseOrder = cmd instanceof SliceFromReadCommand && ((SliceFromReadCommand)cmd).reversed;
            GenericArray<ColumnOrSuperColumn> avroColumns = avronateColumnFamily(cf, cmd.queryPath.superColumnName != null, reverseOrder);
            columnFamiliesList.add(newCoscsMapEntry(ByteBuffer.wrap(cmd.key), avroColumns));
        }
        
        return columnFamiliesList;
    }

    @Override
    public int get_count(ByteBuffer key, ColumnParent columnParent, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_count");
        
        return (int)get_slice(key, columnParent, predicate, consistencyLevel).size();
    }

    @Override
    public GenericArray<CoscsMapEntry> multiget_slice(GenericArray<ByteBuffer> keys, ColumnParent columnParent,
            SlicePredicate predicate, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("multiget_slice");
        
        return multigetSliceInternal(curKeyspace.get(), keys, columnParent, predicate, consistencyLevel);
    }

    @Override
    public Void insert(ByteBuffer key, ColumnParent parent, Column column, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("insert");

        AvroValidation.validateKey(key.array());
        AvroValidation.validateColumnParent(curKeyspace.get(), parent);
        AvroValidation.validateColumn(curKeyspace.get(), parent, column);

        RowMutation rm = new RowMutation(curKeyspace.get(), key.array());
        try
        {
            rm.add(new QueryPath(parent.column_family.toString(),
                   parent.super_column == null ? null : parent.super_column.array(),
                   column.name.array()),
                   column.value.array(),
                   unavronateClock(column.clock),
                   column.ttl == null ? 0 : column.ttl);
        }
        catch (MarshalException e)
        {
            throw newInvalidRequestException(e.getMessage());
        }
        doInsert(consistencyLevel, rm);

        return null;
    }
    
    @Override
    public Void remove(ByteBuffer key, ColumnPath columnPath, Clock clock, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("remove");
        
        AvroValidation.validateKey(key.array());
        AvroValidation.validateColumnPath(curKeyspace.get(), columnPath);
        IClock dbClock = AvroValidation.validateClock(clock);
        
        RowMutation rm = new RowMutation(curKeyspace.get(), key.array());
        byte[] superName = columnPath.super_column == null ? null : columnPath.super_column.array();
        rm.delete(new QueryPath(columnPath.column_family.toString(), superName), dbClock);
        
        doInsert(consistencyLevel, rm);
        
        return null;
    }

    private void doInsert(ConsistencyLevel consistency, RowMutation rm) throws UnavailableException, TimedOutException
    {
        if (consistency != ConsistencyLevel.ZERO)
        {
            try
            {
                schedule();
                StorageProxy.mutateBlocking(Arrays.asList(rm), thriftConsistencyLevel(consistency));
            }
            catch (TimeoutException e)
            {
                throw new TimedOutException();
            }
            catch (org.apache.cassandra.thrift.UnavailableException thriftE)
            {
                throw new UnavailableException();
            }
            finally
            {
                release();
            }
        }
        else
        {
            try
            {
                schedule();
                StorageProxy.mutate(Arrays.asList(rm));
            }
            finally
            {
                release();
            }
        }
    }

    @Override
    public Void batch_mutate(GenericArray<MutationsMapEntry> mutationMap, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("batch_mutate");
        
        List<RowMutation> rowMutations = new ArrayList<RowMutation>();
        
        for (MutationsMapEntry pair: mutationMap)
        {
            AvroValidation.validateKey(pair.key.array());
            Map<Utf8, GenericArray<Mutation>> cfToMutations = pair.mutations;
            
            for (Map.Entry<Utf8, GenericArray<Mutation>> cfMutations : cfToMutations.entrySet())
            {
                String cfName = cfMutations.getKey().toString();
                
                for (Mutation mutation : cfMutations.getValue())
                    AvroValidation.validateMutation(curKeyspace.get(), cfName, mutation);
            }
            rowMutations.add(getRowMutationFromMutations(curKeyspace.get(), pair.key.array(), cfToMutations));
        }
        
        if (consistencyLevel == ConsistencyLevel.ZERO)
        {
            try
            {
                schedule();
                StorageProxy.mutate(rowMutations);
            }
            finally
            {
                release();
            }
        }
        else
        {
            try
            {
                schedule();
                StorageProxy.mutateBlocking(rowMutations, thriftConsistencyLevel(consistencyLevel));
            }
            catch (TimeoutException te)
            {
                throw newTimedOutException();
            }
            // FIXME: StorageProxy.mutateBlocking throws Thrift's UnavailableException
            catch (org.apache.cassandra.thrift.UnavailableException ue)
            {
                throw newUnavailableException();
            }
            finally
            {
                release();
            }
        }
        
        return null;
    }

    private static IClock unavronateClock(Clock clock)
    {
        return new org.apache.cassandra.db.TimestampClock(clock.timestamp);
    }
    
    // FIXME: This is copypasta from o.a.c.db.RowMutation, (RowMutation.getRowMutation uses Thrift types directly).
    private static RowMutation getRowMutationFromMutations(String keyspace, byte[] key, Map<Utf8, GenericArray<Mutation>> cfMap)
    {
        RowMutation rm = new RowMutation(keyspace, key);
        
        for (Map.Entry<Utf8, GenericArray<Mutation>> entry : cfMap.entrySet())
        {
            String cfName = entry.getKey().toString();
            
            for (Mutation mutation : entry.getValue())
            {
                if (mutation.deletion != null)
                    deleteColumnOrSuperColumnToRowMutation(rm, cfName, mutation.deletion);
                else
                    addColumnOrSuperColumnToRowMutation(rm, cfName, mutation.column_or_supercolumn);
            }
        }
        
        return rm;
    }
    
    // FIXME: This is copypasta from o.a.c.db.RowMutation, (RowMutation.getRowMutation uses Thrift types directly).
    private static void addColumnOrSuperColumnToRowMutation(RowMutation rm, String cfName, ColumnOrSuperColumn cosc)
    {
        if (cosc.column == null)
        {
            for (Column column : cosc.super_column.columns)
                rm.add(new QueryPath(cfName, cosc.super_column.name.array(), column.name.array()), column.value.array(), unavronateClock(column.clock));
        }
        else
        {
            rm.add(new QueryPath(cfName, null, cosc.column.name.array()), cosc.column.value.array(), unavronateClock(cosc.column.clock));
        }
    }
    
    // FIXME: This is copypasta from o.a.c.db.RowMutation, (RowMutation.getRowMutation uses Thrift types directly).
    private static void deleteColumnOrSuperColumnToRowMutation(RowMutation rm, String cfName, Deletion del)
    {
        byte[] superName = del.super_column == null ? null : del.super_column.array();
        
        if (del.predicate != null && del.predicate.column_names != null)
        {
            for (ByteBuffer col : del.predicate.column_names)
            {
                if (del.super_column == null && DatabaseDescriptor.getColumnFamilyType(rm.getTable(), cfName) == ColumnFamilyType.Super)
                    rm.delete(new QueryPath(cfName, col.array()), unavronateClock(del.clock));
                else
                    rm.delete(new QueryPath(cfName, superName, col.array()), unavronateClock(del.clock));
            }
        }
        else
        {
            rm.delete(new QueryPath(cfName, superName), unavronateClock(del.clock));
        }
    }
    
    private org.apache.cassandra.thrift.ConsistencyLevel thriftConsistencyLevel(ConsistencyLevel consistency)
    {
        switch (consistency)
        {
            case ZERO: return org.apache.cassandra.thrift.ConsistencyLevel.ZERO;
            case ONE: return org.apache.cassandra.thrift.ConsistencyLevel.ONE;
            case QUORUM: return org.apache.cassandra.thrift.ConsistencyLevel.QUORUM;
            case DCQUORUM: return org.apache.cassandra.thrift.ConsistencyLevel.DCQUORUM;
            case DCQUORUMSYNC: return org.apache.cassandra.thrift.ConsistencyLevel.DCQUORUMSYNC;
            case ALL: return org.apache.cassandra.thrift.ConsistencyLevel.ALL;
        }
        return null;
    }

    @Override
    public Void set_keyspace(Utf8 keyspace) throws InvalidRequestException
    {
        String keyspaceStr = keyspace.toString();
        
        if (DatabaseDescriptor.getTableDefinition(keyspaceStr) == null)
        {
            throw newInvalidRequestException("Keyspace does not exist");
        }
        
        // If switching, invalidate previous access level; force a new login.
        if (this.curKeyspace.get() != null && !this.curKeyspace.get().equals(keyspaceStr))
            loginDone.set(AccessLevel.NONE);
        
        this.curKeyspace.set(keyspaceStr);

        if (DatabaseDescriptor.getRequestSchedulerId().equals(Config.RequestSchedulerId.keyspace)) {
            requestSchedulerId.set(curKeyspace.get());
        }

        return null;
    }

    @Override
    public Void system_add_keyspace(KsDef ksDef) throws AvroRemoteException, InvalidRequestException
    {
        if (StageManager.getStage(StageManager.MIGRATION_STAGE).getQueue().size() > 0)
            throw newInvalidRequestException("This node appears to be handling gossiped migrations.");
        
        try
        {
            Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>((int)ksDef.cf_defs.size());
            for (CfDef cfDef : ksDef.cf_defs)
            {
                String cfType, compare, subCompare, reconcilerName;
                cfType = cfDef.column_type == null ? D_CF_CFTYPE : cfDef.column_type.toString();
                ClockType clockType = ClockType.create(cfDef.clock_type == null ? D_CF_CFCLOCKTYPE : cfDef.clock_type.toString());
                compare = cfDef.comparator_type == null ? D_CF_COMPTYPE : cfDef.comparator_type.toString();
                subCompare = cfDef.subcomparator_type == null ? D_CF_SUBCOMPTYPE : cfDef.subcomparator_type.toString();
                reconcilerName = cfDef.reconciler == null  ? null : cfDef.reconciler.toString();
                
                AbstractReconciler reconciler = DatabaseDescriptor.getReconciler(reconcilerName);
                if (reconciler == null)
                {
                    if (clockType == ClockType.Timestamp)    
                        reconciler = new TimestampReconciler(); // default
                    else
                        throw new ConfigurationException("No reconciler specified for column family " + cfDef.name.toString());

                }

                if (cfDef.id != null)
                    logger.warn("Ignoring 'id' field specified for new column family (%s, %s)", cfDef.keyspace, cfDef.name);

                CFMetaData cfmeta = new CFMetaData(cfDef.keyspace.toString(),
                                                   cfDef.name.toString(),
                                                   ColumnFamilyType.create(cfType),
                                                   clockType,
                                                   DatabaseDescriptor.getComparator(compare),
                                                   subCompare.length() == 0 ? null : DatabaseDescriptor.getComparator(subCompare),
                                                   reconciler,
                                                   cfDef.comment == null ? "" : cfDef.comment.toString(),
                                                   cfDef.row_cache_size == null ? CFMetaData.DEFAULT_ROW_CACHE_SIZE : cfDef.row_cache_size,
                                                   cfDef.preload_row_cache == null ? CFMetaData.DEFAULT_PRELOAD_ROW_CACHE : cfDef.preload_row_cache,
                                                   cfDef.key_cache_size == null ? CFMetaData.DEFAULT_KEY_CACHE_SIZE : cfDef.key_cache_size,
                                                   cfDef.read_repair_chance == null ? CFMetaData.DEFAULT_READ_REPAIR_CHANCE : cfDef.read_repair_chance,
                                                   Collections.<byte[], ColumnDefinition>emptyMap());
                cfDefs.add(cfmeta);
            }
            
            KSMetaData ksmeta = new KSMetaData(
                    ksDef.name.toString(),
                    (Class<? extends AbstractReplicationStrategy>)Class.forName(ksDef.strategy_class.toString()),
                    (int)ksDef.replication_factor,
                    cfDefs.toArray(new CFMetaData[cfDefs.size()]));
            AddKeyspace add = new AddKeyspace(ksmeta);
            add.apply();
            add.announce();
        }
        catch (ClassNotFoundException e)
        {
            InvalidRequestException ire = newInvalidRequestException(e.getMessage());
            ire.initCause(e);
            throw ire;
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ire = newInvalidRequestException(e.getMessage());
            ire.initCause(e);
            throw ire;
        }
        catch (IOException e)
        {
            InvalidRequestException ire = newInvalidRequestException(e.getMessage());
            ire.initCause(e);
            throw ire;
        }
        
        return null;
    }

    @Override
    public GenericArray<Utf8> describe_keyspaces() throws AvroRemoteException
    {
        Set<String> keyspaces = DatabaseDescriptor.getTables();
        Schema schema = Schema.createArray(Schema.create(Schema.Type.STRING));
        GenericArray<Utf8> avroResults = new GenericData.Array<Utf8>(keyspaces.size(), schema);
        
        for (String ksp : keyspaces)
            avroResults.add(new Utf8(ksp));
        
        return avroResults;
    }

    @Override
    public Utf8 describe_cluster_name() throws AvroRemoteException
    {
        return new Utf8(DatabaseDescriptor.getClusterName());
    }
    

    @Override
    public Utf8 describe_version() throws AvroRemoteException
    {
        return API_VERSION;
    }
    
    public Map<String, List<String>> check_schema_agreement()
    {
        logger.debug("checking schema agreement");      
        return StorageProxy.checkSchemaAgreement();
    }

    /**
     * Schedule the current thread for access to the required services
     */
    private void schedule()
    {
        requestScheduler.queue(Thread.currentThread(), requestSchedulerId.get());
    }

    /**
     * Release a count of resources used to the request scheduler
     */
    private void release()
    {
        requestScheduler.release();
    }
}
