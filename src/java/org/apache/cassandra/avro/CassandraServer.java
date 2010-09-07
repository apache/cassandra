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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.util.Utf8;
import org.apache.cassandra.avro.InvalidRequestException;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.migration.DropKeyspace;
import org.apache.cassandra.db.migration.RenameKeyspace;
import org.apache.cassandra.db.migration.UpdateColumnFamily;
import org.apache.cassandra.db.migration.UpdateKeyspace;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.Permission;
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
import org.apache.cassandra.db.migration.AddColumnFamily;
import org.apache.cassandra.db.migration.AddKeyspace;
import org.apache.cassandra.db.migration.DropColumnFamily;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.db.migration.RenameColumnFamily;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.service.ClientState;
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
    private final static String D_CF_RECONCILER = null;
    
    //ColumnDef default values
    public final static String D_COLDEF_INDEXTYPE = "KEYS";
    public final static String D_COLDEF_INDEXNAME = null;
    
    // thread local state containing session information
    final ClientState clientState = new ClientState();

    /*
     * RequestScheduler to perform the scheduling of incoming requests
     */
    private final IRequestScheduler requestScheduler;

    public CassandraServer()
    {
        requestScheduler = DatabaseDescriptor.getRequestScheduler();
    }

    public Void login(AuthenticationRequest auth_request) throws AuthenticationException, AuthorizationException
    {
        try
        {
            clientState.login(auth_request.credentials);
        }
        catch (org.apache.cassandra.thrift.AuthenticationException thriftE) {
            throw new AuthenticationException();
        }
        return null;
    }

    public ColumnOrSuperColumn get(ByteBuffer key, ColumnPath columnPath, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, NotFoundException, UnavailableException, TimedOutException {
        if (logger.isDebugEnabled())
            logger.debug("get");

        AvroValidation.validateColumnPath(clientState.getKeyspace(), columnPath);
        
        // FIXME: This is repetitive.
        byte[] column, super_column;
        column = columnPath.column == null ? null : columnPath.column.array();
        super_column = columnPath.super_column == null ? null : columnPath.super_column.array();
        
        QueryPath path = new QueryPath(columnPath.column_family.toString(), column == null ? null : super_column);
        List<byte[]> nameAsList = Arrays.asList(column == null ? super_column : column);
        AvroValidation.validateKey(key.array());
        ReadCommand command = new SliceByNamesReadCommand(clientState.getKeyspace(), key.array(), path, nameAsList);
        
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
            throw newUnavailableException(e);
        }
        catch (org.apache.cassandra.thrift.InvalidRequestException e)
        {
            throw newInvalidRequestException(e);
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
    private List<Column> avronateSubColumns(Collection<IColumn> columns)
    {
        if (columns == null || columns.isEmpty())
            return EMPTY_SUBCOLUMNS;
        
        List<Column> avroColumns = new ArrayList<Column>(columns.size());

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
            List<Column> subColumns = avronateSubColumns(column.getSubColumns());
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
    
    public List<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent columnParent,
            SlicePredicate predicate, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_slice");
        
        Schema bytesArray = Schema.createArray(Schema.parse("{\"type\": \"bytes\"}"));
        GenericArray<ByteBuffer> keys = new GenericData.Array<ByteBuffer>(1, bytesArray);
        keys.add(key);
        
        return multigetSliceInternal(clientState.getKeyspace(), keys, columnParent, predicate, consistencyLevel).iterator().next().columns;
    }
    
    private List<CoscsMapEntry> multigetSliceInternal(String keyspace, List<ByteBuffer> keys,
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
    
    private List<CoscsMapEntry> getSlice(List<ReadCommand> commands, ConsistencyLevel consistencyLevel)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        Map<DecoratedKey<?>, ColumnFamily> columnFamilies = readColumnFamily(commands, consistencyLevel);
        Schema sch = Schema.createArray(CoscsMapEntry.SCHEMA$);
        List<CoscsMapEntry> columnFamiliesList = new GenericData.Array<CoscsMapEntry>(commands.size(), sch);
        
        for (ReadCommand cmd : commands)
        {
            ColumnFamily cf = columnFamilies.get(StorageService.getPartitioner().decorateKey(cmd.key));
            boolean reverseOrder = cmd instanceof SliceFromReadCommand && ((SliceFromReadCommand)cmd).reversed;
            GenericArray<ColumnOrSuperColumn> avroColumns = avronateColumnFamily(cf, cmd.queryPath.superColumnName != null, reverseOrder);
            columnFamiliesList.add(newCoscsMapEntry(ByteBuffer.wrap(cmd.key), avroColumns));
        }
        
        return columnFamiliesList;
    }

    public int get_count(ByteBuffer key, ColumnParent columnParent, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_count");
        
        return (int)get_slice(key, columnParent, predicate, consistencyLevel).size();
    }

    public List<CoscsMapEntry> multiget_slice(List<ByteBuffer> keys, ColumnParent columnParent,
            SlicePredicate predicate, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("multiget_slice");
        
        return multigetSliceInternal(clientState.getKeyspace(), keys, columnParent, predicate, consistencyLevel);
    }

    public Void insert(ByteBuffer key, ColumnParent parent, Column column, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("insert");

        AvroValidation.validateKey(key.array());
        AvroValidation.validateColumnParent(clientState.getKeyspace(), parent);
        AvroValidation.validateColumn(clientState.getKeyspace(), parent, column);

        RowMutation rm = new RowMutation(clientState.getKeyspace(), key.array());
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
    
    public Void remove(ByteBuffer key, ColumnPath columnPath, Clock clock, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("remove");
        
        AvroValidation.validateKey(key.array());
        AvroValidation.validateColumnPath(clientState.getKeyspace(), columnPath);
        IClock dbClock = AvroValidation.validateClock(clock);
        
        RowMutation rm = new RowMutation(clientState.getKeyspace(), key.array());
        byte[] superName = columnPath.super_column == null ? null : columnPath.super_column.array();
        rm.delete(new QueryPath(columnPath.column_family.toString(), superName), dbClock);
        
        doInsert(consistencyLevel, rm);
        
        return null;
    }

    private void doInsert(ConsistencyLevel consistency, RowMutation rm) throws UnavailableException, TimedOutException
    {
        try
        {
            schedule();
            StorageProxy.mutate(Arrays.asList(rm), thriftConsistencyLevel(consistency));
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
        catch (org.apache.cassandra.thrift.UnavailableException thriftE)
        {
            throw newUnavailableException(thriftE);
        }
        finally
        {
            release();
        }
    }

    public Void batch_mutate(List<MutationsMapEntry> mutationMap, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("batch_mutate");
        
        List<RowMutation> rowMutations = new ArrayList<RowMutation>();
        
        for (MutationsMapEntry pair: mutationMap)
        {
            AvroValidation.validateKey(pair.key.array());
            Map<CharSequence, List<Mutation>> cfToMutations = pair.mutations;
            
            for (Map.Entry<CharSequence, List<Mutation>> cfMutations : cfToMutations.entrySet())
            {
                String cfName = cfMutations.getKey().toString();
                
                for (Mutation mutation : cfMutations.getValue())
                    AvroValidation.validateMutation(clientState.getKeyspace(), cfName, mutation);
            }
            rowMutations.add(getRowMutationFromMutations(clientState.getKeyspace(), pair.key.array(), cfToMutations));
        }
        
        try
        {
            schedule();
            StorageProxy.mutate(rowMutations, thriftConsistencyLevel(consistencyLevel));
        }
        catch (TimeoutException te)
        {
            throw newTimedOutException();
        }
        // FIXME: StorageProxy.mutate throws Thrift's UnavailableException
        catch (org.apache.cassandra.thrift.UnavailableException ue)
        {
            throw newUnavailableException();
        }
        finally
        {
            release();
        }
        
        return null;
    }

    private static IClock unavronateClock(Clock clock)
    {
        return new org.apache.cassandra.db.TimestampClock(clock.timestamp);
    }
    
    // FIXME: This is copypasta from o.a.c.db.RowMutation, (RowMutation.getRowMutation uses Thrift types directly).
    private static RowMutation getRowMutationFromMutations(String keyspace, byte[] key, Map<CharSequence, List<Mutation>> cfMap)
    {
        RowMutation rm = new RowMutation(keyspace, key);
        
        for (Map.Entry<CharSequence, List<Mutation>> entry : cfMap.entrySet())
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
    
    // Copy-pasted from the thrift CassandraServer, using the factory methods to create exceptions.
    // helper method to apply migration on the migration stage. typical migration failures will throw an 
    // InvalidRequestException. atypical failures will throw a RuntimeException.
    private static void applyMigrationOnStage(final Migration m) throws InvalidRequestException
    {
        Future f = StageManager.getStage(Stage.MIGRATION).submit(new Callable()
        {
            public Object call() throws Exception
            {
                m.apply();
                m.announce();
                return null;
            }
        });
        try
        {
            f.get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            // this means call() threw an exception. deal with it directly.
            if (e.getCause() != null)
            {
                throw newInvalidRequestException(e.getCause());
            }
            else
            {
                throw newInvalidRequestException(e);
            }
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

    public Void set_keyspace(CharSequence keyspace) throws InvalidRequestException
    {
        String keyspaceStr = keyspace.toString();
        
        if (DatabaseDescriptor.getTableDefinition(keyspaceStr) == null)
        {
            throw newInvalidRequestException("Keyspace does not exist");
        }
        
        clientState.setKeyspace(keyspaceStr);
        return null;
    }

    public CharSequence system_add_keyspace(KsDef ksDef) throws AvroRemoteException, InvalidRequestException
    {
        if (!(DatabaseDescriptor.getAuthenticator() instanceof AllowAllAuthenticator))
            throw newInvalidRequestException("Unable to create new keyspace while authentication is enabled.");
        
        if (StorageService.instance.getLiveNodes().size() < ksDef.replication_factor)
            throw newInvalidRequestException("Not enough live nodes to support this keyspace");
        
        //generate a meaningful error if the user setup keyspace and/or column definition incorrectly
        for (CfDef cf : ksDef.cf_defs) 
        {
            if (!cf.keyspace.equals(ksDef.name))
            {
                throw newInvalidRequestException("CsDef (" + cf.name +") had a keyspace definition that did not match KsDef");
            }
        }
        
        try
        {
            Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>((int)ksDef.cf_defs.size());
            for (CfDef cfDef : ksDef.cf_defs)
            {    
                cfDefs.add(convertToCFMetaData(cfDef));
            }

            // convert Map<CharSequence, CharSequence> to Map<String, String> 
            Map<String, String> strategyOptions = null;
            if (ksDef.strategy_options != null && !ksDef.strategy_options.isEmpty())
            {
                strategyOptions = new HashMap<String, String>();
                for (Map.Entry<CharSequence, CharSequence> option : ksDef.strategy_options.entrySet())
                {
                    strategyOptions.put(option.getKey().toString(), option.getValue().toString());
                }
            }

            KSMetaData ksmeta = new KSMetaData(
                    ksDef.name.toString(),
                    (Class<? extends AbstractReplicationStrategy>)Class.forName(ksDef.strategy_class.toString()),
                    strategyOptions,
                    ksDef.replication_factor,
                    cfDefs.toArray(new CFMetaData[cfDefs.size()]));
            applyMigrationOnStage(new AddKeyspace(ksmeta));
            return DatabaseDescriptor.getDefsVersion().toString();
            
        }
        catch (ClassNotFoundException e)
        {
            throw newInvalidRequestException(e);
        }
        catch (ConfigurationException e)
        {
            throw newInvalidRequestException(e);
        }
        catch (IOException e)
        {
            throw newInvalidRequestException(e);
        }
    }

    public CharSequence system_add_column_family(CfDef cfDef) throws AvroRemoteException, InvalidRequestException
    {
        checkKeyspaceAndLoginAuthorized(Permission.WRITE);
        try
        {
            applyMigrationOnStage(new AddColumnFamily(convertToCFMetaData(cfDef)));
            return DatabaseDescriptor.getDefsVersion().toString();
        } catch (ConfigurationException e)
        {
            throw newInvalidRequestException(e);
        }
        catch (IOException e)
        {
            throw newInvalidRequestException(e);
        }
    }

    public CharSequence system_update_column_family(CfDef cf_def) throws AvroRemoteException, InvalidRequestException
    {
        checkKeyspaceAndLoginAuthorized(Permission.WRITE);
        
        if (cf_def.keyspace == null || cf_def.name == null)
            throw newInvalidRequestException("Keyspace and CF name must be set.");
        
        CFMetaData oldCfm = DatabaseDescriptor.getCFMetaData(CFMetaData.getId(cf_def.keyspace.toString(), cf_def.name.toString()));
        if (oldCfm == null) 
            throw newInvalidRequestException("Could not find column family definition to modify.");
        
        try
        {
            CFMetaData newCfm = oldCfm.apply(cf_def);
            UpdateColumnFamily update = new UpdateColumnFamily(oldCfm, newCfm);
            applyMigrationOnStage(update);
            return DatabaseDescriptor.getDefsVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = newInvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = newInvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public CharSequence system_update_keyspace(KsDef ks_def) throws AvroRemoteException, InvalidRequestException
    {
        checkKeyspaceAndLoginAuthorized(Permission.WRITE);
        
        if (ks_def.cf_defs != null && ks_def.cf_defs.size() > 0)
            throw newInvalidRequestException("Keyspace update must not contain any column family definitions.");
        
        if (StorageService.instance.getLiveNodes().size() < ks_def.replication_factor)
            throw newInvalidRequestException("Not enough live nodes to support this keyspace");
        if (DatabaseDescriptor.getTableDefinition(ks_def.name.toString()) == null)
            throw newInvalidRequestException("Keyspace does not exist.");
        
        try
        {
            // convert Map<CharSequence, CharSequence> to Map<String, String> 
            Map<String, String> strategyOptions = null;
            if (ks_def.strategy_options != null && !ks_def.strategy_options.isEmpty())
            {
                strategyOptions = new HashMap<String, String>();
                for (Map.Entry<CharSequence, CharSequence> option : ks_def.strategy_options.entrySet())
                {
                    strategyOptions.put(option.getKey().toString(), option.getValue().toString());
                }
            }
            
            KSMetaData ksm = new KSMetaData(
                    ks_def.name.toString(), 
                    (Class<? extends AbstractReplicationStrategy>) FBUtilities.<AbstractReplicationStrategy>classForName(ks_def.strategy_class.toString(), "keyspace replication strategy"),
                    strategyOptions,
                    ks_def.replication_factor);
            applyMigrationOnStage(new UpdateKeyspace(ksm));
            return DatabaseDescriptor.getDefsVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = newInvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = newInvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public GenericArray<CharSequence> describe_keyspaces() throws AvroRemoteException
    {
        Set<String> keyspaces = DatabaseDescriptor.getTables();
        Schema schema = Schema.createArray(Schema.create(Schema.Type.STRING));
        GenericArray<CharSequence> avroResults = new GenericData.Array<CharSequence>(keyspaces.size(), schema);
        
        for (String ksp : keyspaces)
            avroResults.add(new Utf8(ksp));
        
        return avroResults;
    }

    public Utf8 describe_cluster_name() throws AvroRemoteException
    {
        return new Utf8(DatabaseDescriptor.getClusterName());
    }
    

    public Utf8 describe_version() throws AvroRemoteException
    {
        return API_VERSION;
    }
    
    public Map<CharSequence, List<CharSequence>> check_schema_agreement()
    {
        logger.debug("checking schema agreement");
        return (Map) StorageProxy.checkSchemaAgreement();
    }

    protected void checkKeyspaceAndLoginAuthorized(Permission perm) throws InvalidRequestException
    {
        try
        {
            clientState.hasKeyspaceAccess(perm);
        }
        catch (org.apache.cassandra.thrift.InvalidRequestException e)
        {
            throw newInvalidRequestException(e.getWhy());
        }
    }

    /**
     * Schedule the current thread for access to the required services
     */
    private void schedule()
    {
        requestScheduler.queue(Thread.currentThread(), clientState.getSchedulingId());
    }

    /**
     * Release a count of resources used to the request scheduler
     */
    private void release()
    {
        requestScheduler.release();
    }
    
    private CFMetaData convertToCFMetaData(CfDef cf_def) throws InvalidRequestException, ConfigurationException
    {
        String cfType = cf_def.column_type == null ? D_CF_CFTYPE : cf_def.column_type.toString();
        ClockType clockType = ClockType.create(cf_def.clock_type == null ? D_CF_CFCLOCKTYPE : cf_def.clock_type.toString());
        String compare = cf_def.comparator_type == null ? D_CF_COMPTYPE : cf_def.comparator_type.toString();
        String validate = cf_def.default_validation_class == null ? D_CF_COMPTYPE : cf_def.default_validation_class.toString();
        String subCompare = cf_def.subcomparator_type == null ? D_CF_SUBCOMPTYPE : cf_def.subcomparator_type.toString();
        String reconcilerName = cf_def.reconciler == null  ? D_CF_RECONCILER : cf_def.reconciler.toString();
        
        AbstractReconciler reconciler = DatabaseDescriptor.getReconciler(reconcilerName);
        if (reconciler == null)
        {
            if (clockType == ClockType.Timestamp)    
                reconciler = TimestampReconciler.instance; // default
            else
                throw new ConfigurationException("No reconciler specified for column family " + cf_def.name);
        }

        return new CFMetaData(cf_def.keyspace.toString(),
                              cf_def.name.toString(),
                              ColumnFamilyType.create(cfType),
                              clockType,
                              DatabaseDescriptor.getComparator(compare),
                              subCompare.length() == 0 ? null : DatabaseDescriptor.getComparator(subCompare),
                              reconciler,
                              cf_def.comment == null ? "" : cf_def.comment.toString(),
                              cf_def.row_cache_size == null ? CFMetaData.DEFAULT_ROW_CACHE_SIZE : cf_def.row_cache_size,
                              cf_def.preload_row_cache == null ? CFMetaData.DEFAULT_PRELOAD_ROW_CACHE : cf_def.preload_row_cache,
                              cf_def.key_cache_size == null ? CFMetaData.DEFAULT_KEY_CACHE_SIZE : cf_def.key_cache_size,
                              cf_def.read_repair_chance == null ? CFMetaData.DEFAULT_READ_REPAIR_CHANCE : cf_def.read_repair_chance,
                              cf_def.gc_grace_seconds != null ? cf_def.gc_grace_seconds : CFMetaData.DEFAULT_GC_GRACE_SECONDS,
                              DatabaseDescriptor.getComparator(validate),
                              ColumnDefinition.fromColumnDefs((Iterable<ColumnDef>) cf_def.column_metadata));
    }

    public KsDef describe_keyspace(CharSequence keyspace) throws AvroRemoteException, NotFoundException
    {
        KSMetaData ksMetadata = DatabaseDescriptor.getTableDefinition(keyspace.toString());
        if (ksMetadata == null)
            throw new NotFoundException();
        
        KsDef ksDef = new KsDef();
        ksDef.name = keyspace;
        ksDef.replication_factor = ksMetadata.replicationFactor;
        ksDef.strategy_class = ksMetadata.strategyClass.getName();
        if (ksMetadata.strategyOptions != null)
        {
            ksDef.strategy_options = new HashMap<CharSequence, CharSequence>();
            ksDef.strategy_options.putAll(ksMetadata.strategyOptions);
        }
        
        GenericArray<CfDef> cfDefs = new GenericData.Array<CfDef>(ksMetadata.cfMetaData().size(), Schema.createArray(CfDef.SCHEMA$));
        for (CFMetaData cfm : ksMetadata.cfMetaData().values())
        {
            cfDefs.add(CFMetaData.convertToAvro(cfm));
        }
        ksDef.cf_defs = cfDefs;
        
        return ksDef;
    }

    public CharSequence system_rename_column_family(CharSequence old_name, CharSequence new_name)
    throws AvroRemoteException, InvalidRequestException
    {
        checkKeyspaceAndLoginAuthorized(Permission.WRITE);
        
        try
        {
            applyMigrationOnStage(new RenameColumnFamily(clientState.getKeyspace(), old_name.toString(), new_name.toString()));
            return DatabaseDescriptor.getDefsVersion().toString();
        }
        catch (ConfigurationException e)
        {
            throw newInvalidRequestException(e);
        }
        catch (IOException e)
        {
            throw newInvalidRequestException(e);
        }
    }

    public CharSequence system_drop_column_family(CharSequence column_family) throws AvroRemoteException, InvalidRequestException
    {
        checkKeyspaceAndLoginAuthorized(Permission.WRITE);
        
        try
        {
            applyMigrationOnStage(new DropColumnFamily(clientState.getKeyspace(), column_family.toString(), true));
            return DatabaseDescriptor.getDefsVersion().toString();
        }
        catch (ConfigurationException e)
        {
            throw newInvalidRequestException(e);
        }
        catch (IOException e)
        {
            throw newInvalidRequestException(e);
        }
    }

    public CharSequence system_drop_keyspace(CharSequence keyspace) throws AvroRemoteException, InvalidRequestException
    {
        if (!(DatabaseDescriptor.getAuthenticator() instanceof AllowAllAuthenticator))
            throw newInvalidRequestException("Unable to create new keyspace while authentication is enabled.");
        
        try
        {
            applyMigrationOnStage(new DropKeyspace(keyspace.toString(), true));
            return DatabaseDescriptor.getDefsVersion().toString();
        }
        catch (ConfigurationException e)
        {
            throw newInvalidRequestException(e);
        }
        catch (IOException e)
        {
            throw newInvalidRequestException(e);
        }
    }

    public CharSequence system_rename_keyspace(CharSequence old_name, CharSequence new_name) throws AvroRemoteException, InvalidRequestException
    {
        checkKeyspaceAndLoginAuthorized(Permission.WRITE);
        
        try
        {
            applyMigrationOnStage(new RenameKeyspace(old_name.toString(), new_name.toString()));
            return DatabaseDescriptor.getDefsVersion().toString();
        }
        catch (ConfigurationException e)
        {
            throw newInvalidRequestException(e);
        }
        catch (IOException e)
        {
            throw newInvalidRequestException(e);
        }
    }

    public CharSequence describe_partitioner() throws AvroRemoteException
    {
        return StorageService.getPartitioner().getClass().getName();
    }

    public List<CharSequence> describe_splits(CharSequence keyspace, CharSequence cfName, CharSequence start_token, CharSequence end_token, int keys_per_split) {
        Token.TokenFactory<?> tf = StorageService.getPartitioner().getTokenFactory();
        List<Token> tokens = StorageService.instance.getSplits(keyspace.toString(), cfName.toString(), new Range(tf.fromString(start_token.toString()), tf.fromString(end_token.toString())), keys_per_split);
        List<CharSequence> splits = new ArrayList<CharSequence>(tokens.size());
        for (Token token : tokens)
        {
            splits.add(tf.toString(token));
        }
        return splits;
    }

    public List<KeyCountMapEntry> multiget_count(List<ByteBuffer> keys, ColumnParent columnParent, SlicePredicate predicate, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("multiget_count");
        
        checkKeyspaceAndLoginAuthorized(Permission.READ_VALUE);
        String keyspace = clientState.getKeyspace();
        
        List<KeyCountMapEntry> counts = new ArrayList<KeyCountMapEntry>();
        List<CoscsMapEntry> columnFamiliesMap = multigetSliceInternal(keyspace, keys, columnParent, predicate, consistencyLevel);
        
        for (CoscsMapEntry cf : columnFamiliesMap)
        {
            KeyCountMapEntry countEntry = new KeyCountMapEntry();
            countEntry.key = cf.key;
            countEntry.count = cf.columns.size();
            counts.add(countEntry);
        }
        
        return counts;
    }

    public List<TokenRange> describe_ring(CharSequence keyspace) throws AvroRemoteException, InvalidRequestException
    {
        if (keyspace == null || keyspace.toString().equals(Table.SYSTEM_TABLE))
            throw newInvalidRequestException("There is no ring for the keyspace: " + keyspace);
        List<TokenRange> ranges = new ArrayList<TokenRange>();
        Token.TokenFactory<?> tf = StorageService.getPartitioner().getTokenFactory();
        for (Map.Entry<Range, List<String>> entry : StorageService.instance.getRangeToEndpointMap(keyspace.toString()).entrySet())
        {
            Range range = entry.getKey();
            List<String> endpoints = entry.getValue();
            ranges.add(newTokenRange(tf.toString(range.left), tf.toString(range.right), endpoints));
        }
        return ranges;
    }

    public Void truncate(CharSequence columnFamily) throws AvroRemoteException, InvalidRequestException, UnavailableException
    {
        if (logger.isDebugEnabled())
            logger.debug("truncating {} in {}", columnFamily, clientState.getKeyspace());

        try
        {
            clientState.hasKeyspaceAccess(Permission.WRITE_VALUE);
            schedule();
            StorageProxy.truncateBlocking(clientState.getKeyspace(), columnFamily.toString());
        }
        catch (org.apache.cassandra.thrift.InvalidRequestException e)
        {
            throw newInvalidRequestException(e);
        }
        catch (org.apache.cassandra.thrift.UnavailableException e)
        {
            throw newUnavailableException(e);
        }
        catch (TimeoutException e)
        {
            throw newUnavailableException(e);
        }
        catch (IOException e)
        {
            throw newUnavailableException(e);
        }
        finally
        {
            release();
        }
        return null;
    }

    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate slice_predicate, KeyRange range, ConsistencyLevel consistency_level)
    throws InvalidRequestException, TimedOutException
    {
        String keyspace = clientState.getKeyspace();
        try
        {
            clientState.hasKeyspaceAccess(Permission.READ_VALUE);
        }
        catch (org.apache.cassandra.thrift.InvalidRequestException thriftE)
        {
            throw newInvalidRequestException(thriftE);
        }

        AvroValidation.validateColumnParent(keyspace, column_parent);
        AvroValidation.validatePredicate(keyspace, column_parent, slice_predicate);
        AvroValidation.validateKeyRange(range);

        List<Row> rows;
        try
        {
            IPartitioner p = StorageService.getPartitioner();
            AbstractBounds bounds;
            if (range.start_key == null)
            {
                Token.TokenFactory tokenFactory = p.getTokenFactory();
                Token left = tokenFactory.fromString(range.start_token.toString());
                Token right = tokenFactory.fromString(range.end_token.toString());
                bounds = new Range(left, right);
            }
            else
            {
                bounds = new Bounds(p.getToken(range.start_key.array()), p.getToken(range.end_key.array()));
            }
            try
            {
                schedule();
                rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace,
                                                                        thriftColumnParent(column_parent),
                                                                        thriftSlicePredicate(slice_predicate),
                                                                        bounds,
                                                                        range.count),
                                                  thriftConsistencyLevel(consistency_level));
            }
            catch (org.apache.cassandra.thrift.UnavailableException thriftE)
            {
                throw newUnavailableException(thriftE);
            }
            finally
            {
                release();
            }
            assert rows != null;
        }
        catch (TimeoutException e)
        {
        	throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return avronateKeySlices(rows, column_parent, slice_predicate);
    }

    public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("scan");

        try
        {
            clientState.hasKeyspaceAccess(Permission.READ_VALUE);
        }
        catch (org.apache.cassandra.thrift.InvalidRequestException thriftE)
        {
            throw newInvalidRequestException(thriftE);
        }

        String keyspace = clientState.getKeyspace();
        AvroValidation.validateColumnParent(keyspace, column_parent);
        AvroValidation.validatePredicate(keyspace, column_parent, column_predicate);
        AvroValidation.validateIndexClauses(keyspace, column_parent.column_family.toString(), index_clause);

        List<Row> rows;
        try
        {
            rows = StorageProxy.scan(keyspace.toString(),
                                     column_parent.column_family.toString(),
                                     thriftIndexClause(index_clause),
                                     thriftSlicePredicate(column_predicate),
                                     thriftConsistencyLevel(consistency_level));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
        return avronateKeySlices(rows, column_parent, column_predicate);
    }

    private List<KeySlice> avronateKeySlices(List<Row> rows, ColumnParent column_parent, SlicePredicate predicate)
    {
        List<KeySlice> keySlices = new ArrayList<KeySlice>(rows.size());
        boolean reversed = predicate.slice_range != null && predicate.slice_range.reversed;
        for (Row row : rows)
        {
            List<ColumnOrSuperColumn> avronatedColumns = avronateColumnFamily(row.cf, column_parent.super_column != null, reversed);
            keySlices.add(newKeySlice(row.key.key, avronatedColumns));
        }

        return keySlices;
    }

    private org.apache.cassandra.thrift.ColumnParent thriftColumnParent(ColumnParent avro_column_parent)
    {
        org.apache.cassandra.thrift.ColumnParent cp = new org.apache.cassandra.thrift.ColumnParent(avro_column_parent.column_family.toString());
        if (avro_column_parent.super_column != null)
            cp.super_column = avro_column_parent.super_column.array();

        return cp;
    }

    private org.apache.cassandra.thrift.SlicePredicate thriftSlicePredicate(SlicePredicate avro_pred) {
        // One or the other are set, so check for nulls of either

        List<byte[]> bufs = null;
        if (avro_pred.column_names != null)
        {
            bufs = new ArrayList<byte[]>();
            for(ByteBuffer buf : avro_pred.column_names)
                bufs.add(buf.array());
        }

        org.apache.cassandra.thrift.SliceRange slice_range = (avro_pred.slice_range != null)
                                                                ? thriftSliceRange(avro_pred.slice_range)
                                                                : null;

        return new org.apache.cassandra.thrift.SlicePredicate().setColumn_names(bufs).setSlice_range(slice_range);
    }

    private org.apache.cassandra.thrift.SliceRange thriftSliceRange(SliceRange avro_range) {
        return new org.apache.cassandra.thrift.SliceRange(avro_range.start.array(), avro_range.finish.array(), avro_range.reversed, avro_range.count);
    }

    private org.apache.cassandra.thrift.IndexClause thriftIndexClause(IndexClause avro_clause) {
        List<org.apache.cassandra.thrift.IndexExpression> expressions = new ArrayList<org.apache.cassandra.thrift.IndexExpression>();
        for(IndexExpression exp : avro_clause.expressions)
            expressions.add(thriftIndexExpression(exp));

        return new org.apache.cassandra.thrift.IndexClause(expressions, avro_clause.start_key.array(), avro_clause.count);
    }

    private org.apache.cassandra.thrift.IndexExpression thriftIndexExpression(IndexExpression avro_exp) {
        return new org.apache.cassandra.thrift.IndexExpression(avro_exp.column_name.array(), thriftIndexOperator(avro_exp.op), avro_exp.value.array());
    }

    private org.apache.cassandra.thrift.IndexOperator thriftIndexOperator(IndexOperator avro_op) {
        switch (avro_op)
        {
            case EQ: return org.apache.cassandra.thrift.IndexOperator.EQ;
            case GTE: return org.apache.cassandra.thrift.IndexOperator.GTE;
            case GT: return org.apache.cassandra.thrift.IndexOperator.GT;
            case LTE: return org.apache.cassandra.thrift.IndexOperator.LTE;
            case LT: return org.apache.cassandra.thrift.IndexOperator.LT;
        }
        return null;
    }
}
