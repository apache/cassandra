/**
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
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.migration.AddColumnFamily;
import org.apache.cassandra.db.migration.AddKeyspace;
import org.apache.cassandra.db.migration.DropColumnFamily;
import org.apache.cassandra.db.migration.DropKeyspace;
import org.apache.cassandra.db.migration.RenameColumnFamily;
import org.apache.cassandra.db.migration.RenameKeyspace;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndPointSnitch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import static org.apache.cassandra.utils.FBUtilities.UTF8;
import org.apache.thrift.TException;

public class CassandraServer implements Cassandra.Iface
{
    public static String TOKEN_MAP = "token map";
    private static Logger logger = LoggerFactory.getLogger(CassandraServer.class);

    private final static List<ColumnOrSuperColumn> EMPTY_COLUMNS = Collections.emptyList();
    private final static List<Column> EMPTY_SUBCOLUMNS = Collections.emptyList();

    // will be set only by login()
    private ThreadLocal<AccessLevel> loginDone = new ThreadLocal<AccessLevel>() 
    {
        @Override
        protected AccessLevel initialValue()
        {
            return AccessLevel.NONE;
        }
    };

    /*
      * Handle to the storage service to interact with the other machines in the
      * cluster.
      */
	private final StorageService storageService;

    public CassandraServer()
    {
        storageService = StorageService.instance;
    }
    
    protected Map<byte[], ColumnFamily> readColumnFamily(List<ReadCommand> commands, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        // TODO - Support multiple column families per row, right now row only contains 1 column family
        Map<byte[], ColumnFamily> columnFamilyKeyMap = new HashMap<byte[],ColumnFamily>();

        if (consistency_level == ConsistencyLevel.ZERO)
        {
            throw new InvalidRequestException("Consistency level zero may not be applied to read operations");
        }
        if (consistency_level == ConsistencyLevel.ALL)
        {
            throw new InvalidRequestException("Consistency level all is not yet supported on read operations");
        }
        if (consistency_level == ConsistencyLevel.ANY)
        {
            throw new InvalidRequestException("Consistency level any may not be applied to read operations");
        }

        List<Row> rows;
        try
        {
            rows = StorageProxy.readProtocol(commands, consistency_level);
        }
        catch (TimeoutException e) 
        {
        	throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        for (Row row: rows)
        {
            columnFamilyKeyMap.put(row.key.key, row.cf);
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
            Column thrift_column = new Column(column.name(), column.value(), column.timestamp());
            if (column instanceof ExpiringColumn)
            {
                thrift_column.setTtl(((ExpiringColumn) column).getTimeToLive());
            }
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
            Column thrift_column = new Column(column.name(), column.value(), column.timestamp());
            if (column instanceof ExpiringColumn)
            {
                thrift_column.setTtl(((ExpiringColumn) column).getTimeToLive());
            }
            thriftColumns.add(new ColumnOrSuperColumn().setColumn(thrift_column));
        }

        // we have to do the reversing here, since internally we pass results around in ColumnFamily
        // objects, which always sort their columns in the "natural" order
        // TODO this is inconvenient for direct users of StorageProxy
        if (reverseOrder)
            Collections.reverse(thriftColumns);
        return thriftColumns;
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

    private Map<byte[], List<ColumnOrSuperColumn>> getSlice(List<ReadCommand> commands, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        Map<byte[], ColumnFamily> columnFamilies = readColumnFamily(commands, consistency_level);
        Map<byte[], List<ColumnOrSuperColumn>> columnFamiliesMap = new HashMap<byte[], List<ColumnOrSuperColumn>>();
        for (ReadCommand command: commands)
        {
            ColumnFamily cf = columnFamilies.get(command.key);
            boolean reverseOrder = command instanceof SliceFromReadCommand && ((SliceFromReadCommand)command).reversed;
            List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyColumnFamily(cf, command.queryPath.superColumnName != null, reverseOrder);
            columnFamiliesMap.put(command.key, thriftifiedColumns);
        }

        return columnFamiliesMap;
    }

    private List<ColumnOrSuperColumn> thriftifyColumnFamily(ColumnFamily cf, boolean subcolumnsOnly, boolean reverseOrder)
    {
        if (cf == null || cf.getColumnsMap().size() == 0)
            return EMPTY_COLUMNS;
        if (subcolumnsOnly)
        {
            IColumn column = cf.getColumnsMap().values().iterator().next();
            Collection<IColumn> subcolumns = column.getSubColumns();
            if (subcolumns == null || subcolumns.isEmpty())
                return EMPTY_COLUMNS;
            else
                return thriftifyColumns(subcolumns, reverseOrder);
        }
        if (cf.isSuper())
            return thriftifySuperColumns(cf.getSortedColumns(), reverseOrder);        
        else
            return thriftifyColumns(cf.getSortedColumns(), reverseOrder);
    }

    public List<ColumnOrSuperColumn> get_slice(String keyspace, byte[] key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_slice");
        
        checkLoginAuthorized(AccessLevel.READONLY);
        return multigetSliceInternal(keyspace, Arrays.asList(key), column_parent, predicate, consistency_level).get(key);
    }
    
    public Map<byte[], List<ColumnOrSuperColumn>> multiget_slice(String keyspace, List<byte[]> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("multiget_slice");

        checkLoginAuthorized(AccessLevel.READONLY);

        return multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level);
    }

    private Map<byte[], List<ColumnOrSuperColumn>> multigetSliceInternal(String keyspace, List<byte[]> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        ThriftValidation.validateColumnParent(keyspace, column_parent);
        ThriftValidation.validatePredicate(keyspace, column_parent, predicate);

        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        if (predicate.column_names != null)
        {
            for (byte[] key: keys)
            {
                ThriftValidation.validateKey(key);
                commands.add(new SliceByNamesReadCommand(keyspace, key, column_parent, predicate.column_names));
            }
        }
        else
        {
            SliceRange range = predicate.slice_range;
            for (byte[] key: keys)
            {
                ThriftValidation.validateKey(key);
                commands.add(new SliceFromReadCommand(keyspace, key, column_parent, range.start, range.finish, range.reversed, range.count));
            }
        }

        return getSlice(commands, consistency_level);
    }

    public ColumnOrSuperColumn get(String table, byte[] key, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get");

        checkLoginAuthorized(AccessLevel.READONLY);

        ColumnOrSuperColumn column = multigetInternal(table, Arrays.asList(key), column_path, consistency_level).get(key);
        if (!column.isSetColumn() && !column.isSetSuper_column())
        {
            throw new NotFoundException();
        }
        return column;
    }

    /** always returns a ColumnOrSuperColumn for each key, even if there is no data for it */
    public Map<byte[], ColumnOrSuperColumn> multiget(String table, List<byte[]> keys, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("multiget");

        checkLoginAuthorized(AccessLevel.READONLY);

        return multigetInternal(table, keys, column_path, consistency_level);
    }

    private Map<byte[], ColumnOrSuperColumn> multigetInternal(String table, List<byte[]> keys, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        ThriftValidation.validateColumnPath(table, column_path);

        QueryPath path = new QueryPath(column_path.column_family, column_path.column == null ? null : column_path.super_column);
        List<byte[]> nameAsList = Arrays.asList(column_path.column == null ? column_path.super_column : column_path.column);
        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        for (byte[] key: keys)
        {
            ThriftValidation.validateKey(key);
            commands.add(new SliceByNamesReadCommand(table, key, path, nameAsList));
        }

        Map<byte[], ColumnOrSuperColumn> columnFamiliesMap = new HashMap<byte[], ColumnOrSuperColumn>();
        Map<byte[], ColumnFamily> cfamilies = readColumnFamily(commands, consistency_level);


        for (ReadCommand command: commands)
        {
            ColumnFamily cf = cfamilies.get(command.key);
            if (cf == null)
            {
                columnFamiliesMap.put(command.key, new ColumnOrSuperColumn());
            }
            else
            {
                List<ColumnOrSuperColumn> tcolumns = thriftifyColumnFamily(cf, command.queryPath.superColumnName != null, false);
                columnFamiliesMap.put(command.key, tcolumns.size() > 0 ? tcolumns.iterator().next() : new ColumnOrSuperColumn());
            }
        }

        return columnFamiliesMap;
    }

    public int get_count(String table, byte[] key, ColumnParent column_parent, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_count");

        checkLoginAuthorized(AccessLevel.READONLY);

        SliceRange range = new SliceRange(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, Integer.MAX_VALUE);
        SlicePredicate predicate = new SlicePredicate().setSlice_range(range);
        return get_slice(table, key, column_parent, predicate, consistency_level).size();
    }

    public void insert(String table, byte[] key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("insert");

        checkLoginAuthorized(AccessLevel.READWRITE);

        ThriftValidation.validateKey(key);
        ThriftValidation.validateColumnParent(table, column_parent);
        ThriftValidation.validateColumn(table, column_parent, column.name);

        RowMutation rm = new RowMutation(table, key);
        try
        {
            rm.add(new QueryPath(column_parent.column_family, column_parent.super_column, column.name), column.value, column.timestamp, Math.max(column.ttl, 0));
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
        doInsert(consistency_level, rm);
    }
    
    public void batch_insert(String keyspace, byte[] key, Map<String, List<ColumnOrSuperColumn>> cfmap, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("batch_insert");

        checkLoginAuthorized(AccessLevel.READWRITE);

        ThriftValidation.validateKey(key);

        for (String cfName : cfmap.keySet())
        {
            for (ColumnOrSuperColumn cosc : cfmap.get(cfName))
            {
                ThriftValidation.validateColumnOrSuperColumn(keyspace, cfName, cosc);
            }
        }

        doInsert(consistency_level, RowMutation.getRowMutation(keyspace, key, cfmap));
    }

    public void batch_mutate(String keyspace, Map<byte[],Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("batch_mutate");

        AccessLevel needed = AccessLevel.READWRITE;

        TOP:
        for (Map<String, List<Mutation>> submap : mutation_map.values())
        {
            for (List<Mutation> mutations: submap.values())
            {
                for (Mutation m : mutations)
                {
                    if (m.isSetDeletion())
                    {
                        needed = AccessLevel.FULL;
                        break TOP;
                    }
                }
            }
        }
        
        checkLoginAuthorized(needed);

        List<RowMutation> rowMutations = new ArrayList<RowMutation>();
        for (Map.Entry<byte[], Map<String, List<Mutation>>> mutationEntry: mutation_map.entrySet())
        {
            byte[] key = mutationEntry.getKey();

            ThriftValidation.validateKey(key);
            Map<String, List<Mutation>> columnFamilyToMutations = mutationEntry.getValue();
            for (Map.Entry<String, List<Mutation>> columnFamilyMutations : columnFamilyToMutations.entrySet())
            {
                String cfName = columnFamilyMutations.getKey();

                for (Mutation mutation : columnFamilyMutations.getValue())
                {
                    ThriftValidation.validateMutation(keyspace, cfName, mutation);
                }
            }
            rowMutations.add(RowMutation.getRowMutationFromMutations(keyspace, key, columnFamilyToMutations));
        }
        if (consistency_level == ConsistencyLevel.ZERO)
        {
            StorageProxy.mutate(rowMutations);
        }
        else
        {
            try 
            {
            	StorageProxy.mutateBlocking(rowMutations, consistency_level);
            } 
            catch (TimeoutException e) 
            {
            	throw new TimedOutException();
            }
        }
    }

    public void remove(String table, byte[] key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("remove");

        checkLoginAuthorized(AccessLevel.FULL);

        ThriftValidation.validateKey(key);
        ThriftValidation.validateColumnPathOrParent(table, column_path);
        
        RowMutation rm = new RowMutation(table, key);
        rm.delete(new QueryPath(column_path), timestamp);

        doInsert(consistency_level, rm);
    }

    private void doInsert(ConsistencyLevel consistency_level, RowMutation rm) throws UnavailableException, TimedOutException
    {
        if (consistency_level != ConsistencyLevel.ZERO)
        {
            try 
            {
            	StorageProxy.mutateBlocking(Arrays.asList(rm), consistency_level);
            }
            catch (TimeoutException e)
            {
            	throw new TimedOutException();
            }
        }
        else
        {
            StorageProxy.mutate(Arrays.asList(rm));
        }
    }

    public Map<String, Map<String, String>> describe_keyspace(String table) throws NotFoundException
    {
        Map<String, Map<String, String>> columnFamiliesMap = new HashMap<String, Map<String, String>>();

        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(table); 
        if (ksm == null)
            throw new NotFoundException();
        

        for (Map.Entry<String, CFMetaData> stringCFMetaDataEntry : ksm.cfMetaData().entrySet())
        {
            CFMetaData columnFamilyMetaData = stringCFMetaDataEntry.getValue();

            Map<String, String> columnMap = new HashMap<String, String>();
            columnMap.put("Type", columnFamilyMetaData.columnType);
            columnMap.put("Desc", columnFamilyMetaData.comment == null ? columnFamilyMetaData.pretty() : columnFamilyMetaData.comment);
            columnMap.put("CompareWith", columnFamilyMetaData.comparator.getClass().getName());
            if (columnFamilyMetaData.columnType.equals("Super"))
            {
                columnMap.put("CompareSubcolumnsWith", columnFamilyMetaData.subcolumnComparator.getClass().getName());
            }
            columnFamiliesMap.put(columnFamilyMetaData.cfName, columnMap);
        }
        return columnFamiliesMap;
    }

    public List<KeySlice> get_range_slice(String keyspace, ColumnParent column_parent, SlicePredicate predicate, byte[] start_key, byte[] finish_key, int maxRows, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_range_slice " + start_key + " to " + finish_key);

        KeyRange range = new KeyRange().setStart_key(start_key).setEnd_key(finish_key).setCount(maxRows);
        return getRangeSlicesInternal(keyspace, column_parent, predicate, range, consistency_level);
    }

    public List<KeySlice> get_range_slices(String keyspace, ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("range_slice");

        return getRangeSlicesInternal(keyspace, column_parent, predicate, range, consistency_level);
    }

    private List<KeySlice> getRangeSlicesInternal(String keyspace, ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException
    {
        checkLoginAuthorized(AccessLevel.READONLY);

        ThriftValidation.validateColumnParent(keyspace, column_parent);
        ThriftValidation.validatePredicate(keyspace, column_parent, predicate);
        ThriftValidation.validateKeyRange(range);

        List<Row> rows;
        try
        {
            IPartitioner p = StorageService.getPartitioner();
            AbstractBounds bounds;
            if (range.start_key == null)
            {
                Token.TokenFactory tokenFactory = p.getTokenFactory();
                Token left = tokenFactory.fromString(range.start_token);
                Token right = tokenFactory.fromString(range.end_token);
                bounds = new Range(left, right);
            }
            else
            {
                bounds = new Bounds(p.getToken(range.start_key), p.getToken(range.end_key));
            }
            rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace, column_parent, predicate, bounds, range.count), consistency_level);
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

        List<KeySlice> keySlices = new ArrayList<KeySlice>(rows.size());
        boolean reversed = predicate.slice_range != null && predicate.slice_range.reversed;
        for (Row row : rows)
        {
            List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyColumnFamily(row.cf, column_parent.super_column != null, reversed);
            keySlices.add(new KeySlice(row.key.key, thriftifiedColumns));
        }

        return keySlices;
    }

    public Set<String> describe_keyspaces() throws TException
    {
        return DatabaseDescriptor.getTables();
    }

    public String describe_cluster_name() throws TException
    {
        return DatabaseDescriptor.getClusterName();
    }

    public String describe_version() throws TException
    {
        return Constants.VERSION;
    }

    public List<TokenRange> describe_ring(String keyspace)
    {
        List<TokenRange> ranges = new ArrayList<TokenRange>();
        for (Map.Entry<Range, List<String>> entry : StorageService.instance.getRangeToEndPointMap(keyspace).entrySet())
        {
            Range range = entry.getKey();
            List<String> endpoints = entry.getValue();
            ranges.add(new TokenRange(range.left.toString(), range.right.toString(), endpoints));
        }
        return ranges;
    }

    public List<String> describe_splits(String start_token, String end_token, int keys_per_split) throws TException
    {
        Token.TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
        List<Token> tokens = StorageService.instance.getSplits(new Range(tf.fromString(start_token), tf.fromString(end_token)), keys_per_split);
        List<String> splits = new ArrayList<String>(tokens.size());
        for (Token token : tokens)
        {
            splits.add(token.toString());
        }
        return splits;
    }

    public AccessLevel login(String keyspace, AuthenticationRequest auth_request) throws AuthenticationException, AuthorizationException, TException
    {
        AccessLevel level = DatabaseDescriptor.getAuthenticator().login(keyspace, auth_request);
        loginDone.set(level);
        return level;
    }

    protected void checkLoginAuthorized(AccessLevel level) throws InvalidRequestException
    {
        // FIXME: This disables access level checks when the configured
        // authenticator is AllowAllAuthenticator. This is a temporary measure until CASSANDRA-714 is complete.
        if (DatabaseDescriptor.getAuthenticator() instanceof AllowAllAuthenticator)
            return;
        if (loginDone.get() == AccessLevel.NONE) throw new InvalidRequestException("Your login access level was not sufficient to do " + level + " operations");
        if (loginDone.get().getValue() >= level.getValue()) throw new InvalidRequestException("Your login access level was not sufficient to do " + level + " operations");
    }

    public void system_add_column_family(CfDef cf_def) throws InvalidRequestException, TException
    {
        checkLoginAuthorized(AccessLevel.FULL);

        // if there is anything going on in the migration stage, fail.
        if (StageManager.getStage(StageManager.MIGRATION_STAGE).getQueue().size() > 0)
            throw new InvalidRequestException("This node appears to be handling gossiped migrations.");
        
        try
        {
            CFMetaData cfm = new CFMetaData(
                        cf_def.table,
                        cf_def.name,
                        ColumnFamily.getColumnType(cf_def.column_type),
                        DatabaseDescriptor.getComparator(cf_def.comparator_type),
                        cf_def.subcomparator_type.length() == 0 ? null : DatabaseDescriptor.getComparator(cf_def.subcomparator_type),
                        cf_def.comment,
                        cf_def.row_cache_size,
                        cf_def.key_cache_size);
            AddColumnFamily add = new AddColumnFamily(cfm);
            add.apply();
            add.announce();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public void system_drop_column_family(String keyspace, String column_family) throws InvalidRequestException, TException
    {
        checkLoginAuthorized(AccessLevel.FULL);
        
        // if there is anything going on in the migration stage, fail.
        if (StageManager.getStage(StageManager.MIGRATION_STAGE).getQueue().size() > 0)
            throw new InvalidRequestException("This node appears to be handling gossiped migrations.");

        try
        {
            DropColumnFamily drop = new DropColumnFamily(keyspace, column_family, true);
            drop.apply();
            drop.announce();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public void system_rename_column_family(String keyspace, String old_name, String new_name) throws InvalidRequestException, TException
    {
        checkLoginAuthorized(AccessLevel.FULL);
        
        // if there is anything going on in the migration stage, fail.
        if (StageManager.getStage(StageManager.MIGRATION_STAGE).getQueue().size() > 0)
            throw new InvalidRequestException("This node appears to be handling gossiped migrations.");

        try
        {
            RenameColumnFamily rename = new RenameColumnFamily(keyspace, old_name, new_name);
            rename.apply();
            rename.announce();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public void system_add_keyspace(KsDef ks_def) throws InvalidRequestException, TException
    {
        checkLoginAuthorized(AccessLevel.FULL);
        
        // if there is anything going on in the migration stage, fail.
        if (StageManager.getStage(StageManager.MIGRATION_STAGE).getQueue().size() > 0)
            throw new InvalidRequestException("This node appears to be handling gossiped migrations.");

        try
        {
            Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(ks_def.cf_defs.size());
            for (CfDef cfDef : ks_def.cf_defs)
            {
                CFMetaData cfm = new CFMetaData(
                        cfDef.table,
                        cfDef.name,
                        ColumnFamily.getColumnType(cfDef.column_type),
                        DatabaseDescriptor.getComparator(cfDef.comparator_type),
                        cfDef.subcomparator_type.length() == 0 ? null : DatabaseDescriptor.getComparator(cfDef.subcomparator_type),
                        cfDef.comment,
                        cfDef.row_cache_size,
                        cfDef.key_cache_size);
                cfDefs.add(cfm);
            }
            
            KSMetaData ksm = new KSMetaData(
                    ks_def.name, 
                    (Class<? extends AbstractReplicationStrategy>)Class.forName(ks_def.strategy_class), 
                    ks_def.replication_factor, 
                    cfDefs.toArray(new CFMetaData[cfDefs.size()]));
            AddKeyspace add = new AddKeyspace(ksm);
            add.apply();
            add.announce();
        }
        catch (ClassNotFoundException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public void system_drop_keyspace(String keyspace) throws InvalidRequestException, TException
    {
        checkLoginAuthorized(AccessLevel.FULL);
        
        // if there is anything going on in the migration stage, fail.
        if (StageManager.getStage(StageManager.MIGRATION_STAGE).getQueue().size() > 0)
            throw new InvalidRequestException("This node appears to be handling gossiped migrations.");

        try
        {
            DropKeyspace drop = new DropKeyspace(keyspace, true);
            drop.apply();
            drop.announce();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    public void system_rename_keyspace(String old_name, String new_name) throws InvalidRequestException, TException
    {
        checkLoginAuthorized(AccessLevel.FULL);
        
        // if there is anything going on in the migration stage, fail.
        if (StageManager.getStage(StageManager.MIGRATION_STAGE).getQueue().size() > 0)
            throw new InvalidRequestException("This node appears to be handling gossiped migrations.");

        try
        {
            RenameKeyspace rename = new RenameKeyspace(old_name, new_name);
            rename.apply();
            rename.announce();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    // main method moved to CassandraDaemon
}
