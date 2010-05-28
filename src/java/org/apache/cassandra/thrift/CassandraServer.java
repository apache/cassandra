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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
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
import org.apache.cassandra.utils.Pair;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;

import static org.apache.cassandra.thrift.ThriftGlue.createColumnOrSuperColumn_Column;
import static org.apache.cassandra.thrift.ThriftGlue.createColumnOrSuperColumn_SuperColumn;

public class CassandraServer implements Cassandra.Iface
{
    public static String TOKEN_MAP = "token map";
    private static Logger logger = Logger.getLogger(CassandraServer.class);

    private final static List<ColumnOrSuperColumn> EMPTY_COLUMNS = Collections.emptyList();
    private final static List<Column> EMPTY_SUBCOLUMNS = Collections.emptyList();

    // will be set only by login()
    private ThreadLocal<Boolean> loginDone = new ThreadLocal<Boolean>() 
    {
        @Override
        protected Boolean initialValue()
        {
            return false;
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
    
    protected Map<String, ColumnFamily> readColumnFamily(List<ReadCommand> commands, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        // TODO - Support multiple column families per row, right now row only contains 1 column family
        Map<String, ColumnFamily> columnFamilyKeyMap = new HashMap<String,ColumnFamily>();

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
            Column thrift_column = new Column(column.name(), column.value(), column.timestamp());
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
            thriftColumns.add(createColumnOrSuperColumn_Column(thrift_column));
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
            thriftSuperColumns.add(createColumnOrSuperColumn_SuperColumn(superColumn));
        }

        if (reverseOrder)
            Collections.reverse(thriftSuperColumns);

        return thriftSuperColumns;
    }

    private Map<String, List<ColumnOrSuperColumn>> getSlice(List<ReadCommand> commands, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        Map<String, ColumnFamily> columnFamilies = readColumnFamily(commands, consistency_level);
        Map<String, List<ColumnOrSuperColumn>> columnFamiliesMap = new HashMap<String, List<ColumnOrSuperColumn>>();
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

    public List<ColumnOrSuperColumn> get_slice(String keyspace, String key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_slice");

        checkLoginDone();

        return multigetSliceInternal(keyspace, Arrays.asList(key), column_parent, predicate, consistency_level).get(key);
    }
    
    public Map<String, List<ColumnOrSuperColumn>> multiget_slice(String keyspace, List<String> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("multiget_slice");

        checkLoginDone();

        return multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level);
    }

    private Map<String, List<ColumnOrSuperColumn>> multigetSliceInternal(String keyspace, List<String> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        ThriftValidation.validateColumnParent(keyspace, column_parent);
        ThriftValidation.validatePredicate(keyspace, column_parent, predicate);

        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        if (predicate.column_names != null)
        {
            for (String key: keys)
            {
                ThriftValidation.validateKey(key);
                commands.add(new SliceByNamesReadCommand(keyspace, key, column_parent, predicate.column_names));
            }
        }
        else
        {
            SliceRange range = predicate.slice_range;
            for (String key: keys)
            {
                ThriftValidation.validateKey(key);
                commands.add(new SliceFromReadCommand(keyspace, key, column_parent, range.start, range.finish, range.reversed, range.count));
            }
        }

        return getSlice(commands, consistency_level);
    }

    public ColumnOrSuperColumn get(String table, String key, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get");

        checkLoginDone();

        ColumnOrSuperColumn column = multigetInternal(table, Arrays.asList(key), column_path, consistency_level).get(key);
        if (!column.isSetColumn() && !column.isSetSuper_column())
        {
            throw new NotFoundException();
        }
        return column;
    }

    /** always returns a ColumnOrSuperColumn for each key, even if there is no data for it */
    public Map<String, ColumnOrSuperColumn> multiget(String table, List<String> keys, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("multiget");

        checkLoginDone();

        return multigetInternal(table, keys, column_path, consistency_level);
    }

    private Map<String, ColumnOrSuperColumn> multigetInternal(String table, List<String> keys, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        ThriftValidation.validateColumnPath(table, column_path);

        QueryPath path = new QueryPath(column_path.column_family, column_path.column == null ? null : column_path.super_column);
        List<byte[]> nameAsList = Arrays.asList(column_path.column == null ? column_path.super_column : column_path.column);
        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        for (String key: keys)
        {
            ThriftValidation.validateKey(key);
            commands.add(new SliceByNamesReadCommand(table, key, path, nameAsList));
        }

        Map<String, ColumnOrSuperColumn> columnFamiliesMap = new HashMap<String, ColumnOrSuperColumn>();
        Map<String, ColumnFamily> cfamilies = readColumnFamily(commands, consistency_level);

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

    public int get_count(String table, String key, ColumnParent column_parent, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_count");

        checkLoginDone();

        SliceRange range = new SliceRange(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, Integer.MAX_VALUE);
        SlicePredicate predicate = new SlicePredicate().setSlice_range(range);
        return get_slice(table, key, column_parent, predicate, consistency_level).size();
    }

    public void insert(String table, String key, ColumnPath column_path, byte[] value, long timestamp, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("insert");

        checkLoginDone();

        ThriftValidation.validateKey(key);
        ThriftValidation.validateColumnPath(table, column_path);

        RowMutation rm = new RowMutation(table, key);
        try
        {
            rm.add(new QueryPath(column_path), value, timestamp);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
        doInsert(consistency_level, rm);
    }

    public void batch_insert(String keyspace, String key, Map<String, List<ColumnOrSuperColumn>> cfmap, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("batch_insert");

        checkLoginDone();

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

    public void batch_mutate(String keyspace, Map<String,Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("batch_mutate");

        checkLoginDone();

        List<RowMutation> rowMutations = new ArrayList<RowMutation>();
        for (Map.Entry<String, Map<String, List<Mutation>>> mutationEntry: mutation_map.entrySet())
        {
            String key = mutationEntry.getKey();

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

    public void remove(String table, String key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("remove");

        checkLoginDone();

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

    public String get_string_property(String propertyName)
    {
        if (propertyName.equals("cluster name"))
        {
            return DatabaseDescriptor.getClusterName();
        }
        else if (propertyName.equals("config file"))
        {
            String filename = DatabaseDescriptor.getConfigFileName();
            try
            {
                StringBuilder fileData = new StringBuilder(8192);
                BufferedInputStream stream = new BufferedInputStream(new FileInputStream(filename));
                byte[] buf = new byte[1024];
                int numRead;
                while( (numRead = stream.read(buf)) != -1)
                {
                    String str = new String(buf, 0, numRead);
                    fileData.append(str);
                }
                stream.close();
                return fileData.toString();
            }
            catch (IOException e)
            {
                return "file not found!";
            }
        }
        else if (propertyName.equals(TOKEN_MAP))
        {
            return JSONValue.toJSONString(storageService.getStringEndpointMap());
        }
        else if (propertyName.equals("version"))
        {
            return Constants.VERSION;
        }
        else
        {
            return "?";
        }
    }

    public List<String> get_string_list_property(String propertyName)
    {
        if (propertyName.equals("keyspaces"))
        {
            return new ArrayList<String>(DatabaseDescriptor.getTables());        
        }
        return Collections.emptyList();
    }

    public Map<String, Map<String, String>> describe_keyspace(String table) throws NotFoundException
    {
        Map<String, Map<String, String>> columnFamiliesMap = new HashMap<String, Map<String, String>>();

        Map<String, CFMetaData> tableMetaData = DatabaseDescriptor.getTableMetaData(table);
        // table doesn't exist
        if (tableMetaData == null)
        {
            throw new NotFoundException();
        }

        for (Map.Entry<String, CFMetaData> stringCFMetaDataEntry : tableMetaData.entrySet())
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

    public List<KeySlice> get_range_slice(String keyspace, ColumnParent column_parent, SlicePredicate predicate, String start_key, String finish_key, int maxRows, ConsistencyLevel consistency_level)
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
        checkLoginDone();

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
            keySlices.add(new KeySlice(row.key, thriftifiedColumns));
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

    public List<TokenRange> describe_ring(String keyspace)throws InvalidRequestException
    {
        if (!DatabaseDescriptor.getNonSystemTables().contains(keyspace))
            throw new InvalidRequestException("There is no ring for the keyspace: " + keyspace);
        List<TokenRange> ranges = new ArrayList<TokenRange>();
        Token.TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
        for (Map.Entry<Range, List<String>> entry : StorageService.instance.getRangeToEndPointMap(keyspace).entrySet())
        {
            Range range = entry.getKey();
            List<String> endpoints = entry.getValue();
            ranges.add(new TokenRange(tf.toString(range.left), tf.toString(range.right), endpoints));
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
            splits.add(tf.toString(token));
        }
        return splits;
    }

    public void login(String keyspace, AuthenticationRequest auth_request) throws AuthenticationException, AuthorizationException, TException
    {
        DatabaseDescriptor.getAuthenticator().login(keyspace, auth_request);
        loginDone.set(true);
    }

    protected void checkLoginDone() throws InvalidRequestException
    {
        // FIXME: This disables the "you must call login()" requirement when the configured
        // authenticator is AllowAllAuthenticator. This is a temporary measure until CASSANDRA-714 is complete.
        if (DatabaseDescriptor.getAuthenticator() instanceof AllowAllAuthenticator)
            return;
        if (!loginDone.get()) throw new InvalidRequestException("Login is required before any other API calls");
    }

    
    // main method moved to CassandraDaemon
}
