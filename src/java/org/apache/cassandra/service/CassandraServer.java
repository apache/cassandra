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

package org.apache.cassandra.service;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.thrift.TException;

import flexjson.JSONSerializer;

public class CassandraServer implements Cassandra.Iface
{
    public static String TOKEN_MAP = "token map";
	private static Logger logger = Logger.getLogger(CassandraServer.class);

    private final static List<ColumnOrSuperColumn> EMPTY_COLUMNS = Collections.emptyList();
    private final static List<Column> EMPTY_SUBCOLUMNS = Collections.emptyList();

    /*
      * Handle to the storage service to interact with the other machines in the
      * cluster.
      */
	protected StorageService storageService;

    public CassandraServer()
	{
		storageService = StorageService.instance();
	}

	/*
	 * The start function initializes the server and start's listening on the
	 * specified port.
	 */
	public void start() throws IOException
    {
		LogUtil.init();
		//LogUtil.setLogLevel("com.facebook", "DEBUG");
		// Start the storage service
		storageService.initServer();
	}

    protected Map<String, ColumnFamily> readColumnFamily(List<ReadCommand> commands, int consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        // TODO - Support multiple column families per row, right now row only contains 1 column family
        String cfName = commands.get(0).getColumnFamilyName();

        Map<String, ColumnFamily> columnFamilyKeyMap = new HashMap<String,ColumnFamily>();

        if (consistency_level == ConsistencyLevel.ZERO)
        {
            throw new InvalidRequestException("Consistency level zero may not be applied to read operations");
        }
        if (consistency_level == ConsistencyLevel.ALL)
        {
            throw new InvalidRequestException("Consistency level all is not yet supported on read operations");
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
            thriftColumns.add(new ColumnOrSuperColumn(thrift_column, null));
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
            thriftSuperColumns.add(new ColumnOrSuperColumn(null, superColumn));
        }

        if (reverseOrder)
            Collections.reverse(thriftSuperColumns);

        return thriftSuperColumns;
    }

    private Map<String, List<ColumnOrSuperColumn>> getSlice(List<ReadCommand> commands, int consistency_level)
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

    public List<ColumnOrSuperColumn> get_slice(String keyspace, String key, ColumnParent column_parent, SlicePredicate predicate, int consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_slice");
        return multigetSliceInternal(keyspace, Arrays.asList(key), column_parent, predicate, consistency_level).get(key);
    }
    
    public Map<String, List<ColumnOrSuperColumn>> multiget_slice(String keyspace, List<String> keys, ColumnParent column_parent, SlicePredicate predicate, int consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("multiget_slice");
        return multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level);
    }

    private Map<String, List<ColumnOrSuperColumn>> multigetSliceInternal(String keyspace, List<String> keys, ColumnParent column_parent, SlicePredicate predicate, int consistency_level)
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

    public ColumnOrSuperColumn get(String table, String key, ColumnPath column_path, int consistency_level)
    throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get");
        ColumnOrSuperColumn column = multigetInternal(table, Arrays.asList(key), column_path, consistency_level).get(key);
        if (!column.isSetColumn() && !column.isSetSuper_column())
        {
            throw new NotFoundException();
        }
        return column;
    }

    /** no values will be mapped to keys with no data */
    private Map<String, Collection<IColumn>> multigetColumns(List<ReadCommand> commands, int consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        Map<String, ColumnFamily> cfamilies = readColumnFamily(commands, consistency_level);
        Map<String, Collection<IColumn>> columnFamiliesMap = new HashMap<String, Collection<IColumn>>();

        for (ReadCommand command: commands)
        {
            ColumnFamily cfamily = cfamilies.get(command.key);
            if (cfamily == null)
                continue;

            Collection<IColumn> columns = null;
            if (command.queryPath.superColumnName != null)
            {
                IColumn column = cfamily.getColumn(command.queryPath.superColumnName);
                if (column != null)
                {
                    columns = column.getSubColumns();
                }
            }
            else
            {
                columns = cfamily.getSortedColumns();
            }

            if (columns != null && columns.size() != 0)
            {
                columnFamiliesMap.put(command.key, columns);
            }
        }
        return columnFamiliesMap;
    }

    /** always returns a ColumnOrSuperColumn for each key, even if there is no data for it */
    public Map<String, ColumnOrSuperColumn> multiget(String table, List<String> keys, ColumnPath column_path, int consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("multiget");
        return multigetInternal(table, keys, column_path, consistency_level);
    }

    private Map<String, ColumnOrSuperColumn> multigetInternal(String table, List<String> keys, ColumnPath column_path, int consistency_level)
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
        Map<String, Collection<IColumn>> columnsMap = multigetColumns(commands, consistency_level);

        for (ReadCommand command: commands)
        {
            ColumnOrSuperColumn columnorsupercolumn;

            Collection<IColumn> columns = columnsMap.get(command.key);
            if (columns == null)
            {
               columnorsupercolumn = new ColumnOrSuperColumn();
            }
            else
            {
                assert columns.size() == 1;
                IColumn column = columns.iterator().next();


                if (column.isMarkedForDelete())
                {
                    columnorsupercolumn = new ColumnOrSuperColumn();
                }
                else
                {
                    columnorsupercolumn = column instanceof org.apache.cassandra.db.Column
                                          ? new ColumnOrSuperColumn(new Column(column.name(), column.value(), column.timestamp()), null)
                                          : new ColumnOrSuperColumn(null, new SuperColumn(column.name(), thriftifySubColumns(column.getSubColumns())));
                }

            }
            columnFamiliesMap.put(command.key, columnorsupercolumn);
        }

        return columnFamiliesMap;
    }

    public int get_count(String table, String key, ColumnParent column_parent, int consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_count");
        return multigetCountInternal(table, Arrays.asList(key), column_parent, consistency_level).get(key);
    }

    private Map<String, Integer> multigetCountInternal(String table, List<String> keys, ColumnParent column_parent, int consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        // validateColumnParent assumes we require simple columns; g_c_c is the only
        // one of the columnParent-taking apis that can also work at the SC level.
        // so we roll a one-off validator here.
        String cfType = ThriftValidation.validateColumnFamily(table, column_parent.column_family);
        if (cfType.equals("Standard") && column_parent.super_column != null)
        {
            throw new InvalidRequestException("columnfamily alone is required for standard CF " + column_parent.column_family);
        }

        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        for (String key: keys)
        {
            ThriftValidation.validateKey(key);
            commands.add(new SliceFromReadCommand(table, key, column_parent, ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, Integer.MAX_VALUE));
        }

        Map<String, Integer> columnFamiliesMap = new HashMap<String, Integer>();
        Map<String, Collection<IColumn>> columnsMap = multigetColumns(commands, consistency_level);

        for (ReadCommand command: commands)
        {
            Collection<IColumn> columns = columnsMap.get(command.key);
            if(columns == null)
            {
               columnFamiliesMap.put(command.key, 0);
            }
            else
            {
               columnFamiliesMap.put(command.key, columns.size());
            }
        }
        return columnFamiliesMap;
    }

    public void insert(String table, String key, ColumnPath column_path, byte[] value, long timestamp, int consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("insert");
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

    public void batch_insert(String keyspace, String key, Map<String, List<ColumnOrSuperColumn>> cfmap, int consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("batch_insert");
        ThriftValidation.validateKey(key);

        for (String cfName : cfmap.keySet())
        {
            for (ColumnOrSuperColumn cosc : cfmap.get(cfName))
            {
                if (cosc.column != null)
                {
                    ThriftValidation.validateColumnPath(keyspace, new ColumnPath(cfName, null, cosc.column.name));
                }
                if (cosc.super_column != null)
                {
                    for (Column c : cosc.super_column.columns)
                    {
                        ThriftValidation.validateColumnPath(keyspace, new ColumnPath(cfName, cosc.super_column.name, c.name));
                    }
                }
            }
        }

        doInsert(consistency_level, RowMutation.getRowMutation(keyspace, key, cfmap));
    }

    public void remove(String table, String key, ColumnPath column_path, long timestamp, int consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("remove");
        ThriftValidation.validateKey(key);
        ThriftValidation.validateColumnPathOrParent(table, column_path);
        
        RowMutation rm = new RowMutation(table, key);
        rm.delete(new QueryPath(column_path), timestamp);

        doInsert(consistency_level, rm);
	}

    private void doInsert(int consistency_level, RowMutation rm) throws UnavailableException, TimedOutException
    {
        if (consistency_level != ConsistencyLevel.ZERO)
        {
            try
            {
                StorageProxy.insertBlocking(rm, consistency_level);
            }
            catch (TimeoutException e)
            {
                throw new TimedOutException();
            }
        }
        else
        {
            StorageProxy.insert(rm);
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
            return new JSONSerializer().serialize(storageService.getStringEndpointMap());
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
            return DatabaseDescriptor.getTables();        
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

    public List<KeySlice> get_range_slice(String keyspace, ColumnParent column_parent, SlicePredicate predicate, String start_key, String finish_key, int maxRows, int consistency_level)
            throws InvalidRequestException, UnavailableException, TException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("range_slice");
        ThriftValidation.validatePredicate(keyspace, column_parent, predicate);
        if (!StorageService.getPartitioner().preservesOrder())
        {
            throw new InvalidRequestException("range queries may only be performed against an order-preserving partitioner");
        }
        if (maxRows <= 0)
        {
            throw new InvalidRequestException("maxRows must be positive");
        }

        List<Pair<String, ColumnFamily>> rows;
        try
        {
            DecoratedKey startKey = StorageService.getPartitioner().decorateKey(start_key);
            DecoratedKey finishKey = StorageService.getPartitioner().decorateKey(finish_key);
            rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace, column_parent, predicate, startKey, finishKey, maxRows), consistency_level);
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
        for (Pair<String, ColumnFamily> row : rows)
        {
            List<ColumnOrSuperColumn> thriftifiedColumns = thriftifyColumnFamily(row.right, column_parent.super_column != null, reversed);
            keySlices.add(new KeySlice(row.left, thriftifiedColumns));
        }

        return keySlices;
    }

    public List<String> get_key_range(String tablename, String columnFamily, String startWith, String stopAt, int maxResults, int consistency_level)
            throws InvalidRequestException, TException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_key_range");
        ThriftValidation.validateCommand(tablename, columnFamily);
        if (!StorageService.getPartitioner().preservesOrder())
        {
            throw new InvalidRequestException("range queries may only be performed against an order-preserving partitioner");
        }
        if (maxResults <= 0)
        {
            throw new InvalidRequestException("maxResults must be positive");
        }

        try
        {
            return StorageProxy.getKeyRange(new RangeCommand(tablename, columnFamily, startWith, stopAt, maxResults));
        }
        catch (TimeoutException e)
        {
        	throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    // main method moved to CassandraDaemon
}
