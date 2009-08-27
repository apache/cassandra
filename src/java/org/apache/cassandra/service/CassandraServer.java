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
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.cassandra.dht.Token;
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
		storageService.start();
	}

    protected Map<String, ColumnFamily> readColumnFamily(List<ReadCommand> commands, int consistency_level) throws InvalidRequestException
    {
        // TODO - Support multiple column families per row, right now row only contains 1 column family
        String cfName = commands.get(0).getColumnFamilyName();

        Map<String, ColumnFamily> columnFamilyKeyMap = new HashMap<String,ColumnFamily>();

        for (ReadCommand command: commands)
        {
            ThriftValidation.validateKey(command.key);
        }

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
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (TimeoutException e)
        {
            throw new RuntimeException(e);
        }

        for (Row row: rows)
        {
            columnFamilyKeyMap.put(row.key(), row.getColumnFamily(cfName));
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

    private Map<String, List<ColumnOrSuperColumn>> getSlice(List<ReadCommand> commands, int consistency_level) throws InvalidRequestException
    {
        Map<String, ColumnFamily> cfamilies = readColumnFamily(commands, consistency_level);
        Map<String, List<ColumnOrSuperColumn>> columnFamiliesMap = new HashMap<String, List<ColumnOrSuperColumn>>();
        for (ReadCommand command: commands)
        {
            ColumnFamily cfamily = cfamilies.get(command.key);
            boolean reverseOrder = command instanceof SliceFromReadCommand && ((SliceFromReadCommand)command).reversed;

            if (cfamily == null || cfamily.getColumnsMap().size() == 0)
            {
                columnFamiliesMap.put(command.key, EMPTY_COLUMNS);
                continue;
            }
            if (command.queryPath.superColumnName != null)
            {
                IColumn column = cfamily.getColumnsMap().values().iterator().next();
                Collection<IColumn> subcolumns = column.getSubColumns();
                if (subcolumns == null || subcolumns.isEmpty())
                {
                    columnFamiliesMap.put(command.key, EMPTY_COLUMNS);
                    continue;
                }
                columnFamiliesMap.put(command.key, thriftifyColumns(subcolumns, reverseOrder));
                continue;
            }
            if (cfamily.isSuper())
                columnFamiliesMap.put(command.key, thriftifySuperColumns(cfamily.getSortedColumns(), reverseOrder));
            else
                columnFamiliesMap.put(command.key, thriftifyColumns(cfamily.getSortedColumns(), reverseOrder));
        }

        return columnFamiliesMap;
    }

    public List<ColumnOrSuperColumn> get_slice(String keyspace, String key, ColumnParent column_parent, SlicePredicate predicate, int consistency_level)
    throws InvalidRequestException, NotFoundException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_slice");
        return multigetSliceInternal(keyspace, Arrays.asList(key), column_parent, predicate, consistency_level).get(key);
    }
    
    public Map<String, List<ColumnOrSuperColumn>> multiget_slice(String keyspace, List<String> keys, ColumnParent column_parent, SlicePredicate predicate, int consistency_level)
    throws InvalidRequestException
    {
        if (logger.isDebugEnabled())
            logger.debug("multiget_slice");
        return multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level);
    }

    private Map<String, List<ColumnOrSuperColumn>> multigetSliceInternal(String keyspace, List<String> keys, ColumnParent column_parent, SlicePredicate predicate, int consistency_level)
    throws InvalidRequestException
    {
        ThriftValidation.validateColumnParent(keyspace, column_parent);
        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        SliceRange range = predicate.slice_range;

        if (predicate.column_names != null)
        {
            for (String key: keys)
                commands.add(new SliceByNamesReadCommand(keyspace, key, column_parent, predicate.column_names));
            ThriftValidation.validateColumns(keyspace, column_parent, predicate.column_names);
        }
        else
        {
            for (String key: keys)
                commands.add(new SliceFromReadCommand(keyspace, key, column_parent, range.start, range.finish, range.reversed, range.count));
            ThriftValidation.validateRange(keyspace, column_parent, range);
        }

        return getSlice(commands, consistency_level);
    }

    public ColumnOrSuperColumn get(String table, String key, ColumnPath column_path, int consistency_level)
    throws InvalidRequestException, NotFoundException
    {
        if (logger.isDebugEnabled())
            logger.debug("get");
        ColumnOrSuperColumn column = multiget(table, Arrays.asList(key), column_path, consistency_level).get(key);
        if (!column.isSetColumn() && !column.isSetSuper_column())
        {
            throw new NotFoundException();
        }
        return column;
    }

    /** no values will be mapped to keys with no data */
    private Map<String, Collection<IColumn>> multigetColumns(List<ReadCommand> commands, int consistency_level)
    throws InvalidRequestException
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
    throws InvalidRequestException
    {
        if (logger.isDebugEnabled())
            logger.debug("multiget");
        return multigetInternal(table, keys, column_path, consistency_level);
    }

    private Map<String, ColumnOrSuperColumn> multigetInternal(String table, List<String> keys, ColumnPath column_path, int consistency_level)
    throws InvalidRequestException
    {
        ThriftValidation.validateColumnPath(table, column_path);

        QueryPath path = new QueryPath(column_path.column_family, column_path.super_column);
        List<byte[]> nameAsList = Arrays.asList(column_path.column == null ? column_path.super_column : column_path.column);
        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        for (String key: keys)
        {
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
    throws InvalidRequestException
    {
        if (logger.isDebugEnabled())
            logger.debug("get_count");
        return multigetCountInternal(table, Arrays.asList(key), column_parent, consistency_level).get(key);
    }

    private Map<String, Integer> multigetCountInternal(String table, List<String> keys, ColumnParent column_parent, int consistency_level)
    throws InvalidRequestException
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
    throws InvalidRequestException, UnavailableException
    {
        if (logger.isDebugEnabled())
            logger.debug("insert");
        ThriftValidation.validateKey(key);
        ThriftValidation.validateColumnPath(table, column_path);

        RowMutation rm = new RowMutation(table, key.trim());
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

    public void batch_insert(String table, BatchMutation batch_mutation, int consistency_level)
    throws InvalidRequestException, UnavailableException
    {
        if (logger.isDebugEnabled())
            logger.debug("batch_insert");

        for (String cfName : batch_mutation.cfmap.keySet())
        {
            for (ColumnOrSuperColumn cosc : batch_mutation.cfmap.get(cfName))
            {
                if (cosc.column != null)
                {
                    ThriftValidation.validateColumnPath(table, new ColumnPath(cfName, null, cosc.column.name));
                }
                if (cosc.super_column != null)
                {
                    for (Column c : cosc.super_column.columns)
                    {
                        ThriftValidation.validateColumnPath(table, new ColumnPath(cfName, cosc.super_column.name, c.name));
                    }
                }
            }
        }

        doInsert(consistency_level, RowMutation.getRowMutation(table, batch_mutation));
    }

    public void remove(String table, String key, ColumnPath column_path, long timestamp, int consistency_level)
    throws InvalidRequestException, UnavailableException
    {
        if (logger.isDebugEnabled())
            logger.debug("remove");
        ThriftValidation.validateColumnPathOrParent(table, column_path);
        
        RowMutation rm = new RowMutation(table, key.trim());
        rm.delete(new QueryPath(column_path), timestamp);

        doInsert(consistency_level, rm);
	}

    private void doInsert(int consistency_level, RowMutation rm) throws UnavailableException
    {
        if (consistency_level != ConsistencyLevel.ZERO)
        {
            StorageProxy.insertBlocking(rm, consistency_level);
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
            HashMap<String, String> tokenToHostMap = new HashMap<String,String>();
            Map<Token, EndPoint> endpointMap = storageService.getLiveEndPointMap();
            for (Map.Entry<Token, EndPoint> e : endpointMap.entrySet())
                tokenToHostMap.put(e.getKey().toString(), e.getValue().getHost());
            return new JSONSerializer().serialize(tokenToHostMap);
        }
        else if (propertyName.equals("version"))
        {
            return "0.3.0";
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
        else
        {
            return new ArrayList<String>();
        }
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

        Iterator iter = tableMetaData.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<String, CFMetaData> pairs = (Map.Entry<String, CFMetaData>) iter.next();
            CFMetaData columnFamilyMetaData = pairs.getValue();

            String desc = "";


            Map<String, String> columnMap = new HashMap<String, String>();
            desc = columnFamilyMetaData.n_columnMap + "(" + columnFamilyMetaData.n_columnKey + ", " + columnFamilyMetaData.n_columnValue + ", " + columnFamilyMetaData.n_columnTimestamp + ")";
            if (columnFamilyMetaData.columnType.equals("Super"))
            {
                columnMap.put("Type", "Super");
                desc = columnFamilyMetaData.n_superColumnMap + "(" + columnFamilyMetaData.n_superColumnKey + ", " + desc + ")";
            }
            else
            {
                columnMap.put("Type", "Standard");
            }

            desc = columnFamilyMetaData.tableName + "." + columnFamilyMetaData.cfName + "(" +
                   columnFamilyMetaData.n_rowKey + ", " + desc + ")";

            columnMap.put("Desc", desc);
            columnMap.put("CompareWith", columnFamilyMetaData.comparator.getClass().getName());
            if (columnFamilyMetaData.columnType.equals("Super"))
            {
                columnMap.put("CompareSubcolumnsWith", columnFamilyMetaData.subcolumnComparator.getClass().getName());
            }
            columnMap.put("FlushPeriodInMinutes", columnFamilyMetaData.flushPeriodInMinutes + "");
            columnFamiliesMap.put(columnFamilyMetaData.cfName, columnMap);
        }
        return columnFamiliesMap;
    }

    public List<String> get_key_range(String tablename, String columnFamily, String startWith, String stopAt, int maxResults) throws InvalidRequestException, TException
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
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    // main method moved to CassandraDaemon
}
