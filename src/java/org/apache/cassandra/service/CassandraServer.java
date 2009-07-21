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
import org.apache.cassandra.cql.common.CqlResult;
import org.apache.cassandra.cql.driver.CqlDriver;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.LogUtil;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.thrift.TException;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class CassandraServer implements Cassandra.Iface
{
	private static Logger logger = Logger.getLogger(CassandraServer.class);

    private final static List<Column> EMPTY_COLUMNS = Collections.emptyList();
    private final static List<SuperColumn> EMPTY_SUPERCOLUMNS = Collections.emptyList();

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

    protected ColumnFamily readColumnFamily(ReadCommand command) throws InvalidRequestException
    {
        String cfName = command.getColumnFamilyName();
        ThriftValidation.validateKey(command.key);

        Row row;
        try
        {
            row = StorageProxy.readProtocol(command, StorageService.ConsistencyLevel.WEAK);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (TimeoutException e)
        {
            throw new RuntimeException(e);
        }

        if (row == null)
        {
            return null;
        }
        return row.getColumnFamily(cfName);
	}

    public List<Column> thriftifyColumns(Collection<IColumn> columns)
    {
        if (columns == null || columns.isEmpty())
        {
            return EMPTY_COLUMNS;
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

    /** for resultsets of standard columns */
    private List<Column> getSlice(ReadCommand command) throws InvalidRequestException
    {
        ColumnFamily cfamily = readColumnFamily(command);
        if (cfamily == null || cfamily.getColumnsMap().size() == 0)
        {
            return EMPTY_COLUMNS;
        }
        if (cfamily.isSuper())
        {
            IColumn column = cfamily.getColumnsMap().values().iterator().next();
            return thriftifyColumns(column.getSubColumns());
        }
        return thriftifyColumns(cfamily.getSortedColumns());
    }

    public List<Column> get_slice_by_names(String table, String key, ColumnParent column_parent, List<byte[]> column_names)
    throws InvalidRequestException, NotFoundException
    {
        logger.debug("get_slice_by_names");
        ThriftValidation.validateColumnParent(table, column_parent);
        return getSlice(new SliceByNamesReadCommand(table, key, column_parent, column_names));
    }

    public List<Column> get_slice(String table, String key, ColumnParent column_parent, byte[] start, byte[] finish, boolean is_ascending, int count)
    throws InvalidRequestException, NotFoundException
    {
        logger.debug("get_slice_from");
        ThriftValidation.validateColumnParent(table, column_parent);
        // TODO support get_slice on super CFs
        if (count <= 0)
            throw new InvalidRequestException("get_slice requires positive count");

        return getSlice(new SliceFromReadCommand(table, key, column_parent, start, finish, is_ascending, count));
    }

    public Column get_column(String table, String key, ColumnPath column_path)
    throws InvalidRequestException, NotFoundException
    {
        logger.debug("get_column");
        ThriftValidation.validateColumnPath(table, column_path);

        QueryPath path = new QueryPath(column_path.column_family, column_path.super_column);
        ColumnFamily cfamily = readColumnFamily(new SliceByNamesReadCommand(table, key, path, Arrays.asList(column_path.column)));
        // TODO can we leverage getSlice here and just check that it returns one column?
        if (cfamily == null)
        {
            throw new NotFoundException();
        }
        Collection<IColumn> columns = null;
        if (column_path.super_column != null)
        {
            IColumn column = cfamily.getColumn(column_path.super_column);
            if (column != null)
            {
                columns = column.getSubColumns();
            }
        }
        else
        {
            columns = cfamily.getSortedColumns();
        }
        if (columns == null || columns.size() == 0)
        {
            throw new NotFoundException();
        }

        assert columns.size() == 1;
        IColumn column = columns.iterator().next();
        if (column.isMarkedForDelete())
        {
            throw new NotFoundException();
        }

        return new Column(column.name(), column.value(), column.timestamp());
    }

    public int get_column_count(String table, String key, ColumnParent column_parent)
    throws InvalidRequestException
    {
        logger.debug("get_column_count");
        // validateColumnParent assumes we require simple columns; g_c_c is the only
        // one of the columnParent-taking apis that can also work at the SC level.
        // so we roll a one-off validator here.
        String cfType = ThriftValidation.validateColumnFamily(table, column_parent.column_family);
        if (cfType.equals("Standard") && column_parent.super_column != null)
        {
            throw new InvalidRequestException("columnfamily alone is required for standard CF " + column_parent.column_family);
        }

        ColumnFamily cfamily;
        cfamily = readColumnFamily(new SliceFromReadCommand(table, key, column_parent, ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, Integer.MAX_VALUE));
        if (cfamily == null)
        {
            return 0;
        }
        Collection<IColumn> columns = null;
        if (column_parent.super_column != null)
        {
            IColumn column = cfamily.getColumn(column_parent.super_column);
            if (column != null)
            {
                columns = column.getSubColumns();
            }
        }
        else
        {
            columns = cfamily.getSortedColumns();
        }
        if (columns == null || columns.size() == 0)
        {
            return 0;
        }
        return columns.size();
	}

    public void insert(String table, String key, ColumnPath column_path, byte[] value, long timestamp, int block_for)
    throws InvalidRequestException, UnavailableException
    {
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
        doInsert(block_for, rm);
    }

    public void batch_insert(String table, BatchMutation batch_mutation, int block_for)
    throws InvalidRequestException, UnavailableException
    {
        logger.debug("batch_insert");
        RowMutation rm = RowMutation.getRowMutation(table, batch_mutation);
        Set<String> cfNames = rm.columnFamilyNames();
        ThriftValidation.validateKeyCommand(rm.key(), rm.table(), cfNames.toArray(new String[cfNames.size()]));

        doInsert(block_for, rm);
    }

    public void remove(String table, String key, ColumnPathOrParent column_path_or_parent, long timestamp, int block_for)
    throws InvalidRequestException, UnavailableException
    {
        logger.debug("remove");
        ThriftValidation.validateColumnPathOrParent(table, column_path_or_parent);
        
        RowMutation rm = new RowMutation(table, key.trim());
        rm.delete(new QueryPath(column_path_or_parent), timestamp);

        doInsert(block_for, rm);
	}

    private void doInsert(int block, RowMutation rm) throws UnavailableException
    {
        if (block > 0)
        {
            StorageProxy.insertBlocking(rm,block);
        }
        else
        {
            StorageProxy.insert(rm);
        }
    }

    public List<SuperColumn> get_slice_super_by_names(String table, String key, String column_family, List<byte[]> super_column_names)
    throws InvalidRequestException
    {
        logger.debug("get_slice_super_by_names");
        ThriftValidation.validateColumnFamily(table, column_family);

        ColumnFamily cfamily = readColumnFamily(new SliceByNamesReadCommand(table, key, new QueryPath(column_family), super_column_names));
        if (cfamily == null)
        {
            return EMPTY_SUPERCOLUMNS;
        }
        return thriftifySuperColumns(cfamily.getSortedColumns());
    }

    private List<SuperColumn> thriftifySuperColumns(Collection<IColumn> columns)
    {
        if (columns == null || columns.isEmpty())
        {
            return EMPTY_SUPERCOLUMNS;
        }

        ArrayList<SuperColumn> thriftSuperColumns = new ArrayList<SuperColumn>(columns.size());
        for (IColumn column : columns)
        {
            List<Column> subcolumns = thriftifyColumns(column.getSubColumns());
            if (subcolumns.isEmpty())
            {
                continue;
            }
            thriftSuperColumns.add(new SuperColumn(column.name(), subcolumns));
        }

        return thriftSuperColumns;
    }

    public List<SuperColumn> get_slice_super(String table, String key, String column_family, byte[] start, byte[] finish, boolean is_ascending, int count)
    throws InvalidRequestException
    {
        logger.debug("get_slice_super");
        if (!DatabaseDescriptor.getColumnFamilyType(table, column_family).equals("Super"))
            throw new InvalidRequestException("get_slice_super requires a super CF name");
        if (count <= 0)
            throw new InvalidRequestException("get_slice_super requires positive count");

        ColumnFamily cfamily = readColumnFamily(new SliceFromReadCommand(table, key, new QueryPath(column_family), start, finish, is_ascending, count));
        if (cfamily == null)
        {
            return EMPTY_SUPERCOLUMNS;
        }
        Collection<IColumn> columns = cfamily.getSortedColumns();
        return thriftifySuperColumns(columns);
    }


    public SuperColumn get_super_column(String table, String key, SuperColumnPath super_column_path)
    throws InvalidRequestException, NotFoundException
    {
        logger.debug("get_superColumn");
        ThriftValidation.validateSuperColumnPath(table, super_column_path);

        ColumnFamily cfamily = readColumnFamily(new SliceByNamesReadCommand(table, key, new QueryPath(super_column_path.column_family), Arrays.asList(super_column_path.super_column)));
        if (cfamily == null)
        {
            throw new NotFoundException();
        }
        Collection<IColumn> columns = cfamily.getSortedColumns();
        if (columns == null || columns.size() == 0)
        {
            throw new NotFoundException();
        }

        assert columns.size() == 1;
        IColumn column = columns.iterator().next();
        if (column.getSubColumns().size() == 0)
        {
            throw new NotFoundException();
        }

        return new SuperColumn(column.name(), thriftifyColumns(column.getSubColumns()));
    }

    public void batch_insert_super_column(String table, BatchMutationSuper batch_mutation_super, int block_for)
    throws InvalidRequestException, UnavailableException
    {
        logger.debug("batch_insert_SuperColumn");
        RowMutation rm = RowMutation.getRowMutation(table, batch_mutation_super);
        Set<String> cfNames = rm.columnFamilyNames();
        ThriftValidation.validateKeyCommand(rm.key(), rm.table(), cfNames.toArray(new String[cfNames.size()]));

        doInsert(block_for, rm);
    }

    public String getStringProperty(String propertyName)
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
        else if (propertyName.equals("version"))
        {
            return "0.3.0";
        }
        else
        {
            return "?";
        }
    }

    public List<String> getStringListProperty(String propertyName)
    {
        if (propertyName.equals("tables"))
        {
            return DatabaseDescriptor.getTables();        
        }
        else
        {
            return new ArrayList<String>();
        }
    }

    public Map<String,Map<String,String>> describeTable(String tableName) throws NotFoundException
    {
        Map <String, Map<String, String>> columnFamiliesMap = new HashMap<String, Map<String, String>> ();

        Map<String, CFMetaData> tableMetaData = DatabaseDescriptor.getTableMetaData(tableName);
        // table doesn't exist
        if (tableMetaData == null) {
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
            if (columnFamilyMetaData.columnType.equals("Super")) {
                columnMap.put("type", "Super");
                desc = columnFamilyMetaData.n_superColumnMap + "(" + columnFamilyMetaData.n_superColumnKey + ", " + desc + ")"; 
            } else {
                columnMap.put("type", "Standard");
            }
            
            desc = columnFamilyMetaData.tableName + "." + columnFamilyMetaData.cfName + "(" + 
                columnFamilyMetaData.n_rowKey + ", " + desc + ")";

            columnMap.put("desc", desc);
            columnMap.put("type", columnFamilyMetaData.comparator.getClass().getName());
            columnMap.put("flushperiod", columnFamilyMetaData.flushPeriodInMinutes + "");
            columnFamiliesMap.put(columnFamilyMetaData.cfName, columnMap);
        }
        return columnFamiliesMap;
    }

    public org.apache.cassandra.service.CqlResult executeQuery(String query) throws TException
    {
        org.apache.cassandra.service.CqlResult result = new org.apache.cassandra.service.CqlResult();

        CqlResult cqlResult = CqlDriver.executeQuery(query);
        
        // convert CQL result type to Thrift specific return type
        if (cqlResult != null)
        {
            result.error_txt = cqlResult.errorTxt;
            result.result_set = cqlResult.resultSet;
            result.error_code = cqlResult.errorCode;
        }
        return result;
    }

    public List<String> get_key_range(String tablename, String columnFamily, String startWith, String stopAt, int maxResults) throws InvalidRequestException, TException
    {
        logger.debug("get_key_range");
        ThriftValidation.validateCommand(tablename, columnFamily);
        if (!(StorageService.getPartitioner() instanceof OrderPreservingPartitioner))
        {
            throw new InvalidRequestException("range queries may only be performed against an order-preserving partitioner");
        }
        if (maxResults <= 0)
        {
            throw new InvalidRequestException("maxResults must be positive");
        }

        return StorageProxy.getKeyRange(new RangeCommand(tablename, columnFamily, startWith, stopAt, maxResults));
    }

    // main method moved to CassandraDaemon
}
