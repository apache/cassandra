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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql.common.CqlResult;
import org.apache.cassandra.cql.driver.CqlDriver;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnReadCommand;
import org.apache.cassandra.db.ColumnsSinceReadCommand;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceReadCommand;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.ColumnFamilyNotDefinedException;
import org.apache.cassandra.db.TableNotDefinedException;
import org.apache.cassandra.db.RangeCommand;
import org.apache.cassandra.db.RangeReply;
import org.apache.cassandra.utils.LogUtil;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.thrift.TException;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class CassandraServer implements Cassandra.Iface
{
	private static Logger logger_ = Logger.getLogger(CassandraServer.class);

    private final static List<column_t> EMPTY_COLUMNS = Arrays.asList();
    private final static List<superColumn_t> EMPTY_SUPERCOLUMNS = Arrays.asList();

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
	
	private void validateCommand(String key, String tablename, String... columnFamilyNames) throws InvalidRequestException
	{
        if (key.isEmpty())
        {
            throw new InvalidRequestException("Key may not be empty");
        }
		if ( !DatabaseDescriptor.getTables().contains(tablename) )
		{
			throw new TableNotDefinedException("Table " + tablename + " does not exist in this schema.");
		}
        Table table = Table.open(tablename);
        for (String cfName : columnFamilyNames)
        {
            if (!table.getColumnFamilies().contains(cfName))
            {
                throw new ColumnFamilyNotDefinedException("Column Family " + cfName + " is invalid.");
            }
        }
	}
    
	protected ColumnFamily readColumnFamily(ReadCommand command) throws InvalidRequestException
    {
        String cfName = command.getColumnFamilyName();
        validateCommand(command.key, command.table, cfName);

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

    public List<column_t> thriftifyColumns(Collection<IColumn> columns)
    {
        if (columns == null || columns.isEmpty())
        {
            return EMPTY_COLUMNS;
        }

        ArrayList<column_t> thriftColumns = new ArrayList<column_t>(columns.size());
        for (IColumn column : columns)
        {
            if (column.isMarkedForDelete())
            {
                continue;
            }
            column_t thrift_column = new column_t(column.name(), column.value(), column.timestamp());
            thriftColumns.add(thrift_column);
        }

        return thriftColumns;
    }

    public List<column_t> get_columns_since(String tablename, String key, String columnFamily_column, long timeStamp) throws InvalidRequestException
    {
        long startTime = System.currentTimeMillis();
        try
        {
            ColumnFamily cfamily = readColumnFamily(new ColumnsSinceReadCommand(tablename, key, columnFamily_column, timeStamp));
            String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
            if (cfamily == null)
            {
                return EMPTY_COLUMNS;
            }
            Collection<IColumn> columns = null;
            if( values.length > 1 )
            {
                // this is the super column case
                IColumn column = cfamily.getColumn(values[1]);
                if(column != null)
                    columns = column.getSubColumns();
            }
            else
            {
                columns = cfamily.getAllColumns();
            }
            return thriftifyColumns(columns);
        }
        finally
        {
            logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime) + " ms.");
        }
	}
	

    public List<column_t> get_slice_by_names(String tablename, String key, String columnFamily, List<String> columnNames) throws InvalidRequestException
    {
        long startTime = System.currentTimeMillis();
        try
        {
            ColumnFamily cfamily = readColumnFamily(new SliceByNamesReadCommand(tablename, key, columnFamily, columnNames));
            if (cfamily == null)
            {
                return EMPTY_COLUMNS;
            }
            Collection<IColumn> columns = null;
            columns = cfamily.getAllColumns();
            return thriftifyColumns(columns);
        }
        finally
        {
            logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime) + " ms.");
        }
    }
    
    public List<column_t> get_slice(String tablename, String key, String columnFamily_column, int start, int count) throws InvalidRequestException
    {
        long startTime = System.currentTimeMillis();
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
            ColumnFamily cfamily = readColumnFamily(new SliceReadCommand(tablename, key, columnFamily_column, start, count));
            if (cfamily == null)
			{
                return EMPTY_COLUMNS;
			}
			Collection<IColumn> columns = null;
			if( values.length > 1 )
			{
				// this is the super column case 
				IColumn column = cfamily.getColumn(values[1]);
				if(column != null)
					columns = column.getSubColumns();
			}
			else
			{
				columns = cfamily.getAllColumns();
			}
            return thriftifyColumns(columns);
		}
        finally
        {
            logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime) + " ms.");
        }
	}
    
    public column_t get_column(String tablename, String key, String columnFamily_column) throws NotFoundException, InvalidRequestException
    {
        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
        if (values.length < 2)
        {
            throw new InvalidRequestException("get_column requires both parts of columnfamily:column");
        }
        ColumnFamily cfamily = readColumnFamily(new ColumnReadCommand(tablename, key, columnFamily_column));
        if (cfamily == null)
        {
            throw new NotFoundException();
        }
        Collection<IColumn> columns = null;
        if( values.length > 2 )
        {
            // this is the super column case
            IColumn column = cfamily.getColumn(values[1]);
            if(column != null)
                columns = column.getSubColumns();
        }
        else
        {
            columns = cfamily.getAllColumns();
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

        return new column_t(column.name(), column.value(), column.timestamp());
    }
    

    public int get_column_count(String tablename, String key, String columnFamily_column) throws InvalidRequestException
    {
        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
        ColumnFamily cfamily = readColumnFamily(new SliceReadCommand(tablename, key, columnFamily_column, -1, Integer.MAX_VALUE));
        if (cfamily == null)
        {
            return 0;
        }
        Collection<IColumn> columns = null;
        if( values.length > 1 )
        {
            // this is the super column case
            IColumn column = cfamily.getColumn(values[1]);
            if(column != null)
                columns = column.getSubColumns();
        }
        else
        {
            columns = cfamily.getAllColumns();
        }
        if (columns == null || columns.size() == 0)
        {
            return 0;
        }
        return columns.size();
	}

    public void insert(String tablename, String key, String columnFamily_column, byte[] cellData, long timestamp)
	{
        RowMutation rm = new RowMutation(tablename, key.trim());
        rm.add(columnFamily_column, cellData, timestamp);
        try
        {
            validateCommand(rm.key(), rm.table(), rm.columnFamilyNames().toArray(new String[0]));
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        StorageProxy.insert(rm);
	}
    
    public boolean insert_blocking(String tablename, String key, String columnFamily_column, byte[] cellData, long timestamp) throws InvalidRequestException
    {
        RowMutation rm = new RowMutation(tablename, key.trim());
        rm.add(columnFamily_column, cellData, timestamp);
        validateCommand(rm.key(), rm.table(), rm.columnFamilyNames().toArray(new String[0]));
        return StorageProxy.insertBlocking(rm);
    }

    public boolean batch_insert_blocking(batch_mutation_t batchMutation) throws InvalidRequestException
    {
        logger_.debug("batch_insert_blocking");
        RowMutation rm = RowMutation.getRowMutation(batchMutation);
        validateCommand(rm.key(), rm.table(), rm.columnFamilyNames().toArray(new String[0]));
        return StorageProxy.insertBlocking(rm);
    }

	public void batch_insert(batch_mutation_t batchMutation)
    {
        logger_.debug("batch_insert");
        RowMutation rm = RowMutation.getRowMutation(batchMutation);
        try
        {
            validateCommand(rm.key(), rm.table(), rm.columnFamilyNames().toArray(new String[0]));
        }
        catch (InvalidRequestException e)
        {
            // it would be confusing to declare an exception in thrift that can't be returned to the client
            throw new RuntimeException(e);
        }
        StorageProxy.insert(rm);
	}

    public boolean remove(String tablename, String key, String columnFamily_column, long timestamp, boolean block) throws InvalidRequestException
    {
        logger_.debug("remove");
        RowMutation rm = new RowMutation(tablename, key.trim());
        rm.delete(columnFamily_column, timestamp);
        validateCommand(rm.key(), rm.table(), rm.columnFamilyNames().toArray(new String[0]));
        if (block) {
            return StorageProxy.insertBlocking(rm);
        } else {
            StorageProxy.insert(rm);
            return true;
        }
	}

    public List<superColumn_t> get_slice_super_by_names(String tablename, String key, String columnFamily, List<String> superColumnNames) throws InvalidRequestException
    {
        long startTime = System.currentTimeMillis();
		try
		{
			ColumnFamily cfamily = readColumnFamily(new SliceByNamesReadCommand(tablename, key, columnFamily, superColumnNames));
			if (cfamily == null)
			{
                return EMPTY_SUPERCOLUMNS;
			}
			Collection<IColumn> columns = null;
			columns = cfamily.getAllColumns();
            return thriftifySuperColumns(columns);
		}
        finally
        {
            logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime) + " ms.");
        }
    }

    private List<superColumn_t> thriftifySuperColumns(Collection<IColumn> columns)
    {
        if (columns == null || columns.isEmpty())
        {
            return EMPTY_SUPERCOLUMNS;
        }

        ArrayList<superColumn_t> thriftSuperColumns = new ArrayList<superColumn_t>(columns.size());
        for (IColumn column : columns)
        {
            List<column_t> subcolumns = thriftifyColumns(column.getSubColumns());
            if (subcolumns.isEmpty())
            {
                continue;
            }
            thriftSuperColumns.add(new superColumn_t(column.name(), subcolumns));
        }

        return thriftSuperColumns;
    }

    public List<superColumn_t> get_slice_super(String tablename, String key, String columnFamily_superColumnName, int start, int count) throws InvalidRequestException
    {
        ColumnFamily cfamily = readColumnFamily(new SliceReadCommand(tablename, key, columnFamily_superColumnName, start, count));
        if (cfamily == null)
        {
            return EMPTY_SUPERCOLUMNS;
        }
        Collection<IColumn> columns = cfamily.getAllColumns();
        return thriftifySuperColumns(columns);
    }
    
    public superColumn_t get_superColumn(String tablename, String key, String columnFamily_column) throws InvalidRequestException, NotFoundException
    {
        ColumnFamily cfamily = readColumnFamily(new ColumnReadCommand(tablename, key, columnFamily_column));
        if (cfamily == null)
        {
            throw new NotFoundException();
        }
        Collection<IColumn> columns = cfamily.getAllColumns();
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

        return new superColumn_t(column.name(), thriftifyColumns(column.getSubColumns()));
    }
    
    public boolean batch_insert_superColumn_blocking(batch_mutation_super_t batchMutationSuper) throws InvalidRequestException
    {
        logger_.debug("batch_insert_SuperColumn_blocking");
        RowMutation rm = RowMutation.getRowMutation(batchMutationSuper);
        validateCommand(rm.key(), rm.table(), rm.columnFamilyNames().toArray(new String[0]));
        return StorageProxy.insertBlocking(rm);
    }

    public void batch_insert_superColumn(batch_mutation_super_t batchMutationSuper)
    {
        logger_.debug("batch_insert_SuperColumn");
        RowMutation rm = RowMutation.getRowMutation(batchMutationSuper);
        try
        {
            validateCommand(rm.key(), rm.table(), rm.columnFamilyNames().toArray(new String[0]));
        }
        catch (InvalidRequestException e)
        {
            // it would be confusing to declare an exception in thrift that can't be returned to the client
            throw new RuntimeException(e);
        }
        StorageProxy.insert(rm);
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
                StringBuffer fileData = new StringBuffer(8192);
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
            return "1";
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

    public String describeTable(String tableName)
    {
        String desc = "";
        Map<String, CFMetaData> tableMetaData = DatabaseDescriptor.getTableMetaData(tableName);

        if (tableMetaData == null)
        {
            return "Table " + tableName +  " not found.";
        }

        Iterator iter = tableMetaData.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<String, CFMetaData> pairs = (Map.Entry<String, CFMetaData>)iter.next();
            desc = desc + pairs.getValue().pretty() + "-----\n";
        }
        return desc;
    }

    public CqlResult_t executeQuery(String query) throws TException
    {
        CqlResult_t result = new CqlResult_t();

        CqlResult cqlResult = CqlDriver.executeQuery(query);
        
        // convert CQL result type to Thrift specific return type
        if (cqlResult != null)
        {
            result.errorTxt = cqlResult.errorTxt;
            result.resultSet = cqlResult.resultSet;
            result.errorCode = cqlResult.errorCode;
        }
        return result;
    }

    public List<String> get_key_range(String tablename, String startWith, String stopAt, int maxResults) throws InvalidRequestException
    {
        logger_.debug("get_range");

        if (!(StorageService.getPartitioner() instanceof OrderPreservingPartitioner))
        {
            throw new InvalidRequestException("range queries may only be performed against an order-preserving partitioner");
        }

        try
        {
            Message message = new RangeCommand(tablename, startWith, stopAt, maxResults).getMessage();
            EndPoint endPoint = StorageService.instance().findSuitableEndPoint(startWith);
            IAsyncResult iar = MessagingService.getMessagingInstance().sendRR(message, endPoint);

            // read response
            // TODO send more requests if we need to span multiple nodes
            // double the usual timeout since range requests are expensive
            byte[] responseBody = (byte[])iar.get(2 * DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS)[0];
            return RangeReply.read(responseBody).keys;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /*
     * This method is used to ensure that all keys
     * prior to the specified key, as dtermined by
     * the SSTable index bucket it falls in, are in
     * buffer cache.  
    */
    public void touch (String key , boolean fData) 
    {
    	try
    	{
    		StorageProxy.touchProtocol(DatabaseDescriptor.getTables().get(0), key, fData, StorageService.ConsistencyLevel.WEAK);
    	}
    	catch ( Exception e)
    	{
			logger_.info( LogUtil.throwableToString(e) );
    	}
	}
    
    // main method moved to CassandraDaemon
}
