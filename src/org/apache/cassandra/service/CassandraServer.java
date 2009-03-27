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
import java.util.Set;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;
import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocolFactory;
import com.facebook.thrift.server.TThreadPoolServer;
import com.facebook.thrift.server.TThreadPoolServer.Options;
import com.facebook.thrift.transport.TServerSocket;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql.common.CqlResult;
import org.apache.cassandra.cql.driver.CqlDriver;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.RowMutationMessage;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.utils.LogUtil;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class CassandraServer extends FacebookBase implements
		Cassandra.Iface
{

	private static Logger logger_ = Logger.getLogger(CassandraServer.class);
	/*
	 * Handle to the storage service to interact with the other machines in the
	 * cluster.
	 */
	protected StorageService storageService;

	protected CassandraServer(String name)
	{
		super(name);
		// Create the instance of the storage service
		storageService = StorageService.instance();
	}

	public CassandraServer()
	{
		super("CassandraServer");
		// Create the instance of the storage service
		storageService = StorageService.instance();
	}

	/*
	 * The start function initializes the server and start's listening on the
	 * specified port.
	 */
	public void start() throws Throwable
    {
		LogUtil.init();
		//LogUtil.setLogLevel("com.facebook", "DEBUG");
		// Start the storage service
		storageService.start();
	}
	
	private void validateTable(String table) throws CassandraException
	{
		if ( !DatabaseDescriptor.getTables().contains(table) )
		{
			throw new CassandraException("Table " + table + " does not exist in this schema.");
		}
	}
    
	protected ColumnFamily get_cf(String tablename, String key, String columnFamily, List<String> columNames) throws CassandraException, TException
	{
    	ColumnFamily cfamily = null;
		try
		{
			validateTable(tablename);
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily);
	        // check for  values 
	        if( values.length < 1 )
	        {
	        	throw new CassandraException("Column Family " + columnFamily + " is invalid.");	        	
	        }
	        Row row = StorageProxy.readProtocol(tablename, key, columnFamily, columNames, StorageService.ConsistencyLevel.WEAK);
	        if (row == null)
			{
				throw new CassandraException("No row exists for key " + key);			
			}
			Map<String, ColumnFamily> cfMap = row.getColumnFamilyMap();
			if (cfMap == null || cfMap.size() == 0)
			{				
				logger_	.info("ERROR ColumnFamily " + columnFamily + " map is missing.....: " + "   key:" + key );
				throw new CassandraException("Either the key " + key + " is not present or the columns requested are not present.");
			}
			cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily " + columnFamily + " is missing.....: " + "   key:" + key + "  ColumnFamily:" + values[0]);
				throw new CassandraException("Either the key " + key + " is not present or the column family " + values[0] +  " is not present.");
			}
		}
		catch (Throwable ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
		return cfamily;
	}

    public  ArrayList<column_t> get_columns_since(String tablename, String key, String columnFamily_column, long timeStamp) throws CassandraException,TException
	{
		ArrayList<column_t> retlist = new ArrayList<column_t>();
        long startTime = System.currentTimeMillis();
		try
		{
			validateTable(tablename);
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values 
	        if( values.length < 1 )
	        {
	        	throw new CassandraException("Column Family " + columnFamily_column + " is invalid.");	        	
	        }
	        Row row = StorageProxy.readProtocol(tablename, key, columnFamily_column, timeStamp, StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
	        	throw new CassandraException("ERROR No row for this key .....: " + key);	        	
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilyMap();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily " + columnFamily_column + " map is missing.....: " + "   key:" + key);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested are not present.");
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily " + columnFamily_column + " is missing.....: "+"   key:" + key	+ "  ColumnFamily:" + values[0]);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested" + columnFamily_column + "are not present.");
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
				logger_	.info("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + values[0]);
				throw new CassandraException("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + values[0]);
			}
			for(IColumn column : columns)
			{
				column_t thrift_column = new column_t();
				thrift_column.columnName = column.name();
				thrift_column.value = new String(column.value()); // This needs to be Utf8ed
				thrift_column.timestamp = column.timestamp();
				retlist.add(thrift_column);
			}
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
        logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime) + " ms.");
		return retlist;
	}
	

    public List<column_t> get_slice_by_names(String tablename, String key, String columnFamily, List<String> columnNames) throws CassandraException, TException
    {
		ArrayList<column_t> retlist = new ArrayList<column_t>();
        long startTime = System.currentTimeMillis();
		try
		{
			validateTable(tablename);
			ColumnFamily cfamily = get_cf(tablename, key, columnFamily, columnNames);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily " + columnFamily + " is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + columnFamily);
				throw new CassandraException("Either the key " + key + " is not present or the columnFamily requested" + columnFamily + "is not present.");
			}
			Collection<IColumn> columns = null;
			columns = cfamily.getAllColumns();
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + columnFamily);
				throw new CassandraException("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + columnFamily);
			}
			
			for(IColumn column : columns)
			{
				column_t thrift_column = new column_t();
				thrift_column.columnName = column.name();
				thrift_column.value = new String(column.value()); // This needs to be Utf8ed
				thrift_column.timestamp = column.timestamp();
				retlist.add(thrift_column);
			}
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
		
        logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime)
                + " ms.");
		return retlist;
    }
    
    public ArrayList<column_t> get_slice(String tablename, String key, String columnFamily_column, int start, int count) throws CassandraException,TException
	{
		ArrayList<column_t> retlist = new ArrayList<column_t>();
        long startTime = System.currentTimeMillis();
		try
		{
			validateTable(tablename);
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values 
	        if( values.length < 1 )
	        {
	        	throw new CassandraException("Column Family " + columnFamily_column + " is invalid.");	        	
	        }
	        Row row = StorageProxy.readProtocol(tablename, key, columnFamily_column, start, count, StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
	        	throw new CassandraException("ERROR No row for this key .....: " + key);	        	
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilyMap();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily " + columnFamily_column + " map is missing.....: " + "   key:" + key);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested are not present.");
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily " + columnFamily_column + " is missing.....: "	+ "   key:" + key + "  ColumnFamily:" + values[0]);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested" + columnFamily_column + "are not present.");
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
				logger_	.info("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + values[0]);
				throw new CassandraException("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + values[0]);
			}
			for(IColumn column : columns)
			{
				column_t thrift_column = new column_t();
				thrift_column.columnName = column.name();
				thrift_column.value = new String(column.value()); // This needs to be Utf8ed
				thrift_column.timestamp = column.timestamp();
				retlist.add(thrift_column);
			}
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
        logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime)
                + " ms.");
		return retlist;
	}
    
    public column_t get_column(String tablename, String key, String columnFamily_column) throws CassandraException,TException
    {
		column_t ret = null;
		try
		{
			validateTable(tablename);
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values 
	        if( values.length < 2 )
	        {
	        	throw new CassandraException("Column Family " + columnFamily_column + " is invalid.");	        	
	        }
	        Row row = StorageProxy.readProtocol(tablename, key, columnFamily_column, -1, Integer.MAX_VALUE, StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
	        	throw new CassandraException("ERROR No row for this key .....: " + key);	        	
			}
			
			Map<String, ColumnFamily> cfMap = row.getColumnFamilyMap();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested are not present.");
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested" + columnFamily_column + "are not present.");
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
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				throw new CassandraException("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + values[0]);
			}
			ret = new column_t();
			for(IColumn column : columns)
			{
				ret.columnName = column.name();
				ret.value = new String(column.value());
				ret.timestamp = column.timestamp();
			}
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
		return ret;
    }
    

    public int get_column_count(String tablename, String key, String columnFamily_column) throws CassandraException
	{
    	int count = -1;
		try
		{
			validateTable(tablename);
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values 
	        if( values.length < 1 )
	        {
	        	throw new CassandraException("Column Family " + columnFamily_column + " is invalid.");	        	
	        }
	        Row row = StorageProxy.readProtocol(tablename, key, columnFamily_column, -1, Integer.MAX_VALUE, StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
	        	throw new CassandraException("ERROR No row for this key .....: " + key);	        	
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilyMap();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested are not present.");
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested" + columnFamily_column + "are not present.");
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
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				throw new CassandraException("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + values[0]);
			}
			count = columns.size();
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
		return count;
	}

    public void insert(String tablename, String key, String columnFamily_column, String cellData, long timestamp)
	{
		try
		{
			validateTable(tablename);
			RowMutation rm = new RowMutation(tablename, key.trim());
			rm.add(columnFamily_column, cellData.getBytes(), timestamp);
			StorageProxy.insert(rm);
		}
		catch (Exception e)
		{
			logger_.debug( LogUtil.throwableToString(e) );
		}
		return;
	}
    
    public boolean batch_insert_blocking(batch_mutation_t batchMutation)
    {
        logger_.debug("batch_insert_blocking");
        RowMutation rm = RowMutation.getRowMutation(batchMutation);
        return StorageProxy.insertBlocking(rm);
    }

	public void batch_insert(batch_mutation_t batchMutation)
	{
        logger_.debug("batch_insert");
        RowMutation rm = RowMutation.getRowMutation(batchMutation);
        StorageProxy.insert(rm);
	}

    public void remove(String tablename, String key, String columnFamily_column)
	{
		throw new UnsupportedOperationException("Remove is coming soon");
	}

    public boolean remove(String tablename, String key, String columnFamily_column, long timestamp, int block_for)
	{
        logger_.debug("remove");
        RowMutation rm = new RowMutation(tablename, key.trim());
        rm.delete(columnFamily_column, timestamp);
        if (block_for > 0) {
            return StorageProxy.insertBlocking(rm);
        } else {
            StorageProxy.insert(rm);
            return true;
        }
	}

    public List<superColumn_t> get_slice_super_by_names(String tablename, String key, String columnFamily, List<String> superColumnNames) throws CassandraException, TException
    {
		ArrayList<superColumn_t> retlist = new ArrayList<superColumn_t>();
        long startTime = System.currentTimeMillis();
		
		try
		{
			validateTable(tablename);
			ColumnFamily cfamily = get_cf(tablename, key, columnFamily, superColumnNames);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily " + columnFamily + " is missing.....: "+"   key:" + key
							+ "  ColumnFamily:" + columnFamily);
				throw new CassandraException("Either the key " + key + " is not present or the column family requested" + columnFamily + "is not present.");
			}
			Collection<IColumn> columns = null;
			columns = cfamily.getAllColumns();
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + columnFamily);
				throw new CassandraException("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + columnFamily);
			}
			
			for(IColumn column : columns)
			{
				superColumn_t thrift_superColumn = new superColumn_t();
				thrift_superColumn.name = column.name();
				Collection<IColumn> subColumns = column.getSubColumns();
				if(subColumns.size() != 0 )
				{
					thrift_superColumn.columns = new ArrayList<column_t>();
					for( IColumn subColumn : subColumns )
					{
						column_t thrift_column = new column_t();
						thrift_column.columnName = subColumn.name();
						thrift_column.value = new String(subColumn.value());
						thrift_column.timestamp = subColumn.timestamp();
						thrift_superColumn.columns.add(thrift_column);
					}
				}
				retlist.add(thrift_superColumn);
			}
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
        logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime)
                + " ms.");
		return retlist;
    }

    
    public ArrayList<superColumn_t> get_slice_super(String tablename, String key, String columnFamily_superColumnName, int start, int count) throws CassandraException
    {
		ArrayList<superColumn_t> retlist = new ArrayList<superColumn_t>();
		try
		{
			validateTable(tablename);
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_superColumnName);
	        // check for  values 
	        if( values.length < 1 )
	        {
	        	throw new CassandraException("Column Family " + columnFamily_superColumnName + " is invalid.");	        	
	        }
	        Row row = StorageProxy.readProtocol(tablename, key, columnFamily_superColumnName, start, count, StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
	        	throw new CassandraException("ERROR No row for this key .....: " + key);	        	
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilyMap();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested are not present.");
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested" + columnFamily_superColumnName + "are not present.");
			}
			Collection<IColumn> columns = cfamily.getAllColumns();
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				throw new CassandraException("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + values[0]);
			}
			
			for(IColumn column : columns)
			{
				superColumn_t thrift_superColumn = new superColumn_t();
				thrift_superColumn.name = column.name();
				Collection<IColumn> subColumns = column.getSubColumns();
				if(subColumns.size() != 0 )
				{
					thrift_superColumn.columns = new ArrayList<column_t>();
					for( IColumn subColumn : subColumns )
					{
						column_t thrift_column = new column_t();
						thrift_column.columnName = subColumn.name();
						thrift_column.value = new String(subColumn.value());
						thrift_column.timestamp = subColumn.timestamp();
						thrift_superColumn.columns.add(thrift_column);
					}
				}
				retlist.add(thrift_superColumn);
			}
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
		return retlist;
    	
    }
    
    public superColumn_t get_superColumn(String tablename, String key, String columnFamily_column) throws CassandraException
    {
    	superColumn_t ret = null;
		try
		{
			validateTable(tablename);
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
	        // check for  values 
	        if( values.length < 2 )
	        {
	        	throw new CassandraException("Column Family " + columnFamily_column + " is invalid.");	        	
	        }

	        Row row = StorageProxy.readProtocol(tablename, key, columnFamily_column, -1, Integer.MAX_VALUE, StorageService.ConsistencyLevel.WEAK);
			if (row == null)
			{
				logger_.info("ERROR No row for this key .....: " + key);
	        	throw new CassandraException("ERROR No row for this key .....: " + key);	        	
			}

			Map<String, ColumnFamily> cfMap = row.getColumnFamilyMap();
			if (cfMap == null || cfMap.size() == 0)
			{
				logger_	.info("ERROR ColumnFamily map is missing.....: "
							   + "   key:" + key
								);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested are not present.");
			}
			ColumnFamily cfamily = cfMap.get(values[0]);
			if (cfamily == null)
			{
				logger_.info("ERROR ColumnFamily  is missing.....: "
							+"   key:" + key
							+ "  ColumnFamily:" + values[0]);
				throw new CassandraException("Either the key " + key + " is not present or the columns requested" + columnFamily_column + "are not present.");
			}
			Collection<IColumn> columns = cfamily.getAllColumns();
			if (columns == null || columns.size() == 0)
			{
				logger_	.info("ERROR Columns are missing.....: "
							   + "   key:" + key
								+ "  ColumnFamily:" + values[0]);
				throw new CassandraException("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + values[0]);
			}
			
			for(IColumn column : columns)
			{
				ret = new superColumn_t();
				ret.name = column.name();
				Collection<IColumn> subColumns = column.getSubColumns();
				if(subColumns.size() != 0 )
				{
					ret.columns = new ArrayList<column_t>();
					for(IColumn subColumn : subColumns)
					{
						column_t thrift_column = new column_t();
						thrift_column.columnName = subColumn.name();
						thrift_column.value = new String(subColumn.value());
						thrift_column.timestamp = subColumn.timestamp();
						ret.columns.add(thrift_column);
					}
				}
			}
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
		return ret;
    	
    }
    
    public boolean batch_insert_superColumn_blocking(batch_mutation_super_t batchMutationSuper)
    {
        logger_.debug("batch_insert_SuperColumn_blocking");
        RowMutation rm = RowMutation.getRowMutation(batchMutationSuper);
        return StorageProxy.insertBlocking(rm);
    }

    public void batch_insert_superColumn(batch_mutation_super_t batchMutationSuper)
    {
        logger_.debug("batch_insert_SuperColumn");
        RowMutation rm = RowMutation.getRowMutation(batchMutationSuper);
        StorageProxy.insert(rm);
    }

    public String getStringProperty(String propertyName) throws TException
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
            return getVersion();
        }
        else
        {
            return "?";
        }
    }

    public List<String> getStringListProperty(String propertyName) throws TException
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

    public String describeTable(String tableName) throws TException
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
    
    
	public String getVersion()
	{
		return "1";
	}

	public int getStatus()
	{
		return fb_status.ALIVE;
	}

	public String getStatusDetails()
	{
		return null;
	}

	public static void main(String[] args) throws Throwable
	{
		int port = 9160;		

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            public void uncaughtException(Thread t, Throwable e)
            {
                logger_.error("Fatal exception in thread " + t, e);
            }
        });

		try
		{
			CassandraServer peerStorageServer = new CassandraServer();
			peerStorageServer.start();
			Cassandra.Processor processor = new Cassandra.Processor(
					peerStorageServer);
			// Transport
			TServerSocket tServerSocket =  new TServerSocket(port);
			 // Protocol factory
			TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory();
			 // ThreadPool Server
			Options options = new Options();
			options.minWorkerThreads = 64;
			TThreadPoolServer serverEngine = new TThreadPoolServer(processor, tServerSocket, tProtocolFactory);
			serverEngine.serve();

		}
		catch (Exception x)
		{
			System.err.println("UNCAUGHT EXCEPTION IN main()");
			x.printStackTrace();
			System.exit(1);
		}

	}

}
