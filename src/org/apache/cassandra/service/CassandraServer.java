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
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql.common.CqlResult;
import org.apache.cassandra.cql.driver.CqlDriver;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.ColumnFamilyNotDefinedException;
import org.apache.cassandra.utils.LogUtil;
import org.apache.thrift.TException;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class CassandraServer implements Cassandra.Iface
{

	private static Logger logger_ = Logger.getLogger(CassandraServer.class);

    /*
      * Handle to the storage service to interact with the other machines in the
      * cluster.
      */
	protected StorageService storageService;

	protected CassandraServer()
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
	
	private void validateTable(String table) throws CassandraException
	{
		if ( !DatabaseDescriptor.getTables().contains(table) )
		{
			throw new CassandraException("Table " + table + " does not exist in this schema.");
		}
	}
    
	protected ColumnFamily readColumnFamily(ReadCommand command) throws CassandraException, TException, IOException, ColumnFamilyNotDefinedException, TimeoutException
    {
        validateTable(command.table);
        String[] values = RowMutation.getColumnAndColumnFamily(command.columnFamilyColumn);
        if( values.length < 1 )
        {
            throw new CassandraException("Empty column Family is invalid.");
        }
        Table table = Table.open(command.table);
        if (!table.getColumnFamilies().contains(values[0]))
        {
            throw new CassandraException("Column Family " + values[0] + " is invalid.");
        }

        Row row = StorageProxy.readProtocol(command, StorageService.ConsistencyLevel.WEAK);
        if (row == null)
        {
            return null;
        }
        return row.getColumnFamily(values[0]);
	}

    public List<column_t> thriftifyColumns(Collection<IColumn> columns)
    {
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

    public List<column_t> get_columns_since(String tablename, String key, String columnFamily_column, long timeStamp) throws CassandraException,TException
	{
        long startTime = System.currentTimeMillis();
		try
		{
			ColumnFamily cfamily = readColumnFamily(new ReadCommand(tablename, key, columnFamily_column, timeStamp));
            String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
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

            return thriftifyColumns(columns);
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
        finally
        {
            logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime) + " ms.");
        }
	}
	

    public List<column_t> get_slice_by_names(String tablename, String key, String columnFamily, List<String> columnNames) throws CassandraException, TException
    {
        long startTime = System.currentTimeMillis();
		try
		{
			validateTable(tablename);
			ColumnFamily cfamily = readColumnFamily(new ReadCommand(tablename, key, columnFamily, columnNames));
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

            return thriftifyColumns(columns);
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
		finally
        {
            logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime) + " ms.");
        }
    }
    
    public List<column_t> get_slice(String tablename, String key, String columnFamily_column, int start, int count) throws CassandraException,TException
	{
        long startTime = System.currentTimeMillis();
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
			ColumnFamily cfamily = readColumnFamily(new ReadCommand(tablename, key, columnFamily_column, start, count));
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

            return thriftifyColumns(columns);
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
        finally
        {
            logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime) + " ms.");
        }
	}
    
    public column_t get_column(String tablename, String key, String columnFamily_column) throws CassandraException,TException
    {
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
			ColumnFamily cfamily = readColumnFamily(new ReadCommand(tablename, key, columnFamily_column, -1, Integer.MAX_VALUE));
			if (cfamily == null || values.length < 2)
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

            assert columns.size() == 1;
            IColumn column = columns.iterator().next();
            if (column.isMarkedForDelete())
            {
                return null;
            }
            return new column_t(column.name(), column.value(), column.timestamp());
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
    }
    

    public int get_column_count(String tablename, String key, String columnFamily_column) throws CassandraException
	{
    	int count = -1;
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
			ColumnFamily cfamily = readColumnFamily(new ReadCommand(tablename, key, columnFamily_column, -1, Integer.MAX_VALUE));
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

    public void insert(String tablename, String key, String columnFamily_column, byte[] cellData, long timestamp)
	{
		try
		{
			validateTable(tablename);
			RowMutation rm = new RowMutation(tablename, key.trim());
			rm.add(columnFamily_column, cellData, timestamp);
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

    public boolean remove(String tablename, String key, String columnFamily_column, long timestamp, boolean block)
	{
        logger_.debug("remove");
        RowMutation rm = new RowMutation(tablename, key.trim());
        rm.delete(columnFamily_column, timestamp);
        if (block) {
            return StorageProxy.insertBlocking(rm);
        } else {
            StorageProxy.insert(rm);
            return true;
        }
	}

    public List<superColumn_t> get_slice_super_by_names(String tablename, String key, String columnFamily, List<String> superColumnNames) throws CassandraException, TException
    {
        long startTime = System.currentTimeMillis();
		
		try
		{
			validateTable(tablename);
			ColumnFamily cfamily = readColumnFamily(new ReadCommand(tablename, key, columnFamily, superColumnNames));
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

            return thriftifySuperColumns(columns);
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
        finally
        {
            logger_.debug("get_slice2: " + (System.currentTimeMillis() - startTime) + " ms.");
        }
    }

    private List<superColumn_t> thriftifySuperColumns(Collection<IColumn> columns)
    {
        ArrayList<superColumn_t> thriftSuperColumns = new ArrayList<superColumn_t>(columns.size());
        for (IColumn column : columns)
        {
            if (column.getSubColumns().size() == 0)
            {
                continue;
            }
            thriftSuperColumns.add(new superColumn_t(column.name(), thriftifyColumns(column.getSubColumns())));
        }
        return thriftSuperColumns;
    }


    public List<superColumn_t> get_slice_super(String tablename, String key, String columnFamily_superColumnName, int start, int count) throws CassandraException
    {
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_superColumnName);
			ColumnFamily cfamily = readColumnFamily(new ReadCommand(tablename, key, columnFamily_superColumnName, start, count));
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

            return thriftifySuperColumns(columns);
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
    }
    
    public superColumn_t get_superColumn(String tablename, String key, String columnFamily_column) throws CassandraException
    {
		try
		{
	        String[] values = RowMutation.getColumnAndColumnFamily(columnFamily_column);
			ColumnFamily cfamily = readColumnFamily(new ReadCommand(tablename, key, columnFamily_column, -1, Integer.MAX_VALUE));
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

            assert columns.size() == 1;
            IColumn column = columns.iterator().next();
            if (column.getSubColumns().size() == 0)
            {
                logger_	.info("ERROR Columns are missing.....: "
                               + "   key:" + key
                                + "  ColumnFamily:" + values[0]);
                throw new CassandraException("ERROR Columns are missing.....: " + "   key:" + key + "  ColumnFamily:" + values[0]);
            }

            return new superColumn_t(column.name(), thriftifyColumns(column.getSubColumns()));
		}
		catch (Exception ex)
		{
			String exception = LogUtil.throwableToString(ex);
			logger_.info( exception );
			throw new CassandraException(exception);
		}
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
            return "1";
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
    
    // main method moved to CassandraDaemon
}
