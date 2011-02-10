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
package org.apache.cassandra.cql.driver.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

/**
 * Implementation class for {@link Connection}.
 */
public class CassandraConnection implements Connection
{
    
    /** The cassandra con. */
    private org.apache.cassandra.cql.driver.Connection cassandraCon;
    
    /**
     * Instantiates a new cassandra connection.
     *
     * @param url the url
     */
    public CassandraConnection(String url)
    {
        try
        {
            final int splitIndex = url.indexOf('@');
            final String usr_pwd = url.substring(0, splitIndex);
            final String host_port = url.substring(splitIndex + 1);
            final int usr_colonIdx = usr_pwd.lastIndexOf(':');
            final int usr_backwardIdx = usr_pwd.indexOf('/');
            final String userName = usr_pwd.substring(usr_colonIdx + 1, usr_backwardIdx);
            final String password = usr_pwd.substring(usr_backwardIdx + 1);
            final int host_colonIdx = host_port.indexOf(':');
            final String hostName = host_port.substring(0, host_colonIdx);
            final int host_backwardIdx = host_port.indexOf('/');
            final String port = host_port.substring(host_colonIdx + 1, host_backwardIdx);
            final String keyspace = host_port.substring(host_backwardIdx + 1);
            cassandraCon = new org.apache.cassandra.cql.driver.Connection(hostName, Integer.valueOf(port), userName,
                                                                                                                             password);
            final String useQ = "USE " + keyspace;
            cassandraCon.execute(useQ);
        }
        catch (NumberFormatException e)
        {
            throw new DriverResolverException(e.getMessage());
        }
        catch (TTransportException e)
        {
            throw new DriverResolverException(e.getMessage());
        }
        catch (AuthenticationException e)
        {
            throw new DriverResolverException(e.getMessage());
        }
        catch (AuthorizationException e)
        {
            throw new DriverResolverException(e.getMessage());
        }
        catch (TException e)
        {
            throw new DriverResolverException(e.getMessage());
        }
        catch (InvalidRequestException e)
        {
            throw new DriverResolverException(e.getMessage());
        }
        catch (UnavailableException e)
        {
            throw new DriverResolverException(e.getMessage());
        }
        catch (TimedOutException e)
        {
            throw new DriverResolverException(e.getMessage());
        }

    }
    
    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public boolean isWrapperFor(Class<?> arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param <T>
     * @param arg0
     * @return
     * @throws SQLException
     */
    public <T> T unwrap(Class<T> arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }



    /**
     * @throws SQLException
     */
    public void clearWarnings() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * On close of connection.
     *
     * @throws SQLException the sQL exception
     */
    public void close() throws SQLException
    {
        if (cassandraCon != null)
        {
            cassandraCon.close();
        }
    }


    /**
     * @throws SQLException
     */
    public void commit() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public Array createArrayOf(String arg0, Object[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public Blob createBlob() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public Clob createClob() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public NClob createNClob() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public SQLXML createSQLXML() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public Statement createStatement() throws SQLException
    {
        return new CassandraStatement(this.cassandraCon);
    }


    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public Statement createStatement(int arg0, int arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @return
     * @throws SQLException
     */
    public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public Struct createStruct(String arg0, Object[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public boolean getAutoCommit() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public String getCatalog() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public Properties getClientInfo() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public String getClientInfo(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public int getHoldability() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public DatabaseMetaData getMetaData() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public int getTransactionIsolation() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public SQLWarning getWarnings() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @return
     * @throws SQLException
     */
    public boolean isClosed() throws SQLException
    {
        return false;
    }


    /**
     * @return
     * @throws SQLException
     */
    public boolean isReadOnly() throws SQLException
    {
        return false;
    }


    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public boolean isValid(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public String nativeSQL(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public CallableStatement prepareCall(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @return
     * @throws SQLException
     */
    public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @param arg3
     * @return
     * @throws SQLException
     */
    public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param sql
     * @return
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        return new CassandraStatement(this.cassandraCon, sql);
    }


    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException
    {
        return null;
    }


    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @return
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @param arg3
     * @return
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @throws SQLException
     */
    public void releaseSavepoint(Savepoint arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }


    /**
     * @throws SQLException
     */ 
    public void rollback() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }


    /**
     * @param arg0
     * @throws SQLException
     */
    public void rollback(Savepoint arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }


    /**
     * @param arg0
     * @throws SQLException
     */
    public void setAutoCommit(boolean arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }


    /**
     * @param arg0
     * @throws SQLException
     */
    public void setCatalog(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @throws SQLClientInfoException
     */
    public void setClientInfo(Properties arg0) throws SQLClientInfoException
    {
        throw new UnsupportedOperationException("method not supported");
    }


    /**
     * @param arg0
     * @param arg1
     * @throws SQLClientInfoException
     */
    public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException
    {
        throw new UnsupportedOperationException("method not supported");

    }


    /**
     * @param arg0
     * @throws SQLException
     */
    public void setHoldability(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }
    

    /**
     * @param arg0
     * @throws SQLException
     */
    public void setReadOnly(boolean arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }
    

    /**
     * @return
     * @throws SQLException
     */
    public Savepoint setSavepoint() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }
    

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Savepoint setSavepoint(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }
    

    /**
     * @param arg0
     * @throws SQLException
     */
    public void setTransactionIsolation(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }
    

    /**
     * @param arg0
     * @throws SQLException
     */
    public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

}
