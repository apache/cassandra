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
package org.apache.cassandra.cql.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.regex.Pattern;

import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

/**
 * Cassandra statement: implementation class for {@link PreparedStatement}.
 */

class CassandraStatement implements Statement
{
    protected static final Pattern UpdatePattern = Pattern.compile("UPDATE .*", Pattern.CASE_INSENSITIVE);
    
    /** The connection. */
    protected final org.apache.cassandra.cql.jdbc.Connection connection;
    
    /** The cql. */
    protected final String cql;

    /**
     * Constructor using fields.
     * @param con     cassandra connection.
     */
    CassandraStatement(org.apache.cassandra.cql.jdbc.Connection con)
    {
        this(con, null);
    }

    /**
     * Constructor using fields.
     *
     * @param con     cassandra connection
     * @param cql the cql
     */
    CassandraStatement(org.apache.cassandra.cql.jdbc.Connection con, String cql)
    {
        this.connection = con;
        this.cql = cql;
    }

    
    /**
     * @param iface
     * @return
     * @throws SQLException
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }
    
    /**
     * @param <T>
     * @param iface
     * @return
     * @throws SQLException
     */
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param arg0
     * @throws SQLException
     */
    public void addBatch(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @throws SQLException
     */
    public void cancel() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @throws SQLException
     */
    public void clearBatch() throws SQLException
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
     * This is a no-op.
     * @throws SQLException
     */
    public void close() throws SQLException
    {
    }

    
    /**
     * @param query
     * @return
     * @throws SQLException
     */
    public boolean execute(String query) throws SQLException
    {
        try
        {
            return connection.execute(query) != null;
        } 
        catch (InvalidRequestException e)
        {
            throw new SQLException(e.getWhy());
        }
        catch (UnavailableException e)
        {
            throw new SQLException("Cassandra was unavialable", e);
        }
        catch (TimedOutException e)
        {
            throw new SQLException(e.getMessage());
        }
        catch (SchemaDisagreementException e)
        {
            throw new SQLException("schema does not match across nodes, (try again later).");
        }
        catch (TException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    
    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public boolean execute(String arg0, int arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public boolean execute(String arg0, int[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public boolean execute(String arg0, String[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public int[] executeBatch() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param query
     * @return
     * @throws SQLException
     */
    public ResultSet executeQuery(String query) throws SQLException
    {
        try
        {
            CqlResult rSet = connection.execute(query);
            // todo: encapsulate.
            return new CResultSet(rSet, connection.decoder, connection.curKeyspace, connection.curColumnFamily);
        }
        catch (InvalidRequestException e)
        {
            throw new SQLException(e.getWhy());
        }
        catch (UnavailableException e)
        {
            throw new SQLException(e.getMessage());
        }
        catch (TimedOutException e)
        {
            throw new SQLException(e.getMessage());
        }
        catch (SchemaDisagreementException e)
        {
            throw new SQLException("schema does not match across nodes, (try again later).");
        }
        catch (TException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * @param query
     * @return
     * @throws SQLException
     */
    public int executeUpdate(String query) throws SQLException
    {
        if (!UpdatePattern.matcher(query).matches())
            throw new SQLException("Not an update statement.");
        try
        {
            CqlResult rSet = connection.execute(query);
            assert rSet.getType().equals(CqlResultType.VOID);
            // if only we knew how many rows were updated.
            return 0;
        }
        catch (InvalidRequestException e)
        {
            throw new SQLException(e.getWhy());
        }
        catch (UnavailableException e)
        {
            throw new SQLException(e.getMessage());
        }
        catch (TimedOutException e)
        {
            throw new SQLException(e.getMessage());
        }
        catch (SchemaDisagreementException e)
        {
            throw new SQLException("schema does not match across nodes, (try again later).");
        }
        catch (TException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    
    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public int executeUpdate(String arg0, int arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public int executeUpdate(String arg0, int[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public int executeUpdate(String arg0, String[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public int getFetchDirection() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public int getFetchSize() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public ResultSet getGeneratedKeys() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public int getMaxFieldSize() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public int getMaxRows() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public boolean getMoreResults() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public boolean getMoreResults(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public int getQueryTimeout() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public ResultSet getResultSet() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public int getResultSetConcurrency() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public int getResultSetHoldability() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public int getResultSetType() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public int getUpdateCount() throws SQLException
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
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public boolean isPoolable() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param arg0
     * @throws SQLException
     */
    public void setCursorName(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param arg0
     * @throws SQLException
     */
    public void setEscapeProcessing(boolean arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param arg0
     * @throws SQLException
     */
    public void setFetchDirection(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param arg0
     * @throws SQLException
     */
    public void setFetchSize(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param arg0
     * @throws SQLException
     */
    public void setMaxFieldSize(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param arg0
     * @throws SQLException
     */
    public void setMaxRows(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param arg0
     * @throws SQLException
     */
    public void setPoolable(boolean arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param arg0
     * @throws SQLException
     */
    public void setQueryTimeout(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }
}
