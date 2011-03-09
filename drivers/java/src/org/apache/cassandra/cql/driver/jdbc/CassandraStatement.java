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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.cassandra.cql.driver.Results;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

/**
 * Cassandra statement: implementation class for {@link PreparedStatement}.
 */

class CassandraStatement implements PreparedStatement
{
    
    /** The connection. */
    private org.apache.cassandra.cql.driver.Connection connection;
    
    /** The cql. */
    private String cql;

    /**
     * Constructor using fields.
     * @param con     cassandra connection.
     */
    CassandraStatement(org.apache.cassandra.cql.driver.Connection con)
    {
        this.connection = con;
    }

    /**
     * Constructor using fields.
     *
     * @param con     cassandra connection
     * @param cql the cql
     */
    CassandraStatement(org.apache.cassandra.cql.driver.Connection con, String cql)
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
     * @throws SQLException
     */
    public void close() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

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
            return new CassandraResultSet(rSet, connection.decoder, connection.curKeyspace, connection.curColumnFamily);
        }
        catch (InvalidRequestException e)
        {
            throw new SQLException(e.getMessage());
        }
        catch (UnavailableException e)
        {
            throw new SQLException(e.getMessage());
        }
        catch (TimedOutException e)
        {
            throw new SQLException(e.getMessage());
        }
        catch (TException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    
    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public int executeUpdate(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
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

    
    /**
     * @throws SQLException
     */
    public void addBatch() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @throws SQLException
     */
    public void clearParameters() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @return
     * @throws SQLException
     */
    public boolean execute() throws SQLException
    {
        return this.cql != null ? execute(cql) : false;
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public ResultSet executeQuery() throws SQLException
    {
        return this.cql != null ? executeQuery(cql) : null;
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public int executeUpdate() throws SQLException
    {
        return 0;
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return null;
    }

    
    /**
     * @return
     * @throws SQLException
     */
    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        return null;
    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setArray(int parameterIndex, Array x) throws SQLException
    {
    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @param length
     * @throws SQLException
     */
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @param length
     * @throws SQLException
     */
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @param length
     * @throws SQLException
     */
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @param length
     * @throws SQLException
     */
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setBlob(int parameterIndex, Blob x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param inputStream
     * @throws SQLException
     */
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param inputStream
     * @param length
     * @throws SQLException
     */
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setBoolean(int parameterIndex, boolean x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setByte(int parameterIndex, byte x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setBytes(int parameterIndex, byte[] x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param reader
     * @throws SQLException
     */
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param reader
     * @param length
     * @throws SQLException
     */
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param reader
     * @param length
     * @throws SQLException
     */
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setClob(int parameterIndex, Clob x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param reader
     * @throws SQLException
     */
    public void setClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param reader
     * @param length
     * @throws SQLException
     */
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setDate(int parameterIndex, Date x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @param cal
     * @throws SQLException
     */
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setDouble(int parameterIndex, double x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setFloat(int parameterIndex, float x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setInt(int parameterIndex, int x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setLong(int parameterIndex, long x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param value
     * @throws SQLException
     */
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param value
     * @param length
     * @throws SQLException
     */
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param value
     * @throws SQLException
     */
    public void setNClob(int parameterIndex, NClob value) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param reader
     * @throws SQLException
     */
    public void setNClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param reader
     * @param length
     * @throws SQLException
     */
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param value
     * @throws SQLException
     */
    public void setNString(int parameterIndex, String value) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param sqlType
     * @throws SQLException
     */
    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param sqlType
     * @param typeName
     * @throws SQLException
     */
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setObject(int parameterIndex, Object x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @param targetSqlType
     * @throws SQLException
     */
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @param targetSqlType
     * @param scaleOrLength
     * @throws SQLException
     */
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setRef(int parameterIndex, Ref x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setRowId(int parameterIndex, RowId x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param xmlObject
     * @throws SQLException
     */
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setShort(int parameterIndex, short x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setString(int parameterIndex, String x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setTime(int parameterIndex, Time x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @param cal
     * @throws SQLException
     */
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");

    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param parameterIndex
     * @param x
     * @param cal
     * @throws SQLException
     */
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    public void setURL(int parameterIndex, URL x) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    
    /**
     * @param parameterIndex
     * @param x
     * @param length
     * @throws SQLException
     */
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

}
