package org.apache.cassandra.cql.jdbc;
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


import org.apache.cassandra.db.marshal.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CassandraPreparedStatement extends CassandraStatement implements PreparedStatement
{
//    private static final Pattern Parameterizable = Pattern.compile("(SELECT|DELETE|UPDATE)\\s+.*", Pattern.CASE_INSENSITIVE);
    private static final Pattern Select = Pattern.compile("SELECT[\\s+FIRST\\s+\\d+]?[\\s+REVERSED]?\\s+(.*)WHERE\\s+(.*)", Pattern.CASE_INSENSITIVE);
    private static final Pattern Update = Pattern.compile("UPDATE\\s+\\w+.*\\s+SET\\s+(.*)\\s+WHERE KEY(.*)", Pattern.CASE_INSENSITIVE);
    private static final Pattern Delete = Pattern.compile("DELETE\\s+(.*)\\s+FROM\\s+\\w+\\s+WHERE KEY(.*)", Pattern.CASE_INSENSITIVE);
    
    // current set of bound variables.
    private final Map<Integer, Object> variables = new HashMap<Integer, Object>();
    
    // for batching. These are the queries that have been batched and not executed.
    private final List<String> queries = new ArrayList<String>();
    
    CassandraPreparedStatement(Connection con, String cql)
    {
        super(con, cql);
    }
    
    // impl specific methods start here.

    // double quotes strings (in parameters)
    private static String makeCqlString(String s) 
    {
        // escape any single-quotes with double single-quotes.
        return s.replaceAll("\'", "\'\'");
    }
    
    // null type means just call param.toString() and quote it (default for keys).
    private static String applySimpleBindings(String q, AbstractType type, ParameterIterator params) throws SQLException
    {
        assert type != null;
        // we need to keep track of whether or not we are between quotes and ignore any question marks within them
        // so that they are not substituted.  
        StringBuffer sb = new StringBuffer();
        boolean between = false;
        for (char c : q.toCharArray())
        {
            if (c == '\'')
                between = !between;
            if (between)
                sb.append(c);
            else if (c == '?') // !between if we got here.
            {
                try
                {
                    // perform substitution!
                    Object param = params.nextParam();
                    String stringParam = type == null ? param.toString() : type.toString(param);
                    stringParam = makeCqlString(stringParam);
                    if (type == null || type.needsQuotes())
                        stringParam = "'" + stringParam + "'";
                    sb.append(stringParam);
                }
                catch (ClassCastException ex)
                {
                    throw new SQLException("Mismatched types: " + ex.getLocalizedMessage());
                }
            }
            else
                sb.append(c);
                
        }
        return sb.toString();
    }
    
    private static String applyDualBindings(String q, AbstractType ltype, AbstractType rtype, ParameterIterator params) throws SQLException
    {
        StringBuffer sb = new StringBuffer();
        boolean between = false;
        boolean left = true; // we always start on the left-hand side of a statement. we switch state if we reach a comma and we are not between.
        for (char c : q.toCharArray())
        {
            if (c == '\'')
                between = !between;
            if (c == '=' && !between)
                left = false;
            if (c == ',' && !between)
                left = true;
            
            if (c == '?' && !between)
            {
                try
                {
                    Object param = params.nextParam();
                    AbstractType type = left ? ltype : rtype;
                    String stringParam = makeCqlString(type.toString(param));
                    if (type.needsQuotes())
                        stringParam = "'" + stringParam + "'";
                    sb.append(stringParam);
                }
                catch (ClassCastException ex)
                {
                    throw new SQLException("Mismatched types: " + ex.getLocalizedMessage());
                }
            }
            else
                sb.append(c);
        }
        return sb.toString();
    }
    
    /** applies current bindings to produce a string that can be sent to the server. */
    public synchronized String makeCql() throws SQLException
    { 
        // break cql up
        Matcher m;
        m = Delete.matcher(cql);
        if (m.matches())
            return makeDelete(m.end(1));
        m = Update.matcher(cql);
        if (m.matches())
            return makeUpdate(m.end(1));
        m = Select.matcher(cql);
        if (m.matches())
            return makeSelect(m.end(1));
        
        // if we made it this far, cql is not parameterizable. this isn't bad, there is just nothing to be done.
        return cql;
    }
    
    // subs parameters into a delete statement.
    private String makeDelete(int pivot) throws SQLException
    { 
        String keyspace = connection.getKeyspace(cql);
        String columnFamily = connection.getColumnFamily(cql);
        ParameterIterator params = new ParameterIterator();
        String left = cql.substring(0, pivot);
        AbstractType leftType = connection.decoder.getComparator(keyspace, columnFamily);
        if (leftType == null)
            throw new SQLException("Could not find comparator for " + keyspace + "." + columnFamily);
        left = applySimpleBindings(left, leftType, params);
        String right = cql.substring(pivot);
        AbstractType keyVald = connection.decoder.getKeyValidator(keyspace, columnFamily);
        if (keyVald == null)
            throw new SQLException("Could not find key validator for " + keyspace + "." + columnFamily);
        right = applySimpleBindings(right, keyVald, params);
        return left + right;
    }
    
    // subs parameters into a select statement.
    private String makeSelect(int pivot) throws SQLException
    { 
        String keyspace = connection.getKeyspace(cql);
        String columnFamily = connection.getColumnFamily(cql);
        ParameterIterator params = new ParameterIterator();
        String left = cql.substring(0, pivot);
        AbstractType leftType = connection.decoder.getComparator(keyspace, columnFamily);
        if (leftType == null)
            throw new SQLException("Could not find comparator for " + keyspace + "." + columnFamily);
        left = applySimpleBindings(left, leftType, params);
        String right = cql.substring(pivot);
        AbstractType keyVald = connection.decoder.getKeyValidator(keyspace, columnFamily);
        if (keyVald == null)
            throw new SQLException("Could not find key validator for " + keyspace + "." + columnFamily);
        right = applySimpleBindings(right, keyVald, params);
        return left + right;
    }
    
    // subs parameters into an update statement.
    private String makeUpdate(int pivot) throws SQLException
    {
        // this one is a little bit different. left contains key=value pairs. we use the comparator for the left side,
        // the validator for the right side.  right side is treated as a key.
        String keyspace = connection.getKeyspace(cql);
        String columnFamily = connection.getColumnFamily(cql);
        ParameterIterator params = new ParameterIterator();
        String left = cql.substring(0, pivot);
        AbstractType leftComp = connection.decoder.getComparator(keyspace, columnFamily);
        if (leftComp == null)
            throw new SQLException("Could not find comparator for " + keyspace + "." + columnFamily);
        AbstractType leftVald = connection.decoder.getComparator(keyspace, columnFamily);
        if (leftVald == null)
            throw new SQLException("Could not find validator for " + keyspace + "." + columnFamily);
        left = applyDualBindings(left, leftComp, leftVald, params);
        String right = cql.substring(pivot);
        AbstractType keyVald = connection.decoder.getKeyValidator(keyspace, columnFamily);
        if (keyVald == null)
            throw new SQLException("Could not find key validator for " + keyspace + "." + columnFamily);
        right = applySimpleBindings(right, keyVald, params);
        return left + right; 
    }
    
    
    
    // standard API methods follow.
    
    public void addBatch() throws SQLException
    {
        queries.add(makeCql());
    }

    public synchronized void clearParameters() throws SQLException
    {
        variables.clear();
    }

    public boolean execute() throws SQLException
    {
        return this.cql != null && super.execute(makeCql());
    }
    
    public ResultSet executeQuery() throws SQLException
    {
        return this.cql != null ? super.executeQuery(makeCql()) : null;
    }
    
    public int executeUpdate() throws SQLException
    {
        String q = makeCql();
        if (!UpdatePattern.matcher(q).matches())
            throw new SQLException("Not an update statement.");
        super.execute(q);
        // we don't know how many rows were updated.
        return 0;
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        // todo: current impl of RSMD relies on knowing the results. implementing this will require refactoring CRSMD into 
        // two classes: the first will be an implementation whose methods don't rely on knowing the results, the second
        // will implement the full CRSMD interfae and extend or compose the first.
        throw new SQLFeatureNotSupportedException("PreparedStatement.getMetaData() hasn't been implemented yet.");
    }

    public void setByte(int parameterIndex, byte x) throws SQLException
    {
        setObject(parameterIndex, new byte[]{x});
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException
    {
        setObject(parameterIndex, ByteBuffer.wrap(x));
    }

    public void setInt(int parameterIndex, int x) throws SQLException
    {
        setObject(parameterIndex, new BigInteger(Integer.toString(x)));
    }

    public void setLong(int parameterIndex, long x) throws SQLException
    {
        setObject(parameterIndex, x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException
    {
        setString(parameterIndex, value);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException
    {
        variables.put(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException
    {
        setInt(parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException
    {
        setObject(parameterIndex, x);
    }
    
    
    // everything below here is not implemented and will let you know about it.
    
    
    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        throw new SQLFeatureNotSupportedException("PreparedStatement.getParameterMetaData() hasn't been implemented yet.");
    }

    public void setArray(int parameterIndex, Array x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setDate(int parameterIndex, Date x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setDouble(int parameterIndex, double x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setFloat(int parameterIndex, float x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }
    
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }
    
    public void setNull(int parameterIndex, int sqlType) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }
    
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }
    
    public void setTime(int parameterIndex, Time x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setURL(int parameterIndex, URL x) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
    {
        throw new SQLFeatureNotSupportedException("method not supported");
    }
    
    
    // done with API methods.
    
    
    
    // provides a way to iterate through the parameters. it will blow up if it discovers any missing parameters.
    // not thread-safe.
    private class ParameterIterator
    {
        private Map<Integer, Object> params = new HashMap<Integer, Object>(variables);
        private int index = 1;
        
        // throws SQLException if a parameter is not specified.
        private Object nextParam() throws SQLException
        {
            Object p = params.get(index++);
            if (p == null)
                throw new SQLException("No parameter bound to " + (index-1));
            return p;
        }

    }
}
