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
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;

/**
 * The Class CassandraResultSet.
 */
class CassandraResultSet implements ResultSet
{
    
    /** The r set. */
    private CqlResult rSet;
    
    /** The r set iter. */
    private Iterator<CqlRow> rSetIter;
    
    /** The row. */
    private CqlRow row;
    
    /** The values. */
    private List<Object> values = new ArrayList<Object>();
    
    /** The value map. */
    private Map<String, Object> valueMap = new WeakHashMap<String, Object>();

    /**
     * Instantiates a new cassandra result set.
     *
     * @param resultSet the result set
     */
    CassandraResultSet(CqlResult resultSet)
    {
        this.rSet = resultSet;
        rSetIter = rSet.getRowsIterator();
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
     * @return
     * @throws SQLException
     */
    public boolean absolute(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @throws SQLException
     */
    public void afterLast() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @throws SQLException
     */
    public void beforeFirst() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @throws SQLException
     */
    public void cancelRowUpdates() throws SQLException
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
        valueMap.clear();
        values.clear();
        valueMap = null;
        values = null;
    }

    /**
     * @throws SQLException
     */
    public void deleteRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public int findColumn(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public boolean first() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Array getArray(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Array getArray(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public InputStream getAsciiStream(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public InputStream getAsciiStream(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public BigDecimal getBigDecimal(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public BigDecimal getBigDecimal(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public InputStream getBinaryStream(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public InputStream getBinaryStream(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Blob getBlob(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Blob getBlob(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public boolean getBoolean(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public boolean getBoolean(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public byte getByte(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public byte getByte(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public byte[] getBytes(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public byte[] getBytes(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Reader getCharacterStream(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Reader getCharacterStream(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Clob getClob(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Clob getClob(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public int getConcurrency() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public String getCursorName() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Date getDate(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Date getDate(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public Date getDate(int arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public Date getDate(String arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public double getDouble(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public double getDouble(String arg0) throws SQLException
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
     * @param arg0
     * @return
     * @throws SQLException
     */
    public float getFloat(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public float getFloat(String arg0) throws SQLException
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
     * @param arg0
     * @return
     * @throws SQLException
     */
    public int getInt(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public int getInt(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public long getLong(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public long getLong(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public ResultSetMetaData getMetaData() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Reader getNCharacterStream(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Reader getNCharacterStream(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public NClob getNClob(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public NClob getNClob(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public String getNString(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public String getNString(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Object getObject(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Object getObject(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Ref getRef(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Ref getRef(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public int getRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public RowId getRowId(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public RowId getRowId(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public SQLXML getSQLXML(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public SQLXML getSQLXML(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public short getShort(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public short getShort(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public Statement getStatement() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param index
     * @return
     * @throws SQLException
     */
    public String getString(int index) throws SQLException 
    {
        return values.get(index) != null ? values.get(index).toString() : null;
    }

    /**
     * @param name
     * @return
     * @throws SQLException
     */
    public String getString(String name) throws SQLException
    {
        return valueMap.get(name) != null ? valueMap.get(name).toString() : null;
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Time getTime(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Time getTime(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public Time getTime(int arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public Time getTime(String arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Timestamp getTimestamp(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public Timestamp getTimestamp(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @return
     * @throws SQLException
     */
    public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public int getType() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public URL getURL(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public URL getURL(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public InputStream getUnicodeStream(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public InputStream getUnicodeStream(String arg0) throws SQLException
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
     * @throws SQLException
     */
    public void insertRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public boolean isAfterLast() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public boolean isBeforeFirst() throws SQLException
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
    public boolean isFirst() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public boolean isLast() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public boolean last() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @throws SQLException
     */
    public void moveToCurrentRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @throws SQLException
     */
    public void moveToInsertRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public synchronized boolean next() throws SQLException
    {
        if (!values.isEmpty() || !valueMap.isEmpty())
        {
            values.clear();
            valueMap.clear();
        }
        if (rSetIter != null && rSetIter.hasNext())
        {
            row = rSetIter.next();
            List<Column> cols = row.getColumns();
            for (Column col : cols)
            {
                String name = new String(col.getName());
                String value = new String(col.getValue());
                values.add(value);
                valueMap.put(name, value);
            }
            return !(values.isEmpty() && valueMap.isEmpty());
        } 
        else
        {
            return false;
        }
    }

    /**
     * @return
     * @throws SQLException
     */
    public boolean previous() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @throws SQLException
     */
    public void refreshRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @return
     * @throws SQLException
     */
    public boolean relative(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public boolean rowDeleted() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public boolean rowInserted() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public boolean rowUpdated() throws SQLException
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
     * @param arg1
     * @throws SQLException
     */
    public void updateArray(int arg0, Array arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateArray(String arg0, Array arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBlob(int arg0, Blob arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBlob(String arg0, Blob arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBlob(int arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBlob(String arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBoolean(int arg0, boolean arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBoolean(String arg0, boolean arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateByte(int arg0, byte arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateByte(String arg0, byte arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBytes(int arg0, byte[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateBytes(String arg0, byte[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateCharacterStream(int arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateCharacterStream(String arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateClob(int arg0, Clob arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateClob(String arg0, Clob arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateClob(int arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateClob(String arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateDate(int arg0, Date arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateDate(String arg0, Date arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateDouble(int arg0, double arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateDouble(String arg0, double arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateFloat(int arg0, float arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateFloat(String arg0, float arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateInt(int arg0, int arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateInt(String arg0, int arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateLong(int arg0, long arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateLong(String arg0, long arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateNClob(int arg0, NClob arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateNClob(String arg0, NClob arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateNClob(int arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateNClob(String arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateNString(int arg0, String arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateNString(String arg0, String arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @throws SQLException
     */
    public void updateNull(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @throws SQLException
     */
    public void updateNull(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateObject(int arg0, Object arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateObject(String arg0, Object arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateObject(int arg0, Object arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @param arg2
     * @throws SQLException
     */
    public void updateObject(String arg0, Object arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateRef(int arg0, Ref arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateRef(String arg0, Ref arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @throws SQLException
     */
    public void updateRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateRowId(int arg0, RowId arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateRowId(String arg0, RowId arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateShort(int arg0, short arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateShort(String arg0, short arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateString(int arg0, String arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateString(String arg0, String arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateTime(int arg0, Time arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateTime(String arg0, Time arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @param arg0
     * @param arg1
     * @throws SQLException
     */
    public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    /**
     * @return
     * @throws SQLException
     */
    public boolean wasNull() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

}
