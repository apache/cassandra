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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
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
import java.util.UUID;
import java.util.WeakHashMap;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;

// todo: get by index is off by one.
/**
 * The Class CassandraResultSet.
 */
class CassandraResultSet implements ResultSet
{
    
    /** The r set. */
    private final CqlResult rSet; 
    
    private final ColumnDecoder decoder;
    private final String keyspace;
    private final String columnFamily;
    
    /** The r set iter. */
    private Iterator<CqlRow> rSetIter;
    
    // the current row key when iterating through results.
    private byte[] curRowKey = null;
    
    /** The values. */
    private List<TypedColumn> values = new ArrayList<TypedColumn>();
    
    /** The value map. */
    private Map<String, Object> valueMap = new WeakHashMap<String, Object>();
    
    private final RsMetaData meta;
    
    private final AbstractType nameType;
    private final AbstractType valueType;

    /**
     * Instantiates a new cassandra result set.
     *
     * @param resultSet the result set
     */
    CassandraResultSet(CqlResult resultSet, ColumnDecoder decoder, String keyspace, String columnFamily)
    {
        this.rSet = resultSet;
        this.decoder = decoder;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        rSetIter = rSet.getRowsIterator();
        meta = new RsMetaData();
        nameType = decoder.getComparator(keyspace, columnFamily, ColumnDecoder.Specifier.Comparator, null);
        valueType = decoder.getComparator(keyspace, columnFamily, ColumnDecoder.Specifier.Validator, null);
    }

    /**
     * @param iface
     * @return
     * @throws SQLException
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return false;
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
     * @param index
     * @return
     * @throws SQLException
     */
    public byte[] getBytes(int index) throws SQLException
    {
        return values.get(index) != null ? ((ByteBuffer)values.get(index).getValue()).array() : null;
    }

    /**
     * @param name
     * @return
     * @throws SQLException
     */
    public byte[] getBytes(String name) throws SQLException
    {
        String nameAsString = decoder.colNameAsString(keyspace, columnFamily, name);
        return valueMap.get(nameAsString) != null ? ((ByteBuffer)valueMap.get(nameAsString)).array() : null;
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
     * @param index
     * @return
     * @throws SQLException
     */
    public int getInt(int index) throws SQLException
    {
        return values.get(index) != null ? ((BigInteger)values.get(index).getValue()).intValue() : null;
    }

    /**
     * @param name
     * @return
     * @throws SQLException
     */
    public int getInt(String name) throws SQLException
    {
        String nameAsString = decoder.colNameAsString(keyspace, columnFamily, name);
        return valueMap.get(nameAsString) != null ? ((BigInteger)valueMap.get(nameAsString)).intValue() : null;
    }

    /**
     * @param index
     * @return
     * @throws SQLException
     */
    public long getLong(int index) throws SQLException
    {
        return values.get(index) != null ? (Long)values.get(index).getValue() : null;
    }

    /**
     * @param name
     * @return
     * @throws SQLException
     */
    public long getLong(String name) throws SQLException
    {
        String nameAsString = decoder.colNameAsString(keyspace, columnFamily, name);
        return valueMap.get(nameAsString) != null ? (Long)valueMap.get(nameAsString) : null;
    }

    /**
     * @return
     * @throws SQLException
     */
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return meta;
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
     * @param index
     * @return
     * @throws SQLException
     */
    public Object getObject(int index) throws SQLException
    {
        return values.get(index) == null ? null : values.get(index).getValue();
    }

    /**
     * @param name
     * @return
     * @throws SQLException
     */
    public Object getObject(String name) throws SQLException
    {
        String nameAsString = decoder.colNameAsString(keyspace, columnFamily, name);
        return valueMap.get(nameAsString);
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
        return values.get(index) != null ? ColumnDecoder.colValueAsString(values.get(index).getValue()) : null;
    }

    /**
     * @param name
     * @return
     * @throws SQLException
     */
    public String getString(String name) throws SQLException
    {
        String nameAsString = this.decoder.colNameAsString(this.keyspace, this.columnFamily, name);
        return valueMap.get(nameAsString) != null ? ColumnDecoder.colValueAsString(valueMap.get(nameAsString)) : null;
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
     * @param <T>
     * @param iface
     * @return
     * @throws SQLException
     */
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        throw new SQLException("Unsupported unwrap interface: " + iface.getSimpleName());
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
            CqlRow row = rSetIter.next();
            curRowKey = row.getKey();
            List<Column> cols = row.getColumns();
            for (Column col : cols)
            {
                byte[] name = col.getName();
                byte[] value = col.getValue();
                TypedColumn c = decoder.makeCol(keyspace, columnFamily, name, value);
                values.add(c);
                valueMap.put(decoder.colNameAsString(keyspace, columnFamily, name), c.getValue());
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
    
    /**
     * RSMD implementation. Except where explicitly noted the metadata returned refers to the column 
     * values, not the column names. There is an additional interface that describes column name 
     * meta information.
     */
    private class RsMetaData implements CassandraResultSetMetaData, ResultSetMetaData
    {
        public byte[] getKey()
        {
            return curRowKey;
        }

        public boolean isNameCaseSensitive(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            if (nameType instanceof ColumnMetaData)
                return ((ColumnMetaData)nameType).isCaseSensitive();
            else 
                return nameType.getType().equals(String.class);
        }
        
        public boolean isValueCaseSensitive(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            if (valueType instanceof ColumnMetaData)
                return ((ColumnMetaData)valueType).isCaseSensitive();
            else 
                return valueType.getType().equals(String.class);
        }

        public boolean isNameCurrency(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            if (nameType instanceof ColumnMetaData)
                return ((ColumnMetaData)nameType).isCurrency();
            else
                return false;
        }
        
        public boolean isValueCurrency(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            if (valueType instanceof ColumnMetaData)
                return ((ColumnMetaData)valueType).isCurrency();
            else
                return false;
        }

        public boolean isNameSigned(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return Utils.isTypeSigned(nameType);
        }
        
        public boolean isValueSigned(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return Utils.isTypeSigned(valueType);
        }

        public int getNameDisplaySize(int column) throws SQLException
        {
            return getColumnName(column).length();
        }

        public int getValueDisplaySize(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return values.get(column).getValueString().length();
        }

        public int getNamePrecision(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            TypedColumn col = values.get(column);
            if (nameType instanceof ColumnMetaData)
                return ((ColumnMetaData)nameType).getPrecision();
            else if (nameType.getType().equals(String.class))
                return col.getNameString().length();
            else if (nameType == BytesType.instance)
                return col.getNameString().length();
            else if (nameType.getType().equals(UUID.class))
                return 36; // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            else if (nameType == LongType.instance)
                return 19; // number of digits in 2**63-1.
            else 
                return 0;
        }
        
        public int getValuePrecision(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            TypedColumn col = values.get(column);
            if (valueType instanceof ColumnMetaData)
                return ((ColumnMetaData)valueType).getPrecision();
            else if (valueType.getType().equals(String.class))
                return col.getValueString().length();
            else if (valueType == BytesType.instance)
                return col.getValueString().length();
            else if (valueType.getType().equals(UUID.class))
                return 36; // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            else if (valueType == LongType.instance)
                return 19; // number of digits in 2**63-1.
            else 
                return 0;
        }

        public int getNameScale(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return Utils.getTypeScale(nameType);
        }
        
        public int getValueScale(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return Utils.getTypeScale(valueType);
        }

        public int getNameType(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return Utils.getJdbcType(nameType);
        }
        
        public int getValueType(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return Utils.getJdbcType(valueType);
        }
        
        public String getNameTypeName(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return nameType.getClass().getSimpleName();
        }
        
        public String getValueTypeName(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return valueType.getClass().getSimpleName();
        }

        public String getNameClassName(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return nameType.getType().getName();
        }

        public String getValueClassName(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return valueType.getType().getName();
        }
        
        //
        // ResultSetMetaData
        //
        
        private void checkIndex(int i) throws SQLException
        {
            if (i >= values.size())
                throw new SQLException("Invalid column index " + i);
        }
        
        public int getColumnCount() throws SQLException
        {
            return values.size();
        }

        public boolean isAutoIncrement(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return valueType instanceof CounterColumnType; // todo: check Value is correct.
        }

        public boolean isCaseSensitive(int column) throws SQLException
        {
            return isValueCaseSensitive(column);
        }

        public boolean isSearchable(int column) throws SQLException
        {
            return false;
        }

        public boolean isCurrency(int column) throws SQLException
        {
            return isValueCurrency(column);
        }

        public int isNullable(int column) throws SQLException
        {
            // no such thing as null in cassandra.
            return ResultSetMetaData.columnNullableUnknown;
        }

        public boolean isSigned(int column) throws SQLException
        {
            return isValueSigned(column);
        }

        public int getColumnDisplaySize(int column) throws SQLException
        {
            return getValueDisplaySize(column);
        }

        public String getColumnLabel(int column) throws SQLException
        {
            return getColumnName(column);
        }

        public String getColumnName(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return values.get(column).getNameString();
        }

        public String getSchemaName(int column) throws SQLException
        {
            return keyspace;
        }

        public int getPrecision(int column) throws SQLException
        {
            return getValuePrecision(column);
        }

        public int getScale(int column) throws SQLException
        {
            return getValueScale(column);
        }

        public String getTableName(int column) throws SQLException
        {
            return columnFamily;
        }

        public String getCatalogName(int column) throws SQLException
        {
            throw new SQLFeatureNotSupportedException("Cassandra has no catalogs");
        }

        public int getColumnType(int column) throws SQLException
        {
            return getValueType(column);
        }

        // todo: spec says "database specific type name". this means the abstract type.
        public String getColumnTypeName(int column) throws SQLException
        {
            return getValueTypeName(column);
        }

        public boolean isReadOnly(int column) throws SQLException
        {
            return column == 0;
        }

        public boolean isWritable(int column) throws SQLException
        {
            return column > 0;
        }

        public boolean isDefinitelyWritable(int column) throws SQLException
        {
            return isWritable(column);
        }

        public String getColumnClassName(int column) throws SQLException
        {
            return getValueClassName(column);
        }

        // todo: once the kinks are worked out, allow unwrapping as CassandraResultSetMetaData.
        public <T> T unwrap(Class<T> iface) throws SQLException
        {
            if (iface.equals(CassandraResultSetMetaData.class))
                return (T)this;
            else
                throw new SQLFeatureNotSupportedException("No wrappers");
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException
        {
            return CassandraResultSetMetaData.class.isAssignableFrom(iface);
        }
    }
}
