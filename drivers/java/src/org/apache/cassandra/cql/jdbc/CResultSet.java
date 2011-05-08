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
import java.util.*;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CResultSet implements CassandraResultSet
{
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
    // TODO should map <String, TypedColumn> so we can throw appropriate exception if user asks for non-existant column name
    private Map<String, Object> valueMap = new WeakHashMap<String, Object>();
    
    private final CResultSetMetaData meta;
    
    private boolean wasNull;

    /**
     * Instantiates a new cassandra result set.
     *
     * @param resultSet the result set
     */
    CResultSet(CqlResult resultSet, ColumnDecoder decoder, String keyspace, String columnFamily)
    {
        this.decoder = decoder;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        rSetIter = resultSet.getRowsIterator();
        meta = new CResultSetMetaData();
    }

    public byte[] getKey()
    {
        return curRowKey;
    }

    public TypedColumn getColumn(int i)
    {
        return values.get(i);
    }

    public TypedColumn getColumn(String name)
    {
        throw new UnsupportedOperationException("need to convert valueMap to TypedColumn first");
    }

    public boolean absolute(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void afterLast() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void beforeFirst() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void cancelRowUpdates() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void clearWarnings() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void close() throws SQLException
    {
        valueMap.clear();
        values.clear();
        valueMap = null;
        values = null;
    }

    public void deleteRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public int findColumn(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean first() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Array getArray(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Array getArray(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public InputStream getAsciiStream(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public InputStream getAsciiStream(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public BigDecimal getBigDecimal(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public BigDecimal getBigDecimal(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public InputStream getBinaryStream(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public InputStream getBinaryStream(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Blob getBlob(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Blob getBlob(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean getBoolean(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean getBoolean(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public byte getByte(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public byte getByte(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public byte[] getBytes(int index) throws SQLException
    {
        TypedColumn column = values.get(index - 1);
        assert column != null;
        Object value = column.getValue();
        wasNull = value == null;
        return value == null ? null : ByteBufferUtil.clone((ByteBuffer) value).array();
    }

    public byte[] getBytes(String name) throws SQLException
    {
        String nameAsString = decoder.colNameAsString(keyspace, columnFamily, name);
        Object value = valueMap.get(nameAsString);
        wasNull = value == null;
        return value == null ? null : ByteBufferUtil.clone((ByteBuffer) value).array();
    }

    public Reader getCharacterStream(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Reader getCharacterStream(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Clob getClob(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

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

    public Date getDate(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Date getDate(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Date getDate(int arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Date getDate(String arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public double getDouble(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public double getDouble(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public int getFetchDirection() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public int getFetchSize() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public float getFloat(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public float getFloat(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public int getHoldability() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public int getInt(int index) throws SQLException
    {
        TypedColumn column = values.get(index - 1);
        assert column != null;
        Object value = column.getValue();
        wasNull = value == null;
        return value == null ? 0 : ((BigInteger) value).intValue();
    }

    public int getInt(String name) throws SQLException
    {
        String nameAsString = decoder.colNameAsString(keyspace, columnFamily, name);
        Object value = valueMap.get(nameAsString);
        wasNull = value == null;
        return value == null ? 0 : ((BigInteger) value).intValue();
    }

    public long getLong(int index) throws SQLException
    {
        assert values != null;
        TypedColumn column = values.get(index - 1);
        assert column != null;
        Object value = column.getValue();
        wasNull = value == null;
        return value == null ? 0 : (Long) value;
    }

    /**
     * @param name
     * @return
     * @throws SQLException
     */
    public long getLong(String name) throws SQLException
    {
        String nameAsString = decoder.colNameAsString(keyspace, columnFamily, name);
        Object value = valueMap.get(nameAsString);
        wasNull = value == null;
        return value == null ? 0 : (Long) value;
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        return meta;
    }

    public Reader getNCharacterStream(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Reader getNCharacterStream(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public NClob getNClob(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public NClob getNClob(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public String getNString(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public String getNString(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Object getObject(int index) throws SQLException
    {
        TypedColumn column = values.get(index - 1);
        assert column != null;
        Object value = column.getValue();
        wasNull = value == null;
        return value;
    }

    public Object getObject(String name) throws SQLException
    {
        String nameAsString = decoder.colNameAsString(keyspace, columnFamily, name);
        Object value = valueMap.get(nameAsString);
        wasNull = value == null;
        return value;
    }

    public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Ref getRef(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Ref getRef(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public int getRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public RowId getRowId(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public RowId getRowId(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public SQLXML getSQLXML(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public SQLXML getSQLXML(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public short getShort(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public short getShort(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Statement getStatement() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public String getString(int index) throws SQLException
    {
        TypedColumn column = values.get(index - 1);
        assert column != null;
        Object value = column.getValue();
        wasNull = value == null;
        return value == null ? null : ColumnDecoder.colValueAsString(value);
    }

    public String getString(String name) throws SQLException
    {
        String nameAsString = this.decoder.colNameAsString(this.keyspace, this.columnFamily, name);
        Object value = valueMap.get(nameAsString);
        wasNull = value == null;
        return value == null ? null : ColumnDecoder.colValueAsString(value);
    }

    public Time getTime(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Time getTime(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Time getTime(int arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Time getTime(String arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Timestamp getTimestamp(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Timestamp getTimestamp(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public int getType() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public URL getURL(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public URL getURL(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public InputStream getUnicodeStream(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public InputStream getUnicodeStream(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public SQLWarning getWarnings() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void insertRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean isAfterLast() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean isBeforeFirst() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean isClosed() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean isFirst() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean isLast() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean last() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void moveToCurrentRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void moveToInsertRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }
    
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        if (iface.equals(CassandraResultSet.class))
            return (T) this;
        throw new SQLException("Unsupported unwrap interface: " + iface.getSimpleName());
    }
    
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return CassandraResultSet.class.isAssignableFrom(iface);
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

                TypedColumn c = decoder.makeCol(keyspace, columnFamily, col);
                values.add(c);
                valueMap.put(decoder.colNameAsString(keyspace, columnFamily, col.getName()), c.getValue());
            }
            return !(values.isEmpty() && valueMap.isEmpty());
        } 
        else
        {
            return false;
        }
    }

    public boolean previous() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void refreshRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean relative(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean rowDeleted() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean rowInserted() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean rowUpdated() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void setFetchDirection(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void setFetchSize(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateArray(int arg0, Array arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateArray(String arg0, Array arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

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

    public void updateBlob(String arg0, Blob arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBlob(int arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBlob(String arg0, InputStream arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBoolean(int arg0, boolean arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBoolean(String arg0, boolean arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateByte(int arg0, byte arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateByte(String arg0, byte arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBytes(int arg0, byte[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateBytes(String arg0, byte[] arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateCharacterStream(int arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateCharacterStream(String arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateClob(int arg0, Clob arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateClob(String arg0, Clob arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateClob(int arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateClob(String arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateDate(int arg0, Date arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateDate(String arg0, Date arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateDouble(int arg0, double arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateDouble(String arg0, double arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateFloat(int arg0, float arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateFloat(String arg0, float arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateInt(int arg0, int arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateInt(String arg0, int arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateLong(int arg0, long arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateLong(String arg0, long arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNClob(int arg0, NClob arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNClob(String arg0, NClob arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNClob(int arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNClob(String arg0, Reader arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNString(int arg0, String arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNString(String arg0, String arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNull(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateNull(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateObject(int arg0, Object arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateObject(String arg0, Object arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateObject(int arg0, Object arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateObject(String arg0, Object arg1, int arg2) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateRef(int arg0, Ref arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateRef(String arg0, Ref arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateRowId(int arg0, RowId arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateRowId(String arg0, RowId arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateShort(int arg0, short arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateShort(String arg0, short arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateString(int arg0, String arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateString(String arg0, String arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateTime(int arg0, Time arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateTime(String arg0, Time arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public boolean wasNull() throws SQLException
    {
        return wasNull;
    }
    
    /**
     * RSMD implementation.  The metadata returned refers to the column
     * values, not the column names.
     */
    class CResultSetMetaData implements ResultSetMetaData
    {
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
            return values.get(column).getValueType() instanceof CounterColumnType; // todo: check Value is correct.
        }

        public boolean isCaseSensitive(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            TypedColumn tc = values.get(column);
            if (tc.getValueType() instanceof ColumnMetaData)
                return ((ColumnMetaData)tc.getValueType()).isCaseSensitive();
            else
                return tc.getValueType().getType().equals(String.class);
        }

        public boolean isSearchable(int column) throws SQLException
        {
            return false;
        }

        public boolean isCurrency(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            TypedColumn tc = values.get(column);
            if (tc.getValueType() instanceof ColumnMetaData)
                return ((ColumnMetaData)tc.getValueType()).isCurrency();
            else
                return false;
        }

        public int isNullable(int column) throws SQLException
        {
            // no such thing as null in cassandra.
            return ResultSetMetaData.columnNullableUnknown;
        }

        public boolean isSigned(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            TypedColumn tc = values.get(column);
            return Utils.isTypeSigned(tc.getValueType());
        }

        public int getColumnDisplaySize(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return values.get(column).getValueString().length();
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
            column--;
            checkIndex(column);
            TypedColumn col = values.get(column);
            if (col.getValueType() instanceof ColumnMetaData)
                return ((ColumnMetaData)col.getValueType()).getPrecision();
            else if (col.getValueType().getType().equals(String.class))
                return col.getValueString().length();
            else if (col.getValueType() == BytesType.instance)
                return col.getValueString().length();
            else if (col.getValueType().getType().equals(UUID.class))
                return 36; // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            else if (col.getValueType() == LongType.instance)
                return 19; // number of digits in 2**63-1.
            else
                return 0;
        }

        public int getScale(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return Utils.getTypeScale(values.get(column).getValueType());
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
            column--;
            checkIndex(column);
            return Utils.getJdbcType(values.get(column).getValueType());
        }

        // todo: spec says "database specific type name". this means the abstract type.
        public String getColumnTypeName(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            return values.get(column).getValueType().getClass().getSimpleName();
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
            column--;
            checkIndex(column);
            return values.get(column).getValueType().getType().getName();
        }

        public <T> T unwrap(Class<T> iface) throws SQLException
        {
            throw new SQLException("No wrapping implemented");
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException
        {
            return false;
        }
    }
}
