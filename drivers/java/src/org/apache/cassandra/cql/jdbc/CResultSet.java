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
import java.sql.*;
import java.sql.Date;
import java.util.*;

import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CResultSet extends AbstractResultSet implements CassandraResultSet
{
    private final ColumnDecoder decoder;
    private final String keyspace;
    private final String columnFamily;
    
    /** The r set iter. */
    private Iterator<CqlRow> rSetIter;
    int rowNumber = 0;
    
    // the current row key when iterating through results.
    private byte[] curRowKey = null;
    
    /** The values. */
    private List<TypedColumn> values = new ArrayList<TypedColumn>();
    
    /** The value map. */
    private Map<String, TypedColumn> valueMap = new HashMap<String, TypedColumn>();
    
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
        return valueMap.get(name);
    }

    public void close() throws SQLException
    {
        valueMap = null;
        values = null;
    }

    private byte[] getBytes(TypedColumn column)
    {
        ByteBuffer value = (ByteBuffer) column.getValue();
        wasNull = value == null;
        return value == null ? null : ByteBufferUtil.clone(value).array();
    }

    public byte[] getBytes(int index) throws SQLException
    {
        return getBytes(values.get(index - 1));
    }

    public byte[] getBytes(String name) throws SQLException
    {
        return getBytes(valueMap.get(name));
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

    public float getFloat(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public float getFloat(String arg0) throws SQLException
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

    private BigInteger getBigInteger(TypedColumn column)
    {
        BigInteger value = (BigInteger) column.getValue();
        wasNull = value == null;
        return value;
    }

    public BigInteger getBigInteger(int i)
    {
        return getBigInteger(values.get(i - 1));
    }

    public BigInteger getBigInteger(String name)
    {
        return getBigInteger(valueMap.get(name));
    }

    private int getInt(TypedColumn column) throws SQLException
    {
        // bit of a hack, this, but asking for getInt seems so common that we should accomodate it
        if (column.getValue() instanceof BigInteger)
            return getBigInteger(column).intValue();
        else if (column.getValue() instanceof Long)
            return getLong(column).intValue();
        else
            throw new SQLException("Non-integer value " + column.getValue());
    }

    public int getInt(int index) throws SQLException
    {
        return getInt(values.get(index - 1));
    }

    public int getInt(String name) throws SQLException
    {
        return getInt(valueMap.get(name));
    }

    private Long getLong(TypedColumn column)
    {
        Long value = (Long) column.getValue();
        wasNull = value == null;
        return value == null ? 0 : value;
    }

    public long getLong(int index) throws SQLException
    {
        return getLong(values.get(index - 1));
    }

    public long getLong(String name) throws SQLException
    {
        return getLong(valueMap.get(name));
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        return meta;
    }

    private Object getObject(TypedColumn column)
    {
        Object value = column.getValue();
        wasNull = value == null;
        return value;
    }

    public Object getObject(int index) throws SQLException
    {
        return getObject(values.get(index - 1));
    }

    public Object getObject(String name) throws SQLException
    {
        return getObject(valueMap.get(name));
    }

    public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public int getRow() throws SQLException
    {
        return rowNumber;
    }

    private String getString(TypedColumn column)
    {
        String value = (String) column.getValue();
        wasNull = value == null;
        return value == null ? null : value;
    }

    public String getString(int index) throws SQLException
    {
        return getString(values.get(index - 1));
    }

    public String getString(String name) throws SQLException
    {
        return getString(valueMap.get(name));
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
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public boolean isAfterLast() throws SQLException
    {
        return rowNumber == Integer.MAX_VALUE;
    }

    public boolean isBeforeFirst() throws SQLException
    {
        return rowNumber == 0;
    }

    public boolean isClosed() throws SQLException
    {
        return valueMap == null;
    }

    public boolean isFirst() throws SQLException
    {
        return rowNumber == 1;
    }

    public boolean isLast() throws SQLException
    {
        return !rSetIter.hasNext();
    }

    public boolean last() throws SQLException
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
            rowNumber++;
            curRowKey = row.getKey();
            List<Column> cols = row.getColumns();
            for (Column col : cols)
            {

                TypedColumn c = decoder.makeCol(keyspace, columnFamily, col);
                values.add(c);
                valueMap.put(decoder.colNameAsString(keyspace, columnFamily, col.name), c);
            }
            return !(values.isEmpty() && valueMap.isEmpty());
        } 
        else
        {
            rowNumber = Integer.MAX_VALUE;
            return false;
        }
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
            return tc.getValueType().isCaseSensitive();
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
            return tc.getValueType().isCurrency();
        }

        /** absence is the equivalent of null in Cassandra */
        public int isNullable(int column) throws SQLException
        {
            return ResultSetMetaData.columnNullable;
        }

        public boolean isSigned(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            TypedColumn tc = values.get(column);
            return tc.getValueType().isSigned();
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
            return col.getValueType().getPrecision(col.getValue());
        }

        public int getScale(int column) throws SQLException
        {
            column--;
            checkIndex(column);
            TypedColumn tc = values.get(column);
            return tc.getValueType().getScale(tc.getValue());
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
            return values.get(column).getValueType().getJdbcType();
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
