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


import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;

/** a class to hold all the unimplemented crap */
class AbstractResultSet
{
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

    public byte getByte(int arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public byte getByte(String arg0) throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
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

    public int getConcurrency() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public String getCursorName() throws SQLException
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

    public int getHoldability() throws SQLException
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

    public void moveToCurrentRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
    }

    public void moveToInsertRow() throws SQLException
    {
        throw new UnsupportedOperationException("method not supported");
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

    //
    // all the update methods are unsupported, requires a separate statement in Cassandra
    //

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
}
