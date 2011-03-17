package org.apache.cassandra.cql.jdbc;

import java.sql.SQLException;

/**
 * ResultSetMetaData give lots of nice detailed type inforamtion about column values.
 * This interface aims to do the same thing but distinguishes column names and values.
 */
public interface CassandraResultSetMetaData
{
    /** exposes row key */
    public byte[] getKey();
    
    // the rest of these methods have similar calls in java.sql.ResultSetMetaData.
    
    public boolean isNameCaseSensitive(int column) throws SQLException;
    public boolean isNameCurrency(int column) throws SQLException;
    public boolean isNameSigned(int column) throws SQLException;
    public int getNameDisplaySize(int column) throws SQLException;
    public int getNamePrecision(int column) throws SQLException;
    public int getNameScale(int column) throws SQLException;
    public int getNameType(int column) throws SQLException;
    public String getNameTypeName(int column) throws SQLException;
    public String getNameClassName(int column) throws SQLException;
    
    public boolean isValueCaseSensitive(int column) throws SQLException;
    public boolean isValueCurrency(int column) throws SQLException;
    public boolean isValueSigned(int column) throws SQLException;
    public int getValueDisplaySize(int column) throws SQLException;
    public int getValuePrecision(int column) throws SQLException;
    public int getValueScale(int column) throws SQLException;
    public int getValueType(int column) throws SQLException;
    public String getValueTypeName(int column) throws SQLException;
    public String getValueClassName(int column) throws SQLException;
}
