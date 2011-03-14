package org.apache.cassandra.cql.jdbc;

/**
 * Addresses the metadata needs of a sparse row of columns. ResultSetMetaData doesn't fit the bill because it
 * expects every row to have the same set of columns.
 * 
 * I didn't feel compelled to go the distance and implement distinct getXXXType(), getXXXTypeName(), getXXX() 
 * methods for both column names and values a la ResultSetMetaData.  If we did want that, I recommend an optional
 * method in AbstractType called getSqlType() or something like that.
 */
public interface RowMetaData
{
    /** number of columns in a row of data */
    public int getColumnCount();
    
    /** column name and value */
    public TypedColumn getColumn(int index);
    
    /** key */
    public byte[] getKey(); // todo: update when typed keys are available.
}
