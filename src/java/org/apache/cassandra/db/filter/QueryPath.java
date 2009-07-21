package org.apache.cassandra.db.filter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ColumnPath;
import org.apache.cassandra.service.ColumnPathOrParent;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.db.ColumnSerializer;

public class QueryPath
{
    public final String columnFamilyName;
    public final byte[] superColumnName;
    public final byte[] columnName;

    public QueryPath(String columnFamilyName, byte[] superColumnName, byte[] columnName)
    {
        this.columnFamilyName = columnFamilyName;
        this.superColumnName = superColumnName;
        this.columnName = columnName;
    }

    public QueryPath(ColumnParent columnParent)
    {
        this(columnParent.column_family, columnParent.super_column, null);
    }

    public QueryPath(String columnFamilyName, byte[] superColumnName)
    {
        this(columnFamilyName, superColumnName, null);
    }

    public QueryPath(String columnFamilyName)
    {
        this(columnFamilyName, null);
    }

    public QueryPath(ColumnPath column_path)
    {
        this(column_path.column_family, column_path.super_column, column_path.column);
    }

    public QueryPath(ColumnPathOrParent column_path_or_parent)
    {
        this(column_path_or_parent.column_family, column_path_or_parent.super_column, column_path_or_parent.column);
    }

    public static QueryPath column(byte[] columnName)
    {
        return new QueryPath(null, null, columnName);
    }

    @Override
    public String toString()
    {
        return "QueryPath(" +
               "columnFamilyName='" + columnFamilyName + '\'' +
               ", superColumnName='" + superColumnName + '\'' +
               ", columnName='" + columnName + '\'' +
               ')';
    }

    public void serialize(DataOutputStream dos) throws IOException
    {
        assert !"".equals(columnFamilyName);
        assert superColumnName == null || superColumnName.length > 0;
        assert columnName == null || columnName.length > 0;
        dos.writeUTF(columnFamilyName == null ? "" : columnFamilyName);
        ColumnSerializer.writeName(superColumnName == null ? ArrayUtils.EMPTY_BYTE_ARRAY : superColumnName, dos);
        ColumnSerializer.writeName(columnName == null ? ArrayUtils.EMPTY_BYTE_ARRAY : columnName, dos);
    }

    public static QueryPath deserialize(DataInputStream din) throws IOException
    {
        String cfName = din.readUTF();
        byte[] scName = ColumnSerializer.readName(din);
        byte[] cName = ColumnSerializer.readName(din);
        return new QueryPath(cfName.isEmpty() ? null : cfName, scName.length == 0 ? null : scName, cName.length == 0 ? null : cName);
    }
}
