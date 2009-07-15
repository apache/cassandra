package org.apache.cassandra.db.filter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInputStream;

import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ColumnPath;
import org.apache.cassandra.service.ColumnPathOrParent;

public class QueryPath
{
    public final String columnFamilyName;
    public final String superColumnName;
    public final String columnName;

    public QueryPath(String columnFamilyName, String superColumnName, String columnName)
    {
        // TODO remove these when we're sure the last vestiges of the old api are gone
        assert columnFamilyName == null || !columnFamilyName.contains(":");
        assert superColumnName == null || !superColumnName.contains(":");
        assert columnName == null || !columnName.contains(":");

        this.columnFamilyName = columnFamilyName;
        this.superColumnName = superColumnName;
        this.columnName = columnName;
    }

    public QueryPath(ColumnParent columnParent)
    {
        this(columnParent.column_family, columnParent.super_column, null);
    }

    public QueryPath(String columnFamilyName, String superColumnName)
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

    public static QueryPath column(String columnName)
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
        assert !"".equals(superColumnName);
        assert !"".equals(columnName);
        dos.writeUTF(columnFamilyName == null ? "" : columnFamilyName);
        dos.writeUTF(superColumnName == null ? "" : superColumnName);
        dos.writeUTF(columnName == null ? "" : columnName);
    }

    public static QueryPath deserialize(DataInputStream din) throws IOException
    {
        String cfName = din.readUTF();
        String scName = din.readUTF();
        String cName = din.readUTF();
        return new QueryPath(cfName.isEmpty() ? null : cfName, scName.isEmpty() ? null : scName, cName.isEmpty() ? null : cName);
    }
}
