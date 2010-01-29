package org.apache.cassandra.thrift;

import java.util.List;


public class ThriftGlue
{
    private static ColumnOrSuperColumn createColumnOrSuperColumn(Column col, SuperColumn scol)
    {
        ColumnOrSuperColumn ret = new ColumnOrSuperColumn();
        ret.setColumn(col);
        ret.setSuper_column(scol);
        return ret;
    }

    public static ColumnOrSuperColumn createColumnOrSuperColumn_Column(Column col)
    {
        return createColumnOrSuperColumn(col, null);
    }

    public static ColumnOrSuperColumn createColumnOrSuperColumn_SuperColumn(SuperColumn scol)
    {
        return createColumnOrSuperColumn(null, scol);
    }

    public static ColumnParent createColumnParent(String columnFamily, byte[] super_column)
    {
        ColumnParent ret = new ColumnParent(columnFamily);
        ret.setSuper_column(super_column);
        return ret;
    }

    public static ColumnPath createColumnPath(String columnFamily, byte[] superColumnName, byte[] columnName)
    {
        ColumnPath ret = new ColumnPath(columnFamily);
        ret.setSuper_column(superColumnName);
        ret.setColumn(columnName);
        return ret;
    }

    public static SlicePredicate createSlicePredicate(List<byte[]> columns, SliceRange range)
    {
        SlicePredicate ret = new SlicePredicate();
        ret.setColumn_names(columns);
        ret.setSlice_range(range);
        return ret;
    }

}
