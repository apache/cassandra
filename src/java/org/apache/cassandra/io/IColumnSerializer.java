package org.apache.cassandra.io;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IColumn;

public interface IColumnSerializer extends ICompactSerializer2<IColumn>
{
    public IColumn deserialize(DataInput in, ColumnFamilyStore interner) throws IOException;
}
