package org.apache.cassandra.db;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.util.Collection;

import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.db.marshal.AbstractType;

public class ColumnFamilySerializer implements ICompactSerializer2<ColumnFamily>
{
    /*
     * Serialized ColumnFamily format:
     *
     * [serialized for intra-node writes only, e.g. returning a query result]
     * <cf name>
     * <cf type [super or standard]>
     * <cf comparator name>
     * <cf subcolumn comparator name>
     *
     * [in sstable only]
     * <column bloom filter>
     * <sparse column index, start/finish columns every ColumnIndexSizeInKB of data>
     *
     * [always present]
     * <local deletion time>
     * <client-provided deletion time>
     * <column count>
     * <columns, serialized individually>
    */
    public void serialize(ColumnFamily columnFamily, DataOutput dos) throws IOException
    {
        dos.writeUTF(columnFamily.name());
        dos.writeUTF(columnFamily.type_);
        dos.writeUTF(columnFamily.getComparatorName());
        dos.writeUTF(columnFamily.getSubComparatorName());
        serializeForSSTable(columnFamily, dos);
    }

    public void serializeForSSTable(ColumnFamily columnFamily, DataOutput dos) throws IOException
    {
        dos.writeInt(columnFamily.localDeletionTime);
        dos.writeLong(columnFamily.markedForDeleteAt);

        Collection<IColumn> columns = columnFamily.getSortedColumns();
        dos.writeInt(columns.size());
        for ( IColumn column : columns )
        {
            columnFamily.getColumnSerializer().serialize(column, dos);
        }
    }

    public void serializeWithIndexes(ColumnFamily columnFamily, DataOutput dos) throws IOException
    {
        ColumnIndexer.serialize(columnFamily, dos);
        serializeForSSTable(columnFamily, dos);
    }

    public ColumnFamily deserialize(DataInput dis) throws IOException
    {
        ColumnFamily cf = deserializeFromSSTableNoColumns(dis.readUTF(), dis.readUTF(), readComparator(dis), readComparator(dis), dis);
        int size = dis.readInt();
        IColumn column;
        for (int i = 0; i < size; ++i)
        {
            column = cf.getColumnSerializer().deserialize(dis);
            cf.addColumn(column);
        }
        return cf;
    }

    private AbstractType readComparator(DataInput dis) throws IOException
    {
        String className = dis.readUTF();
        if (className.equals(""))
        {
            return null;
        }

        try
        {
            return (AbstractType)Class.forName(className).getConstructor().newInstance();
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Unable to load comparator class '" + className + "'.  probably this means you have obsolete sstables lying around", e);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public ColumnFamily deserializeFromSSTableNoColumns(String name, String type, AbstractType comparator, AbstractType subComparator, DataInput input) throws IOException
    {
        ColumnFamily cf = new ColumnFamily(name, type, comparator, subComparator);
        return deserializeFromSSTableNoColumns(cf, input);
    }

    public ColumnFamily deserializeFromSSTableNoColumns(ColumnFamily cf, DataInput input) throws IOException
    {
        cf.delete(input.readInt(), input.readLong());
        return cf;
    }
}
