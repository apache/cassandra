package org.apache.cassandra.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class ColumnFamilySplit extends InputSplit implements Writable
{
    private String startToken;
    private String endToken;
    private String table;
    private String columnFamily;
    private String[] dataNodes;

    public ColumnFamilySplit(String table, String columnFamily, String startToken, String endToken, String[] dataNodes)
    {
        assert startToken != null;
        assert endToken != null;
        this.startToken = startToken;
        this.endToken = endToken;
        this.columnFamily = columnFamily;
        this.table = table;
        this.dataNodes = dataNodes;
    }

    public String getStartToken()
    {
        return startToken;
    }

    public String getEndToken()
    {
        return endToken;
    }

    public String getTable()
    {
        return table;
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }

    // getLength and getLocations satisfy the InputSplit abstraction
    
    public long getLength()
    {
        // only used for sorting splits. we don't have the capability, yet.
        return 0;
    }

    public String[] getLocations()
    {
        return dataNodes;
    }

    // This should only be used by KeyspaceSplit.read();
    protected ColumnFamilySplit() {}

    // These three methods are for serializing and deserializing
    // KeyspaceSplits as needed by the Writable interface.
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(table);
        out.writeUTF(columnFamily);
        out.writeUTF(startToken);
        out.writeUTF(endToken);

        out.writeInt(dataNodes.length);
        for (String endPoint : dataNodes)
        {
            out.writeUTF(endPoint);
        }
    }
    
    public void readFields(DataInput in) throws IOException
    {
        table = in.readUTF();
        columnFamily = in.readUTF();
        startToken = in.readUTF();
        endToken = in.readUTF();

        int numOfEndPoints = in.readInt();
        dataNodes = new String[numOfEndPoints];
        for(int i = 0; i < numOfEndPoints; i++)
        {
            dataNodes[i] = in.readUTF();
        }
    }
    
    public static ColumnFamilySplit read(DataInput in) throws IOException
    {
        ColumnFamilySplit w = new ColumnFamilySplit();
        w.readFields(in);
        return w;
    }
}