package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class SliceByRangeReadCommand extends ReadCommand {

	
	public final String columnFamily;
	public final String startColumn;
	public final String endColumn;
	public final int count;
	
	public SliceByRangeReadCommand(String table, String key, String columnFamily, String startCol, String endCol, int count)
	{
		super(table, key, CMD_TYPE_GET_SLICE_BY_RANGE);
		this.columnFamily = columnFamily;
		this.startColumn = startCol;
		this.endColumn = endCol;
		this.count = count;
	}
	
	@Override
	public ReadCommand copy() 
	{
		ReadCommand readCommand= new SliceByRangeReadCommand(table, key, columnFamily, startColumn, endColumn,count);
		readCommand.setDigestQuery(isDigestQuery());
		return readCommand;
	}

	@Override
	public String getColumnFamilyName() 
	{
		return columnFamily;
	}

	@Override
	public Row getRow(Table table) throws IOException 
	{
		return table.getRow(key, columnFamily, startColumn, endColumn, count);
	}
	
	@Override
    public String toString()
    {
        return "GetSliceByNamesReadMessage(" +
               "table='" + table + '\'' +
               ", key='" + key + '\'' +
               ", columnFamily='" + columnFamily + '\'' +
               ", startColumn=" + startColumn +
               ", endColumn=" + endColumn +
               ')';
    }
}

class SliceByRangeReadCommandSerializer extends ReadCommandSerializer
{
	@Override
	public void serialize(ReadCommand rm, DataOutputStream dos) throws IOException
	{
		SliceByRangeReadCommand realRM = (SliceByRangeReadCommand)rm;
		dos.writeBoolean(realRM.isDigestQuery());
		dos.writeUTF(realRM.table);
		dos.writeUTF(realRM.key);
		dos.writeUTF(realRM.columnFamily);
		dos.writeUTF(realRM.startColumn);
		dos.writeUTF(realRM.endColumn);
		dos.writeInt(realRM.count);
	}

	@Override
	public ReadCommand deserialize(DataInputStream dis) throws IOException
	{
		boolean isDigest = dis.readBoolean();
		String table = dis.readUTF();
		String key = dis.readUTF();
		String columnFamily = dis.readUTF();
		String startColumn = dis.readUTF();
		String endColumn = dis.readUTF();
		int count = dis.readInt();
		SliceByRangeReadCommand rm = new SliceByRangeReadCommand(table, key, columnFamily, startColumn, endColumn, count);
		rm.setDigestQuery(isDigest);
		return rm;
	}
}