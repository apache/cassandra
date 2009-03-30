/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.continuations.Suspendable;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ReadMessage implements Serializable
{
    private static ICompactSerializer<ReadMessage> serializer_;	
    public static final String doRepair_ = "READ-REPAIR";
	
    static
    {
        serializer_ = new ReadMessageSerializer();
    }

    static ICompactSerializer<ReadMessage> serializer()
    {
        return serializer_;
    }
    
    public static Message makeReadMessage(ReadMessage readMessage) throws IOException
    {
    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        ReadMessage.serializer().serialize(readMessage, dos);
        Message message = new Message(StorageService.getLocalStorageEndPoint(), StorageService.readStage_, StorageService.readVerbHandler_, new Object[]{bos.toByteArray()});         
        return message;
    }
    
    private String table_;
    private String key_;
    private String columnFamily_column_ = null;
    private int start_ = -1;
    private int count_ = -1 ;
    private long sinceTimestamp_ = -1 ;
    private List<String> columns_ = new ArrayList<String>();
    private boolean isDigestQuery_ = false;
        
    private ReadMessage()
    {
    }
    
    public ReadMessage(String table, String key)
    {
        table_ = table;
        key_ = key;
    }

    public ReadMessage(String table, String key, String columnFamily_column)
    {
        table_ = table;
        key_ = key;
        columnFamily_column_ = columnFamily_column;
    }
    
    public ReadMessage(String table, String key, String columnFamily, List<String> columns)
    {
    	table_ = table;
    	key_ = key;
    	columnFamily_column_ = columnFamily;
    	columns_ = columns;
    }
    
    public ReadMessage(String table, String key, String columnFamily_column, int start, int count)
    {
        table_ = table;
        key_ = key;
        columnFamily_column_ = columnFamily_column;
        start_ = start ;
        count_ = count;
    }

    public ReadMessage(String table, String key, String columnFamily_column, long sinceTimestamp)
    {
        table_ = table;
        key_ = key;
        columnFamily_column_ = columnFamily_column;
        sinceTimestamp_ = sinceTimestamp ;
    }

    String table()
    {
        return table_;
    }
    
    public String key()
    {
        return key_;
    }

    String columnFamily_column()
    {
        return columnFamily_column_;
    }

    int start()
    {
        return start_;
    }

    int count()
    {
        return count_;
    }

    long sinceTimestamp()
    {
        return sinceTimestamp_;
    }

    public boolean isDigestQuery()
    {
    	return isDigestQuery_;
    }
    
    public void setIsDigestQuery(boolean isDigestQuery)
    {
    	isDigestQuery_ = isDigestQuery;
    }
    
    public List<String> getColumnNames()
    {
    	return columns_;
    }

    public String toString()
    {
        return "ReadMessage(" +
               "table='" + table_ + '\'' +
               ", key='" + key_ + '\'' +
               ", columnFamily_column='" + columnFamily_column_ + '\'' +
               ", start=" + start_ +
               ", count=" + count_ +
               ", sinceTimestamp=" + sinceTimestamp_ +
               ", columns=[" + StringUtils.join(columns_, ", ") + "]" +
               ", isDigestQuery=" + isDigestQuery_ +
               ')';
    }
}

class ReadMessageSerializer implements ICompactSerializer<ReadMessage>
{
	public void serialize(ReadMessage rm, DataOutputStream dos) throws IOException
	{
		dos.writeUTF(rm.table());
		dos.writeUTF(rm.key());
		dos.writeUTF(rm.columnFamily_column());
		dos.writeInt(rm.start());
		dos.writeInt(rm.count());
		dos.writeLong(rm.sinceTimestamp());
		dos.writeBoolean(rm.isDigestQuery());
		List<String> columns = rm.getColumnNames();
		dos.writeInt(columns.size());
		if ( columns.size() > 0 )
		{
			for ( String column : columns )
			{
				dos.writeInt(column.getBytes().length);
				dos.write(column.getBytes());
			}
		}
	}
	
    public ReadMessage deserialize(DataInputStream dis) throws IOException
    {
		String table = dis.readUTF();
		String key = dis.readUTF();
		String columnFamily_column = dis.readUTF();
		int start = dis.readInt();
		int count = dis.readInt();
		long sinceTimestamp = dis.readLong();
		boolean isDigest = dis.readBoolean();
		
		int size = dis.readInt();
		List<String> columns = new ArrayList<String>();		
		for ( int i = 0; i < size; ++i )
		{
			byte[] bytes = new byte[dis.readInt()];
			dis.readFully(bytes);
			columns.add( new String(bytes) );
		}
		ReadMessage rm = null;
		if ( columns.size() > 0 )
		{
			rm = new ReadMessage(table, key, columnFamily_column, columns);
		}
		else if( sinceTimestamp > 0 )
		{
			rm = new ReadMessage(table, key, columnFamily_column, sinceTimestamp);
		}
		else
		{
			rm = new ReadMessage(table, key, columnFamily_column, start, count);
		}
		rm.setIsDigestQuery(isDigest);
    	return rm;
    }
}

