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

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;


/*
 * The read response message is sent by the server when reading data 
 * this encapsulates the tablename and the row that has been read.
 * The table name is needed so that we can use it to create repairs.
 */
public class ReadResponse
{
private static IVersionedSerializer<ReadResponse> serializer_;

    static
    {
        serializer_ = new ReadResponseSerializer();
    }

    public static IVersionedSerializer<ReadResponse> serializer()
    {
        return serializer_;
    }
    
	private final Row row_;
	private final ByteBuffer digest_;

	public ReadResponse(ByteBuffer digest )
    {
        assert digest != null;
		digest_= digest;
        row_ = null;
	}

	public ReadResponse(Row row)
    {
        assert row != null;
		row_ = row;
        digest_ = null;
	}

	public Row row() 
    {
		return row_;
    }
        
	public ByteBuffer digest() 
    {
		return digest_;
	}

	public boolean isDigestQuery()
    {
    	return digest_ != null;
    }
}

class ReadResponseSerializer implements IVersionedSerializer<ReadResponse>
{
	public void serialize(ReadResponse response, DataOutput dos, int version) throws IOException
	{
        dos.writeInt(response.isDigestQuery() ? response.digest().remaining() : 0);
        ByteBuffer buffer = response.isDigestQuery() ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        ByteBufferUtil.write(buffer, dos);
        dos.writeBoolean(response.isDigestQuery());
        if (!response.isDigestQuery())
            Row.serializer().serialize(response.row(), dos, version);
    }
	
    public ReadResponse deserialize(DataInput dis, int version) throws IOException
    {
        byte[] digest = null;
        int digestSize = dis.readInt();
        if (digestSize > 0)
        {
            digest = new byte[digestSize];
            dis.readFully(digest, 0, digestSize);
        }
        boolean isDigest = dis.readBoolean();
        assert isDigest == digestSize > 0;

        Row row = null;
        if (!isDigest)
        {
            // This is coming from a remote host
            row = Row.serializer().deserialize(dis, version, IColumnSerializer.Flag.FROM_REMOTE, ArrayBackedSortedColumns.factory());
        }

        return isDigest ? new ReadResponse(ByteBuffer.wrap(digest)) : new ReadResponse(row);
    }

    public long serializedSize(ReadResponse response, int version)
    {
        int size = DBConstants.intSize;
        size += DBConstants.boolSize;
        if (response.isDigestQuery())
            size += response.digest().remaining();
        else
            size += Row.serializer().serializedSize(response.row(), version);
        return size;
    }
}
