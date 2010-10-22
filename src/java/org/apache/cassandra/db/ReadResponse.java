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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.ArrayUtils;


/*
 * The read response message is sent by the server when reading data 
 * this encapsulates the tablename and the row that has been read.
 * The table name is needed so that we can use it to create repairs.
 */
public class ReadResponse
{
private static ICompactSerializer<ReadResponse> serializer_;

    static
    {
        serializer_ = new ReadResponseSerializer();
    }

    public static ICompactSerializer<ReadResponse> serializer()
    {
        return serializer_;
    }
    
	private Row row_;
	private ByteBuffer digest_ = FBUtilities.EMPTY_BYTE_BUFFER;
    private boolean isDigestQuery_ = false;

	public ReadResponse(ByteBuffer digest )
    {
        assert digest != null;
		digest_= digest;
	}

	public ReadResponse(Row row)
    {
		row_ = row;
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
    	return isDigestQuery_;
    }
    
    public void setIsDigestQuery(boolean isDigestQuery)
    {
    	isDigestQuery_ = isDigestQuery;
    }
}

class ReadResponseSerializer implements ICompactSerializer<ReadResponse>
{
	public void serialize(ReadResponse rm, DataOutputStream dos) throws IOException
	{
        dos.writeInt(rm.digest().remaining());
        dos.write(rm.digest().array(),rm.digest().position()+rm.digest().arrayOffset(),rm.digest().remaining());
        dos.writeBoolean(rm.isDigestQuery());
        
        if( !rm.isDigestQuery() && rm.row() != null )
        {            
            Row.serializer().serialize(rm.row(), dos);
        }				
	}
	
    public ReadResponse deserialize(DataInputStream dis) throws IOException
    {
        int digestSize = dis.readInt();
        byte[] digest = new byte[digestSize];
        dis.read(digest, 0 , digestSize);
        boolean isDigest = dis.readBoolean();
        
        Row row = null;
        if (!isDigest)
        {
            row = Row.serializer().deserialize(dis);
        }

        ReadResponse rmsg = isDigest ? new ReadResponse(ByteBuffer.wrap(digest)) : new ReadResponse(row);
        rmsg.setIsDigestQuery(isDigest);
    	return rmsg;
    } 
}
