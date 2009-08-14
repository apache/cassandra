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
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.commons.lang.ArrayUtils;


/*
 * The read response message is sent by the server when reading data 
 * this encapsulates the tablename and the row that has been read.
 * The table name is needed so that we can use it to create repairs.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
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
    
	public static Message makeReadResponseMessage(ReadResponse readResponse) throws IOException
    {
    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        ReadResponse.serializer().serialize(readResponse, dos);
        Message message = new Message(StorageService.getLocalStorageEndPoint(), MessagingService.responseStage_, MessagingService.responseVerbHandler_, bos.toByteArray());         
        return message;
    }
	
	private Row row_;
	private byte[] digest_ = ArrayUtils.EMPTY_BYTE_ARRAY;
    private boolean isDigestQuery_ = false;

	public ReadResponse(byte[] digest )
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
        
	public byte[] digest() 
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
        dos.writeInt(rm.digest().length);
        dos.write(rm.digest());
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
        if ( !isDigest )
        {
            row = Row.serializer().deserialize(dis);
        }
		
		ReadResponse rmsg = null;
    	if( isDigest  )
        {
    		rmsg =  new ReadResponse(digest);
        }
    	else
        {
    		rmsg =  new ReadResponse(row);
        }
        rmsg.setIsDigestQuery(isDigest);
    	return rmsg;
    } 
}
