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

import javax.xml.bind.annotation.XmlElement;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;


/*
 * This message is sent back the row mutation verb handler 
 * and basically specifes if the write succeeded or not for a particular 
 * key in a table
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class WriteResponse implements Serializable
{
private static ICompactSerializer<WriteResponse> serializer_;

    static
    {
        serializer_ = new WriteResponseSerializer();
    }

    static ICompactSerializer<WriteResponse> serializer()
    {
        return serializer_;
    }
	
    public static Message makeWriteResponseMessage(WriteResponse writeResponse) throws IOException
    {
    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        WriteResponse.serializer().serialize(writeResponse, dos);
        Message message = new Message(StorageService.getLocalStorageEndPoint(), MessagingService.responseStage_, MessagingService.responseVerbHandler_, new Object[]{bos.toByteArray()});         
        return message;
    }
    
	@XmlElement(name = "Table")
	private String table_;

	@XmlElement(name = "key")
	private String key_;
	
	@XmlElement(name = "Status")
	private boolean status_;
	
	private WriteResponse() {
	}

	public WriteResponse(String table, String key, boolean bVal) {
		table_ = table;
		key_ = key;
		status_ = bVal;
	}

	public String table() 
	{
		return table_;
	}

	public String key() 
	{
		return key_;
	}
	
	public boolean isSuccess()
	{
		return status_;
	}
}

class WriteResponseSerializer implements ICompactSerializer<WriteResponse>
{
	public void serialize(WriteResponse wm, DataOutputStream dos) throws IOException
	{
		dos.writeUTF(wm.table());
		dos.writeUTF(wm.key());
		dos.writeBoolean(wm.isSuccess());
	}
	
    public WriteResponse deserialize(DataInputStream dis) throws IOException
    {
    	String table = dis.readUTF();
    	String key = dis.readUTF();
    	boolean status = dis.readBoolean();
    	return new WriteResponse(table, key, status);
    }
}