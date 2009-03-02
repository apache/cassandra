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
 * The read response message is sent by the server when reading data 
 * this encapsulates the tablename and teh row that has been read.
 * The table name is needed so that we can use it to create repairs.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class ReadResponseMessage implements Serializable 
{
private static ICompactSerializer<ReadResponseMessage> serializer_;	
	
    static
    {
        serializer_ = new ReadResponseMessageSerializer();
    }

    public static ICompactSerializer<ReadResponseMessage> serializer()
    {
        return serializer_;
    }
    
	public static Message makeReadResponseMessage(ReadResponseMessage readResponseMessage) throws IOException
    {
    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        ReadResponseMessage.serializer().serialize(readResponseMessage, dos);
        Message message = new Message(StorageService.getLocalStorageEndPoint(), MessagingService.responseStage_, MessagingService.responseVerbHandler_, new Object[]{bos.toByteArray()});         
        return message;
    }
	
	@XmlElement(name = "Table")
	private String table_;

	@XmlElement(name = "Row")
	private Row row_;

	@XmlElement(name = "Digest")
	private byte[] digest_ = new byte[0];

    @XmlElement(name="isDigestQuery")
    private boolean isDigestQuery_ = false;
	
	private ReadResponseMessage() {
	}

	public ReadResponseMessage(String table, byte[] digest ) {
		table_ = table;
		digest_= digest;
	}

	public ReadResponseMessage(String table, Row row) {
		table_ = table;
		row_ = row;
	}

	public String table() {
		return table_;
	}

	public Row row() 
    {
		return row_;
    }
        
	public byte[] digest() {
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


class ReadResponseMessageSerializer implements ICompactSerializer<ReadResponseMessage>
{
	public void serialize(ReadResponseMessage rm, DataOutputStream dos) throws IOException
	{
		dos.writeUTF(rm.table());
        dos.writeInt(rm.digest().length);
        dos.write(rm.digest());
        dos.writeBoolean(rm.isDigestQuery());
        
        if( !rm.isDigestQuery() && rm.row() != null )
        {            
            Row.serializer().serialize(rm.row(), dos);
        }				
	}
	
    public ReadResponseMessage deserialize(DataInputStream dis) throws IOException
    {
    	String table = dis.readUTF();
        int digestSize = dis.readInt();
        byte[] digest = new byte[digestSize];
        dis.read(digest, 0 , digestSize);
        boolean isDigest = dis.readBoolean();
        
        Row row = null;
        if ( !isDigest )
        {
            row = Row.serializer().deserialize(dis);
        }
		
		ReadResponseMessage rmsg = null;
    	if( isDigest  )
        {
    		rmsg =  new ReadResponseMessage(table, digest);
        }
    	else
        {
    		rmsg =  new ReadResponseMessage(table, row);
        }
        rmsg.setIsDigestQuery(isDigest);
    	return rmsg;
    } 
}