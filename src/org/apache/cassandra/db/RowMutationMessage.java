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
import java.util.*;

import javax.xml.bind.annotation.XmlElement;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;



/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class RowMutationMessage implements Serializable
{   
    public static final String hint_ = "HINT";
    private static ICompactSerializer<RowMutationMessage> serializer_;	
	
    static
    {
        serializer_ = new RowMutationMessageSerializer();
    }

    static ICompactSerializer<RowMutationMessage> serializer()
    {
        return serializer_;
    }

    public static Message makeRowMutationMessage(RowMutationMessage rowMutationMessage) throws IOException
    {         
        return makeRowMutationMessage(rowMutationMessage, StorageService.mutationVerbHandler_);
    }
    
    public static Message makeRowMutationMessage(RowMutationMessage rowMutationMessage, String verbHandlerName) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        RowMutationMessage.serializer().serialize(rowMutationMessage, dos);
        EndPoint local = StorageService.getLocalStorageEndPoint();
        EndPoint from = ( local != null ) ? local : new EndPoint(FBUtilities.getHostName(), 7000); 
        Message message = new Message(from, StorageService.mutationStage_, verbHandlerName, new Object[]{bos.toByteArray()});         
        return message;
    }
    
    @XmlElement(name="RowMutation")
    private RowMutation rowMutation_;
    
    private RowMutationMessage()
    {}
    
    public RowMutationMessage(RowMutation rowMutation)
    {
        rowMutation_ = rowMutation;
    }
    
   public RowMutation getRowMutation()
   {
       return rowMutation_;
   }
}

class RowMutationMessageSerializer implements ICompactSerializer<RowMutationMessage>
{
	public void serialize(RowMutationMessage rm, DataOutputStream dos) throws IOException
	{
		RowMutation.serializer().serialize(rm.getRowMutation(), dos);
	}
	
    public RowMutationMessage deserialize(DataInputStream dis) throws IOException
    {
    	RowMutation rm = RowMutation.serializer().deserialize(dis);
    	return new RowMutationMessage(rm);
    }
}