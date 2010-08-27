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

import javax.xml.bind.annotation.XmlElement;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.concurrent.StageManager;

public class RowMutationMessage
{   
    public static final String hint_ = "HINT";
    private static RowMutationMessageSerializer serializer_ = new RowMutationMessageSerializer();
	
    static RowMutationMessageSerializer serializer()
    {
        return serializer_;
    }

    public Message makeRowMutationMessage() throws IOException
    {         
        return makeRowMutationMessage(StorageService.Verb.MUTATION);
    }
    
    public Message makeRowMutationMessage(StorageService.Verb verb) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        RowMutationMessage.serializer().serialize(this, dos);
        return new Message(FBUtilities.getLocalAddress(), StageManager.MUTATION_STAGE, verb, bos.toByteArray());
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
