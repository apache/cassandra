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
import org.apache.cassandra.service.StorageService;


public class CalloutDeployMessage
{
    private static ICompactSerializer<CalloutDeployMessage> serializer_;
    
    static
    {
        serializer_ = new CalloutDeployMessageSerializer();
    }
    
    public static ICompactSerializer<CalloutDeployMessage> serializer()
    {
        return serializer_;
    }
    
    public static Message getCalloutDeployMessage(CalloutDeployMessage cdMessage) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        serializer_.serialize(cdMessage, dos);
        Message message = new Message(StorageService.getLocalStorageEndPoint(), "", StorageService.calloutDeployVerbHandler_, new Object[]{bos.toByteArray()});
        return message;
    }
    
    /* Name of the callout */
    private String callout_;
    /* The actual procedure */
    private String script_;
    
    public CalloutDeployMessage(String callout, String script)
    {
        callout_ = callout;
        script_ = script;
    }
    
    String getCallout()
    {
        return callout_;
    }
    
    String getScript()
    {
        return script_;
    }
}

class CalloutDeployMessageSerializer implements ICompactSerializer<CalloutDeployMessage>
{
    public void serialize(CalloutDeployMessage cdMessage, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(cdMessage.getCallout());
        dos.writeUTF(cdMessage.getScript());
    }
    
    public CalloutDeployMessage deserialize(DataInputStream dis) throws IOException
    {
        String callout = dis.readUTF();
        String script = dis.readUTF();
        return new CalloutDeployMessage(callout, script);
    }
}
