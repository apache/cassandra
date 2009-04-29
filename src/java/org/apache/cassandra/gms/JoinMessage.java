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

package org.apache.cassandra.gms;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.cassandra.io.ICompactSerializer;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class JoinMessage
{
    private static ICompactSerializer<JoinMessage> serializer_;
    static
    {
        serializer_ = new JoinMessageSerializer();
    }
    
    static ICompactSerializer<JoinMessage> serializer()
    {
        return serializer_;
    }
    
    String clusterId_;
    
    JoinMessage(String clusterId)
    {
        clusterId_ = clusterId;
    }
}

class JoinMessageSerializer implements ICompactSerializer<JoinMessage>
{
    public void serialize(JoinMessage joinMessage, DataOutputStream dos) throws IOException
    {    
        dos.writeUTF(joinMessage.clusterId_);         
    }

    public JoinMessage deserialize(DataInputStream dis) throws IOException
    {
        String clusterId = dis.readUTF();
        return new JoinMessage(clusterId);
    }
}
