package org.apache.cassandra.net;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.*;

import org.apache.cassandra.io.IVersionedSerializer;

public class MessageSerializer implements IVersionedSerializer<Message>
{
    public void serialize(Message t, DataOutput dos, int version) throws IOException
    {
        assert t.getVersion() == version : "internode protocol version mismatch"; // indicates programmer error.
        Header.serializer().serialize( t.header_, dos, version);
        byte[] bytes = t.getMessageBody();
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    public Message deserialize(DataInput dis, int version) throws IOException
    {
        Header header = Header.serializer().deserialize(dis, version);
        int size = dis.readInt();
        byte[] bytes = new byte[size];
        dis.readFully(bytes);
        return new Message(header, bytes, version);
    }

    public long serializedSize(Message message, int version)
    {
        throw new UnsupportedOperationException();
    }
}
