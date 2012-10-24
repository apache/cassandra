/*
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;


/*
 * This message is sent back the row mutation verb handler
 * and basically specifies if the write succeeded or not for a particular
 * key in a table
 */
public class WriteResponse
{
    public static final WriteResponseSerializer serializer = new WriteResponseSerializer();

    public MessageOut<WriteResponse> createMessage()
    {
        return new MessageOut<WriteResponse>(MessagingService.Verb.REQUEST_RESPONSE, this, serializer);
    }

    public static class WriteResponseSerializer implements IVersionedSerializer<WriteResponse>
    {
        public void serialize(WriteResponse wm, DataOutput dos, int version) throws IOException
        {
            if (version < MessagingService.VERSION_12)
            {
                dos.writeUTF("");
                ByteBufferUtil.writeWithShortLength(ByteBufferUtil.EMPTY_BYTE_BUFFER, dos);
                dos.writeBoolean(true);
            }
        }

        public WriteResponse deserialize(DataInput dis, int version) throws IOException
        {
            if (version < MessagingService.VERSION_12)
            {
                dis.readUTF();
                ByteBufferUtil.readWithShortLength(dis);
                dis.readBoolean();
            }
            return new WriteResponse();
        }

        public long serializedSize(WriteResponse response, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            if (version < MessagingService.VERSION_12)
                return sizes.sizeof("") + sizes.sizeof((short) 0) + sizes.sizeof(true);
            return 0;
        }
    }
}
