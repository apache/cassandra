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
import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

/*
 * This empty response is sent by a replica to inform the coordinator that the write succeeded
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
        public void serialize(WriteResponse wm, DataOutputPlus out, int version) throws IOException
        {
        }

        public WriteResponse deserialize(DataInput in, int version) throws IOException
        {
            return new WriteResponse();
        }

        public long serializedSize(WriteResponse response, int version)
        {
            return 0;
        }
    }
}
