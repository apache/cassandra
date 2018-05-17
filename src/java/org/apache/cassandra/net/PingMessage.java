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

package org.apache.cassandra.net;

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A backport of the version from 4.0, intentionnaly added as versions 4.0 or greater aren't guaranteed
 * to know the c* versions they communicate with before they connect.
 *
 * It is intentional that no {@link IVerbHandler} is provided as we do not want process the message;
 * the intent is to not break the stream by leaving it in an unclean state, with unconsumed bytes.
 * We do, however, assign a {@link org.apache.cassandra.concurrent.StageManager} stage
 * to maintain proper message flow.
 * See CASSANDRA-13393 for a discussion.
 */
public class PingMessage
{
    public static IVersionedSerializer<PingMessage> serializer = new PingMessageSerializer();

    public static class PingMessageSerializer implements IVersionedSerializer<PingMessage>
    {
        public void serialize(PingMessage t, DataOutputPlus out, int version)
        {
            throw new UnsupportedOperationException();
        }

        public PingMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            // throw away the one byte of the payload
            in.readByte();
            return new PingMessage();
        }

        public long serializedSize(PingMessage t, int version)
        {
            return 1;
        }
    }
}