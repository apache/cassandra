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

package org.apache.cassandra.io;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

/**
 * Serializes a dummy byte that can't be set. Will always write 0 and return 0 in a correctly formed message.
 */
public class DummyByteVersionedSerializer implements IVersionedSerializer<byte[]>
{
    public static final DummyByteVersionedSerializer instance = new DummyByteVersionedSerializer();

    private DummyByteVersionedSerializer() {}

    public void serialize(byte[] bytes, DataOutputPlus out, int version) throws IOException
    {
        Preconditions.checkArgument(bytes == MessagingService.ONE_BYTE);
        out.write(0);
    }

    public byte[] deserialize(DataInputPlus in, int version) throws IOException
    {
        assert(0 == in.readByte());
        return MessagingService.ONE_BYTE;
    }

    public long serializedSize(byte[] bytes, int version)
    {
        //Payload
        return 1;
    }
}
