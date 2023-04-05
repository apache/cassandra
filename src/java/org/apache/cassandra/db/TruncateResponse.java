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

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * This message is sent back the truncate operation and basically specifies if
 * the truncate succeeded.
 */
public class TruncateResponse
{
    public static final TruncateResponseSerializer serializer = new TruncateResponseSerializer();

    public final String keyspace;
    public final String columnFamily;
    public final boolean success;

    public TruncateResponse(String keyspace, String columnFamily, boolean success)
    {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.success = success;
    }

    public static class TruncateResponseSerializer implements IVersionedSerializer<TruncateResponse>
    {
        public void serialize(TruncateResponse tr, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(tr.keyspace);
            out.writeUTF(tr.columnFamily);
            out.writeBoolean(tr.success);
        }

        public TruncateResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            boolean success = in.readBoolean();
            return new TruncateResponse(keyspace, columnFamily, success);
        }

        public long serializedSize(TruncateResponse tr, int version)
        {
            return TypeSizes.sizeof(tr.keyspace)
                 + TypeSizes.sizeof(tr.columnFamily)
                 + TypeSizes.sizeof(tr.success);
        }
    }
}
