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
 * A truncate operation descriptor
 */
public class TruncateRequest
{
    public static final IVersionedSerializer<TruncateRequest> serializer = new Serializer();

    public final String keyspace;
    public final String table;

    public TruncateRequest(String keyspace, String table)
    {
        this.keyspace = keyspace;
        this.table = table;
    }

    @Override
    public String toString()
    {
        return String.format("TruncateRequest(keyspace='%s', table='%s')'", keyspace, table);
    }

    private static class Serializer implements IVersionedSerializer<TruncateRequest>
    {
        public void serialize(TruncateRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(request.keyspace);
            out.writeUTF(request.table);
        }

        public TruncateRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            String table = in.readUTF();
            return new TruncateRequest(keyspace, table);
        }

        public long serializedSize(TruncateRequest request, int version)
        {
            return TypeSizes.sizeof(request.keyspace) + TypeSizes.sizeof(request.table);
        }
    }
}
