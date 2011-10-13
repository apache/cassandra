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

import java.io.*;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A truncate operation descriptor
 */
public class Truncation implements MessageProducer
{
    private static IVersionedSerializer<Truncation> serializer;

    public final String keyspace;
    public final String columnFamily;

    static
    {
        serializer = new TruncationSerializer();
    }

    public static IVersionedSerializer<Truncation> serializer()
    {
        return serializer;
    }

    public Truncation(String keyspace, String columnFamily)
    {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    public Message getMessage(Integer version) throws IOException
    {
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        serializer().serialize(this, dos, version);
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.TRUNCATE, bos.toByteArray(), version);
    }

    public String toString()
    {
        return "Truncation(" + "keyspace='" + keyspace + '\'' + ", cf='" + columnFamily + "\')";
    }
}

class TruncationSerializer implements IVersionedSerializer<Truncation>
{
    public void serialize(Truncation t, DataOutput dos, int version) throws IOException
    {
        dos.writeUTF(t.keyspace);
        dos.writeUTF(t.columnFamily);
    }

    public Truncation deserialize(DataInput dis, int version) throws IOException
    {
        String keyspace = dis.readUTF();
        String columnFamily = dis.readUTF();
        return new Truncation(keyspace, columnFamily);
    }

    public long serializedSize(Truncation truncation, int version)
    {
        throw new UnsupportedOperationException();
    }
}
