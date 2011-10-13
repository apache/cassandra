/**
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
 */

package org.apache.cassandra.db;

import java.io.*;
import java.util.Arrays;

import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.cassandra.thrift.TBinaryProtocol;

public class IndexScanCommand implements MessageProducer
{
    private static final IndexScanCommandSerializer serializer = new IndexScanCommandSerializer();

    public final String keyspace;
    public final String column_family;
    public final IndexClause index_clause;
    public final SlicePredicate predicate;
    public final AbstractBounds range;

    public IndexScanCommand(String keyspace, String column_family, IndexClause index_clause, SlicePredicate predicate, AbstractBounds range)
    {

        this.keyspace = keyspace;
        this.column_family = column_family;
        this.index_clause = index_clause;
        this.predicate = predicate;
        this.range = range;
    }

    public Message getMessage(Integer version)
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        try
        {
            serializer.serialize(this, dob);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return new Message(FBUtilities.getBroadcastAddress(),
                           StorageService.Verb.INDEX_SCAN,
                           Arrays.copyOf(dob.getData(), dob.getLength()),
                           version);
    }

    public static IndexScanCommand read(Message message) throws IOException
    {
        byte[] bytes = message.getMessageBody();
        FastByteArrayInputStream bis = new FastByteArrayInputStream(bytes);
        return serializer.deserialize(new DataInputStream(bis));
    }

    private static class IndexScanCommandSerializer implements ISerializer<IndexScanCommand>
    {
        public void serialize(IndexScanCommand o, DataOutput out) throws IOException
        {
            out.writeUTF(o.keyspace);
            out.writeUTF(o.column_family);
            TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
            FBUtilities.serialize(ser, o.index_clause, out);
            FBUtilities.serialize(ser, o.predicate, out);
            AbstractBounds.serializer().serialize(o.range, out);
        }

        public IndexScanCommand deserialize(DataInput in) throws IOException
        {
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();

            TDeserializer dser = new TDeserializer(new TBinaryProtocol.Factory());
            IndexClause indexClause = new IndexClause();
            FBUtilities.deserialize(dser, indexClause, in);
            SlicePredicate predicate = new SlicePredicate();
            FBUtilities.deserialize(dser, predicate, in);
            AbstractBounds range = AbstractBounds.serializer().deserialize(in);

            return new IndexScanCommand(keyspace, columnFamily, indexClause, predicate, range);
        }

        public long serializedSize(IndexScanCommand object)
        {
            throw new UnsupportedOperationException();
        }
    }
}
