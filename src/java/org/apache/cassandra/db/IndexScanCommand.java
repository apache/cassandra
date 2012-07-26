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

import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;

public class IndexScanCommand
{
    public static final IndexScanCommandSerializer serializer = new IndexScanCommandSerializer();

    public final String keyspace;
    public final String column_family;
    public final IndexClause index_clause;
    public final SlicePredicate predicate;
    public final AbstractBounds<RowPosition> range;

    public IndexScanCommand(String keyspace, String column_family, IndexClause index_clause, SlicePredicate predicate, AbstractBounds<RowPosition> range)
    {

        this.keyspace = keyspace;
        this.column_family = column_family;
        this.index_clause = index_clause;
        this.predicate = predicate;
        this.range = range;
    }

    public MessageOut<IndexScanCommand> createMessage()
    {
        return new MessageOut<IndexScanCommand>(MessagingService.Verb.INDEX_SCAN, this, serializer);
    }

    static class IndexScanCommandSerializer implements IVersionedSerializer<IndexScanCommand>
    {
        public void serialize(IndexScanCommand o, DataOutput out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_12; // 1.2 only uses RangeScanCommand

            out.writeUTF(o.keyspace);
            out.writeUTF(o.column_family);
            TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
            FBUtilities.serialize(ser, o.index_clause, out);
            FBUtilities.serialize(ser, o.predicate, out);
            AbstractBounds.serializer.serialize(o.range, out, version);
        }

        public IndexScanCommand deserialize(DataInput in, int version) throws IOException
        {
            assert version < MessagingService.VERSION_12; // 1.2 only uses RangeScanCommand

            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();

            IndexClause indexClause = new IndexClause();
            SlicePredicate predicate = new SlicePredicate();
            TDeserializer dser = new TDeserializer(new TBinaryProtocol.Factory());
            FBUtilities.deserialize(dser, indexClause, in);
            FBUtilities.deserialize(dser, predicate, in);
            AbstractBounds<RowPosition> range = AbstractBounds.serializer.deserialize(in, version).toRowBounds();
            return new IndexScanCommand(keyspace, columnFamily, indexClause, predicate, range);
        }

        public long serializedSize(IndexScanCommand object, int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}
