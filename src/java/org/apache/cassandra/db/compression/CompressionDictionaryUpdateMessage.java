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

package org.apache.cassandra.db.compression;

import java.io.IOException;

import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;

public class CompressionDictionaryUpdateMessage
{
    public static final IVersionedSerializer<CompressionDictionaryUpdateMessage> serializer = new DictionaryUpdateMessageSerializer();

    public final TableId tableId;
    public final DictId dictionaryId;

    public CompressionDictionaryUpdateMessage(TableId tableId, DictId dictionaryId)
    {
        this.tableId = tableId;
        this.dictionaryId = dictionaryId;
    }

    public static class DictionaryUpdateMessageSerializer implements IVersionedSerializer<CompressionDictionaryUpdateMessage>
    {
        @Override
        public void serialize(CompressionDictionaryUpdateMessage message, DataOutputPlus out, int version) throws IOException
        {
            TableId.serializer.serialize(message.tableId, out, version);
            out.writeByte(message.dictionaryId.kind.ordinal());
            out.writeLong(message.dictionaryId.id);
        }

        @Override
        public CompressionDictionaryUpdateMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId tableId = TableId.serializer.deserialize(in, version);
            int kindOrdinal = in.readByte();
            long dictionaryId = in.readLong();
            DictId dictId = new DictId(CompressionDictionary.Kind.values()[kindOrdinal], dictionaryId);
            return new CompressionDictionaryUpdateMessage(tableId, dictId);
        }

        @Override
        public long serializedSize(CompressionDictionaryUpdateMessage message, int version)
        {
            return TableId.serializer.serializedSize(message.tableId, version) +
                   1 + // byte for kind ordinal
                   8; // long for dictionaryId
        }
    }
}
