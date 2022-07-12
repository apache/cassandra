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

package org.apache.cassandra.nodes;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;

final class SerHelper
{
    static Module createMsgpackModule()
    {
        SimpleModule module = new SimpleModule();
        module.addSerializer(CassandraVersion.class, new CassandraVersionSerializer());
        module.addDeserializer(CassandraVersion.class, new CassandraVersionDeserializer());
        module.addSerializer(Token.class, new TokenSerializer());
        module.addDeserializer(Token.class, new TokenDeserializer());
        module.addSerializer(InetAddressAndPort.class, new InetAddressAndPortSerializer());
        module.addDeserializer(InetAddressAndPort.class, new InetAddressAndPortDeserializer());
        module.addSerializer(UUID.class, new UUIDSerializer());
        module.addDeserializer(UUID.class, new UUIDDeserializer());
        module.addSerializer(LocalInfo.TruncationRecord.class, new TruncationRecordSerializer());
        module.addDeserializer(LocalInfo.TruncationRecord.class, new TruncationRecordDeserializer());
        return module;
    }

    // following inner classes are custom Jackson serializers to (de)serialize custom classes and serialize
    // standard classes in a more efficient way (i.e. binary, not string).

    private static final class CassandraVersionSerializer extends JsonSerializer<CassandraVersion>
    {
        @Override
        public void serialize(CassandraVersion t, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
        {
            jsonGenerator.writeString(t.toString());
        }
    }

    private static final class CassandraVersionDeserializer extends JsonDeserializer<CassandraVersion>
    {
        @Override
        public CassandraVersion deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException
        {
            return new CassandraVersion(jsonParser.getText());
        }
    }

    private static final class TokenSerializer extends JsonSerializer<Token>
    {
        @Override
        public void serialize(Token t, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
        {
            jsonGenerator.writeBinary(ByteBufferUtil.getArray(DatabaseDescriptor.getPartitioner().getTokenFactory().toByteArray(t)));
        }
    }

    private static final class TokenDeserializer extends JsonDeserializer<Token>
    {
        @Override
        public Token deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException
        {
            return DatabaseDescriptor.getPartitioner().getTokenFactory().fromByteArray(ByteBuffer.wrap(jsonParser.getBinaryValue()));
        }
    }

    private static final class InetAddressAndPortSerializer extends JsonSerializer<InetAddressAndPort>
    {
        @Override
        public void serialize(InetAddressAndPort t, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
        {
            jsonGenerator.writeStartArray();
            jsonGenerator.writeBinary(t.addressBytes);
            jsonGenerator.writeNumber(t.port);
            jsonGenerator.writeEndArray();

        }
    }

    private static final class InetAddressAndPortDeserializer extends JsonDeserializer<InetAddressAndPort>
    {
        @Override
        public InetAddressAndPort deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException
        {
            jsonParser.isExpectedStartArrayToken();
            jsonParser.nextToken();
            InetAddress address = InetAddress.getByAddress(jsonParser.getBinaryValue());
            jsonParser.nextToken();
            int port = jsonParser.getIntValue();
            jsonParser.nextToken();
            return InetAddressAndPort.getByAddressOverrideDefaults(address, port);
        }
    }

    private static final class UUIDSerializer extends JsonSerializer<UUID>
    {
        @Override
        public void serialize(UUID t, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
        {
            jsonGenerator.writeStartArray();
            jsonGenerator.writeNumber(t.getMostSignificantBits());
            jsonGenerator.writeNumber(t.getLeastSignificantBits());
            jsonGenerator.writeEndArray();
        }
    }

    private static final class UUIDDeserializer extends JsonDeserializer<UUID>
    {
        @Override
        public UUID deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException
        {
            jsonParser.isExpectedStartArrayToken();
            jsonParser.nextToken();
            long msb = jsonParser.getLongValue();
            jsonParser.nextToken();
            long lsb = jsonParser.getLongValue();
            jsonParser.nextToken();
            return new UUID(msb, lsb);
        }
    }

    private static final class TruncationRecordSerializer extends JsonSerializer<LocalInfo.TruncationRecord>
    {
        @Override
        public void serialize(LocalInfo.TruncationRecord t, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
        {
            jsonGenerator.writeStartArray();
            jsonGenerator.writeNumber(t.truncatedAt);
            jsonGenerator.writeNumber(t.position.segmentId);
            jsonGenerator.writeNumber(t.position.position);
            jsonGenerator.writeEndArray();
        }
    }

    private static final class TruncationRecordDeserializer extends JsonDeserializer<LocalInfo.TruncationRecord>
    {
        @Override
        public LocalInfo.TruncationRecord deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException
        {
            jsonParser.isExpectedStartArrayToken();
            jsonParser.nextToken();
            long truncatedAt = jsonParser.getLongValue();
            jsonParser.nextToken();
            long segmentId = jsonParser.getLongValue();
            jsonParser.nextToken();
            int position = jsonParser.getIntValue();
            jsonParser.nextToken();
            return new LocalInfo.TruncationRecord(new CommitLogPosition(segmentId, position), truncatedAt);
        }
    }
}
