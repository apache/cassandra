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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Generators;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static accord.utils.Property.qt;

public class TableMetadatasTest
{
    @Test
    public void test()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(tables()).check(tables -> {
            TableMetadatas metadatas = toMetadatas(tables);
            Schema.instance = buildSchema(tables);
            Serializers.testSerde(output, SelfSerializer.instance, metadatas);

            UnversionedSerializer<TableMetadata> serializer = tableSerializer(metadatas);
            for (var metadata : tables.values())
                Serializers.testSerde(output, serializer, metadata);
        });
    }

    static SchemaProvider buildSchema(Map<TableId, TableMetadata> tables) throws UnknownTableException
    {
        SchemaProvider schema = Mockito.mock(SchemaProvider.class);
        Mockito.when(schema.getTableMetadata(Mockito.<TableId>any())).thenAnswer(new Answer<TableMetadata>()
        {
            @Override
            public TableMetadata answer(InvocationOnMock invocationOnMock) throws Throwable
            {
                TableId id = invocationOnMock.getArgument(0);
                var metadata = tables.get(id);
                if (metadata == null) throw new UnknownTableException("Unknown table " + id, id);
                return metadata;
            }
        });
        return schema;
    }

    static TableMetadatas toMetadatas(Map<TableId, TableMetadata> map)
    {
        TableMetadatas.Collector collector = new TableMetadatas.Collector();
        map.values().forEach(collector::add);
        return collector.build();
    }

    static Gen<LinkedHashMap<TableId, TableMetadata>> tables()
    {
        Gen<TableId> idGen = Generators.toGen(CassandraGenerators.TABLE_ID_GEN);
        return rs -> {
            TableId[] ids = Gens.arrays(TableId.class, idGen).unique().ofSizeBetween(0, 100).next(rs);
            LinkedHashMap<TableId, TableMetadata> map = new LinkedHashMap<>();
            for (int i = 0; i < ids.length; i++)
                map.put(ids[i], forId(ids[i]));
            return map;
        };
    }

    private static TableMetadata forId(TableId id)
    {
        TableMetadata metadata = TableMetadata.minimal("ks", "tbl", id);
        if (!metadata.id().equals(id)) throw new AssertionError("Unexpected table id: " + metadata.id() + "; expected " + id);
        return metadata;
    }

    private static UnversionedSerializer<TableMetadata> tableSerializer(TableMetadatas metadatas)
    {
        return new UnversionedSerializer<>()
        {
            @Override
            public void serialize(TableMetadata t, DataOutputPlus out) throws IOException
            {
                metadatas.serialize(t, out);
            }

            @Override
            public TableMetadata deserialize(DataInputPlus in) throws IOException
            {
                return metadatas.deserialize(in);
            }

            @Override
            public long serializedSize(TableMetadata t)
            {
                return metadatas.serializedSize(t);
            }
        };
    }

    private enum SelfSerializer implements UnversionedSerializer<TableMetadatas>
    {
        instance;

        @Override
        public void serialize(TableMetadatas t, DataOutputPlus out) throws IOException
        {
            t.serializeSelf(out);
        }

        @Override
        public TableMetadatas deserialize(DataInputPlus in) throws IOException
        {
            return TableMetadatas.deserializeSelf(in);
        }

        @Override
        public long serializedSize(TableMetadatas t)
        {
            return t.serializedSelfSize();
        }
    }
}