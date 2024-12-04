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

package org.apache.cassandra.service.accord.txn;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.conditions.ColumnConditionTest;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Generators;

import static accord.utils.Property.qt;

//TOOD (maintaince): rather than copy the condition supported kinds, maybe references directly from the type?
public class TxnConditionTest
{
    private static final SchemaProvider SCHEMA = new SchemaProvider();
    static
    {
        // ColumnMetadata serializer only stores the ks/table/name and uses Schema to load it
        Schema.instance = SCHEMA;
    }

    private static Gen<TxnCondition.Kind> BOOLEAN_KIND_GEN = Gens.pick(TxnCondition.Kind.AND, TxnCondition.Kind.OR);
    private static Gen<TxnCondition.Kind> EXISTS_KIND_GEN = Gens.pick(TxnCondition.Kind.IS_NOT_NULL, TxnCondition.Kind.IS_NULL);
    private static Gen<TxnCondition.Kind> VALUE_KIND_GEN = Gens.pick(TxnCondition.Kind.EQUAL, TxnCondition.Kind.NOT_EQUAL,
                                                                     TxnCondition.Kind.GREATER_THAN, TxnCondition.Kind.GREATER_THAN_OR_EQUAL,
                                                                     TxnCondition.Kind.LESS_THAN, TxnCondition.Kind.LESS_THAN_OR_EQUAL);
    private static Gen<ProtocolVersion> PROTOCOL_VERSION_GEN = Gens.enums().all(ProtocolVersion.class);
    private static Gen<ColumnMetadata> COLUM_METADATA_GEN = Generators.toGen(CassandraGenerators.columnMetadataGen()).map(cm -> {
        SCHEMA.add(cm);
        return cm;
    });
    private static Gen<ByteBuffer> BYTES_GEN = Generators.toGen(Generators.directAndHeapBytes(0, 10));
    private static Gen<TxnReference> TXN_REF_GEN = rs -> {
        return rs.nextBoolean() ? new TxnReference(rs.nextInt(0, Integer.MAX_VALUE), COLUM_METADATA_GEN.next(rs))
                                : new TxnReference(rs.nextInt(0, Integer.MAX_VALUE), COLUM_METADATA_GEN.next(rs), CellPath.create(BYTES_GEN.next(rs)));
    };
    private static Gen<Clustering<?>> CLUSTERING_GEN = Generators.toGen(CassandraGenerators.CLUSTERING_GEN);
    private static Gen<ColumnCondition.Bound> BOUND_GEN = ColumnConditionTest.boundGen().map(b -> {
        SCHEMA.add(b.column);
        return b;
    });

    @Test
    public void serde()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(txnConditionGen()).check(condition -> {
            for (Version version : Version.V1.greaterThanOrEqual())
                Serializers.testSerde(output, TxnCondition.serializer, condition, version);
            SCHEMA.clear();
        });
    }

    private Gen<TxnCondition> txnConditionGen()
    {
        return rs -> {
            switch (rs.nextInt(1, 5))
            {
                case 0: return TxnCondition.none();
                case 1: return new TxnCondition.Exists(TXN_REF_GEN.next(rs), EXISTS_KIND_GEN.next(rs));
                case 2: return new TxnCondition.Value(TXN_REF_GEN.next(rs), VALUE_KIND_GEN.next(rs), BYTES_GEN.next(rs), PROTOCOL_VERSION_GEN.next(rs));
                case 3: return new TxnCondition.ColumnConditionsAdapter(CLUSTERING_GEN.next(rs), Gens.lists(BOUND_GEN).ofSizeBetween(0, 3).next(rs));
                case 4: return new TxnCondition.BooleanGroup(BOOLEAN_KIND_GEN.next(rs), Gens.lists(txnConditionGen()).ofSizeBetween(0, 3).next(rs));
                default: throw new AssertionError();
            }
        };
    }

    private static class SchemaProvider extends MockSchema.MockSchemaProvider
    {
        private final class Key
        {
            private final String keyspace, table;
            private final ByteBuffer name;

            private Key(String keyspace, String table, ByteBuffer name)
            {
                this.keyspace = keyspace;
                this.table = table;
                this.name = name;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Key key = (Key) o;
                return keyspace.equals(key.keyspace) && table.equals(key.table) && name.equals(key.name);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(keyspace, table, name);
            }

            @Override
            public String toString()
            {
                try
                {
                    return keyspace + "." + table + "/" + ByteBufferUtil.string(name);
                }
                catch (CharacterCodingException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
        private final Map<Key, ColumnMetadata> columns = new HashMap<>();

        public void add(ColumnMetadata cm)
        {
            columns.put(new Key(cm.ksName, cm.cfName, cm.name.bytes), cm);
        }

        public void clear()
        {
            columns.clear();
        }

        @Nullable
        @Override
        public ColumnMetadata getColumnMetadata(String keyspace, String table, ByteBuffer name)
        {
            Key key = new Key(keyspace, table, name);
            ColumnMetadata match = columns.get(key);
            if (match == null)
            {
                throw new AssertionError("Unable to find ColumnMetadata for " + key + "; known columns are " + columns.keySet());
            }
            return match;
        }
    }
}