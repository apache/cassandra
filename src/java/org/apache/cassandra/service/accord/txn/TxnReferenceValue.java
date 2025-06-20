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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ParameterisedUnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class TxnReferenceValue
{
    private interface Serializer<T extends TxnReferenceValue>
    {
        void serialize(T t, TableMetadatas tables, DataOutputPlus out) throws IOException;
        T deserialize(TableMetadatas tables, DataInputPlus in, Kind kind) throws IOException;
        long serializedSize(T t, TableMetadatas tables);
    }

    enum Kind
    {
        CONSTANT(Constant.serializer),
        SUBSTITUTION(Substitution.serializer);

        @SuppressWarnings("rawtypes")
        final Serializer serializer;

        Kind(Serializer<? extends TxnReferenceValue> serializer)
        {
            this.serializer = serializer;
        }
    }

    protected abstract Kind kind();
    abstract ByteBuffer compute(TxnData data, AbstractType<?> receiver);
    abstract void collect(TableMetadatas.Collector collector);

    public static class Constant extends TxnReferenceValue
    {
        @Nullable
        private final ByteBuffer value;

        public Constant(@Nullable ByteBuffer value)
        {
            this.value = value;
        }

        public ByteBuffer getValue()
        {
            return value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Constant constant = (Constant) o;
            return Objects.equals(value, constant.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value);
        }

        @Override
        public String toString()
        {
            return "Constant=" + value == null ? "null" : ByteBufferUtil.bytesToHex(value);
        }

        @Override
        public Kind kind()
        {
            return Kind.CONSTANT;
        }

        @Override
        public ByteBuffer compute(TxnData data, AbstractType<?> receiver)
        {
            return value;
        }

        @Override
        void collect(TableMetadatas.Collector collector)
        {
        }

        private static final Serializer<Constant> serializer = new Serializer<Constant>()
        {
            @Override
            public void serialize(Constant constant, TableMetadatas tables, DataOutputPlus out) throws IOException
            {
                out.writeBoolean(constant.value != null);
                if (constant.value != null)
                    ByteBufferUtil.writeWithVIntLength(constant.value, out);
            }

            @Override
            public Constant deserialize(TableMetadatas tables, DataInputPlus in, Kind kind) throws IOException
            {
                return new Constant(in.readBoolean() ? ByteBufferUtil.readWithVIntLength(in) : null);
            }

            @Override
            public long serializedSize(Constant constant, TableMetadatas tables)
            {
                long size = TypeSizes.sizeof(constant.value != null);
                if (constant.value != null)
                    size += ByteBufferUtil.serializedSizeWithVIntLength(constant.value);
                return size;
            }
        };
    }

    public static class Substitution extends TxnReferenceValue
    {
        private final TxnReference.ColumnReference reference;

        public Substitution(TxnReference.ColumnReference reference)
        {
            this.reference = reference;
        }

        @Override
        public String toString()
        {
            return reference.toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Substitution that = (Substitution) o;
            return reference.equals(that.reference);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(reference);
        }

        @Override
        public Kind kind()
        {
            return Kind.SUBSTITUTION;
        }

        @Override
        public ByteBuffer compute(TxnData data, AbstractType<?> receiver)
        {
            return reference.toByteBuffer(data, receiver);
        }

        @Override
        void collect(TableMetadatas.Collector collector)
        {
            reference.collect(collector);
        }

        private static final Serializer<Substitution> serializer = new Serializer<>()
        {
            @Override
            public void serialize(Substitution substitution, TableMetadatas tables, DataOutputPlus out) throws IOException
            {
                TxnReference.columnSerializer.serialize(substitution.reference, tables, out);
            }

            @Override
            public Substitution deserialize(TableMetadatas tables, DataInputPlus in, Kind kind) throws IOException
            {
                return new Substitution(TxnReference.columnSerializer.deserialize(tables, in));
            }

            @Override
            public long serializedSize(Substitution substitution, TableMetadatas tables)
            {
                return TxnReference.columnSerializer.serializedSize(substitution.reference, tables);
            }
        };
    }

    static final ParameterisedUnversionedSerializer<TxnReferenceValue, TableMetadatas> serializer = new ParameterisedUnversionedSerializer<>()
    {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(TxnReferenceValue value, TableMetadatas tables, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(value.kind().ordinal());
            value.kind().serializer.serialize(value, tables, out);
        }

        @Override
        public TxnReferenceValue deserialize(TableMetadatas tables, DataInputPlus in) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedVInt32()];
            return kind.serializer.deserialize(tables, in, kind);
        }

        @SuppressWarnings("unchecked")
        @Override
        public long serializedSize(TxnReferenceValue value, TableMetadatas tables)
        {
            return TypeSizes.sizeofUnsignedVInt(value.kind().ordinal()) + value.kind().serializer.serializedSize(value, tables);
        }
    };
}
