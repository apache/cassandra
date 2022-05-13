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

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Result;
import accord.api.Update;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.service.accord.AccordSerializers.deserialize;
import static org.apache.cassandra.service.accord.AccordSerializers.serialize;

// TODO: This is currently unused, but we might want to use it to support returning the condition result.
public class TxnAppliedQuery implements Query
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnAppliedQuery(ByteBufferUtil.EMPTY_BYTE_BUFFER));
    public static class Applied implements Result
    {
        public static final Applied TRUE = new Applied();
        public static final Applied FALSE = new Applied();
        private static final long SIZE = ObjectSizes.measure(TRUE);

        private Applied() {}

        public boolean wasApplied()
        {
            return this == TRUE;
        }

        public static Applied valueOf(boolean b)
        {
            return b ? TRUE : FALSE;
        }

        public long estimatedSizeOnHeap()
        {
            return SIZE;
        }

        public static final IVersionedSerializer<Applied> serializer = new IVersionedSerializer<Applied>()
        {
            @Override
            public void serialize(Applied applied, DataOutputPlus out, int version) throws IOException
            {
                out.writeBoolean(applied.wasApplied());
            }

            @Override
            public Applied deserialize(DataInputPlus in, int version) throws IOException
            {
                return Applied.valueOf(in.readBoolean());
            }

            @Override
            public long serializedSize(Applied applied, int version)
            {
                return TypeSizes.BOOL_SIZE;
            }
        };
    }
    private final ByteBuffer serializedCondition;

    public TxnAppliedQuery(TxnCondition condition)
    {
        this.serializedCondition = serialize(condition, TxnCondition.serializer);
    }

    public TxnAppliedQuery(ByteBuffer serializedCondition)
    {
        this.serializedCondition = serializedCondition;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnAppliedQuery query = (TxnAppliedQuery) o;
        return Objects.equals(serializedCondition, query.serializedCondition);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(serializedCondition);
    }

    @Override
    public String toString()
    {
        return "TxnAppliedQuery{serializedCondition=" + deserialize(serializedCondition, TxnCondition.serializer) + '}';
    }

    public long estimatedSizeOnHeap()
    {
        return EMPTY_SIZE + ByteBufferUtil.estimatedSizeOnHeap(serializedCondition);
    }

    @Override
    public Result compute(TxnId txnId, Data data, @Nullable Read read, @Nullable Update update)
    {
        TxnCondition condition = deserialize(serializedCondition, TxnCondition.serializer);
        return condition.applies((TxnData) data) ? Applied.TRUE : Applied.FALSE;
    }

    public static final IVersionedSerializer<TxnAppliedQuery> serializer = new IVersionedSerializer<TxnAppliedQuery>()
    {
        @Override
        public void serialize(TxnAppliedQuery query, DataOutputPlus out, int version) throws IOException
        {
            ByteBufferUtil.writeWithVIntLength(query.serializedCondition, out);
        }

        @Override
        public TxnAppliedQuery deserialize(DataInputPlus in, int version) throws IOException
        {
            return new TxnAppliedQuery(ByteBufferUtil.readWithVIntLength(in));
        }

        @Override
        public long serializedSize(TxnAppliedQuery query, int version)
        {
            return ByteBufferUtil.serializedSizeWithVIntLength(query.serializedCondition);
        }
    };
}
