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
import javax.annotation.Nullable;

import accord.api.Data;
import accord.api.Update;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public abstract class AccordUpdate implements Update
{
    public enum Kind
    {
        TXN(0),
        UNRECOVERABLE_REPAIR(1),
        NONE(2),
        ;

        int val;

        Kind(int val)
        {
            this.val = val;
        }

        public static Kind valueOf(int val)
        {
            switch(val)
            {
                case 0:
                    return TXN;
                case 1:
                    return UNRECOVERABLE_REPAIR;
                default:
                    throw new IllegalArgumentException("Unrecognized AccordUpdate.Kind value " + val);
            }
        }
    }

    public static Kind kind(@Nullable Update update)
    {
        if (update == null)
            return Kind.NONE;
        return ((AccordUpdate)update).kind();
    }

    public boolean checkCondition(Data data)
    {
        throw new UnsupportedOperationException();
    }

    public abstract ConsistencyLevel cassandraCommitCL();

    public abstract Kind kind();

    public abstract long estimatedSizeOnHeap();

    public interface AccordUpdateSerializer<T extends AccordUpdate> extends IVersionedSerializer<T>
    {
        void serialize(T update, DataOutputPlus out, int version) throws IOException;
        T deserialize(DataInputPlus in, int version) throws IOException;
        long serializedSize(T update, int version);
    }

    private static AccordUpdateSerializer serializerFor(AccordUpdate toSerialize)
    {
        return serializerFor(toSerialize.kind());
    }

    private static AccordUpdateSerializer serializerFor(Kind kind)
    {
        switch (kind)
        {
            case TXN:
                return TxnUpdate.serializer;
            case UNRECOVERABLE_REPAIR:
                return UnrecoverableRepairUpdate.serializer;
            default:
                throw new IllegalStateException("Unsupported AccordUpdate Kind " + kind);
        }
    }

    public static final AccordUpdateSerializer<AccordUpdate> serializer = new AccordUpdateSerializer<AccordUpdate>()
    {
        @Override
        public void serialize(AccordUpdate update, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(update.kind().val);
            serializerFor(update).serialize(update, out, version);
        }

        @Override
        public AccordUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.valueOf(in.readByte());
            return serializerFor(kind).deserialize(in, version);
        }

        @Override
        public long serializedSize(AccordUpdate update, int version)
        {
            return 1 + serializerFor(update).serializedSize(update, version);
        }
    };
}