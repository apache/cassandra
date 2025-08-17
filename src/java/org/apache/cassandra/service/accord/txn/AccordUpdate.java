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
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.TxnSerializer;
import org.apache.cassandra.service.accord.serializers.Version;

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

    public void failCondition()
    {

    }

    public boolean checkCondition(Data data)
    {
        throw new UnsupportedOperationException();
    }

    public abstract ConsistencyLevel cassandraCommitCL();

    public abstract Kind kind();

    public abstract long estimatedSizeOnHeap();

    public interface AccordUpdateSerializer<T extends AccordUpdate> extends TxnSerializer<T>
    {
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

    public static final AccordUpdateSerializer<AccordUpdate> serializer = new AccordUpdateSerializer<>()
    {
        @Override
        public void serialize(AccordUpdate update, TableMetadatasAndKeys tablesAndKeys, DataOutputPlus out, Version version) throws IOException
        {
            out.writeByte(update.kind().val);
            serializerFor(update).serialize(update, tablesAndKeys, out, version);
        }

        @Override
        public AccordUpdate deserialize(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            Kind kind = Kind.valueOf(in.readByte());
            return (AccordUpdate) serializerFor(kind).deserialize(tablesAndKeys, in, version);
        }

        @Override
        public void skip(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            Kind kind = Kind.valueOf(in.readByte());
            serializerFor(kind).skip(tablesAndKeys, in, version);
        }

        @Override
        public long serializedSize(AccordUpdate update, TableMetadatasAndKeys tablesAndKeys, Version version)
        {
            return 1 + serializerFor(update).serializedSize(update, tablesAndKeys, version);
        }
    };
}