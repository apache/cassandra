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

import accord.local.Command;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ListenerSerializers
{
    public enum Kind
    {
        COMMAND;

        private static Kind of(Command.DurableAndIdempotentListener listener)
        {
            if (listener instanceof Command.ProxyListener)
                return COMMAND;

            throw new IllegalArgumentException("Unsupported listener type: " + listener.getClass().getName());
        }
    }


    private static final IVersionedSerializer<Command.ProxyListener> commandListener = new IVersionedSerializer<Command.ProxyListener>()
    {
        @Override
        public void serialize(Command.ProxyListener listener, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(listener.txnId(), out, version);
        }

        @Override
        public Command.ProxyListener deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Command.ProxyListener(CommandSerializers.txnId.deserialize(in, version));
        }

        @Override
        public long serializedSize(Command.ProxyListener listener, int version)
        {
            return CommandSerializers.txnId.serializedSize(listener.txnId(), version);
        }
    };

    public static final IVersionedSerializer<Command.DurableAndIdempotentListener> listener = new IVersionedSerializer<Command.DurableAndIdempotentListener>()
    {
        @Override
        public void serialize(Command.DurableAndIdempotentListener listener, DataOutputPlus out, int version) throws IOException
        {
            Kind kind = Kind.of(listener);
            out.write(kind.ordinal());
            switch (kind)
            {
                case COMMAND:
                    commandListener.serialize((Command.ProxyListener) listener, out, version);
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public Command.DurableAndIdempotentListener deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.values()[in.readByte()];
            switch (kind)
            {
                case COMMAND:
                    return commandListener.deserialize(in, version);
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public long serializedSize(Command.DurableAndIdempotentListener listener, int version)
        {
            Kind kind = Kind.of(listener);
            long size = TypeSizes.BYTE_SIZE;
            switch (kind)
            {
                case COMMAND:
                    size += commandListener.serializedSize((Command.ProxyListener) listener, version);
                    break;
                default:
                    throw new IllegalArgumentException();
            }

            return size;
        }
    };
}
