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

import accord.impl.CommandsForKey;
import accord.local.Command;
import accord.local.CommandListener;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.api.PartitionKey;

public class ListenerSerializers
{
    public enum Kind
    {
        COMMAND, COMMANDS_FOR_KEY;

        private static Kind of(CommandListener listener)
        {
            if (listener instanceof Command.Listener)
                return COMMAND;

            if (listener instanceof CommandsForKey.Listener)
                return COMMANDS_FOR_KEY;

            throw new IllegalArgumentException("Unsupported listener type: " + listener.getClass().getName());
        }
    }


    private static final IVersionedSerializer<Command.Listener> commandListener = new IVersionedSerializer<Command.Listener>()
    {
        @Override
        public void serialize(Command.Listener listener, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(listener.txnId(), out, version);
        }

        @Override
        public Command.Listener deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Command.Listener(CommandSerializers.txnId.deserialize(in, version));
        }

        @Override
        public long serializedSize(Command.Listener listener, int version)
        {
            return CommandSerializers.txnId.serializedSize(listener.txnId(), version);
        }
    };

    private static final IVersionedSerializer<CommandsForKey.Listener> cfkListener = new IVersionedSerializer<CommandsForKey.Listener>()
    {
        @Override
        public void serialize(CommandsForKey.Listener listener, DataOutputPlus out, int version) throws IOException
        {
            PartitionKey.serializer.serialize((PartitionKey) listener.key(), out, version);
        }

        @Override
        public CommandsForKey.Listener deserialize(DataInputPlus in, int version) throws IOException
        {
            return CommandsForKey.SerializerSupport.listener(PartitionKey.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(CommandsForKey.Listener listener, int version)
        {
            return PartitionKey.serializer.serializedSize((PartitionKey) listener.key(), version);
        }
    };

    public static final IVersionedSerializer<CommandListener> listener = new IVersionedSerializer<CommandListener>()
    {
        @Override
        public void serialize(CommandListener listener, DataOutputPlus out, int version) throws IOException
        {
            Invariants.checkArgument(!listener.isTransient());
            Kind kind = Kind.of(listener);
            out.write(kind.ordinal());
            switch (kind)
            {
                case COMMAND:
                    commandListener.serialize((Command.Listener) listener, out, version);
                    break;
                case COMMANDS_FOR_KEY:
                    cfkListener.serialize((CommandsForKey.Listener) listener, out, version);
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public CommandListener deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.values()[in.readByte()];
            switch (kind)
            {
                case COMMAND:
                    return commandListener.deserialize(in, version);
                case COMMANDS_FOR_KEY:
                    return cfkListener.deserialize(in, version);
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public long serializedSize(CommandListener listener, int version)
        {
            Invariants.checkArgument(!listener.isTransient());
            Kind kind = Kind.of(listener);
            long size = TypeSizes.BYTE_SIZE;
            switch (kind)
            {
                case COMMAND:
                    size += commandListener.serializedSize((Command.Listener) listener, version);
                    break;
                case COMMANDS_FOR_KEY:
                    size += cfkListener.serializedSize((CommandsForKey.Listener) listener, version);
                    break;
                default:
                    throw new IllegalArgumentException();
            }

            return size;
        }
    };
}
