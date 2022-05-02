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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Preconditions;

import accord.api.Query;
import accord.api.Read;
import accord.api.Update;
import accord.local.Status;
import accord.txn.Dependencies;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.async.AsyncContext;

import static org.apache.cassandra.service.accord.serializers.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.service.accord.serializers.NullableSerializer.serializeNullable;
import static org.apache.cassandra.service.accord.serializers.NullableSerializer.serializedSizeNullable;

/**
 * To reduce the number of reads we have to do before an operation, command data is duplicated into related commands
 * and commands per key. This class contains serializers for these summaries, as well as the logic for instantiating
 * a read only summary or returning a cached instance if available.
 */
public class CommandSummaries
{
    private static class KindOnlyTxn extends Txn
    {
        private final Txn.Kind kind;
        public KindOnlyTxn(Txn.Kind kind) { this.kind = kind; }
        @Override public Kind kind() { return kind; }
        @Override public Keys keys() { throw new UnsupportedOperationException(); }
        @Override public Read read() { throw new UnsupportedOperationException(); }
        @Override public Query query() { throw new UnsupportedOperationException(); }
        @Override public Update update() { throw new UnsupportedOperationException(); }
    }

    private static final Txn DUMMY_TXN = new Txn()
    {
        @Override public Kind kind() { throw new UnsupportedOperationException(); }
        @Override public Keys keys() { throw new UnsupportedOperationException(); }
        @Override public Read read() { throw new UnsupportedOperationException(); }
        @Override public Query query() { throw new UnsupportedOperationException(); }
        @Override public Update update() { throw new UnsupportedOperationException(); }
    };

    public enum Version
    {
        VERSION_0(0, MessagingService.current_version);
        final byte version;
        final int msg_version;

        Version(int version, int msg_version)
        {
            this.version = (byte) version;
            this.msg_version = msg_version;
        }

        public static final Version current = VERSION_0;

        public static Version fromByte(byte b)
        {
            switch (b)
            {
                case 0:
                    return VERSION_0;
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    public static abstract class SummarySerializer
    {
        public void serialize(AccordCommand command, DataOutputPlus out, Version version) throws IOException
        {
            out.write(version.version);
            CommandSerializers.txnId.serialize(command.txnId(), out, version.msg_version);
            serializeBody(command, out, version);
        }

        public ByteBuffer serialize(AccordCommand command)
        {
            Version version = Version.current;
            int size = serializedBodySize(command, version);
            try (DataOutputBuffer out = new DataOutputBuffer(size))
            {
                serialize(command, out, version);
                return out.buffer(false);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public Version deserializeVersion(DataInputPlus in) throws IOException
        {
            return Version.fromByte(in.readByte());
        }

        // check for cached command first, otherwise deserialize
        public AccordCommand deserialize(AccordCommandStore commandStore, DataInputPlus in) throws IOException
        {
            Version version = deserializeVersion(in);
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version.msg_version);
            AsyncContext context = commandStore.getContext();
            AccordCommand command = context.commands.get(txnId);
            if (command != null)
                return command;

            AccordCommand.ReadOnly summary = context.commands.summary(txnId);
            if (summary == null)
                summary = new AccordCommand.ReadOnly(commandStore, txnId);

            Preconditions.checkState(summary.isReadOnlyInstance());
            context.commands.addSummary(summary);

            deserializeBody(summary, in, version);

            return summary;
        }

        public AccordCommand deserialize(AccordCommandStore commandStore, ByteBuffer bytes)
        {
            try (DataInputBuffer in = new DataInputBuffer(bytes, true))
            {
                return deserialize(commandStore, in);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public int serializedSize(AccordCommand command)
        {
            Version version = Version.current;
            int size = TypeSizes.sizeof(version.version);
            size += CommandSerializers.txnId.serializedSize();
            return size + serializedBodySize(command, version);
        }

        public abstract void serializeBody(AccordCommand command, DataOutputPlus out, Version version) throws IOException;
        public abstract void deserializeBody(AccordCommand.ReadOnly command, DataInputPlus in, Version version) throws IOException;
        public abstract int serializedBodySize(AccordCommand command, Version version);

        /**
         * Determines if current modifications require updating command data duplicated elsewhere
         */
        public abstract boolean needsUpdate(AccordCommand command);
    }

    private static final SummarySerializer statusExecute = new SummarySerializer()
    {
        @Override
        public void serializeBody(AccordCommand command, DataOutputPlus out, Version version) throws IOException
        {
            out.write(command.status().ordinal());
            boolean hasExecuteAt = command.executeAt() != null;
            out.writeBoolean(hasExecuteAt);
            if (hasExecuteAt)
                CommandSerializers.timestamp.serialize(command.executeAt(), out, version.msg_version);
        }

        @Override
        public void deserializeBody(AccordCommand.ReadOnly command, DataInputPlus in, Version version) throws IOException
        {
            command.status.load(Status.values()[in.readByte()]);
            Timestamp executeAt = in.readBoolean() ? CommandSerializers.timestamp.deserialize(in, version.msg_version) : null;
            command.executeAt.load(executeAt);
        }

        @Override
        public int serializedBodySize(AccordCommand command, Version version)
        {
            int size = TypeSizes.sizeof((byte) command.status.get().ordinal());
            size += TypeSizes.BOOL_SIZE; // has executeAt
            if (command.executeAt() != null)
                size += CommandSerializers.timestamp.serializedSize();
            return size;
        }

        @Override
        public boolean needsUpdate(AccordCommand command)
        {
            return command.status.hasModifications() || command.executeAt.hasModifications();
        }
    };

    public static final SummarySerializer waitingOn = statusExecute;

    public static final SummarySerializer commandsPerKey = new SummarySerializer(){

        @Override
        public void serializeBody(AccordCommand command, DataOutputPlus out, Version version) throws IOException
        {
            statusExecute.serializeBody(command, out, version);
            // TODO: switch to KindOnlyTxn once we don't need to transmit txns everywhere
            serializeNullable(command.txn(), out, version.msg_version, CommandSerializers.txn);

            // TODO: switch to txnId -> DUMMY_TXN once we don't need to transmit txns everywhere
            serializeNullable(command.savedDeps(), out, version.msg_version, CommandSerializers.deps);
        }

        @Override
        public void deserializeBody(AccordCommand.ReadOnly command, DataInputPlus in, Version version) throws IOException
        {
            statusExecute.deserializeBody(command, in, version);
            command.txn.load(deserializeNullable(in, version.msg_version, CommandSerializers.txn));
            command.deps.load(deserializeNullable(in, version.msg_version, CommandSerializers.deps));
        }

        @Override
        public int serializedBodySize(AccordCommand command, Version version)
        {
            int size = statusExecute.serializedBodySize(command, version);
            size += serializedSizeNullable(command.txn(), version.msg_version, CommandSerializers.txn);
            size += serializedSizeNullable(command.savedDeps(), version.msg_version, CommandSerializers.deps);
            return size;
        }

        @Override
        public boolean needsUpdate(AccordCommand command)
        {
            return statusExecute.needsUpdate(command)
                   || command.txn.hasModifications()
                   || command.deps.hasModifications();
        }
    };

}
