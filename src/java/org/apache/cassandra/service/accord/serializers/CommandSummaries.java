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

import accord.local.Status;
import accord.txn.Dependencies;
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

public class CommandSummaries
{
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
            AccordCommand command = context.command(txnId);
            if (command != null)
                return command;

            command = context.summary(txnId);
            if (command == null)
                command = new AccordCommand(commandStore, txnId);

            context.addSummary(command);
            if (command.isLoaded())
                return command;

            deserializeBody(command, in, version);

            return command;
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
        public abstract void deserializeBody(AccordCommand command, DataInputPlus in, Version version) throws IOException;
        public abstract int serializedBodySize(AccordCommand command, Version version);

        /**
         * Determines if current modifications require updating command data duplicated elsewhere
         */
        public abstract boolean needsUpdate(AccordCommand command);
    }

    /*
      commands for keys:
        txn_id
        txn
        txn is_write
        status <- to support deps
        execute_at
        deps (txn_ids)
      deps:
        txn_id
        status
        execute_at
        txn  (these are serialized in the deps map)
      waiting on apply:
        txn_id
        txn
        status
        executeAt
      waiting on commit:
        txn_id
        status
     */

    public static class TxnStatusExecuteAtSerializer extends SummarySerializer
    {
        @Override
        public void serializeBody(AccordCommand command, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(version.msg_version);
            CommandSerializers.txn.serialize(command.txn(), out, version.msg_version);
            out.write(command.status().ordinal());
            if (command.hasBeen(Status.Committed))
                CommandSerializers.timestamp.serialize(command.executeAt(), out, version.msg_version);
        }

        public Txn deserializeTxn(DataInputPlus in, Version version) throws IOException
        {
            int txn_version = in.readInt();
            return CommandSerializers.txn.deserialize(in, txn_version);
        }

        @Override
        public void deserializeBody(AccordCommand command, DataInputPlus in, Version version) throws IOException
        {
            command.txn.load(deserializeTxn(in, version));
            command.status.load(Status.values()[in.readByte()]);
            if (command.hasBeen(Status.Committed))
                command.executeAt.load(CommandSerializers.timestamp.deserialize(in, version.msg_version));
        }

        @Override
        public int serializedBodySize(AccordCommand command, Version version)
        {
            int size = TypeSizes.INT_SIZE; // txn version
            size += TypeSizes.sizeof((byte) command.status.get().ordinal());
            size += TypeSizes.BOOL_SIZE;
            if (command.hasBeen(Status.Committed))
                size += CommandSerializers.timestamp.serializedSize();
            return size;
        }

        @Override
        public boolean needsUpdate(AccordCommand command)
        {
            return command.txn.hasModifications()
                   || command.status.hasModifications()
                   || command.executeAt.hasModifications();
        }
    };

    public static final TxnStatusExecuteAtSerializer txnStatusExecute = new TxnStatusExecuteAtSerializer();
    public static final TxnStatusExecuteAtSerializer dependencies = txnStatusExecute;
    public static final TxnStatusExecuteAtSerializer waitingOnApply = txnStatusExecute;

    public static final SummarySerializer commandsPerKey = new SummarySerializer(){

        @Override
        public void serializeBody(AccordCommand command, DataOutputPlus out, Version version) throws IOException
        {
            txnStatusExecute.serializeBody(command, out, version);

            Dependencies deps = command.savedDeps();
            out.writeInt(deps.size());
            for (Map.Entry<TxnId, Txn> entry : deps)
                CommandSerializers.txnId.serialize(entry.getKey(), out, version.msg_version);
        }

        @Override
        public void deserializeBody(AccordCommand command, DataInputPlus in, Version version) throws IOException
        {
            txnStatusExecute.deserializeBody(command, in, version);

            TreeMap<TxnId, Txn> depsMap = new TreeMap<>();
            int numDeps = in.readInt();
            for (int i=0; i<numDeps; i++)
                depsMap.put(CommandSerializers.txnId.deserialize(in, version.msg_version), null);
            command.deps.load(new Dependencies(depsMap));
        }

        @Override
        public int serializedBodySize(AccordCommand command, Version version)
        {
            int size = txnStatusExecute.serializedBodySize(command, version);

//            int numDeps = command.deps.getView().size();
            int numDeps = command.deps.get().size();
            size += TypeSizes.sizeof(numDeps);
            size += numDeps * CommandSerializers.txnId.serializedSize();
            return size;
        }

        @Override
        public boolean needsUpdate(AccordCommand command)
        {
            return txnStatusExecute.needsUpdate(command) || command.deps.hasModifications();
        }
    };

}
