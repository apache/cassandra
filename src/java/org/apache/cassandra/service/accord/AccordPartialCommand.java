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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import accord.api.Key;
import accord.local.Command;
import accord.local.CommandsForKey;
import accord.local.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;

import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedSizeNullable;

public class AccordPartialCommand extends CommandsForKey.TxnIdWithExecuteAt
{
    public static final PartialCommandSerializer serializer = new PartialCommandSerializer();

    // TODO (soon): this should only be a list of TxnId (the deps for the key we are persisted against); but should also be stored separately and not brought into memory
    private final List<TxnId> deps;
    // TODO (soon): we only require this for Accepted; perhaps more tightly couple query API for efficiency
    private final Status status;

    AccordPartialCommand(TxnId txnId, Timestamp executeAt, List<TxnId> deps, Status status)
    {
        super(txnId, executeAt);
        this.deps = deps;
        this.status = status;
    }

    public AccordPartialCommand(Key key, Command command)
    {
        this(command.txnId(), command.executeAt(),
             command.partialDeps() == null ? Collections.emptyList() : command.partialDeps().txnIds(key),
             command.status());
    }

    public TxnId txnId()
    {
        return txnId;
    }

    public Timestamp executeAt()
    {
        return executeAt;
    }

    public List<TxnId> deps()
    {
        return deps;
    }

    public boolean hasDep(TxnId txnId)
    {
        return Collections.binarySearch(deps, txnId) >= 0;
    }

    public Status status()
    {
        return status;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj.getClass() != AccordPartialCommand.class)
            return false;
        AccordPartialCommand that = (AccordPartialCommand) obj;
        return txnId.equals(that.txnId)
               && Objects.equals(executeAt, that.executeAt)
               && Objects.equals(deps, that.deps)
               && status == that.status;
    }

    public static class PartialCommandSerializer
    {
        public void serialize(AccordPartialCommand command, DataOutputPlus out, AccordSerializerVersion version) throws IOException
        {
            out.write(version.version);
            CommandSerializers.txnId.serialize(command.txnId(), out, version.msgVersion);
            serializeNullable(command.executeAt(), out, version.msgVersion, CommandSerializers.timestamp);
            CommandSerializers.status.serialize(command.status(), out, version.msgVersion);
            serializeCollection(command.deps, out, version.msgVersion, CommandSerializers.txnId);
        }

        public ByteBuffer serialize(AccordPartialCommand command)
        {
            AccordSerializerVersion version = AccordSerializerVersion.CURRENT;
            int size = serializedSize(command, version);
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

        public AccordSerializerVersion deserializeVersion(DataInputPlus in) throws IOException
        {
            return AccordSerializerVersion.serializer.deserialize(in);
        }

        // check for cached command first, otherwise deserialize
        private AccordPartialCommand deserialize(AccordCommandsForKey commandsForKey, AccordCommandStore commandStore, DataInputPlus in) throws IOException
        {
            AccordSerializerVersion version = deserializeVersion(in);
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version.msgVersion);
            AsyncContext context = commandStore.getContext();
            AccordPartialCommand command = getCachedFull(commandsForKey, txnId, context);
            if (command != null)
                return command;

            Timestamp executeAt = deserializeNullable(in, version.msgVersion, CommandSerializers.timestamp);
            Status status = CommandSerializers.status.deserialize(in, version.msgVersion);
            List<TxnId> deps = deserializeList(in, version.msgVersion, CommandSerializers.txnId);
            AccordPartialCommand partial = new AccordPartialCommand(txnId, executeAt, deps, status);
            addToContext(partial, context);
            return partial;
        }

        public AccordPartialCommand deserialize(AccordCommandsForKey commandsForKey, AccordCommandStore commandStore, ByteBuffer bytes)
        {
            try (DataInputBuffer in = new DataInputBuffer(bytes, true))
            {
                return deserialize(commandsForKey, commandStore, in);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public int serializedSize(AccordPartialCommand command, AccordSerializerVersion version)
        {
            int size = Math.toIntExact(AccordSerializerVersion.serializer.serializedSize(version));
            size += CommandSerializers.txnId.serializedSize();
            size += serializedSizeNullable(command.executeAt(), version.msgVersion, CommandSerializers.timestamp);
            size += CommandSerializers.status.serializedSize(command.status(), version.msgVersion);
            size += serializedCollectionSize(command.deps, version.msgVersion, CommandSerializers.txnId);
            return size;
        }

        private AccordPartialCommand getCachedFull(AccordCommandsForKey commandsForKey, TxnId txnId, AsyncContext context)
        {
            AccordCommand command = context.commands.get(txnId);
            if (command == null)
                return null;
            return new AccordPartialCommand(commandsForKey.key(), command);
        }

        private void addToContext(AccordPartialCommand command, AsyncContext context)
        {
            context.commands.addPartialCommand(command);
        }

        /**
         * Determines if current modifications require updating command data duplicated elsewhere
         */
        public boolean needsUpdate(AccordCommand command)
        {
            return command.executeAt.hasModifications() || command.status.hasModifications() || command.partialDeps.hasModifications();
        }
    }
}