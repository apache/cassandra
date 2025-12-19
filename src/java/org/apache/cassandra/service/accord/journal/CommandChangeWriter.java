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

package org.apache.cassandra.service.accord.journal;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import accord.impl.CommandChange;
import accord.local.Cleanup;
import accord.local.Command;
import accord.primitives.SaveStatus;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.DepsSerializers;
import org.apache.cassandra.service.accord.serializers.ResultSerializers;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.serializers.WaitingOnSerializer;

import static accord.impl.CommandChange.anyFieldChanged;
import static accord.impl.CommandChange.describeFlags;
import static accord.impl.CommandChange.getFlags;
import static accord.impl.CommandChange.isNull;
import static accord.impl.CommandChange.nextSetField;
import static accord.impl.CommandChange.toIterableSetFields;
import static accord.impl.CommandChange.unsetIterable;
import static accord.impl.CommandChange.validateFlags;

public class CommandChangeWriter implements Journal.Writer
{
    final Command after;
    final int flags;

    private CommandChangeWriter(Command after, int flags)
    {
        this.after = after;
        this.flags = flags;
    }

    public static CommandChangeWriter make(Command before, Command after)
    {
        if (before == after
            || after == null
            || after.saveStatus() == SaveStatus.Uninitialised)
            return null;

        int flags = validateFlags(getFlags(before, after));
        if (!anyFieldChanged(flags))
            return null;

        return new CommandChangeWriter(after, flags);
    }

    @Override
    public void write(DataOutputPlus out, int userVersion) throws IOException
    {
        write(out, Version.fromVersion(userVersion));
    }

    public void write(DataOutputPlus out, Version userVersion) throws IOException
    {
        serialize(after, flags, out, userVersion);
    }

    private static void serialize(Command command, int flags, DataOutputPlus out, Version userVersion) throws IOException
    {
        Invariants.require(flags != 0);
        out.writeInt(flags);

        int iterable = toIterableSetFields(flags);
        while (iterable != 0)
        {
            CommandChange.Field field = nextSetField(iterable);
            if (isNull(field, flags))
            {
                iterable = unsetIterable(field, iterable);
                continue;
            }

            switch (field)
            {
                case EXECUTE_AT:
                    CommandSerializers.ExecuteAtSerializer.serialize(command.txnId(), command.executeAt(), out);
                    break;
                case EXECUTES_AT_LEAST:
                    CommandSerializers.ExecuteAtSerializer.serialize(command.executesAtLeast(), out);
                    break;
                case MIN_UNIQUE_HLC:
                    Invariants.require(command.waitingOn().minUniqueHlc() != 0);
                    out.writeUnsignedVInt(command.waitingOn().minUniqueHlc());
                    break;
                case SAVE_STATUS:
                    out.writeByte(command.saveStatus().ordinal());
                    break;
                case DURABILITY:
                    out.writeByte(command.durability().encoded());
                    break;
                case ACCEPTED:
                    CommandSerializers.ballot.serialize(command.acceptedOrCommitted(), out);
                    break;
                case PROMISED:
                    CommandSerializers.ballot.serialize(command.promised(), out);
                    break;
                case PARTICIPANTS:
                    CommandSerializers.participants.serialize(command.participants(), out);
                    break;
                case PARTIAL_TXN:
                    CommandSerializers.partialTxn.serialize(command.partialTxn(), out, userVersion);
                    break;
                case PARTIAL_DEPS:
                    DepsSerializers.partialDepsById.serialize(command.partialDeps(), out);
                    break;
                case WAITING_ON:
                    Command.WaitingOn waitingOn = command.waitingOn();
                    WaitingOnSerializer.serializeBitSetsOnly(command.txnId(), waitingOn, out);
                    break;
                case WRITES:
                    CommandSerializers.writes.serialize(command.writes(), out, userVersion);
                    break;
                case RESULT:
                    ResultSerializers.result.serialize(command.result(), out);
                    break;
                case CLEANUP:
                    Cleanup cleanup;
                    switch (command.saveStatus())
                    {
                        default:
                            throw new UnhandledEnum(command.saveStatus());
                        case Erased:
                            cleanup = Cleanup.ERASE;
                            break;
                        case Invalidated:
                            cleanup = Cleanup.INVALIDATE;
                            break;
                    }
                    out.writeByte(cleanup.ordinal());
                    break;
            }

            iterable = unsetIterable(field, iterable);
        }
    }

    private boolean hasField(CommandChange.Field fields)
    {
        return !isNull(fields, flags);
    }

    public boolean hasParticipants()
    {
        return hasField(CommandChange.Field.PARTICIPANTS);
    }

    @Override
    public String toString()
    {
        return after.saveStatus() + " " + describeFlags(flags);
    }

    public static @Nullable ByteBuffer asSerializedChange(Command before, Command after, Version userVersion) throws IOException
    {
        // TODO (expected): reusable buffer to build, or pre-size
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            CommandChangeWriter writer = CommandChangeWriter.make(before, after);
            if (writer == null)
                return null;

            writer.write(out, userVersion);
            return out.asNewBuffer();
        }
    }
}
