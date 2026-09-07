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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import accord.api.Journal;
import accord.impl.CommandChange;
import accord.local.Cleanup;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.PartialDeps;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.DepsSerializers;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.serializers.WaitingOnSerializer;
import org.apache.cassandra.service.accord.txn.TxnDataResult;

import static accord.api.Journal.Load.ALL;
import static accord.impl.CommandChange.Field.CLEANUP;
import static accord.impl.CommandChange.isChanged;
import static accord.impl.CommandChange.nextSetField;
import static accord.impl.CommandChange.toIterableNonNullFields;
import static accord.impl.CommandChange.toIterableSetFields;
import static accord.impl.CommandChange.unsetIterable;
import static accord.impl.CommandChange.validateFlags;
import static accord.local.Cleanup.Input.FULL;

public class CommandChanges extends CommandChange.Builder implements Merger
{
    private final boolean deserializeDeps;

    public CommandChanges()
    {
        this(Journal.Load.ALL);
    }

    public CommandChanges(Journal.Load load)
    {
        this(null, load);
    }

    public CommandChanges(TxnId txnId)
    {
        this(txnId, Journal.Load.ALL);
    }

    public CommandChanges(TxnId txnId, Journal.Load load)
    {
        super(txnId, load);
        deserializeDeps = load == ALL;
    }

    // applies cleanup and returns null if no command should be returned
    public static CommandChanges cleanupAndFilter(CommandChanges builder, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        if (builder.isEmpty())
            return null;

        Cleanup cleanup = builder.shouldCleanup(FULL, redundantBefore, durableBefore);
        switch (cleanup)
        {
            case VESTIGIAL:
            case EXPUNGE:
            case ERASE:
                return null;
        }
        Invariants.require(builder.saveStatus() != null, "No saveSatus loaded, but next was called and cleanup was not: %s", builder);
        return builder;
    }

    @Override
    public PartialDeps partialDeps()
    {
        if (partialDeps instanceof ByteBuffer)
        {
            try
            {
                partialDeps = DepsSerializers.partialDepsById.deserialize((ByteBuffer) partialDeps);
            }
            catch (IOException e)
            {
                throw new IllegalStateException("Failed to materialise partially deserialised deps", e);
            }
        }
        return (PartialDeps) partialDeps;
    }

    public void reset(JournalKey key)
    {
        reset(key.id);
    }

    public ByteBuffer asByteBuffer(Version userVersion) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serialize(out, userVersion);
            return out.asNewBuffer();
        }
    }

    public void serialize(DataOutputPlus out, Version userVersion) throws IOException
    {
        Invariants.require(mask == 0);
        Invariants.require(flags != 0);

        int flags = validateFlags(this.flags);
        serialize(flags, out, userVersion);
    }

    private void serialize(int flags, DataOutputPlus out, Version userVersion) throws IOException
    {
        Invariants.require(flags != 0);
        out.writeInt(flags);

        int iterable = toIterableNonNullFields(flags);
        for (CommandChange.Field field = nextSetField(iterable); field != null; iterable = unsetIterable(field, iterable), field = nextSetField(iterable))
        {
            switch (field)
            {
                default:
                    throw new UnhandledEnum(field);
                case CLEANUP:
                    out.writeByte(cleanup.ordinal());
                    break;
                case EXECUTE_AT:
                    Invariants.require(txnId != null, "%s", this);
                    Invariants.require(executeAt != null, "%s", this);
                    CommandSerializers.ExecuteAtSerializer.serialize(txnId, executeAt, out);
                    break;
                case EXECUTES_AT_LEAST:
                    Invariants.require(executesAtLeast != null);
                    CommandSerializers.ExecuteAtSerializer.serialize(executesAtLeast, out);
                    break;
                case MIN_UNIQUE_HLC:
                    Invariants.require(minUniqueHlc != 0, "%s", this);
                    out.writeUnsignedVInt(minUniqueHlc);
                    break;
                case SAVE_STATUS:
                    Invariants.require(saveStatus != null, "%s", this);
                    out.writeByte(saveStatus.ordinal());
                    break;
                case DURABILITY:
                    Invariants.require(durability != null, "%s", this);
                    out.writeByte(durability.encoded());
                    break;
                case ACCEPTED:
                    Invariants.require(acceptedOrCommitted != null, "%s", this);
                    CommandSerializers.ballot.serialize(acceptedOrCommitted, out);
                    break;
                case PROMISED:
                    Invariants.require(promised != null, "%s", this);
                    CommandSerializers.ballot.serialize(promised, out);
                    break;
                case PARTICIPANTS:
                    Invariants.require(participants != null, "%s", this);
                    CommandSerializers.participants.serialize(participants, out);
                    break;
                case PARTIAL_TXN:
                    Invariants.require(partialTxn != null, "%s", this);
                    CommandSerializers.partialTxn.serialize(partialTxn, out, userVersion);
                    break;
                case PARTIAL_DEPS:
                    Invariants.require(partialDeps != null, "%s", this);
                    if (partialDeps instanceof ByteBuffer) out.write(((ByteBuffer) partialDeps).duplicate());
                    else DepsSerializers.partialDepsById.serialize((PartialDeps) partialDeps, out);
                    break;
                case WAITING_ON:
                    Invariants.require(waitingOn != null, "%s", this);
                    ((WaitingOnSerializer.WaitingOnBitSetsAndLength) waitingOn).reserialize(out);
                    break;
                case WRITES:
                    Invariants.require(writes != null, "%s", this);
                    CommandSerializers.writes.serialize(writes, out, userVersion);
                    break;
                case RESULT:
                    Invariants.require(result != null, "%s", this);
                    TxnDataResult.persistable.serialize(result, out);
                    break;
            }
        }
    }

    public void deserializeNext(ByteBuffer buffer, Version userVersion)
    {
        try (DataInputBuffer in = new DataInputBuffer(buffer, false))
        {
            deserializeNext(in, userVersion);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public void deserializeNext(DataInputPlus in, Version userVersion) throws IOException
    {
        Invariants.require(txnId != null);
        int readFlags = in.readInt();
        Invariants.require(readFlags != 0);
        hasUpdate = true;
        count++;

        // batch-apply any new nulls
        setNulls(false, readFlags);
        // iterator sets low 16 bits; low readFlag bits are nulls, so masking with ~readFlags restricts to non-null changed fields
        int iterable = toIterableSetFields(readFlags) & ~readFlags;
        for (CommandChange.Field field = nextSetField(iterable); field != null; field = nextSetField(iterable = unsetIterable(field, iterable)))
        {
            // Since we are iterating in reverse order, we skip the fields that were
            // set by entries written later (i.e. already read ones) or if the mask did not include the field.
            if ((isChanged(field, flags) || ((mask & (1 << field.ordinal())) != 0)) && field != CLEANUP)
                skip(txnId, field, in, userVersion);
            else
                deserialize(field, in, userVersion);
        }

        // upper 16 bits are changed flags, lower are nulls; by masking upper by ~lower we restrict to only non-null changed fields
        this.flags |= readFlags & (~readFlags << 16);
    }

    private void deserialize(CommandChange.Field field, DataInputPlus in, Version userVersion) throws IOException
    {
        switch (field)
        {
            case EXECUTE_AT:
                executeAt = CommandSerializers.ExecuteAtSerializer.deserialize(txnId, in);
                break;
            case EXECUTES_AT_LEAST:
                executesAtLeast = CommandSerializers.ExecuteAtSerializer.deserialize(in);
                break;
            case MIN_UNIQUE_HLC:
                minUniqueHlc = in.readUnsignedVInt();
                break;
            case SAVE_STATUS:
                saveStatus = SaveStatus.values()[in.readByte()];
                break;
            case DURABILITY:
                durability = Status.Durability.forEncoded(in.readUnsignedByte());
                break;
            case ACCEPTED:
                acceptedOrCommitted = CommandSerializers.ballot.deserialize(in);
                break;
            case PROMISED:
                promised = CommandSerializers.ballot.deserialize(in);
                break;
            case PARTICIPANTS:
                participants = CommandSerializers.participants.deserialize(in);
                break;
            case PARTIAL_TXN:
                partialTxn = CommandSerializers.partialTxn.deserialize(in, userVersion);
                break;
            case PARTIAL_DEPS:
                // TODO (expected): this optimisation will be easily disabled;
                //  should either operate natively on ByteBuffer
                //  or else use some explicit API for copying bytes while skipping
                if (deserializeDeps || !(in instanceof DataInputBuffer))
                {
                    partialDeps = DepsSerializers.partialDepsById.deserialize(in);
                }
                else
                {
                    ByteBuffer buf = ((DataInputBuffer) in).buffer();
                    int start = buf.position();
                    DepsSerializers.partialDepsById.skip(in);
                    int end = buf.position();
                    partialDeps = buf.duplicate().position(start).limit(end);
                }
                break;
            case WAITING_ON:
                waitingOn = WaitingOnSerializer.deserializeBitSets(txnId, in);
                break;
            case WRITES:
                writes = CommandSerializers.writes.deserialize(in, userVersion);
                break;
            case CLEANUP:
                Cleanup newCleanup = Cleanup.forOrdinal(in.readByte());
                if (cleanup == null || newCleanup.compareTo(cleanup) > 0)
                    cleanup = newCleanup;
                break;
            case RESULT:
                result = TxnDataResult.persistable.deserialize(in);
                break;
        }
    }

    private static void skip(TxnId txnId, CommandChange.Field field, DataInputPlus in, Version userVersion) throws IOException
    {
        switch (field)
        {
            default:
                throw new UnhandledEnum(field);
            case EXECUTE_AT:
                CommandSerializers.ExecuteAtSerializer.skip(txnId, in);
                break;
            case EXECUTES_AT_LEAST:
                CommandSerializers.ExecuteAtSerializer.skip(in);
                break;
            case MIN_UNIQUE_HLC:
                in.readUnsignedVInt();
                break;
            case SAVE_STATUS:
            case DURABILITY:
            case CLEANUP:
                in.readByte();
                break;
            case ACCEPTED:
            case PROMISED:
                CommandSerializers.ballot.skip(in);
                break;
            case PARTICIPANTS:
                CommandSerializers.participants.skip(in);
                break;
            case PARTIAL_TXN:
                CommandSerializers.partialTxn.skip(in, userVersion);
                break;
            case PARTIAL_DEPS:
                DepsSerializers.partialDepsById.skip(in);
                break;
            case WAITING_ON:
                WaitingOnSerializer.skip(txnId, in);
                break;
            case WRITES:
                // TODO (expected): skip
                CommandSerializers.writes.skip(in, userVersion);
                break;
            case RESULT:
                TxnDataResult.persistable.skip(in);
                break;
        }
    }
}
