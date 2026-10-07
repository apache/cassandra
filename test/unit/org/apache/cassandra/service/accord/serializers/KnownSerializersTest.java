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
import java.util.EnumMap;
import java.util.Map;

import org.junit.Test;

import accord.primitives.Known;
import accord.primitives.Known.Definition;
import accord.primitives.Known.KnownDeps;
import accord.primitives.Known.KnownExecuteAt;
import accord.primitives.Known.KnownRoute;
import accord.primitives.Known.Outcome;
import accord.primitives.Known.PrivilegedVote;
import accord.primitives.SaveStatus;

import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static accord.primitives.Known.KnownDeps.DepsCommitted;
import static accord.primitives.Known.KnownDeps.DepsErased;
import static accord.primitives.Known.KnownDeps.DepsFromCoordinator;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsProposed;
import static accord.primitives.Known.KnownDeps.DepsProposedFixed;
import static accord.primitives.Known.KnownDeps.DepsProposedFromCoordinator;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Known.KnownDeps.NoDeps;
import static accord.primitives.SaveStatus.AcceptedInvalidate;
import static accord.primitives.SaveStatus.AcceptedInvalidateWithDefinition;
import static accord.primitives.SaveStatus.AcceptedMedium;
import static accord.primitives.SaveStatus.AcceptedMediumWithDefAndVote;
import static accord.primitives.SaveStatus.AcceptedMediumWithDefinition;
import static accord.primitives.SaveStatus.AcceptedPrivCoord;
import static accord.primitives.SaveStatus.AcceptedPrivCoordRecovery;
import static accord.primitives.SaveStatus.AcceptedSlow;
import static accord.primitives.SaveStatus.AcceptedSlowWithDefAndVote;
import static accord.primitives.SaveStatus.AcceptedSlowWithDefinition;
import static accord.primitives.SaveStatus.Applied;
import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.SaveStatus.Committed;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.FastAcceptedPrivCoord;
import static accord.primitives.SaveStatus.Invalidated;
import static accord.primitives.SaveStatus.NotDefined;
import static accord.primitives.SaveStatus.PreAccepted;
import static accord.primitives.SaveStatus.PreAcceptedPrivCoord;
import static accord.primitives.SaveStatus.PreAcceptedPrivCoordRecovery;
import static accord.primitives.SaveStatus.PreAcceptedWithDeps;
import static accord.primitives.SaveStatus.PreAcceptedWithVote;
import static accord.primitives.SaveStatus.PreApplied;
import static accord.primitives.SaveStatus.PreCommitted;
import static accord.primitives.SaveStatus.PreCommittedWithDefAndDeps;
import static accord.primitives.SaveStatus.PreCommittedWithDefAndFixedDeps;
import static accord.primitives.SaveStatus.PreCommittedWithDefinition;
import static accord.primitives.SaveStatus.PreCommittedWithDeps;
import static accord.primitives.SaveStatus.PreCommittedWithFixedDeps;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.primitives.SaveStatus.TruncatedApplyWithOutcome;
import static accord.primitives.SaveStatus.TruncatedUnapplied;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.SaveStatus.Vestigial;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link Known} and {@link KnownDeps} are sent to peers, so their serialized forms must be stable across versions.
 * {@link KnownDeps#DepsProposedFromCoordinator} was introduced after these formats were established; it is inserted
 * in the logical (ordinal) order of {@link KnownDeps}, so both are serialized by {@link KnownDeps#id}, and the serialized
 * form of {@link Known} (see {@link Known#serializeCompat}) requires an extra bit to encode it. Note that
 * {@link Known#encoded} is a purely local representation, that is not serialized.
 *
 * @author LLM
 */
public class KnownSerializersTest
{
    /**
     * The ordinals of KnownDeps prior to the introduction of KnownDeps.id, which serialization must continue to use
     */
    private static final Map<KnownDeps, Integer> LEGACY_ORDINALS = new EnumMap<>(KnownDeps.class);
    static
    {
        LEGACY_ORDINALS.put(DepsUnknown, 0);
        LEGACY_ORDINALS.put(DepsFromCoordinator, 1);
        LEGACY_ORDINALS.put(DepsProposedFixed, 2);
        LEGACY_ORDINALS.put(DepsProposed, 3);
        LEGACY_ORDINALS.put(DepsCommitted, 4);
        LEGACY_ORDINALS.put(DepsErased, 5);
        LEGACY_ORDINALS.put(DepsKnown, 6);
        LEGACY_ORDINALS.put(NoDeps, 7);
    }

    private static <T> byte[] serialize(UnversionedSerializer<T> serializer, T value) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(value, out);
            assertEquals(serializer.serializedSize(value), out.getLength());
            return out.toByteArray();
        }
    }

    private static <T> T deserialize(UnversionedSerializer<T> serializer, byte[] bytes) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            T result = serializer.deserialize(in);
            assertEquals(0, in.available());
            return result;
        }
    }

    @Test
    public void knownDeps() throws IOException
    {
        assertNull(deserialize(CommandSerializers.knownDeps, serialize(CommandSerializers.knownDeps, null)));
        for (KnownDeps deps : KnownDeps.values())
        {
            byte[] bytes = serialize(CommandSerializers.knownDeps, deps);
            assertSame(deps, deserialize(CommandSerializers.knownDeps, bytes));
            // EncodeAsVInt32.withNulls: 0 is null, otherwise 1 + id
            assertArrayEquals(deps.toString(), new byte[] { (byte) (1 + deps.id) }, bytes);
            if (LEGACY_ORDINALS.containsKey(deps))
                assertEquals(deps.toString(), (int) LEGACY_ORDINALS.get(deps), deps.id);
        }
        assertEquals(LEGACY_ORDINALS.size() + 1, KnownDeps.values().length);
        assertEquals(8, DepsProposedFromCoordinator.id);

        // ordinal must not be used for serialization, as DepsProposedFromCoordinator is not the last value
        assertTrue(DepsProposedFromCoordinator.ordinal() < DepsCommitted.ordinal());
    }

    @Test
    public void knownDepsRejectsUnknownIds() throws IOException
    {
        for (int id : new int[] { 9, 10, 11, 12, 15, 16 })
        {
            try
            {
                deserialize(CommandSerializers.knownDeps, new byte[] { (byte) (1 + id) });
                fail("Expected failure to deserialize KnownDeps id " + id);
            }
            catch (IllegalArgumentException expected)
            {
            }
        }
    }

    @Test
    public void knownRoundTripsAllCombinations() throws IOException
    {
        int valid = 0;
        for (KnownRoute route : KnownRoute.values())
        for (Definition definition : Definition.values())
        for (KnownExecuteAt executeAt : KnownExecuteAt.values())
        for (KnownDeps deps : KnownDeps.values())
        for (Outcome outcome : Outcome.values())
        for (PrivilegedVote vote : PrivilegedVote.values())
        {
            Known known;
            try { known = new Known(route, definition, executeAt, deps, outcome, vote); }
            catch (IllegalStateException invalidCombination) { continue; }
            ++valid;

            Known roundTrip = deserialize(CommandSerializers.known, serialize(CommandSerializers.known, known));
            assertEquals(known, roundTrip);
            assertSame(route, roundTrip.route());
            assertSame(definition, roundTrip.definition());
            assertSame(executeAt, roundTrip.executeAt());
            assertSame(deps, roundTrip.deps());
            assertSame(outcome, roundTrip.outcome());
            assertSame(vote, roundTrip.privilegedVote());

            // the layout of every value that existed prior to DepsProposedFromCoordinator must be unchanged
            int legacyEncoded = route.ordinal()
                                | definition.ordinal() << 2
                                | executeAt.ordinal() << 4
                                | (deps.id & 0x7) << 7
                                | outcome.ordinal() << 10
                                | vote.ordinal() << 13;

            if (deps == DepsProposedFromCoordinator)
            {
                // encoded using the extension bit
                assertEquals(legacyEncoded | 1 << 14, known.serializeCompat());
            }
            else
            {
                assertEquals(LEGACY_ORDINALS.get(deps).intValue(), deps.id);
                assertEquals(known.toString(), legacyEncoded, known.serializeCompat());
            }
        }
        assertTrue(valid > 0);
    }

    @Test
    public void knownRejectsUnknownDepsIds()
    {
        for (int id : new int[] { 9, 10, 11, 12, 15 })
        {
            int serialized = ((id & 0x7) << 7) | ((id >>> 3) << 14);
            try
            {
                Known.deserializeCompat(serialized);
                fail("Expected failure to decode KnownDeps id " + id);
            }
            catch (IllegalStateException | IllegalArgumentException expected)
            {
            }
        }
    }

    private static void assertEncoded(SaveStatus saveStatus, int expectEncoded) throws IOException
    {
        assertEquals(saveStatus.toString(), expectEncoded, saveStatus.known.serializeCompat());
        Known roundTrip = deserialize(CommandSerializers.known, serialize(CommandSerializers.known, saveStatus.known));
        assertEquals(saveStatus.toString(), saveStatus.known, roundTrip);
    }

    /**
     * Encodings captured from the prior version for each SaveStatus; only the AcceptedPrivCoord statuses (previously
     * AcceptedMediumPrivCoord, which were derived from AcceptedMediumWithDefinition) have changed: they are now derived
     * from AcceptedSlowWithDefinition, adopting DepsProposedFromCoordinator (id 8, so encoded only by the extension bit)
     */
    @Test
    public void saveStatusKnownEncodings() throws IOException
    {
        assertEncoded(Uninitialised, 0x0);
        assertEncoded(NotDefined, 0x0);
        assertEncoded(PreAccepted, 0xe);
        assertEncoded(PreAcceptedPrivCoord, 0xe);
        assertEncoded(PreAcceptedPrivCoordRecovery, 0xe);
        assertEncoded(PreAcceptedWithVote, 0x200e);
        assertEncoded(PreAcceptedWithDeps, 0x208e);
        assertEncoded(FastAcceptedPrivCoord, 0xe);
        assertEncoded(AcceptedInvalidate, 0x0);
        assertEncoded(AcceptedInvalidateWithDefinition, 0xe);
        assertEncoded(AcceptedMedium, 0x111);
        assertEncoded(AcceptedMediumWithDefinition, 0x11e);
        assertEncoded(AcceptedMediumWithDefAndVote, 0x211e);
        assertEncoded(AcceptedSlow, 0x191);
        assertEncoded(AcceptedSlowWithDefinition, 0x19e);
        assertEncoded(AcceptedPrivCoord, 0x401e);         // previously AcceptedMediumPrivCoord, 0x11e
        assertEncoded(AcceptedPrivCoordRecovery, 0x401e); // previously AcceptedMediumPrivCoordRecovery, 0x11e
        assertEncoded(AcceptedSlowWithDefAndVote, 0x219e);
        assertEncoded(PreCommitted, 0x32);
        assertEncoded(PreCommittedWithDefinition, 0x3e);
        assertEncoded(PreCommittedWithDeps, 0x1b2);
        assertEncoded(PreCommittedWithFixedDeps, 0x132);
        assertEncoded(PreCommittedWithDefAndDeps, 0x1be);
        assertEncoded(PreCommittedWithDefAndFixedDeps, 0x13e);
        assertEncoded(Committed, 0x23e);
        assertEncoded(Stable, 0x33e);
        assertEncoded(ReadyToExecute, 0x33e);
        assertEncoded(PreApplied, 0xf4e);
        assertEncoded(Applying, 0xf4e);
        assertEncoded(Applied, 0xf4e);
        assertEncoded(TruncatedApplyWithOutcome, 0xec6);
        assertEncoded(TruncatedApply, 0xac6);
        assertEncoded(TruncatedUnapplied, 0xab4);
        assertEncoded(Vestigial, 0x0);
        assertEncoded(Erased, 0x6a4);
        assertEncoded(Invalidated, 0x13d8);
        assertEquals(36, SaveStatus.values().length);

        assertSame(DepsProposedFromCoordinator, AcceptedPrivCoord.known.deps());
        assertSame(DepsProposedFromCoordinator, AcceptedPrivCoordRecovery.known.deps());
        // we report the status without the local distinction to peers
        assertSame(AcceptedSlowWithDefinition, AcceptedPrivCoord.report());
        assertSame(AcceptedSlowWithDefinition, AcceptedPrivCoordRecovery.report());
        assertEquals(AcceptedPrivCoord.report().known, AcceptedPrivCoord.known.with(DepsProposed));
        assertEquals(AcceptedPrivCoordRecovery.report().known, AcceptedPrivCoordRecovery.known.with(DepsProposed));
    }
}
