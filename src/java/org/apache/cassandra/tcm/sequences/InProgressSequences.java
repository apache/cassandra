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

package org.apache.cassandra.tcm.sequences;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;

import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.tcm.MultiStepOperation.Kind.LEAVE;
import static org.apache.cassandra.tcm.serialization.Version.V2;

public class InProgressSequences implements MetadataValue<InProgressSequences>, Iterable<MultiStepOperation<?>>
{
    public static final Serializer serializer = new Serializer();

    public static InProgressSequences EMPTY = new InProgressSequences(Epoch.EMPTY, ImmutableMap.of());
    private final ImmutableMap<MultiStepOperation.SequenceKey, MultiStepOperation<?>> state;
    private final Epoch lastModified;

    private InProgressSequences(Epoch lastModified, ImmutableMap<MultiStepOperation.SequenceKey, MultiStepOperation<?>> state)
    {
        this.lastModified = lastModified;
        this.state = state;
    }

    public static void finishInProgressSequences(MultiStepOperation.SequenceKey sequenceKey)
    {
        finishInProgressSequences(sequenceKey, false);
    }

    public static void finishInProgressSequences(MultiStepOperation.SequenceKey sequenceKey, boolean onlyStartupSafeSequences)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        while (true)
        {
            MultiStepOperation<?> sequence = metadata.inProgressSequences.get(sequenceKey);
            if (sequence == null)
                break;
            if (onlyStartupSafeSequences && !sequence.finishDuringStartup())
                break;
            if (isLeave(sequence))
                StorageService.instance.maybeInitializeServices();
            if (resume(sequence))
                metadata = ClusterMetadata.current();
            else
                return;
        }
    }

    public static boolean cancelInProgressSequences(String sequenceOwner, String expectedSequenceKind)
    {
        NodeId owner = NodeId.fromString(sequenceOwner);
        MultiStepOperation<?> seq = ClusterMetadata.current().inProgressSequences.get(owner);
        if (seq == null)
            throw new IllegalArgumentException("No in progress sequence for "+sequenceOwner);
        MultiStepOperation.Kind expectedKind = MultiStepOperation.Kind.valueOf(expectedSequenceKind);
        if (seq.kind() != expectedKind)
            throw new IllegalArgumentException("No in progress sequence of kind " + expectedKind + " for " + owner + " (only " + seq.kind() +" in progress)");

        return StorageService.cancelInProgressSequences(owner);
    }

    @Override
    public InProgressSequences withLastModified(Epoch epoch)
    {
        return new InProgressSequences(epoch, state);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    public boolean contains(MultiStepOperation.SequenceKey key)
    {
        return state.containsKey(key);
    }

    public MultiStepOperation<?> get(MultiStepOperation.SequenceKey key)
    {
        return state.get(key);
    }

    public boolean isEmpty()
    {
        return state.isEmpty();
    }

    public InProgressSequences with(MultiStepOperation.SequenceKey key, MultiStepOperation<?> sequence)
    {
        if (contains(key))
        {
            throw new Transformation.RejectedTransformationException(String.format("Can not add a new in-progress sequence for %s, " +
                                                                                   "since there's already one associated with it: %s",
                                                                                   key,
                                                                                   get(key)));
        }

        ImmutableMap.Builder<MultiStepOperation.SequenceKey, MultiStepOperation<?>> builder = ImmutableMap.builder();
        builder.put(key, sequence);
        for (Map.Entry<MultiStepOperation.SequenceKey, MultiStepOperation<?>> e : state.entrySet())
        {
            if (e.getKey().equals(key))
                continue;
            builder.put(e.getKey(), e.getValue());
        }
        return new InProgressSequences(lastModified, builder.build());
    }

    public <T2, T1 extends MultiStepOperation<T2>> InProgressSequences with(MultiStepOperation.SequenceKey key, Function<T1, T1> update)
    {
        ImmutableMap.Builder<MultiStepOperation.SequenceKey, MultiStepOperation<?>> builder = ImmutableMap.builder();

        for (Map.Entry<MultiStepOperation.SequenceKey, MultiStepOperation<?>> e : state.entrySet())
        {
            if (e.getKey().equals(key))
                builder.put(e.getKey(), update.apply((T1) e.getValue()));
            else
                builder.put(e.getKey(), e.getValue());
        }
        return new InProgressSequences(lastModified, builder.build());
    }

    public InProgressSequences without(MultiStepOperation.SequenceKey key)
    {
        ImmutableMap.Builder<MultiStepOperation.SequenceKey, MultiStepOperation<?>> builder = ImmutableMap.builder();
        boolean removed = false;
        for (Map.Entry<MultiStepOperation.SequenceKey, MultiStepOperation<?>> e : state.entrySet())
        {
            if (e.getKey().equals(key))
                removed = true;
            else
                builder.put(e.getKey(), e.getValue());
        }
        assert removed : String.format("Expected to remove an in-progress sequence for %s, but it wasn't found in in-progress sequences", key);
        return new InProgressSequences(lastModified, builder.build());

    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InProgressSequences that = (InProgressSequences) o;
        return Objects.equals(state, that.state) && Objects.equals(lastModified, that.lastModified);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(state, lastModified);
    }

    @VisibleForTesting
    public static BiFunction<MultiStepOperation<?>, SequenceState, SequenceState> listener = (s, o) -> o;

    @VisibleForTesting
    public static BiFunction<MultiStepOperation<?>, SequenceState, SequenceState> replaceListener(BiFunction<MultiStepOperation<?>, SequenceState, SequenceState> newListener)
    {
        BiFunction<MultiStepOperation<?>, SequenceState, SequenceState> prev = listener;
        listener = newListener;
        return prev;
    }

    public static boolean resume(MultiStepOperation<?> sequence)
    {
        SequenceState state;
        if (sequence.barrier().await())
            state = listener.apply(sequence, sequence.executeNext());
        else
            state = listener.apply(sequence, SequenceState.blocked());

        if (state.isError())
            throw ((SequenceState.Error)state).cause();

        return state.isContinuable();
    }

    public static boolean isLeave(MultiStepOperation<?> sequence)
    {
        return sequence.kind() == LEAVE;
    }

    @Override
    public Iterator<MultiStepOperation<?>> iterator()
    {
        return state.values().iterator();
    }

    public static class Serializer implements MetadataSerializer<InProgressSequences>
    {
        public void serialize(InProgressSequences t, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(t.lastModified, out, version);
            out.writeInt(t.state.size());
            for (Map.Entry<MultiStepOperation.SequenceKey, MultiStepOperation<?>> entry : t.state.entrySet())
            {
                if (version.isBefore(V2))
                {
                    NodeId.serializer.serialize((NodeId) entry.getKey(), out, version);
                    MultiStepOperation<?> seq = entry.getValue();
                    out.writeUTF(seq.kind().name());
                    entry.getValue().kind().serializer.serialize(seq, out, version);
                }
                else
                {
                    // Starting V2, we serialize the sequence first since we rely on the type during deserialization
                    MultiStepOperation<?> seq = entry.getValue();
                    out.writeUTF(seq.kind().name());
                    entry.getValue().kind().serializer.serialize(seq, out, version);
                    MetadataSerializer<MultiStepOperation.SequenceKey> keySerializer = (MetadataSerializer<MultiStepOperation.SequenceKey>) seq.keySerializer();
                    keySerializer.serialize(entry.getKey(), out, version);
                }
            }
        }

        public InProgressSequences deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            int ipsSize = in.readInt();
            ImmutableMap.Builder<MultiStepOperation.SequenceKey, MultiStepOperation<?>> res = ImmutableMap.builder();
            for (int i = 0; i < ipsSize; i++)
            {
                if (version.isBefore(V2))
                {
                    NodeId nodeId = NodeId.serializer.deserialize(in, version);
                    MultiStepOperation.Kind kind = MultiStepOperation.Kind.valueOf(in.readUTF());
                    MultiStepOperation<?> ips = kind.serializer.deserialize(in, version);
                    res.put(nodeId, ips);
                }
                else
                {
                    MultiStepOperation.Kind kind = MultiStepOperation.Kind.valueOf(in.readUTF());
                    MultiStepOperation<?> ips = kind.serializer.deserialize(in, version);
                    MultiStepOperation.SequenceKey key = ips.keySerializer().deserialize(in, version);
                    res.put(key, ips);
                }
            }
            return new InProgressSequences(lastModified, res.build());
        }

        public long serializedSize(InProgressSequences t, Version version)
        {
            long size = Epoch.serializer.serializedSize(t.lastModified, version);
            size += sizeof(t.state.size());
            for (Map.Entry<MultiStepOperation.SequenceKey, MultiStepOperation<?>> entry : t.state.entrySet())
            {
                if (version.isBefore(V2))
                {
                    size += NodeId.serializer.serializedSize((NodeId) entry.getKey(), version);
                    MultiStepOperation<?> seq = entry.getValue();
                    size += sizeof(seq.kind().name());
                    size += entry.getValue().kind().serializer.serializedSize(seq, version);
                }
                else
                {
                    MultiStepOperation<?> seq = entry.getValue();
                    size += sizeof(seq.kind().name());
                    size += entry.getValue().kind().serializer.serializedSize(seq, version);
                    MetadataSerializer<MultiStepOperation.SequenceKey> keySerializer = (MetadataSerializer<MultiStepOperation.SequenceKey>) seq.keySerializer();
                    size += keySerializer.serializedSize(entry.getKey(), version);
                }
            }
            return size;
        }
    }

    @Override
    public String toString()
    {
        return "InProgressSequences{" +
               "lastModified=" + lastModified +
               ", state=" + state +
               '}';
    }
}
