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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.BeginConsensusMigrationForTableAndRange;
import org.apache.cassandra.tcm.transformations.MaybeFinishConsensusMigrationForTableAndRange;

// TODO: rename to metadata state transformation?
public interface Transformation
{
    Serializer serializer = new Serializer();

    Kind kind();
    Result execute(ClusterMetadata clusterMetadata);

    /**
     * {@link #afterCommit(ClusterMetadata, ClusterMetadata)} can be executed 0 times, once, or more times over the
     * lifetime of the node, but not more than once between startup and shutdown of the node. Its visibility is not
     * atomic with the commit itself.
     *
     * Make sure to not to rely on any side effects for correctness, use it only as a performance optimisation, or
     * for moving between transient states.
     */
    default void afterCommit(ClusterMetadata prev, ClusterMetadata next) {}

    interface Result
    {
        boolean isSuccess();
        boolean isRejected();

        Success success();
        Rejected rejected();
    }

    final class Success implements Result
    {
        public final ClusterMetadata metadata;
        public final LockedRanges.AffectedRanges affectedRanges;

        public Success(ClusterMetadata metadata, LockedRanges.AffectedRanges affectedRanges)
        {
            this.metadata = metadata;
            this.affectedRanges = affectedRanges;
        }

        public boolean isSuccess()
        {
            return true;
        }

        public boolean isRejected()
        {
            return false;
        }

        public Success success()
        {
            return this;
        }

        public Rejected rejected()
        {
            throw new IllegalStateException("Can't dereference Rejection upon successful execution");
        }

        public String toString()
        {
            return "Result{" +
                    "newState=" + metadata +
                    '}';
        }
    }

    final class Rejected implements Result
    {
        public final String reason;

        public Rejected(String reason)
        {
            this.reason = reason;
        }

        public boolean isSuccess()
        {
            return false;
        }

        public boolean isRejected()
        {
            return true;
        }

        public Success success()
        {
            throw new IllegalStateException("Can't dereference Success for a rejected execution");
        }

        public Rejected rejected()
        {
            return this;
        }
    }

    enum Kind
    {
        BEGIN_CONSENSUS_MIGRATION_FOR_TABLE_AND_RANGE(() -> BeginConsensusMigrationForTableAndRange.serializer),
        MAYBE_FINISH_CONSENSUS_MIGRATION_FOR_TABLE_AND_RANGE(() -> MaybeFinishConsensusMigrationForTableAndRange.serializer),
        SET_CONSENSUS_MIGRATION_TARGET_PROTOCOL(() -> MaybeFinishConsensusMigrationForTableAndRange.serializer);

        private final Supplier<AsymmetricMetadataSerializer<Transformation, ? extends Transformation>> serializer;

        Kind(Supplier<AsymmetricMetadataSerializer<Transformation, ? extends Transformation>> serializer)
        {
            this.serializer = serializer;
        }

        public AsymmetricMetadataSerializer<Transformation, ? extends Transformation> serializer()
        {
            return serializer.get();
        }

        public ByteBuffer toVersionedBytes(Transformation transform) throws IOException
        {
            long size = VerboseMetadataSerializer.serializedSize(serializer.get(), transform);
            ByteBuffer bb = ByteBuffer.allocate(Ints.checkedCast(size));
            try (DataOutputBuffer out = new DataOutputBuffer(bb))
            {
                VerboseMetadataSerializer.serialize(serializer.get(), transform, out);
            }
            bb.flip();
            return bb;
        }

        public Transformation fromVersionedBytes(ByteBuffer bb) throws IOException
        {
            try (DataInputBuffer in = new DataInputBuffer(bb, true))
            {
                return VerboseMetadataSerializer.deserialize(serializer.get(), in);
            }
        }
    }

    class Serializer implements AsymmetricMetadataSerializer<Transformation, Transformation>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUnsignedVInt(t.kind().ordinal());
            t.kind().serializer().serialize(t, out, version);
        }

        public Transformation deserialize(DataInputPlus in, Version version) throws IOException
        {
            Kind kind = Kind.values()[(int)in.readUnsignedVInt()];
            return kind.serializer().deserialize(in, version);
        }

        public long serializedSize(Transformation t, Version version)
        {
            return TypeSizes.sizeofUnsignedVInt(t.kind().ordinal()) +
                    t.kind().serializer().serializedSize(t, version);
        }
    }

}