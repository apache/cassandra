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

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.sequences.CancelCMSReconfiguration;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.AddAccordKeyspace;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.apache.cassandra.tcm.transformations.Assassinate;
import org.apache.cassandra.tcm.transformations.BeginConsensusMigrationForTableAndRange;
import org.apache.cassandra.tcm.transformations.CancelInProgressSequence;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;
import org.apache.cassandra.tcm.transformations.MaybeFinishConsensusMigrationForTableAndRange;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.SealPeriod;
import org.apache.cassandra.tcm.transformations.SetConsensusMigrationTargetProtocol;
import org.apache.cassandra.tcm.transformations.Startup;
import org.apache.cassandra.tcm.transformations.Unregister;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;
import org.apache.cassandra.tcm.transformations.cms.AdvanceCMSReconfiguration;
import org.apache.cassandra.tcm.transformations.cms.FinishAddToCMS;
import org.apache.cassandra.tcm.transformations.cms.Initialize;
import org.apache.cassandra.tcm.transformations.cms.PreInitialize;
import org.apache.cassandra.tcm.transformations.cms.RemoveFromCMS;
import org.apache.cassandra.tcm.transformations.cms.StartAddToCMS;
import org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration;

public interface Transformation
{
    Serializer transformationSerializer = new Serializer();

    Kind kind();

    /**
     * Performs the core function of the transformation, to transition ClusterMetadata from one state to the next.
     * Returns a {@link Result}, either {@link Success} or {@link Rejected}. The former contains the transformed
     * metadata along with an indication of which specific components were modified. It also includes the set of token
     * ranges impacted by the change (if any). A {@link Rejected} result contains an {@link ExceptionCode} and reason string.
     * @param metadata the starting state
     * @return a result object indicating a success or failure in applying transformation
     */
    Result execute(ClusterMetadata metadata);

    default boolean allowDuringUpgrades()
    {
        return false;
    }

    static Success success(ClusterMetadata.Transformer transformer, LockedRanges.AffectedRanges affectedRanges)
    {
        ClusterMetadata.Transformer.Transformed transformed = transformer.build();
        return new Success(transformed.metadata, affectedRanges, transformed.modifiedKeys);
    }

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
        public final ImmutableSet<MetadataKey> affectedMetadata;

        public Success(ClusterMetadata metadata, LockedRanges.AffectedRanges affectedRanges, ImmutableSet<MetadataKey> affectedKeys)
        {
            this.metadata = metadata;
            this.affectedRanges = affectedRanges;
            this.affectedMetadata = affectedKeys;
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
        public final ExceptionCode code;
        public final String reason;

        public Rejected(ExceptionCode code, String reason)
        {
            this.code = code;
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

        public String toString()
        {
            return "Rejected{" +
                   "code=" + code +
                   ", reason='" + reason + '\'' +
                   '}';
        }
    }

    enum Kind
    {
        PRE_INITIALIZE_CMS(() -> PreInitialize.serializer),
        INITIALIZE_CMS(() -> Initialize.serializer),
        FORCE_SNAPSHOT(() -> ForceSnapshot.serializer),
        SEAL_PERIOD(() -> SealPeriod.serializer),
        SCHEMA_CHANGE(() -> AlterSchema.serializer),
        REGISTER(() -> Register.serializer),
        UNREGISTER(() -> Unregister.serializer),

        UNSAFE_JOIN(() -> UnsafeJoin.serializer),
        PREPARE_JOIN(() -> PrepareJoin.serializer),
        START_JOIN(() -> PrepareJoin.StartJoin.serializer),
        MID_JOIN(() -> PrepareJoin.MidJoin.serializer),
        FINISH_JOIN(() -> PrepareJoin.FinishJoin.serializer),

        PREPARE_MOVE(() -> PrepareMove.serializer),
        START_MOVE(() -> PrepareMove.StartMove.serializer),
        MID_MOVE(() -> PrepareMove.MidMove.serializer),
        FINISH_MOVE(() -> PrepareMove.FinishMove.serializer),

        PREPARE_LEAVE(() -> PrepareLeave.serializer),
        START_LEAVE(() -> PrepareLeave.StartLeave.serializer),
        MID_LEAVE(() -> PrepareLeave.MidLeave.serializer),
        FINISH_LEAVE(() -> PrepareLeave.FinishLeave.serializer),
        ASSASSINATE(() -> Assassinate.serializer),

        PREPARE_REPLACE(() -> PrepareReplace.serializer),
        START_REPLACE(() -> PrepareReplace.StartReplace.serializer),
        MID_REPLACE(() -> PrepareReplace.MidReplace.serializer),
        FINISH_REPLACE(() -> PrepareReplace.FinishReplace.serializer),

        CANCEL_SEQUENCE(() -> CancelInProgressSequence.serializer),

        @Deprecated(since = "CEP-21")
        START_ADD_TO_CMS(() -> StartAddToCMS.serializer),
        @Deprecated(since = "CEP-21")
        FINISH_ADD_TO_CMS(() -> FinishAddToCMS.serializer),
        @Deprecated(since = "CEP-21")
        REMOVE_FROM_CMS(() -> RemoveFromCMS.serializer),

        STARTUP(() -> Startup.serializer),

        ADD_ACCORD_KEYSPACE(() -> AddAccordKeyspace.serializer),

        BEGIN_CONSENSUS_MIGRATION_FOR_TABLE_AND_RANGE(() -> BeginConsensusMigrationForTableAndRange.serializer),
        MAYBE_FINISH_CONSENSUS_MIGRATION_FOR_TABLE_AND_RANGE(() -> MaybeFinishConsensusMigrationForTableAndRange.serializer),
        SET_CONSENSUS_MIGRATION_TARGET_PROTOCOL(() -> SetConsensusMigrationTargetProtocol.serializer),

        CUSTOM(() -> CustomTransformation.serializer),

        PREPARE_SIMPLE_CMS_RECONFIGURATION(() -> PrepareCMSReconfiguration.Simple.serializer),
        PREPARE_COMPLEX_CMS_RECONFIGURATION(() -> PrepareCMSReconfiguration.Complex.serializer),
        ADVANCE_CMS_RECONFIGURATION(() -> AdvanceCMSReconfiguration.serializer),
        CANCEL_CMS_RECONFIGURATION(() -> CancelCMSReconfiguration.serializer)
        ;

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
            Version serializationVersion = Version.minCommonSerializationVersion();
            long size = VerboseMetadataSerializer.serializedSize(serializer.get(), transform, serializationVersion);
            ByteBuffer bb = ByteBuffer.allocate(Ints.checkedCast(size));
            try (DataOutputBuffer out = new DataOutputBuffer(bb))
            {
                VerboseMetadataSerializer.serialize(serializer.get(), transform, out, serializationVersion);
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
            out.writeUnsignedVInt32(t.kind().ordinal());
            t.kind().serializer().serialize(t, out, version);
        }

        public Transformation deserialize(DataInputPlus in, Version version) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedVInt32()];
            return kind.serializer().deserialize(in, version);
        }

        public long serializedSize(Transformation t, Version version)
        {
            return TypeSizes.sizeofUnsignedVInt(t.kind().ordinal()) +
                   t.kind().serializer().serializedSize(t, version);
        }
    }

    /**
     * Used on a CMS node that attempts to commit the transformation to avoid
     * calling `execute` twice.
     */
    final class Executed implements Transformation
    {
        private final Transformation delegate;
        private final Result result;

        public Executed(Transformation delegate, Result result)
        {
            this.delegate = delegate;
            this.result = result;
        }

        public Kind kind()
        {
            return delegate.kind();
        }

        public Result execute(ClusterMetadata clusterMetadata)
        {
            return result;
        }

        public Transformation original()
        {
            return delegate;
        }

        @Override
        public String toString()
        {
            return "EXECUTED {" + delegate.toString() + "}";
        }
    }

    /**
     * Allowed to be thrown inside transformations to signal that the error is expected by the transformation, and
     * does not constitute a reason for retry.
     */
    class RejectedTransformationException extends RuntimeException
    {
        public RejectedTransformationException(String message)
        {
            super(message);
        }
    }
}
