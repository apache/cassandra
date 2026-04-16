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
package org.apache.cassandra.tcm.transformations;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.satellites.KeyspaceFailoverState;
import org.apache.cassandra.locator.satellites.SatelliteFailoverProcessState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;

public class AdvanceSatelliteFailoverState implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(AdvanceSatelliteFailoverState.class);
    public static final Serializer serializer = new Serializer();

    public enum TargetState
    {
        TRANSITION,
        NORMAL
    }

    @Nonnull
    public final String keyspace;

    @Nonnull
    public final NormalizedRanges<Token> ranges;

    @Nonnull
    public final TargetState targetState;

    public AdvanceSatelliteFailoverState(@Nonnull String keyspace,
                                         @Nonnull NormalizedRanges<Token> ranges,
                                         @Nonnull TargetState targetState)
    {
        checkNotNull(keyspace, "keyspace should not be null");
        checkArgument(ranges != null && !ranges.isEmpty(), "ranges should not be null/empty");
        checkNotNull(targetState, "targetState should not be null");
        this.keyspace = keyspace;
        this.ranges = ranges;
        this.targetState = targetState;
    }

    @Override
    public Kind kind()
    {
        return Kind.ADVANCE_SATELLITE_FAILOVER_STATE;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        KeyspaceFailoverState ksState = prev.satelliteFailoverState.getKeyspaceState(keyspace);

        if (ksState == null)
        {
            logger.warn("Attempted to advance failover for keyspace {} which has no active transfer", keyspace);
            return new Rejected(INVALID, String.format("Keyspace %s has no active failover transfer", keyspace));
        }

        SatelliteFailoverProcessState newState;
        switch (targetState)
        {
            case TRANSITION:
                newState = prev.satelliteFailoverState.withRangesTransitioning(keyspace, ranges);
                break;
            case NORMAL:
                newState = prev.satelliteFailoverState.withRangesNormal(keyspace, ranges);
                break;
            default:
                throw new IllegalStateException("Unknown target state: " + targetState);
        }

        logger.info("Advanced failover for keyspace {}: {} ranges to {}", keyspace, ranges.size(), targetState);

        return Transformation.success(
            prev.transformer().with(newState),
            LockedRanges.AffectedRanges.EMPTY);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, AdvanceSatelliteFailoverState>
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            AdvanceSatelliteFailoverState v = (AdvanceSatelliteFailoverState) t;
            out.writeUTF(v.keyspace);
            out.writeInt(v.targetState.ordinal());
            serializeCollection(v.ranges, out, version, Range.serializer);
        }

        @Override
        public AdvanceSatelliteFailoverState deserialize(DataInputPlus in, Version version) throws IOException
        {
            String keyspace = in.readUTF();
            TargetState targetState = TargetState.values()[in.readInt()];
            NormalizedRanges<Token> ranges = NormalizedRanges.normalizedRanges(deserializeList(in, version, Range.serializer));
            return new AdvanceSatelliteFailoverState(keyspace, ranges, targetState);
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            AdvanceSatelliteFailoverState v = (AdvanceSatelliteFailoverState) t;
            return TypeSizes.sizeof(v.keyspace)
                   + TypeSizes.INT_SIZE
                   + serializedCollectionSize(v.ranges, version, Range.serializer);
        }
    }
}
