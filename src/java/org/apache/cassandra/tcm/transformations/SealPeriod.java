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

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Transformation that seals the period and requests local state to take a snapshot. Snapshot taking is an
 * asynchonous action, and we generally do not rely on the fact snapshot is, in fact going to be
 * there all the time. Snapshots are used as a performance optimization.
 */
public class SealPeriod implements Transformation
{
    public static final Serializer serializer = new Serializer();

    public static SealPeriod instance = new SealPeriod();

    private SealPeriod(){}

    @Override
    public Kind kind()
    {
        return Kind.SEAL_PERIOD;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        if (prev.lastInPeriod)
            return new Rejected("Have just sealed this period");

        return success(prev.transformer(true), LockedRanges.AffectedRanges.EMPTY);
    }

    static class Serializer implements AsymmetricMetadataSerializer<Transformation, SealPeriod>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t == instance;
        }

        public SealPeriod deserialize(DataInputPlus in, Version version) throws IOException
        {
            return instance;
        }

        public long serializedSize(Transformation t, Version version)
        {
            return 0;
        }
    }
}
