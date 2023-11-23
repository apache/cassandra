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

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

/**
 * Period is an entity that is completely transparent to the users of CMS, and is a consequence of a fact that
 * LWTs only work on a single partition. It would be unwise to hold all epochs in a single partition, as it
 * will eventually get extremely large. At the same time, we can not use Epoch as a primary key (even though
 * IF NOT EXISTS queries would technically work for append puposes), since it would make log scans much more
 * expensive.
 *
 * Another reason for having periods is that replaying an entire log for a freshly starting log in an old
 * cluster can be very expensive, so in such cases the node can be caught up using a snapshot serving as a base
 * state and a small number of entries instead.
 *
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
    public boolean allowDuringUpgrades()
    {
        return true;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        if (prev.lastInPeriod)
            return new Rejected(INVALID, "Have just sealed this period");

        return Transformation.success(prev.transformer(true), LockedRanges.AffectedRanges.EMPTY);
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

    @Override
    public String toString()
    {
        return "SealPeriod{}";
    }
}
