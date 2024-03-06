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
 * Snapshots are used during startup or catchup between peers to avoid having to replay or transmit the entire log.
 * This transformation simply inserts a marker entry into the metadata log. Enacting the epoch on a peer triggers the
 * snapshot action on that peer. By default, taking a snapshot taking is an asynchonous action, and we generally do not
 * rely on the fact snapshot is, in fact going to be available immediately (or even durably).
 */
public class TriggerSnapshot implements Transformation
{
    public static final Serializer serializer = new Serializer();

    public static TriggerSnapshot instance = new TriggerSnapshot();

    private TriggerSnapshot(){}

    @Override
    public Kind kind()
    {
        return Kind.TRIGGER_SNAPSHOT;
    }

    @Override
    public boolean allowDuringUpgrades()
    {
        return true;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        return Transformation.success(prev.transformer(), LockedRanges.AffectedRanges.EMPTY);
    }

    static class Serializer implements AsymmetricMetadataSerializer<Transformation, TriggerSnapshot>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t == instance;
        }

        public TriggerSnapshot deserialize(DataInputPlus in, Version version) throws IOException
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
        return "TriggerSnapshot{}";
    }
}
